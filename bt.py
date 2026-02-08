import backtrader as bt
import yfinance as yf
import pandas as pd
import datetime
import os
import schedule
import time
import sys
import tushare as ts
import numpy as np

# --- Numpy 兼容性修复 (必须放在最前面) ---
if not hasattr(np, 'bool8'):
    np.bool8 = np.bool_
if not hasattr(np, 'int'):
    np.int = int
if not hasattr(np, 'float'):
    np.float = float
if not hasattr(np, 'object'):
    np.object = object
if not hasattr(np, 'str'):
    np.str = str

# 尝试导入交互式绘图
try:
    from backtrader_plotting import Bokeh
    from backtrader_plotting.schemes import Tradimo

    HAS_BOKEH = True
except ImportError:
    HAS_BOKEH = False

# --- 1. 配置 ---
proxy = 'http://127.0.0.1:7897'
os.environ['HTTP_PROXY'] = proxy
os.environ['HTTPS_PROXY'] = proxy

ts.set_token('55bffd7163bfe7f7c22019424271c5c5eb216372fb4202258b848832')
pro = ts.pro_api()

TARGET_TICKERS = [
    'NVDA', 'TSLA', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META',
    'AMD', 'AVGO', 'TSM',
    'PLTR', 'COIN', 'MSTR', 'SMCI',
    'QQQ', 'SPY',
    '600519.SH', '000001.SZ', '300750.SZ', '600489.SH', '601606.SH', '000547.SZ', '000592.SZ', '002361.SZ', '002131.SZ', '601606.SH'
]


# --- 2. 数据获取与处理 ---

def is_ashare(ticker):
    return ticker.endswith('.SH') or ticker.endswith('.SZ')


def update_data(ticker):
    print(f"正在更新 {ticker} 数据...", end="")
    if not os.path.exists("./data"):
        os.makedirs("./data")
    csv_file = f"./data/{ticker}_daily.csv"
    try:
        df = pd.DataFrame()
        if is_ashare(ticker):
            end_dt = datetime.datetime.now()
            start_dt = end_dt - datetime.timedelta(days=365)
            s_date_str = start_dt.strftime('%Y%m%d')
            e_date_str = end_dt.strftime('%Y%m%d')
            df = pro.daily(ts_code=ticker, start_date=s_date_str, end_date=e_date_str)
            if df.empty:
                print(" [失败: Tushare数据为空]")
                return None
            df = df.rename(
                columns={'trade_date': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                         'vol': 'Volume'})
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            df = df.sort_index(ascending=True)
            df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
        else:
            df = yf.Ticker(ticker).history(period="1y")
            if df.empty:
                print(" [失败: Yfinance数据为空]")
                return None
            df.index = df.index.tz_localize(None)
            df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
        df.to_csv(csv_file)
        print(" [成功]")
        return csv_file
    except Exception as e:
        print(f" [出错: {e}]")
        return None


# --- 3. 策略部分 ---

class AnalysisStrategy(bt.Strategy):
    """仅用于生成每日信号报告"""
    params = (('pfast', 10), ('pslow', 20), ('ticker_name', 'Unknown'))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.sma1 = bt.ind.SMA(period=self.params.pfast)
        self.sma2 = bt.ind.SMA(period=self.params.pslow)
        self.crossover = bt.ind.CrossOver(self.sma1, self.sma2)

    def stop(self):
        if len(self.datas[0]) == 0: return
        last_date = self.datas[0].datetime.date(0)
        close_price = self.dataclose[0]
        signal = self.crossover[0]
        log_msg = f"[{self.params.ticker_name}] 日期:{last_date} 收盘:{close_price:.2f} | "
        if signal > 0:
            log_msg += "★ 出现金叉！建议:【买入信号】"
        elif signal < 0:
            log_msg += "▼ 出现死叉！建议:【卖出信号】"
        else:
            log_msg += "无新信号 (观望/持仓)"
        print(log_msg)
        with open("daily_report.txt", "a", encoding="utf-8") as f:
            f.write(log_msg + "\n")


class BacktestStrategy(bt.Strategy):
    """用于历史回测"""
    params = (('pfast', 10), ('pslow', 20), ('atr_period', 14))

    def __init__(self):
        self.dataclose = self.datas[0].close

        # --- 这里的改动是关键 ---
        # 1. 计算均线
        self.sma1 = bt.ind.SMA(period=self.params.pfast)
        self.sma2 = bt.ind.SMA(period=self.params.pslow)
        self.atr = bt.ind.ATR(self.data, period=self.params.atr_period)
        # 2. 强制均线显示在主图 (和K线在一起)
        self.sma1.plotinfo.subplot = False
        self.sma2.plotinfo.subplot = False
        self.atr.plotinfo.subplot = False
        # 3. 给均线起个名字 (图例里显示)
        self.sma1.plotinfo.plotname = f'Fast SMA ({self.params.pfast})'
        self.sma2.plotinfo.plotname = f'Slow SMA ({self.params.pslow})'
        self.atr.plotinfo.plotname = '波动率 (ATR)'
        # 4. 计算交叉信号
        self.crossover = bt.ind.CrossOver(self.sma1, self.sma2)

        # 5. 隐藏交叉信号线 (只是一条1和-1的线，不美观，隐藏掉)
        self.crossover.plotinfo.plot = False
        # ----------------------

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        else:
            if self.crossover < 0:
                self.close()

    def stop(self):
        final_val = self.broker.getvalue()
        print(f"策略结束 - 最终资金: {final_val:.2f}")


class BollingerStrategy(bt.Strategy):
    """
    布林带均值回归策略
    逻辑：
    1. 当价格跌破下轨 (Lower Band) -> 视为超卖，买入
    2. 当价格突破上轨 (Upper Band) -> 视为超买，卖出平仓
    """
    params = (('period', 20), ('devfactor', 2.0))

    def __init__(self):
        self.dataclose = self.datas[0].close

        # 1. 计算布林带
        self.boll = bt.ind.BollingerBands(
            period=self.params.period,
            devfactor=self.params.devfactor
        )

        # 2. 绘图设置：让布林带显示在主图上
        self.boll.plotinfo.subplot = False
        self.boll.plotinfo.plotname = f'Bollinger({self.params.period}, {self.params.devfactor})'

        # 3. 填充颜色 (Backtrader自带绘图支持，Bokeh可能支持有限，但值得一试)
        self.boll.plotinfo.plotlinelabels = True

    def next(self):
        # 还没有仓位
        if not self.position:
            # 收盘价跌破下轨 -> 买入
            if self.dataclose[0] < self.boll.lines.bot[0]:
                self.buy()

        # 已经有仓位
        else:
            # 收盘价突破上轨 -> 卖出止盈
            if self.dataclose[0] > self.boll.lines.top[0]:
                self.close()

                # (进阶: 这里也可以选择在回到中轨 mid 时就平仓，胜率更高但盈亏比低)

    def stop(self):
        final_val = self.broker.getvalue()
        print(f"策略(布林带)结束 - 最终资金: {final_val:.2f}")
# --- 4. 运行逻辑 ---

def run_daily_analysis_job():
    print(f"\n{'=' * 20} 开始执行每日分析报告 {datetime.datetime.now()} {'=' * 20}")
    with open("daily_report.txt", "w", encoding="utf-8") as f:
        f.write(f"生成时间: {datetime.datetime.now()}\n\n")

    for ticker in TARGET_TICKERS:
        csv_file = update_data(ticker)
        if not csv_file: continue
        cerebro = bt.Cerebro()
        data = bt.feeds.GenericCSVData(dataname=csv_file, nullvalue=0.0, dtformat='%Y-%m-%d', datetime=0, open=1,
                                       high=2, low=3, close=4, volume=5, openinterest=-1)
        cerebro.adddata(data)
        cerebro.addstrategy(AnalysisStrategy, ticker_name=ticker, pfast=10, pslow=20)
        cerebro.run()
    print(f"\n报告已生成: daily_report.txt")


def run_single_backtest(ticker, strategy_type='sma', p1=10, p2=20):
    if ticker.isdigit():
        ticker = f"{ticker}.SH" if ticker.startswith('6') else f"{ticker}.SZ"
        print(f"已自动修正代码为: {ticker}")

    print(f"\n--- 正在回测: {ticker} [策略: {strategy_type}] ---")
    csv_file = f"./data/{ticker}_daily.csv"
    if not os.path.exists(csv_file):
        print("本地数据不存在，尝试下载...")
        csv_file = update_data(ticker)
        if not csv_file: return

    cerebro = bt.Cerebro()
    data = bt.feeds.GenericCSVData(dataname=csv_file, nullvalue=0.0, dtformat='%Y-%m-%d', datetime=0, open=1, high=2,
                                   low=3, close=4, volume=5, openinterest=-1)
    cerebro.adddata(data)

    # --- 策略选择 ---
    if strategy_type == 'sma':
        # p1=快线, p2=慢线
        cerebro.addstrategy(BacktestStrategy, pfast=p1, pslow=p2)
    elif strategy_type == 'boll':
        # p1=周期(默认20), p2=标准差倍数(默认2.0)
        cerebro.addstrategy(BollingerStrategy, period=p1, devfactor=float(p2))

    # --- 观察者配置 ---
    cerebro.addobserver(bt.observers.BuySell, barplot=True, bardist=0.02)
    cerebro.addobserver(bt.observers.Trades)
    cerebro.addobserver(bt.observers.Cash)

    start_cash = 100000.0
    cerebro.broker.setcash(start_cash)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=95)

    print(f'初始资金: {start_cash:.2f}')
    cerebro.run()
    end_cash = cerebro.broker.getvalue()
    roi = (end_cash - start_cash) / start_cash * 100
    print(f'最终资金: {end_cash:.2f}')
    print(f'收益率: {roi:.2f}%')

    if HAS_BOKEH:
        print("\n正在生成交互式图表...")
        b = Bokeh(style='bar',
                  plot_mode='single',
                  scheme=Tradimo(),
                  output_mode='show',
                  filename=f"{ticker}_{strategy_type}_result.html",  # 文件名区分策略
                  toolbar_location='above')
        cerebro.plot(b)
    else:
        cerebro.plot(style='candlestick', volume=False)


if __name__ == '__main__':
    while True:
        print("\n" + "=" * 30)
        print("1. 执行每日信号分析 (SMA)")
        print("2. 回测 - 双均线策略 (Trend)")
        print("3. 回测 - 布林带策略 (Mean Reversion) ★新功能")
        print("4. 启动定时任务")
        print("0. 退出")
        choice = input("请输入选项: ")

        if choice == '1':
            run_daily_analysis_job()

        elif choice == '2':
            t = input("请输入股票代码: ").upper()
            if t:
                try:
                    vf = input("快线 (默认10): ") or 10
                    vs = input("慢线 (默认20): ") or 20
                    run_single_backtest(t, 'sma', int(vf), int(vs))
                except ValueError:
                    print("输入错误")

        elif choice == '3':
            t = input("请输入股票代码: ").upper()
            if t:
                try:
                    period = input("周期 (默认20): ") or 20
                    dev = input("标准差倍数 (默认2.0): ") or 2.0
                    run_single_backtest(t, 'boll', int(period), dev)
                except ValueError:
                    print("输入错误")

        elif choice == '4':
            # 定时任务代码保持不变
            run_daily_analysis_job()
            schedule.every().day.at("08:00").do(run_daily_analysis_job)
            print("\n系统已进入定时模式...")
            while True:
                schedule.run_pending()
                time.sleep(60)
        elif choice == '0':
            sys.exit()