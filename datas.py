import baostock as bs
import pandas as pd
from multiprocessing import Pool
import akshare as ak
import os
from multiprocessing import Pool
from datetime import date
import pandas as pd
import yfinance as yf
import time

proxy = 'http://127.0.0.1:7897'
os.environ['HTTP_PROXY'] = proxy
os.environ['HTTPS_PROXY'] = proxy


def get_nasdaq_listed_tickers(include_test=False, include_etf=True):
    url = "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    df = pd.read_csv(url, sep="|", dtype=str, engine="python")
    df = df[df["Symbol"] != "File Creation Time"]
    if not include_test and "Test Issue" in df.columns:
        df = df[df["Test Issue"] == "N"]
    if not include_etf and "ETF" in df.columns:
        df = df[df["ETF"] == "N"]
    tickers = df["Symbol"].dropna().tolist()
    return tickers, df


def get_nasdaq_listed_tickers_from_ftp(include_test=False, include_etf=True):
    return get_nasdaq_listed_tickers(include_test=include_test, include_etf=include_etf)

def download_date_data(code, flag):
    try:
        fg = '' if flag not in ['qfq', 'hfq'] else flag
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date='19901219', adjust=fg)
        stock_zh_a_hist_df.to_csv(f'./data_{flag}/{code}.csv')
    except Exception as e:
        print(f"download {flag} stock {code} error!!!")

def download_all_date_data(flag):
    # 获取所有股票代码，akshare接口
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    list_code = stock_zh_a_spot_em_df['代码'].to_list()

    fg = 'bfq' if flag not in ['qfq', 'hfq'] else flag # bfq: 不复权; qfq: 前复权; hfq: 后复权
    # 创建保存路径
    path = f'data_{fg}'
    if not os.path.isdir(path):
        os.makedirs(path)

    # 创建进程池来下载股票日数据
    count = os.cpu_count()
    pool = Pool(min(count*4, 60))
    for code in list_code:
        pool.apply_async(download_date_data, (code, flag))

    pool.close()
    pool.join()

def get_all_date_data(start_time, end_time, list_assets):
    data_path = 'data_bfq'

    # 从本地保存的数据中读出需要的股票日数据
    list_all = []
    for c in list_assets:
        df = pd.read_csv(f'{data_path}/{c}.csv')
        df['asset'] = c
        list_all.append(df[(df['日期'] >= start_time) & (df['日期'] <= end_time)])
        
    print(len(list_all))

    # 所有股票日数据拼接成一张表
    df_all = pd.concat(list_all)
        
    # 修改列名
    df_all = df_all.rename(columns={
        "日期": "date", 
        "开盘": "open", 
        "收盘": "close", 
        "最高": "high", 
        "最低": "low", 
        "成交量": "volume", 
        "成交额": "amount",
        "涨跌幅": "pctChg"})
    # 计算平均成交价
    df_all['vwap'] =  df_all.amount / df_all.volume / 100

    # 返回计算因子需要的列
    df_all = df_all.reset_index()
    df_all = df_all[['asset','date', "open", "close", "high", "low", "volume", 'vwap', "pctChg"]]
    return df_all

def get_zz500_stocks(time):
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

    # 获取中证500成分股
    rs = bs.query_zz500_stocks('2019-01-01')
    print('query_zz500 error_code:'+rs.error_code)
    print('query_zz500  error_msg:'+rs.error_msg)

    # 打印结果集
    zz500_stocks = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        zz500_stocks.append(rs.get_row_data())
    result = pd.DataFrame(zz500_stocks, columns=rs.fields)

    lists = result['code'].to_list()
    lists = [x.split('.')[1] for x in lists]

    # 登出系统
    bs.logout()
    return lists, result

def get_hs300_stocks(time):
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

    # 获取沪深300成分股
    rs = bs.query_hs300_stocks(time)
    print('query_hs300 error_code:'+rs.error_code)
    print('query_hs300  error_msg:'+rs.error_msg)

    # 打印结果集
    hs300_stocks = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        hs300_stocks.append(rs.get_row_data())
    result = pd.DataFrame(hs300_stocks, columns=rs.fields)
    
    lists = result['code'].to_list()
    lists = [x.split('.')[1] for x in lists]

    # 登出系统
    bs.logout()
    return lists, result

def download_index_data(code):
    path = 'index'
    stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol=code)
    # 创建保存路径
    if not os.path.isdir(path):
        os.makedirs(path)
    stock_zh_index_daily_df.to_csv(f'{path}/{code}.csv')

def download_us_stock_history(ticker, start, end, interval="1d", auto_adjust=True, out_dir="data_us"):
    try:
        df = yf.download(
            tickers=ticker,
            start=start,
            end=end,
            interval=interval,
            auto_adjust=auto_adjust,
            progress=False,
            group_by="column",
        )
        if df.empty:
            print(f"no data: {ticker}")
            return
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir)
        df = df.reset_index()
        df.to_csv(f"{out_dir}/{ticker}.csv", index=False)
    except Exception as exc:
        print(f"download stock {ticker} error: {exc}")


def download_all_us_stock_history(list_tickers, start, end, interval="1d", auto_adjust=True, out_dir="data_us"):
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    count = os.cpu_count() or 2
    pool = Pool(min(count * 4, 60))
    for ticker in list_tickers:
        pool.apply_async(
            download_us_stock_history,
            (ticker, start, end, interval, auto_adjust, out_dir),
        )
    pool.close()
    pool.join()


def download_us_stock_history_from_nasdaq_file(
    file_path,
    start,
    end,
    interval="1d",
    auto_adjust=True,
    out_dir="data_us",
    include_test=False,
    include_etf=False,
    common_stock_only=True,
    sequential=True,
    rate_limit_seconds=0.5,
):
    df = pd.read_csv(file_path, sep="|", dtype=str)
    df = df[df["Symbol"] != "File Creation Time"]
    if not include_test and "Test Issue" in df.columns:
        df = df[df["Test Issue"] == "N"]
    if not include_etf and "ETF" in df.columns:
        df = df[df["ETF"] == "N"]
    if common_stock_only and "Security Name" in df.columns:
        name = df["Security Name"].str.lower()
        keep = (
            name.str.contains("common stock")
            | name.str.contains("ordinary share")
            | name.str.contains("ordinary shares")
            | name.str.contains("class a common stock")
            | name.str.contains("class b common stock")
        )
        df = df[keep]
    tickers = df["Symbol"].dropna().tolist()
    if not tickers:
        print("no tickers in file")
        return []
    if sequential:
        for ticker in tickers:
            download_us_stock_history(
                ticker,
                start,
                end,
                interval=interval,
                auto_adjust=auto_adjust,
                out_dir=out_dir,
            )
            if rate_limit_seconds and rate_limit_seconds > 0:
                time.sleep(rate_limit_seconds)
    else:
        download_all_us_stock_history(
            tickers,
            start,
            end,
            interval=interval,
            auto_adjust=auto_adjust,
            out_dir=out_dir,
        )
    return tickers


def get_all_us_stock_data(start_time, end_time, list_assets, data_path="data_us"):
    list_all = []
    for ticker in list_assets:
        df = pd.read_csv(f"{data_path}/{ticker}.csv")
        df = df[(df["Date"] >= start_time) & (df["Date"] <= end_time)]
        df["asset"] = ticker
        list_all.append(df)
    if not list_all:
        return pd.DataFrame()
    df_all = pd.concat(list_all)
    df_all = df_all.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "Close": "close",
            "High": "high",
            "Low": "low",
            "Volume": "volume",
            "Adj Close": "adj_close",
        }
    )
    for col in ["open", "close", "high", "low", "volume", "adj_close"]:
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors="coerce")
    if "volume" in df_all.columns:
        df_all["vwap"] = (df_all["high"] + df_all["low"] + df_all["close"]) / 3.0
    df_all = df_all.reset_index(drop=True)
    keep_cols = [
        "asset",
        "date",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "vwap",
    ]
    if "adj_close" in df_all.columns:
        keep_cols.append("adj_close")
    return df_all[keep_cols]


def get_us_options_expirations(ticker):
    tk = yf.Ticker(ticker)
    return list(tk.options)


def download_us_options_chain(ticker, expiration=None, out_dir="options_us"):
    try:
        tk = yf.Ticker(ticker)
        expirations = [expiration] if expiration else list(tk.options)
        if not expirations:
            print(f"no options: {ticker}")
            return
        base_dir = f"{out_dir}/{ticker}"
        if not os.path.isdir(base_dir):
            os.makedirs(base_dir)
        for exp in expirations:
            chain = tk.option_chain(exp)
            calls = chain.calls.copy()
            calls["option_type"] = "call"
            puts = chain.puts.copy()
            puts["option_type"] = "put"
            df = pd.concat([calls, puts], ignore_index=True)
            df["expiration"] = exp
            df.to_csv(f"{base_dir}/{exp}.csv", index=False)
    except Exception as exc:
        print(f"download options {ticker} error: {exc}")


if __name__ == "__main__":
    today = date.today().strftime("%Y-%m-%d")
    print(today)
    download_us_stock_history("GOOG", "2010-01-01", today)
    download_us_options_chain("GOOG")
    download_us_stock_history_from_nasdaq_file(
    "data_us/nasdaq_listed.csv",
    start="2018-01-01",
    end=today,
    sequential=True,
    rate_limit_seconds=1.0,
    )   