import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 控制是否使用SDK获取数据(True)或从CSV文件读取(False)
USE_SDK_DATA = False  # 设置为False可从本地CSV文件读取数据，True则调用SDK

def get_stock_list():
    """
    获取A股股票列表，并过滤掉ST股票和非600、000开头的股票
    """
    pro = ts.pro_api()

    # 获取股票基本信息
    stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')

    # 过滤ST股票（名称中包含ST）
    non_st_stocks = stock_basic[~stock_basic['name'].str.contains('ST')]

    # 筛选以600或000开头的股票
    filtered_stocks = non_st_stocks[
        (non_st_stocks['ts_code'].str.startswith('600')) |
        (non_st_stocks['ts_code'].str.startswith('000'))
    ]

    return filtered_stocks

def calculate_ma(data, windows=[5, 10, 20, 30, 60]):
    """
    计算多个周期的移动平均线
    """
    for window in windows:
        data[f'ma{window}'] = data['close'].rolling(window=window).mean()
    return data

def calculate_kdj(df):
    """
    计算KDJ指标
    """
    low_min = df['low'].rolling(window=9).min()
    high_max = df['high'].rolling(window=9).max()

    rsv = (df['close'] - low_min) / (high_max - low_min) * 100
    df['k'] = rsv.ewm(com=2).mean()
    df['d'] = df['k'].ewm(com=2).mean()
    df['j'] = 3 * df['k'] - 2 * df['d']

    return df

def calculate_macd(df):
    """
    计算MACD指标
    """
    exp1 = df['close'].ewm(span=12).mean()
    exp2 = df['close'].ewm(span=26).mean()
    df['dif'] = exp1 - exp2
    df['dea'] = df['dif'].ewm(span=9).mean()
    df['macd'] = (df['dif'] - df['dea']) * 2

    return df

def save_stock_data_to_csv(df, ts_code, name):
    """
    将股票数据保存到CSV文件
    """
    filename = f"stock_data_{ts_code}_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(filename, index=False)
    print(f"股票 {ts_code} ({name}) 的数据已保存到 {filename}")

def load_stock_data_from_csv(ts_code, name):
    """
    从CSV文件加载股票数据
    """
    # 查找最新的数据文件，而不是限定特定日期
    import glob
    import os
    pattern = f"stock_data_{ts_code}_*.csv"
    files = glob.glob(pattern)

    if not files:
        print(f"找不到股票 {ts_code} ({name}) 的数据文件")
        return None

    # 选择最新文件
    latest_file = max(files, key=os.path.getctime)
    try:
        df = pd.read_csv(latest_file)
        print(f"从文件 {latest_file} 加载股票 {ts_code} ({name}) 的数据")
        return df
    except FileNotFoundError:
        print(f"文件 {latest_file} 不存在")
        return None

def pick_stocks(save_to_csv=True):
    """
    主要的选股函数
    """
    # 获取股票列表
    stocks = get_stock_list()
    print(f"总共有 {len(stocks)} 只符合条件的股票待检查")

    # 准备数据获取日期范围（最近60+个交易日的数据用于计算MA60）
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=200)).strftime('%Y%m%d')  # 获取更多历史数据确保有足够数据计算技术指标

    selected_stocks = []

    # 批量获取股票数据以减少API调用
    pro = ts.pro_api()

    for i, (index, stock) in enumerate(stocks.iterrows()):
        ts_code = stock['ts_code']

        try:
            # 根据USE_SDK_DATA变量决定数据来源
            if USE_SDK_DATA:
                # 获取股票历史数据
                df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

                # 保存数据到CSV文件
                if save_to_csv:
                    save_stock_data_to_csv(df, ts_code, stock['name'])
            else:
                # 从CSV文件加载数据
                df = load_stock_data_from_csv(ts_code, stock['name'])
                if df is None:
                    continue  # 如果文件不存在，则跳过该股票

            if len(df) < 60:  # 如果数据不足60天，跳过这只股票
                print(f"股票 {ts_code} 的数据不足60天，跳过")
                continue

            # 计算KDJ指标
            df = calculate_kdj(df)

            # 计算MACD指标
            df = calculate_macd(df)

            # 计算多条移动平均线
            df = calculate_ma(df, windows=[5, 10, 20, 30, 60])

            # 当USE_SDK_DATA为False时，也需要保存包含指标的数据到CSV
            if not USE_SDK_DATA and save_to_csv:
                save_stock_data_with_indicators_to_csv(df, ts_code, stock['name'])

            # 去除含有NaN值的行（只影响前面几天的指标，保留最新的完整数据）
            df = df.dropna()

            if len(df) < 50:  # 确保有足够的数据来检查最近50天
                print(f"股票 {ts_code} 的数据不足50天，跳过，实际数据天数：{len(df)}")
                continue

            # 检查最近50个交易日是否收盘价都在60日均线上方
            if check_price_above_ma_for_last_n_days(df, ma_window=60, n=50):
                selected_stocks.append({
                    'ts_code': ts_code,
                    'name': stock['name']
                })

            # 显示进度
            if (i + 1) % 100 == 0:
                print(f"已处理 {i + 1}/{len(stocks)} 只股票")

            # 只测试第一只股票并打印详细日志
            if i == 0:
                print(f"测试股票 {ts_code} ({stock['name']}) 的数据:")
                print(f"数据长度: {len(df)}")

                # 显示最近几行数据，按日期降序排列确保显示最新的
                df_desc = df.sort_values('trade_date', ascending=False)
                print("最近几行数据:")
                print(df_desc[['trade_date', 'close', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60']].first(10))

                # 检查最近50天收盘价是否都在60日均线上方
                recent_data = df.tail(50)
                price_above_ma = recent_data['close'] >= recent_data['ma60']
                print(f"最近50天价格是否在60日均线上方: {price_above_ma.all()}")

                break  # 只测试第一只股票

        except Exception as e:
            print(f"处理股票 {ts_code} 时出错: {str(e)}")
            continue

    return selected_stocks

def check_price_above_ma_for_last_n_days(df, ma_window=60, n=50):
    """
    检查最近n个交易日收盘价是否都在60日均线上方
    """
    recent_data = df.tail(n)
    ma_col = f'ma{ma_window}'

    # 添加调试信息
    print(f"检查最近{n}天数据，{ma_col}列是否存在: {ma_col in recent_data.columns}")
    if ma_col in recent_data.columns:
        print(f"最近{n}天收盘价与{ma_col}比较:")
        comparison = recent_data['close'] >= recent_data[ma_col]
        print(f"每日报价比较结果: {comparison.values}")
        print(f"全部满足条件: {comparison.all()}")
        return comparison.all()
    else:
        print(f"错误: {ma_col} 列不存在")
        return False

def save_stock_data_with_indicators_to_csv(df, ts_code, name):
    """
    将包含技术指标的股票数据保存到CSV文件
    """
    filename = f"stock_data_{ts_code}_{datetime.now().strftime('%Y%m%d')}_with_indicators.csv"
    df.to_csv(filename, index=False)
    print(f"股票 {ts_code} ({name}) 的数据（含技术指标）已保存到 {filename}")

def check_price_above_ma_for_last_n_days(df, ma_window=60, n=50):
    """
    检查最近n个交易日收盘价是否都在60日均线上方
    """
    recent_data = df.tail(n)
    ma_col = f'ma{ma_window}'
    return all(recent_data['close'] >= recent_data[ma_col])

if __name__ == "__main__":
    result = pick_stocks()
    print(f"\n符合条件的股票数量: {len(result)}")
    for stock in result:
        print(f"{stock['ts_code']}: {stock['name']}")
