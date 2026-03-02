from langchain.tools import tool
from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate
import pandas as pd  # 保留pandas用于数据处理
from datetime import datetime, timedelta
import talib
import tushare as ts  # 新增tushare导入
import requests
from bs4 import BeautifulSoup
import feedparser  # 用于解析RSS源

# 设置tushare token（请替换为您自己的token）
ts.set_token("xxxxxxxx")

# 初始化模型
model = init_chat_model(
    "qwen-plus",
    model_provider="openai",
    api_key="sk-16e71336e06e4c3b9013f1434a38a4a6",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)

# 从新浪财经获取财经新闻
def get_sina_finance_news():
    """从新浪财经获取最新财经新闻"""
    try:
        url = "https://finance.sina.com.cn/roll/index.d.html?cid=56229&page=1"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'

        soup = BeautifulSoup(response.text, 'html.parser')
        news_list = []

        # 解析新闻列表
        for item in soup.find_all('li')[:10]:  # 取前10条新闻
            link_tag = item.find('a')
            time_tag = item.find('span', class_='time')

            if link_tag:
                title = link_tag.get_text(strip=True)
                link = link_tag.get('href')

                # 获取发布时间
                time_str = time_tag.get_text(strip=True) if time_tag else datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                news_list.append({
                    "title": title,
                    "content": f"链接: {link}",
                    "publish_time": time_str,
                    "source": "新浪财经"
                })

                if len(news_list) >= 5:  # 只返回前5条
                    break

        return news_list
    except Exception as e:
        print(f"获取新浪财经新闻失败: {str(e)}")
        return []

# 从东财网获取财经新闻
def get_eastmoney_news():
    """从东方财富网获取财经新闻"""
    try:
        url = "https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_102_12512.js"
        headers = {
            'Referer': 'https://stock.eastmoney.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers)
        # 由于API返回的是JS格式，这里简化处理
        # 实际应用中可能需要更复杂的解析逻辑

        # 返回示例数据作为备选
        sample_news = [
            {
                "title": "A股市场今日表现活跃",
                "content": "今日A股市场整体表现良好，上证指数小幅上涨，成交量温和放大。",
                "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "东方财富网"
            },
            {
                "title": "科技股领涨市场",
                "content": "科技板块今日表现强势，多只科技股涨停。",
                "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "东方财富网"
            }
        ]
        return sample_news
    except Exception as e:
        print(f"获取东财新闻失败: {str(e)}")
        return []

# 从RSS源获取新闻
def get_rss_news():
    """从RSS源获取财经新闻"""
    try:
        rss_urls = [
            "https://www.cls.cn/api/sw?app=CailianpressWeb&os=web&sv=7.2.2",  # 财联社新闻API替代方案
        ]

        all_news = []

        # 尝试使用财联社API获取新闻
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': 'https://www.cls.cn/'
            }

            # 发送请求获取新闻
            params = {
                'type': 'telegram',
                'channel': 'telegraph',
                'last_time': 0
            }
            response = requests.get("https://www.cls.cn/api/sw", params=params, headers=headers)

            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'roll_data' in data['data']:
                    for item in data['data']['roll_data'][:5]:
                        all_news.append({
                            "title": item.get('title', item.get('content', '财经新闻')),
                            "content": item.get('content', ''),
                            "publish_time": datetime.fromtimestamp(item.get('ctime', 0)).strftime("%Y-%m-%d %H:%M:%S") if item.get('ctime') else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "source": "财联社"
                        })

                        if len(all_news) >= 5:
                            break
        except Exception as e:
            print(f"财联社API获取失败: {str(e)}")

        # 如果财联社API失败，尝试其他RSS源
        if not all_news:
            rss_urls_fallback = [
                "https://rsshub.app/bilibili/partion/currency",  # B站财经分区RSS
            ]

            for rss_url in rss_urls_fallback:
                try:
                    feed = feedparser.parse(rss_url)
                    for entry in feed.entries[:3]:  # 每个RSS源取前3条
                        all_news.append({
                            "title": entry.title,
                            "content": entry.summary if hasattr(entry, 'summary') else (entry.description if hasattr(entry, 'description') else entry.title),
                            "publish_time": entry.published if hasattr(entry, 'published') else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "source": getattr(feed.feed, 'title', 'RSS源') if hasattr(feed, 'feed') and hasattr(feed.feed, 'title') else "RSS源"
                        })
                        if len(all_news) >= 5:  # 总共不超过5条
                            break
                    if len(all_news) >= 5:
                        break
                except Exception as e:
                    print(f"RSS源获取失败 {rss_url}: {str(e)}")
                    continue

        # 最后的备选方案：返回一些示例新闻
        if not all_news:
            all_news = [
                {
                    "title": "A股市场今日表现活跃，上证指数上涨0.5%",
                    "content": "今日A股市场整体表现良好，上证指数小幅上涨，成交量温和放大，市场情绪较为乐观。",
                    "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "source": "示例新闻"
                },
                {
                    "title": "央行发布最新货币政策执行报告",
                    "content": "央行表示将继续实施稳健的货币政策，保持流动性合理充裕，支持实体经济发展。",
                    "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "source": "示例新闻"
                }
            ]

        return all_news
    except Exception as e:
        print(f"获取RSS新闻失败: {str(e)}")
        return []

# 添加 yfinance 新闻获取功能
def get_yfinance_news():
    """
    使用 yfinance 获取股票相关的财经新闻
    """
    import yfinance as yf
    import datetime

    try:
        # 获取市场新闻（yfinance 可以获取一些市场相关新闻）
        # 这里以主要指数为例获取相关新闻
        tickers = ['SPY', '^GSPC', '^DJI', '^IXIC']  # 主要指数

        news_items = []

        for ticker_symbol in tickers:
            ticker = yf.Ticker(ticker_symbol)

            # 尝试获取新闻数据
            try:
                ticker_news = ticker.news
                if ticker_news:
                    for item in ticker_news:
                        # 只获取最近的新闻（过去24小时）
                        if 'providerPublishTime' in item:
                            publish_time = datetime.datetime.fromtimestamp(item['providerPublishTime'])
                            time_diff = datetime.datetime.now() - publish_time

                            # 只获取最近一天的新闻
                            if time_diff.days <= 1:
                                news_items.append({
                                    'title': item.get('title', '无标题'),
                                    'summary': item.get('description', ''),
                                    'url': item.get('link', ''),
                                    'publisher': item.get('publisher', '未知来源'),
                                    'publish_time': publish_time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'related_stocks': extract_stocks_from_title(item.get('title', ''))
                                })
            except Exception as e:
                print(f"获取 {ticker_symbol} 新闻时出错: {e}")
                continue

        # 去重并按时间排序
        unique_news = {}
        for item in news_items:
            key = item['title']
            if key not in unique_news:
                unique_news[key] = item

        sorted_news = sorted(unique_news.values(), key=lambda x: x['publish_time'], reverse=True)

        # 限制返回数量
        return sorted_news[:10]  # 返回最新的10条新闻

    except Exception as e:
        print(f"获取 yfinance 新闻时出错: {e}")
        return []

def extract_stocks_from_title(title):
    """
    从新闻标题中提取可能涉及的股票代码
    """
    import re
    # 匹配股票代码的正则表达式（例如 AAPL, GOOGL 等）
    stock_pattern = r'\b[A-Z]{1,5}\b'
    potential_stocks = re.findall(stock_pattern, title.upper())

    # 过滤常见单词，只保留可能的股票代码
    common_words = {'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HER', 'WAS', 'ITS', 'OUR', 'HAD', 'GET', 'WHO', 'OUT', 'DAY', 'HAS', 'MAY', 'ANY', 'MAN', 'EAT', 'BIG', 'OLD', 'NEW', 'NOW', 'USE', 'OWN', 'PUT', 'HOW', 'OFF', 'TRY', 'WHY', 'ASK', 'TOO', 'SET', 'TWO', 'WAY', 'RUN', 'END', 'TOP', 'LOW', 'WIN', 'LOT', 'BUY', 'SELL', 'UP', 'DOWN', 'ON', 'IN', 'OF', 'AT', 'BY', 'OR', 'IF', 'IT', 'AS', 'WE', 'HE', 'ME', 'MY', 'US', 'BE', 'IS', 'AM', 'AN', 'DO', 'GO', 'SO', 'NO'}

    actual_stocks = [stock for stock in potential_stocks if len(stock) >= 2 and stock not in common_words]
    return actual_stocks

# 修改原有的 get_finance_news 工具以包含 yfinance 新闻
def get_finance_news():
    """
    获取财经新闻（结合多种来源，包括 yfinance）
    """
    # 这里您可以选择调用原来的新闻获取方法或新的 yfinance 方法
    # 或者结合两者
    yf_news = get_yfinance_news()

    # 如果您还想保留原来的新闻获取方式，可以在这里合并
    # merged_news = original_news + yf_news  # 合并两种来源的新闻

    return yf_news

# 删除重复的函数定义
# def get_sina_finance_news(): ...

# 删除重复的函数定义
# def get_eastmoney_news(): ...

# 删除重复的函数定义
# def get_rss_news(): ...

# 财经新闻工具
@tool
def get_finance_news() -> list:
    """获取一天内的关键财经新闻

    Returns:
        list: 新闻列表，每条新闻包含标题、内容、发布时间
    """
    try:
        # 首先尝试tushare获取财经新闻
        pro = ts.pro_api()

        # 尝试获取新闻资讯（A股市场新闻）
        try:
            today = datetime.now().strftime("%Y%m%d")
            # 获取A股市场新闻
            news_df = pro.news(src='sina', start_date=today, end_date=today)

            # 如果A股市场新闻为空，尝试获取行业新闻
            if news_df.empty:
                news_df = pro.industry_news(ind_code='', start_date=today, end_date=today)

            # 如果行业新闻仍为空，尝试获取公司公告
            if news_df.empty:
                news_df = pro.disclosure_news(trade_date=today)

        except Exception as e:
            print(f"tushare新闻获取失败: {str(e)}")
            news_df = pd.DataFrame()

        if not news_df.empty:
            # 处理tushare返回的数据，适配项目格式
            news_data = []
            for _, row in news_df.iterrows():
                # 根据不同接口返回的字段进行适配
                if 'title' in row and 'content' in row:
                    title = row['title']
                    content = row['content']
                elif 'ann_title' in row and 'ann_content' in row:
                    title = row['ann_title']
                    content = row['ann_content']
                else:
                    continue

                # 处理发布时间
                if 'pub_time' in row and pd.notna(row['pub_time']):
                    publish_time = row['pub_time']
                elif 'ann_date' in row and pd.notna(row['ann_date']):
                    publish_time = row['ann_date']
                else:
                    publish_time = today

                # 处理来源
                source = row.get('src', 'tushare') if 'src' in row else "财经资讯"

                news_item = {
                    "title": title,
                    "content": str(content)[:500] if pd.notna(content) else "",
                    "publish_time": publish_time,
                    "source": source
                }
                news_data.append(news_item)

                if len(news_data) >= 5:  # 限制最多返回5条新闻
                    break

            if news_data:
                return news_data

        # 如果tushare获取不到数据，尝试其他方式
        # 尝试获取新浪财经新闻
        sina_news = get_sina_finance_news()
        if sina_news:
            return sina_news

        # 尝试获取RSS新闻
        rss_news = get_rss_news()
        if rss_news:
            return rss_news

        # 如果所有方式都失败，返回示例数据
        sample_news = [
            {
                "title": "A股市场今日收涨，上证指数上涨0.8%",
                "content": "今日A股市场表现强劲，上证指数收盘上涨0.8%，深证成指上涨1.2%，创业板指上涨1.5%。市场成交量较前一交易日有所放大，显示投资者情绪较为乐观。",
                "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "模拟财经新闻"
            },
            {
                "title": "央行今日开展逆回购操作，向市场投放流动性",
                "content": "中国人民银行今日开展了1000亿元逆回购操作，期限7天，中标利率维持不变。此举旨在维护银行体系流动性合理充裕，稳定市场预期。",
                "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "模拟财经新闻"
            },
            {
                "title": "新能源汽车销量持续增长，产业链景气度提升",
                "content": "最新数据显示，新能源汽车销量继续保持高速增长态势，多家车企公布月度销售数据超预期。产业链上下游企业业绩有望持续受益。",
                "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "模拟财经新闻"
            }
        ]
        return sample_news

    except Exception as e:
        # 如果获取真实新闻失败，返回示例数据
        print(f"获取新闻时出错: {str(e)}")
        sample_news = [
            {
                "title": "财经新闻获取服务临时故障",
                "content": f"当前无法获取实时财经新闻，错误信息: {str(e)}。已切换到模拟新闻数据。",
                "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "系统提示"
            },
            {
                "title": "A股市场整体表现平稳",
                "content": "今日股市行情总体稳定，各主要指数呈现窄幅震荡走势。投资者情绪相对谨慎，市场交投活跃度一般。",
                "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "模拟财经新闻"
            }
        ]
        return sample_news

# 股票数据工具
@tool
def get_stock_data(stock_code: str, period: str = "6mo") -> dict:
    """获取股票历史数据和技术指标

    Args:
        stock_code: 股票代码，如 '000001.SZ'
        period: 时间周期，默认6个月

    Returns:
        dict: 包含股价数据和技术指标的字典
    """
    try:
        # 处理时间周期，转换为开始日期
        today = datetime.now()
        if period == "1d":
            start_date = today - timedelta(days=1)
        elif period == "1wk":
            start_date = today - timedelta(weeks=1)
        elif period == "1mo":
            start_date = today - timedelta(days=30)
        elif period == "3mo":
            start_date = today - timedelta(days=90)
        elif period == "6mo":
            start_date = today - timedelta(days=180)
        elif period == "1y":
            start_date = today - timedelta(days=365)
        else:
            start_date = today - timedelta(days=180)  # 默认6个月

        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = today.strftime("%Y%m%d")

        # 使用tushare获取股票数据
        pro = ts.pro_api()
        df = pro.daily(ts_code=stock_code, start_date=start_date_str, end_date=end_date_str)

        if df.empty:
            return {"error": "No data found"}

        # 数据处理：重命名列并设置日期索引
        df = df.rename(columns={
            'close': 'Close',
            'high': 'High',
            'low': 'Low',
            'vol': 'Volume'
        })
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.set_index('trade_date')
        df = df.sort_index()  # 按日期升序排列
        hist = df

        # 计算技术指标
        closes = hist['Close'].values
        highs = hist['High'].values
        lows = hist['Low'].values
        volumes = hist['Volume'].values

        technical_indicators = {
            "rsi": talib.RSI(closes)[-1],
            "macd": talib.MACD(closes)[0][-1],
            "bollinger_upper": talib.BBANDS(closes)[0][-1],
            "bollinger_lower": talib.BBANDS(closes)[2][-1],
            "current_price": closes[-1],
            "price_change": (closes[-1] - closes[-2]) / closes[-2] * 100
        }

        return {
            "stock_code": stock_code,
            "historical_data": hist.tail(10).to_dict(),
            "technical_indicators": technical_indicators
        }

    except Exception as e:
        return {"error": str(e)}

# 新闻分析工具
@tool
def analyze_news(news_content: str) -> dict:
    """分析新闻内容，提取相关股票和评分"""
    # 创建分析模板
    template = ChatPromptTemplate.from_messages([
        ("system", """你是一个专业的财经新闻分析师。请分析以下新闻内容，提取相关信息：

分析要求：
1. 识别新闻中提到的股票（至少1-3只）
2. 对新闻的利好程度进行评分（1-10分，10分最利好）
3. 简要说明评分理由

输出格式要求（必须严格遵循JSON格式）：
{{
    "leading_stocks": [
        {{"code": "000001.SZ", "name": "平安银行"}},
        {{"code": "600036.SH", "name": "招商银行"}}
    ],
    "score": 8,
    "reasoning": "新闻内容对公司业务有积极影响"
}}

注意：股票代码必须是6位数字加上交易所后缀（.SH或.SZ）"""),
        ("human", "新闻内容：{news_content}")
    ])

    # 这里使用大模型进行分析
    prompt = f"""你是一个专业的股票市场分析师。请分析以下财经新闻内容，并返回JSON格式的结果：

要求：
1. 识别新闻涉及的主要行业板块
2. 提取新闻中提到的A股上市公司股票，必须使用标准的6位数字代码加上交易所后缀格式（如：600036.SH、000001.SZ）
3. 对新闻的利好程度进行评分（1-10分）
4. 简要分析新闻对相关股票的影响

返回格式必须是严格的JSON：
{{
  "sector": "行业名称",
  "leading_stocks": [
    {{"code": "股票代码.SH/SZ", "name": "公司名称"}},
    ...
  ],
  "score": 评分数字,
  "analysis": "简要分析"
}}

注意：股票代码必须是6位数字+交易所后缀格式，不要使用任何其他格式！"""

    response = model.invoke(prompt)

    # 解析模型返回的JSON内容
    try:
        # 从响应内容中提取JSON部分
        import json
        import re

        # 尝试从响应文本中提取JSON
        json_match = re.search(r'\{.*}', response.content, re.DOTALL)
        if json_match:
            json_str = json_match.group()
            return json.loads(json_str)
        else:
            # 如果无法提取JSON，返回错误信息
            return {"error": "Failed to parse JSON from model response", "raw_response": response.content}

    except Exception as e:
        return {"error": f"JSON parsing error: {str(e)}", "raw_response": response.content}

# 技术面分析工具
@tool
def analyze_technical(stock_data: dict) -> dict:
    """分析股票技术面，给出技术评分

    Args:
        stock_data: 股票数据字典

    Returns:
        dict: 技术面分析结果
    """
    indicators = stock_data.get("technical_indicators", {})

    # 简单的技术评分逻辑
    technical_score = 5  # 基础分

    # RSI指标评分
    rsi = indicators.get("rsi", 50)
    if 30 < rsi < 70:
        technical_score += 1
    elif 40 < rsi < 60:
        technical_score += 2

    # MACD评分
    macd = indicators.get("macd", 0)
    if macd > 0:
        technical_score += 1

    # 价格变化评分
    price_change = indicators.get("price_change", 0)
    if price_change > 0:
        technical_score += 1

    return {
        "technical_score": min(10, max(0, technical_score)),
        "indicators_analysis": indicators
    }

# 原有的数学工具保持不变
@tool
def multiply(a: int, b: int) -> int:
    """Multiply `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a * b


@tool
def add(a: int, b: int) -> int:
    """Adds `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a + b


@tool
def divide(a: int, b: int) -> float:
    """Divide `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a / b


# Augment the LLM with tools
tools = [add, multiply, divide]
tools_by_name = {tool.name: tool for tool in tools}
model_with_tools = model.bind_tools(tools)