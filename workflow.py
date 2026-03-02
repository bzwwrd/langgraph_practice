import logging  # 添加日志模块
import re
from typing import TypedDict, List, Dict, Any

from langgraph.graph import StateGraph, END

# 导入配置文件
from config.sector_config import SECTOR_CONFIG, STOCK_NAME_MAPPING
from tools import get_finance_news, analyze_news, get_stock_data, analyze_technical

# 配置日志
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')


# 定义状态
class AnalysisState(TypedDict):
    news_list: List[Dict]
    analyzed_news: List[Dict]
    stock_data: Dict[str, Any]
    technical_scores: Dict[str, int]
    final_scores: Dict[str, float]
    sorted_stocks: List[Dict]
    invalid_stocks: List[str]  # 添加无效股票代码列表


# 节点1: 获取新闻
def get_news_node(state: AnalysisState) -> AnalysisState:
    print("正在获取财经新闻...")
    news = get_finance_news.invoke({})

    # 添加调试信息
    print(f"获取到 {len(news)} 条新闻")
    for i, item in enumerate(news):
        print(f"新闻 {i + 1}: 标题='{item.get('title', '无标题')}'")
        print(f"      内容长度: {len(item.get('content', ''))} 字符")

    # 修复：返回完整的状态结构
    return {
        "news_list": news,
        "analyzed_news": state.get("analyzed_news", []),
        "stock_data": state.get("stock_data", {}),
        "technical_scores": state.get("technical_scores", {}),
        "final_scores": state.get("final_scores", {}),
        "sorted_stocks": state.get("sorted_stocks", []),
        "invalid_stocks": state.get("invalid_stocks", [])  # 保留无效股票列表
    }


# 节点2: 分析新闻 - 修改以更好地处理 yfinance 新闻数据
def analyze_news_node(state: AnalysisState) -> AnalysisState:
    print("正在分析新闻内容...")
    analyzed_results = []
    import json
    import re

    for i, news in enumerate(state["news_list"]):
        print(f"\n=== 分析第 {i + 1} 条新闻 ===")

        # 为 yfinance 新闻构建更好的上下文
        title = news.get('title', '无标题')
        summary = news.get('summary', '')
        publisher = news.get('publisher', '未知来源')
        publish_time = news.get('publish_time', '未知时间')

        print(f"新闻标题: {title}")
        print(f"发布方: {publisher}")
        print(f"发布时间: {publish_time}")

        # 构建更详细的新闻内容用于分析
        news_content = f"""
        标题: {title}
        
        摘要: {summary}
        
        发布方: {publisher}
        发布时间: {publish_time}
        """

        # 如果摘要太短，使用标题作为主要内容
        if len(summary.strip()) < 10:
            news_content = f"标题: {title}\n\n内容: {title}"

        result = analyze_news.invoke({"news_content": news_content})

        # 打印原始结果类型和内容
        print(f"分析结果类型: {type(result)}")
        if hasattr(result, 'content'):
            print(f"分析结果内容 (AIMessage): {result.content}")
        else:
            print(f"分析结果内容: {result}")

        # 统一转换为字典格式
        analyzed_dict = {}
        if isinstance(result, dict):
            analyzed_dict = result
            # 新增：验证分析结果的合理性
            if not validate_analysis_result(title, summary, analyzed_dict):
                print(f"警告: 分析结果与新闻内容不匹配，重新分析...")
                # 如果验证失败，使用更简单的分析逻辑
                analyzed_dict = create_fallback_analysis(title, summary)
        elif hasattr(result, 'content'):
            content = result.content
            # 处理可能包含在markdown代码块中的JSON
            json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)

            try:
                analyzed_dict = json.loads(content.strip())
                # 新增：验证分析结果的合理性
                if not validate_analysis_result(title, summary, analyzed_dict):
                    print(f"警告: 分析结果与新闻内容不匹配，重新分析...")
                    analyzed_dict = create_fallback_analysis(title, summary)
            except (json.JSONDecodeError, AttributeError):
                # 如果JSON解析失败，尝试手动提取关键信息
                analyzed_dict = create_fallback_analysis(title, summary)
        else:
            analyzed_dict = create_fallback_analysis(title, summary)

        # 检查是否有从新闻标题中提取的股票代码
        related_stocks = news.get('related_stocks', [])
        if related_stocks:
            # 将 yfinance 提取的股票代码与AI分析的股票代码合并
            ai_stocks = analyzed_dict.get("leading_stocks", [])
            all_stocks = list(set(related_stocks + ai_stocks))

            # 校验股票代码
            validated_stocks = []
            for stock_item in all_stocks:
                stock_code = None

                if isinstance(stock_item, dict):
                    if "code" in stock_item:
                        stock_code = stock_item["code"]
                    elif "symbol" in stock_item:
                        stock_code = stock_item["symbol"]
                # 处理字符串格式的股票代码
                elif isinstance(stock_item, str):
                    stock_code = stock_item

                # 校验股票代码
                if stock_code and is_valid_stock_code(stock_code):
                    validated_stocks.append(stock_code)
                elif stock_code:
                    print(f"警告: 发现无效股票代码 {stock_code}，尝试自动修正...")
                    corrected_code = normalize_stock_code(stock_code)
                    if corrected_code and is_valid_stock_code(corrected_code):
                        validated_stocks.append(corrected_code)
                        print(f"已修正: {stock_code} -> {corrected_code}")
                    else:
                        print(f"无法修正无效股票代码: {stock_code}")

            analyzed_dict["leading_stocks"] = validated_stocks

        print(f"转换后的分析结果: {analyzed_dict}")
        analyzed_results.append(analyzed_dict)

    # 修复：返回完整的状态结构
    return {
        "news_list": state["news_list"],
        "analyzed_news": analyzed_results,
        "stock_data": state.get("stock_data", {}),
        "technical_scores": state.get("technical_scores", {}),
        "final_scores": state.get("final_scores", {}),
        "sorted_stocks": state.get("sorted_stocks", [])  # 保留无效股票列表
    }


# 节点3: 获取股票数据
def get_stock_data_node(state: AnalysisState) -> AnalysisState:
    print("正在获取股票数据...")
    stock_data = {}
    all_stocks = set()
    invalid_stocks = state.get("invalid_stocks", [])  # 获取已有的无效股票列表

    # 收集所有新闻中提到的股票
    for i, news in enumerate(state["analyzed_news"]):
        stocks = news.get("leading_stocks", [])
        print(f"从新闻分析结果中提取到股票: {stocks}")

        for stock in stocks:
            # 校验股票代码格式
            if not is_valid_stock_code(stock):
                logging.warning(f"发现无效股票代码: {stock}")
                invalid_stocks.append(stock)
                continue
            all_stocks.add(stock)  # 使用完整的股票代码（包含后缀）

    print(f"发现需要获取数据的股票: {list(all_stocks)}")
    print(f"发现的无效股票代码: {invalid_stocks}")

    for stock_code in all_stocks:
        print(f"正在获取股票 {stock_code} 的数据...")
        try:
            # 使用完整的股票代码（包含后缀）获取数据
            data = get_stock_data.invoke({"stock_code": stock_code})

            # 添加调试信息：打印完整的数据结构
            print(f"股票 {stock_code} 的完整数据结构: {data}")
            print(f"数据类型: {type(data)}")
            if isinstance(data, dict):
                print(f"数据键: {list(data.keys())}")
                if 'historical_prices' in data:
                    print(f"历史价格数据: {data['historical_prices']}")
                    print(f"历史价格类型: {type(data['historical_prices'])}")
                    if isinstance(data['historical_prices'], list):
                        print(f"历史价格数量: {len(data['historical_prices'])}")
                        if len(data['historical_prices']) > 0:
                            print(f"第一条历史价格记录: {data['historical_prices'][0]}")
                    elif hasattr(data['historical_prices'], '__len__'):
                        print(f"历史价格数量: {len(data['historical_prices'])}")

            if 'error' in data:
                print(f"股票 {stock_code} 数据获取失败: {data['error']}")
                logging.error(f"股票 {stock_code} 数据获取失败: {data['error']}")  # 新增错误日志
                # 为失败的股票提供默认数据结构
                stock_data[stock_code] = {
                    "error": data['error'],
                    "technical_indicators": {
                        "current_price": 0,
                        "rsi": 50,
                        "macd": 0,
                        "bollinger_upper": 0,
                        "bollinger_lower": 0,
                        "bollinger_middle": 0
                    }
                }
            else:
                # 验证股票数据是否有效（放宽验证条件）
                if not validate_stock_data(stock_code, data):
                    logging.warning(f"股票数据验证失败: 代码 {stock_code}，数据可能不完整")
                    # 但仍然使用获取到的数据，只是标记为可能有问题的数据
                    data['validation_warning'] = '数据验证失败，可能不完整'
                    stock_data[stock_code] = data
                    print(f"股票 {stock_code} 数据验证失败，但仍保留数据")
                    continue

                stock_data[stock_code] = data
                print(f"股票 {stock_code} 数据获取成功")
                print(f"  - 当前价格: {data.get('technical_indicators', {}).get('current_price', 'N/A')}")
        except Exception as e:
            print(f"获取股票 {stock_code} 数据时发生异常: {e}")
            logging.error(f"获取股票 {stock_code} 数据异常: {str(e)}")  # 改进日志信息
            # 异常情况下提供默认数据
            stock_data[stock_code] = {
                "error": str(e),
                "technical_indicators": {
                    "current_price": 0,
                    "rsi": 50,
                    "macd": 0,
                    "bollinger_upper": 0,
                    "bollinger_lower": 0,
                    "bollinger_middle": 0
                }
            }

    # 修复：返回完整的状态结构，保留之前的所有数据
    return {
        "news_list": state["news_list"],
        "analyzed_news": state["analyzed_news"],
        "stock_data": stock_data,
        "technical_scores": state.get("technical_scores", {}),
        "final_scores": state.get("final_scores", {}),
        "sorted_stocks": state.get("sorted_stocks", []),
        "invalid_stocks": invalid_stocks  # 更新无效股票列表
    }


# 节点4: 技术面分析
def technical_analysis_node(state: AnalysisState) -> AnalysisState:
    print("正在进行技术面分析...")
    technical_scores = {}

    print("技术面分析详情:")
    for stock_code, data in state["stock_data"].items():
        if "error" not in data:
            try:
                # 检查数据是否足够进行技术分析 - 修正数据键
                if not validate_stock_data_for_analysis(data):
                    print(f"  {stock_code}: 数据不足，无法进行技术分析，使用默认评分5")
                    technical_scores[stock_code] = 5
                    continue

                # 准备用于技术分析的数据
                prepared_data = data.copy()

                # 如果数据结构不同，进行适配
                if 'historical_data' in data and 'historical_prices' not in data:
                    # 将historical_data转换为historical_prices格式
                    hist_data = data['historical_data']
                    if isinstance(hist_data, dict) and 'open' in hist_data:
                        # 将字典格式转换为列表格式
                        dates = list(hist_data['open'].keys())
                        prepared_historical = []
                        for date in dates:
                            record = {
                                'date': date,
                                'open': hist_data['open'][date],
                                'high': hist_data['High'][date],
                                'low': hist_data['Low'][date],
                                'close': hist_data['Close'][date],
                                'volume': hist_data['Volume'][date] if 'Volume' in hist_data else 0
                            }
                            prepared_historical.append(record)

                        prepared_data['historical_prices'] = prepared_historical

                tech_analysis = analyze_technical.invoke({"stock_data": prepared_data})
                # 新增日志：打印原始技术分析结果
                logging.info(f"股票 {stock_code} 技术分析原始结果: {tech_analysis}")

                # 检查tech_analysis的结构是否正确
                if isinstance(tech_analysis, dict) and "technical_score" in tech_analysis:
                    technical_score = tech_analysis["technical_score"]
                    technical_scores[stock_code] = technical_score
                    print(f"  {stock_code}: 技术面评分 = {technical_score}")
                else:
                    print(f"  {stock_code}: 技术分析结果格式不正确，使用默认评分5")
                    logging.warning(f"股票 {stock_code} 技术分析结果格式不正确: {tech_analysis}")
                    technical_scores[stock_code] = 5
            except Exception as e:
                print(f"  {stock_code}: 技术面分析失败，使用默认评分5。错误: {e}")
                logging.error(f"股票 {stock_code} 技术面分析失败: {str(e)}")  # 改进日志信息
                technical_scores[stock_code] = 5
        else:
            # 打印具体错误原因
            print(f"  {stock_code}: 数据获取失败({data['error']})，使用默认评分5")
            logging.warning(f"股票 {stock_code} 数据获取失败: {data['error']}，使用默认评分5")  # 新增警告日志
            technical_scores[stock_code] = 5

    return {
        "news_list": state["news_list"],
        "analyzed_news": state["analyzed_news"],
        "stock_data": state["stock_data"],
        "technical_scores": technical_scores,
        "final_scores": state.get("final_scores", {}),
        "sorted_stocks": state.get("sorted_stocks", [])
    }


# 节点5: 综合评分
def comprehensive_scoring_node(state: AnalysisState) -> AnalysisState:
    print("正在进行综合评分...")
    final_scores = {}

    # 收集所有新闻中对每个股票的评分
    stock_news_scores = {}
    for i, news in enumerate(state["analyzed_news"]):
        # 确保 news 是字典格式
        if isinstance(news, dict):
            sector_score = news.get("score", 5)
            leading_stocks = news.get("leading_stocks", [])
        else:
            # 如果不是字典，使用默认值
            sector_score = 5
            leading_stocks = []

        print(f"新闻{i + 1}: 评分={sector_score}, 相关股票={leading_stocks}")

        for stock in leading_stocks:
            # 跳过无效股票
            if stock in state.get("invalid_stocks", []):
                continue
            if stock not in stock_news_scores:
                stock_news_scores[stock] = []
            stock_news_scores[stock].append(sector_score)

    print("\n各股票新闻评分详情:")
    for stock_code, scores in stock_news_scores.items():
        # 跳过无效股票
        if stock_code in state.get("invalid_stocks", []):
            continue

        news_score = sum(scores) / len(scores) if scores else 5
        tech_score = state["technical_scores"].get(stock_code, 5)

        print(f"  {stock_code}:")
        print(f"    - 新闻评分列表: {scores}")
        print(f"    - 平均新闻评分: {news_score:.2f}")
        print(f"    - 技术面评分: {tech_score}")

        # 综合评分：新闻面权重0.6，技术面权重0.4
        final_score = news_score * 0.6 + tech_score * 0.4
        final_scores[stock_code] = round(final_score, 2)
        print(f"    - 综合评分: {final_score:.2f} (新闻面60% + 技术面40%)")

    # 修复：返回完整的状态，包含之前的所有数据
    return {
        "news_list": state["news_list"],
        "analyzed_news": state["analyzed_news"],
        "stock_data": state["stock_data"],
        "technical_scores": state["technical_scores"],
        "final_scores": final_scores,
        "invalid_stocks": state.get("invalid_stocks", [])  # 保留无效股票列表
    }


# 节点6: 排序输出
def sort_stocks_node(state: AnalysisState) -> AnalysisState:
    print("正在排序股票...")
    sorted_stocks = []

    print("\n=== 中间结果汇总 ===")
    print("技术面评分:", state["technical_scores"])
    print("最终评分:", state["final_scores"])
    print("股票数据 keys:", list(state["stock_data"].keys()))

    # 添加调试信息
    for stock_code, stock_info in state["stock_data"].items():
        print(f"股票 {stock_code} 数据详情: {stock_info}")
        print(f"  - 是否有历史价格: {'historical_prices' in stock_info}")
        if 'historical_prices' in stock_info:
            print(f"  - 历史价格数量: {len(stock_info['historical_prices']) if stock_info['historical_prices'] else 0}")

    for stock_code, score in state["final_scores"].items():
        # 查找该股票的新闻评分
        news_score = 5
        for news in state["analyzed_news"]:
            if isinstance(news, dict) and stock_code in news.get("leading_stocks", []):
                news_score = news.get("score", 5)
                break

        stock_info = {
            "stock_code": stock_code,
            "final_score": score,
            "news_score": news_score,
            "technical_score": state["technical_scores"].get(stock_code, 5),
            "current_price": state["stock_data"].get(stock_code, {}).get("technical_indicators", {}).get(
                "current_price", 0)
        }
        sorted_stocks.append(stock_info)

    # 按分数从高到低排序
    sorted_stocks.sort(key=lambda x: x["final_score"], reverse=True)

    print("\n=== 最终排序结果 ===")
    for i, stock in enumerate(sorted_stocks, 1):
        print(f"{i}. {stock['stock_code']}: 总分{stock['final_score']} "
              f"(消息面: {stock['news_score']}, 技术面: {stock['technical_score']}) "
              f"当前价格: {stock['current_price']}")

    # 修复：返回完整的状态结构，包含之前的所有数据
    return {
        "news_list": state["news_list"],
        "analyzed_news": state["analyzed_news"],
        "stock_data": state["stock_data"],
        "technical_scores": state["technical_scores"],
        "final_scores": state["final_scores"],
        "sorted_stocks": sorted_stocks,
        "invalid_stocks": state.get("invalid_stocks", [])  # 保留无效股票列表
    }


def validate_stock_data_for_analysis(data: Dict) -> bool:
    """验证股票数据是否足够进行技术分析"""
    # 检查是否有历史价格数据 - 修正键名
    if 'historical_data' not in data or not data['historical_data']:
        print("缺少历史价格数据")
        print(f"数据结构: {data}")
        return False

    # 检查历史价格数据是否足够（至少需要几个数据点才能计算技术指标）
    historical_data = data['historical_data']
    print(
        f"历史价格数据类型: {type(historical_data)}, 长度: {len(historical_data) if hasattr(historical_data, '__len__') else 'N/A'}")

    # 获取实际的历史价格列表
    if 'historical_data' in data:
        historical_prices = data['historical_data'].get('open', {})
        if isinstance(historical_prices, dict) and len(historical_prices) < 3:
            print(f"历史价格数据不足，只有 {len(historical_prices)} 条记录")
            return False

    # 如果是pandas DataFrame，检查行数
    if hasattr(historical_data, '__len__') and len(historical_data) < 3:
        print(f"历史价格数据不足，只有 {len(historical_data)} 条记录")
        return False

    # 检查是否包含必要的价格列 - 修正列名
    if isinstance(historical_data, dict):
        required_columns = ['open', 'High', 'Low', 'Close']
        missing_columns = [col for col in required_columns if col not in historical_data]
        if missing_columns:
            print(f"缺少必要的价格列: {missing_columns}")
            return False

    return True


# 在文件末尾添加股票代码校验函数
def normalize_stock_code(stock_code: str) -> str:
    """规范化股票代码格式"""
    import re

    # 移除可能的多余字符
    cleaned_code = re.sub(r'[^\d\.A-Za-z]', '', stock_code.upper())

    # 处理没有后缀的情况
    if '.' not in cleaned_code and len(cleaned_code) == 6:
        # 根据股票代码前缀判断交易所
        if cleaned_code.startswith(('600', '601', '603', '605')):
            return cleaned_code + '.SH'  # 上海交易所
        elif cleaned_code.startswith(('000', '001', '002', '003')):
            return cleaned_code + '.SZ'  # 深圳交易所
        else:
            return None  # 无法确定交易所

    # 处理格式基本正确但大小写不一致的情况
    if '.' in cleaned_code:
        parts = cleaned_code.split('.')
        if len(parts) == 2 and len(parts[0]) == 6 and len(parts[1]) == 2:
            return parts[0] + '.' + parts[1].upper()

    return None


def is_valid_stock_code(stock_code: str) -> bool:
    """验证股票代码格式是否有效"""
    # 基本格式校验：数字.交易所代码
    import re
    pattern = r'^\d{6}\.[A-Z]{2}$'
    if not re.match(pattern, stock_code):
        return False

    # 验证交易所代码是否有效
    exchange = stock_code.split('.')[1]
    valid_exchanges = ['SZ', 'SH']  # 主要支持A股市场

    # 过滤掉300和688开头的股票（创业板和科创板）
    stock_prefix = stock_code.split('.')[0]
    if stock_prefix.startswith(('300', '688')):
        return False

    return exchange in valid_exchanges


def validate_stock_data(stock_code: str, data: Dict) -> bool:
    """验证返回的股票数据是否与代码匹配"""
    # 放宽验证条件：只要包含必要的技术指标数据就认为有效
    required_fields = ['technical_indicators']
    for field in required_fields:
        if field not in data:
            return False

    # 检查技术指标中是否有必要的数据
    tech_indicators = data.get('technical_indicators', {})
    if 'current_price' not in tech_indicators:
        return False

    return True


# 修改 validate_analysis_result 函数以使用配置
def validate_analysis_result(title: str, content: str, analysis_result: Dict) -> bool:
    """验证分析结果是否与新闻内容相关"""
    # 检查关键字段是否存在
    required_fields = ['sector', 'leading_stocks', 'score']
    for field in required_fields:
        if field not in analysis_result:
            return False

    # 检查分数范围是否合理
    if not (1 <= analysis_result.get('score', 5) <= 10):
        return False

    # 使用配置中的关键词进行验证
    sector = analysis_result.get('sector', '').lower()
    title_lower = title.lower()
    content_lower = content.lower()

    # 检查分析结果中的行业是否与新闻内容匹配
    for expected_sector, config in SECTOR_CONFIG.items():
        if expected_sector.lower() in sector:
            # 检查新闻内容中是否包含该行业的关键词
            content_has_keyword = any(keyword in content_lower for keyword in config['keywords'])
            title_has_keyword = any(keyword in title_lower for keyword in config['keywords'])

            if not (content_has_keyword or title_has_keyword):
                print(f"验证失败: 分析行业 '{sector}' 与新闻内容不匹配")
                return False
            break  # 找到匹配的行业就退出循环

    return True


# 修改 create_fallback_analysis 函数以使用配置
def create_fallback_analysis(title: str, content: str) -> Dict:
    """创建备用的分析结果"""
    # 尝试从内容和标题中提取股票代码
    stock_pattern = r'[0-9]{6}\.[A-Z]{2}'
    stocks = re.findall(stock_pattern, content)
    title_stocks = re.findall(stock_pattern, title)
    all_stocks = list(set(stocks + title_stocks))

    # 过滤掉300和688开头的股票
    all_stocks = [stock for stock in all_stocks if not stock.split('.')[0].startswith(('300', '688'))]

    # 如果没找到标准格式的股票代码，尝试从文本中提取可能的股票名称
    if not all_stocks:
        combined_text = title + ' ' + content
        for name, code in STOCK_NAME_MAPPING.items():
            if name in combined_text and not code.split('.')[0].startswith(('300', '688')):
                all_stocks.append(code)
                break

    # 使用配置进行行业分类
    content_lower = content.lower()
    title_lower = title.lower()
    combined_lower = content_lower + ' ' + title_lower

    matched_sector = "其他"
    for sector, config in SECTOR_CONFIG.items():
        if any(keyword in combined_lower for keyword in config['keywords']):
            matched_sector = sector
            # 如果没有找到股票，使用该行业的默认股票
            if not all_stocks:
                all_stocks.extend(config['default_stocks'])
            break

    return {
        "sector": matched_sector,
        "leading_stocks": all_stocks,
        "score": 5,
        "reasoning": f"备用分析: 基于关键词'{matched_sector}'匹配的简单分析，提取到{len(all_stocks)}只股票"
    }


# 构建工作流
def create_workflow():
    workflow = StateGraph(AnalysisState)

    # 添加节点
    workflow.add_node("get_news", get_news_node)
    workflow.add_node("analyze_news", analyze_news_node)
    workflow.add_node("get_stock_data", get_stock_data_node)
    workflow.add_node("technical_analysis", technical_analysis_node)
    workflow.add_node("comprehensive_scoring", comprehensive_scoring_node)
    workflow.add_node("sort_stocks", sort_stocks_node)

    # 设置边
    workflow.set_entry_point("get_news")
    workflow.add_edge("get_news", "analyze_news")
    workflow.add_edge("analyze_news", "get_stock_data")
    workflow.add_edge("get_stock_data", "technical_analysis")
    workflow.add_edge("technical_analysis", "comprehensive_scoring")
    workflow.add_edge("comprehensive_scoring", "sort_stocks")
    workflow.add_edge("sort_stocks", END)

    return workflow.compile()


# 运行工作流
if __name__ == "__main__":
    app = create_workflow()
    result = app.invoke({})
    print("工作流执行完成！")
