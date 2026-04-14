import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests
import sys

import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import json
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

# 控制是否使用SDK获取数据(True)或从CSV文件读取(False)
file_dir_path = f"/Users/bytedance/Downloads/stock_data/"
# =========================
# 运行配置（限流/重试/日志）
# =========================
# Tushare 常见限制：每分钟约 800 次（具体以账号权限和接口为准）
QPS_PER_MINUTE = 800
MIN_REQUEST_INTERVAL_SEC = 0  # 强制请求间隔（默认每请求 1 秒）
RETRY_BACKOFF_SECONDS = [10, 30, 60]  # 指数退避：1/2/3 次失败等待
ALERT_CONSECUTIVE_FAILURES = 3  # 连续失败 >=3 触发告警日志
TEST_MODE = False  # 测试模式：仅处理这些股票代码；置空可恢复全量
TEST_ONLY_TS_CODES = {"000601.SZ"}  # 测试模式：仅处理这些股票代码；置空可恢复全量

# 盘中实时能力说明：
# - Tushare Pro 提供分钟线等更高频数据接口，但很多接口为“采集入库后提供”，并非交易所级别的严格实时。
# - pro.daily 为日线接口，盘中时段可能尚未产出当日数据或存在延迟。
INTRADAY_MODE = "try_realtime_quotes"  # 可选: "daily_only" / "try_realtime_quotes"
AI_SCORE_ENABLED = True  # 是否对筛选结果做 AI 消息面打分
AI_NEWS_LOOKBACK_DAYS = 30
AI_MAX_NEWS_ITEMS = 5
AI_QWEN_MODEL = os.environ.get("QWEN_MODEL", "qwen-plus")
AI_QWEN_MODELS = [
    model.strip()
    for model in os.environ.get("QWEN_MODELS", AI_QWEN_MODEL).split(",")
    if model.strip()
]
AI_QWEN_API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
AI_QWEN_TIMEOUT_SEC = 30
AI_USE_QWEN_WEB_SEARCH = True  # 不再调用 tushare 新闻接口，改由千问联网检索近期公开信息

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("filter_stock_by_index")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    _fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _fh = RotatingFileHandler(
        os.path.join(LOG_DIR, "filter_stock.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    _fh.setFormatter(_fmt)
    _ch = logging.StreamHandler()
    _ch.setFormatter(_fmt)
    logger.addHandler(_fh)
    logger.addHandler(_ch)

# 设置 token（优先使用环境变量，避免硬编码）
_token_env = os.environ.get("TUSHARE_TOKEN")
if _token_env:
    ts.set_token(_token_env)
    logger.info("使用环境变量 TUSHARE_TOKEN 设置 tushare token")
else:
    # 保留原有硬编码 token 的兼容行为（建议迁移到环境变量）
    ts.set_token('qqpo836795038082a6484a0d1e43c54d3efc3efc8cd47131fd2d00ea')
    logger.warning("未设置环境变量 TUSHARE_TOKEN，当前使用脚本内 token（建议改为环境变量）")
# 使用tushare API获取增量数据
pro = ts.pro_api()

@dataclass
class DataFreshness:
    fetch_time: datetime
    data_may_delay: bool
    delay_reason: str = ""


class RateLimiter:
    """
    简单的分钟级限流器 + 可选请求间隔。
    - 严格遵循每分钟最多 N 次
    - 默认不做请求间隔等待；仅在配置 min_interval_sec > 0 时启用
    """
    def __init__(self, per_minute: int = QPS_PER_MINUTE, min_interval_sec: float = MIN_REQUEST_INTERVAL_SEC):
        self.per_minute = per_minute
        self.min_interval_sec = min_interval_sec
        self._lock = threading.Lock()
        self._window_start = time.time()
        self._count_in_window = 0
        self._last_request_ts: Optional[float] = None

    def acquire(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()

            # 可选请求间隔：默认关闭，只有显式配置 > 0 才启用
            if self.min_interval_sec > 0 and self._last_request_ts is not None:
                elapsed = now - self._last_request_ts
                if elapsed < self.min_interval_sec:
                    sleep_s = self.min_interval_sec - elapsed
                    logger.info(
                        "sleep cause=min_interval wait=%.2fs elapsed=%.2fs min_interval=%.2fs",
                        sleep_s,
                        elapsed,
                        self.min_interval_sec,
                    )
                    time.sleep(sleep_s)
                    now = time.time()

            # 分钟窗口重置
            if now - self._window_start >= 60:
                self._window_start = now
                self._count_in_window = 0

            # 如果达到每分钟上限，等待到窗口结束
            if self._count_in_window >= self.per_minute:
                wait_s = max(0.0, 60 - (now - self._window_start)) + 0.05
                logger.warning(
                    "sleep cause=minute_window_limit wait=%.2fs count_in_window=%s per_minute=%s",
                    wait_s,
                    self._count_in_window,
                    self.per_minute,
                )
                time.sleep(wait_s)
                now = time.time()
                self._window_start = now
                self._count_in_window = 0

            self._count_in_window += 1
            self._last_request_ts = now
            return {
                "window_start": self._window_start,
                "count_in_window": self._count_in_window,
                "per_minute": self.per_minute,
                "min_interval_sec": self.min_interval_sec,
                "request_ts": now,
            }


class TushareClient:
    def __init__(self, pro_api, limiter: Optional[RateLimiter] = None):
        self.pro = pro_api
        self.limiter = limiter or RateLimiter()
        self.total_calls = 0
        self.failed_calls = 0
        self.total_retries = 0
        self.consecutive_failures = 0

    @staticmethod
    def _is_rate_limited_error(exc: Exception) -> bool:
        msg = str(exc)
        # tushare/requests 的 429，或 tushare 常见提示文案
        keywords = ["429", "Too Many Requests", "每分钟", "频率", "freq", "访问限制", "limit"]
        return any(k in msg for k in keywords)

    def call(self, api_name: str, fn: Callable[..., Any], **kwargs) -> Tuple[Any, DataFreshness]:
        """
        带限流 + 重试 + 监控日志的统一调用入口。
        返回 (data, freshness)
        """
        self.total_calls += 1
        fetch_time = datetime.now()

        # 限流前记录一次“预计请求”信息
        lim = self.limiter.acquire()
        logger.info(
            "tushare_call api=%s count_in_window=%s/%s min_interval=%.2fs kwargs=%s",
            api_name,
            lim["count_in_window"],
            lim["per_minute"],
            lim["min_interval_sec"],
            {k: kwargs.get(k) for k in sorted(kwargs.keys())},
        )

        last_exc: Optional[Exception] = None
        for attempt in range(len(RETRY_BACKOFF_SECONDS) + 1):
            try:
                data = fn(**kwargs)
                self.consecutive_failures = 0

                freshness = DataFreshness(
                    fetch_time=fetch_time,
                    data_may_delay=False,
                    delay_reason="",
                )
                return data, freshness
            except Exception as e:
                last_exc = e
                self.failed_calls += 1
                self.consecutive_failures += 1

                # 告警：连续失败
                if self.consecutive_failures >= ALERT_CONSECUTIVE_FAILURES:
                    logger.error(
                        "ALERT 连续失败达到阈值: consecutive_failures=%s api=%s last_error=%s",
                        self.consecutive_failures,
                        api_name,
                        str(e),
                    )

                # 是否需要重试
                if attempt >= len(RETRY_BACKOFF_SECONDS):
                    break

                wait_s = RETRY_BACKOFF_SECONDS[attempt]
                self.total_retries += 1
                if self._is_rate_limited_error(e):
                    logger.warning(
                        "tushare_retry rate_limited api=%s attempt=%s wait=%ss error=%s",
                        api_name,
                        attempt + 1,
                        wait_s,
                        str(e),
                    )
                else:
                    logger.warning(
                        "tushare_retry api=%s attempt=%s wait=%ss error=%s",
                        api_name,
                        attempt + 1,
                        wait_s,
                        str(e),
                    )
                logger.warning(
                    "sleep cause=retry_backoff api=%s attempt=%s wait=%ss",
                    api_name,
                    attempt + 1,
                    wait_s,
                )
                time.sleep(wait_s)

        # 最终失败：返回异常
        raise last_exc if last_exc else RuntimeError(f"tushare_call_failed api={api_name}")


ts_client = TushareClient(pro_api=pro)

def _parse_trade_date_series_to_yyyy_mm_dd(series: pd.Series) -> pd.Series:
    """
    兼容多种 trade_date 形态：
    - 'YYYY-MM-DD'
    - 'YYYYMMDD'
    - 8 位数字（20001107），pandas 默认会当成 ns 导致日期错误，所以必须按 YYYYMMDD 解析
    - 带时间戳字符串（'2019-07-22 00:00:00.000000000'）
    """
    s = series.astype(str).str.strip()
    # 先尝试解析 8 位数字日期
    is_yyyymmdd = s.str.fullmatch(r"\d{8}", na=False)
    out = pd.Series([pd.NaT] * len(s), index=s.index)
    if is_yyyymmdd.any():
        out.loc[is_yyyymmdd] = pd.to_datetime(s.loc[is_yyyymmdd], format="%Y%m%d", errors="coerce")
    # 其余按通用规则解析（兼容 YYYY-MM-DD 和带时间的字符串）
    if (~is_yyyymmdd).any():
        out.loc[~is_yyyymmdd] = pd.to_datetime(s.loc[~is_yyyymmdd], errors="coerce")
    return out.dt.strftime("%Y-%m-%d")


def _normalize_trade_date_to_yyyy_mm_dd(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "trade_date" in df.columns:
        # pro.daily 通常返回 YYYYMMDD，这里统一成 YYYY-MM-DD，便于后续排序/拼接
        df = df.copy()
        df["trade_date"] = _parse_trade_date_series_to_yyyy_mm_dd(df["trade_date"])
        df = df.dropna(subset=["trade_date"])
    return df


def validate_ohlcv_df(df: pd.DataFrame, ts_code: str, min_rows: int = 1) -> pd.DataFrame:
    """
    数据有效性验证：
    - 必要列存在
    - trade_date 可解析且不为空
    - 价格/成交量非负
    - 去重、按日期升序
    """
    if df is None or df.empty:
        raise ValueError(f"空数据: ts_code={ts_code}")

    required_cols = ["trade_date", "open", "high", "low", "close", "vol"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"缺少必要列: ts_code={ts_code} missing={missing} columns={list(df.columns)}")

    df = _normalize_trade_date_to_yyyy_mm_dd(df)
    df = df.drop_duplicates(subset=["trade_date"], keep="last")
    df = df.sort_values("trade_date").reset_index(drop=True)

    # 非法值处理：价格/成交量为负，直接剔除
    for col in ["open", "high", "low", "close", "vol"]:
        df = df[pd.to_numeric(df[col], errors="coerce").notna()]
        df = df[df[col] >= 0]

    # high/low 关系校验（如果不满足，标记并剔除异常行）
    df = df[df["high"] >= df["low"]]

    if len(df) < min_rows:
        raise ValueError(f"数据量不足: ts_code={ts_code} rows={len(df)} min_rows={min_rows}")
    return df


def try_get_realtime_quote_row(ts_code: str, name: str) -> Optional[pd.DataFrame]:
    """
    旧版 tushare 的实时行情兜底方案。
    说明：
    - 仅作为盘中补充数据源，字段口径可能与 pro.daily 存在差异。
    - 如果 SDK / 网络 / 权限不支持，则返回 None。
    """
    short_code = ts_code.split(".")[0]
    try:
        realtime_df = ts.get_realtime_quotes(short_code)
        if realtime_df is None or realtime_df.empty:
            logger.warning("realtime_quotes 空结果 ts_code=%s", ts_code)
            return None

        row = realtime_df.iloc[0]
        trade_date_raw = str(row.get("date", "")).strip()
        trade_time_raw = str(row.get("time", "")).strip()
        trade_date = pd.to_datetime(trade_date_raw, errors="coerce")
        if pd.isna(trade_date):
            logger.warning("realtime_quotes 日期解析失败 ts_code=%s raw_date=%s", ts_code, trade_date_raw)
            return None

        def _to_float(value: Any) -> float:
            return float(value) if str(value).strip() not in {"", "None", "nan"} else 0.0

        today_row = pd.DataFrame([{
            "ts_code": ts_code,
            "trade_date": trade_date.strftime("%Y-%m-%d"),
            "open": _to_float(row.get("open")),
            "high": _to_float(row.get("high")),
            "low": _to_float(row.get("low")),
            "close": _to_float(row.get("price")),
            "pre_close": _to_float(row.get("pre_close")),
            "vol": _to_float(row.get("volume")),
            "amount": _to_float(row.get("amount")),
            "name": row.get("name", name),
            "realtime_time": trade_time_raw,
            "data_source": "tushare_realtime_quotes",
        }])
        logger.info("realtime_quotes 兜底成功 ts_code=%s trade_date=%s time=%s", ts_code, today_row.iloc[0]["trade_date"], trade_time_raw)
        return today_row
    except Exception as e:
        logger.warning("realtime_quotes 兜底失败 ts_code=%s err=%s", ts_code, str(e))
        return None


def _to_jsonable(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _sort_by_latest(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    for col in ["end_date", "ann_date", "trade_date", "pub_time", "datetime", "date"]:
        if col in df.columns:
            return df.sort_values(col, ascending=False).reset_index(drop=True)
    return df.reset_index(drop=True)


def _df_to_records(df: Optional[pd.DataFrame], limit: int = 3, preferred_fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df = _sort_by_latest(df)
    fields = preferred_fields if preferred_fields else list(df.columns[:12])
    fields = [field for field in fields if field in df.columns]
    records: List[Dict[str, Any]] = []
    for _, row in df.head(limit).iterrows():
        record = {
            field: _to_jsonable(row[field])
            for field in fields
            if field in row.index and not pd.isna(row[field])
        }
        if record:
            records.append(record)
    return records


def fetch_stock_research_context(ts_code: str, name: str) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "ts_code": ts_code,
        "name": name,
        "fina_indicator": [],
        "income": [],
        "forecast": [],
        "express": [],
        "news": [],
        "news_fetch_mode": "qwen_web_search",
    }

    api_specs = [
        ("fina_indicator", pro.fina_indicator, {"ts_code": ts_code}, 1, ["ts_code", "ann_date", "end_date", "roe", "roa", "grossprofit_margin", "netprofit_margin", "debt_to_assets", "op_income", "eps"]),
        ("income", pro.income, {"ts_code": ts_code}, 1, ["ts_code", "ann_date", "end_date", "revenue", "operate_profit", "n_income", "basic_eps", "total_profit"]),
        ("forecast", pro.forecast, {"ts_code": ts_code}, 2, ["ts_code", "ann_date", "end_date", "type", "p_change_min", "p_change_max", "net_profit_min", "net_profit_max", "summary"]),
        ("express", pro.express, {"ts_code": ts_code}, 1, ["ts_code", "ann_date", "end_date", "revenue", "operate_profit", "n_income", "eps", "diluted_eps", "roe"]),
    ]

    for api_name, api_fn, kwargs, limit, fields in api_specs:
        try:
            df, _ = ts_client.call(api_name, api_fn, **kwargs)
            context[api_name] = _df_to_records(df, limit=limit, preferred_fields=fields)
        except Exception as e:
            logger.warning("ai_context_fetch_failed api=%s ts_code=%s err=%s", api_name, ts_code, str(e))

    return context


def _extract_json_block(text: str) -> Dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if stripped.startswith("json"):
            stripped = stripped[4:].strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    candidate = stripped[start:end + 1] if start != -1 and end != -1 and end > start else stripped
    return json.loads(candidate)


def call_qwen_for_stock_score(stock: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    # api_key = os.environ.get("DASHSCOPE_API_KEY")
    api_key = "sk-16e71336e06e4c3b9013f1434a38a4a6"
    if not api_key:
        return {
            "score": None,
            "sentiment": "未评分",
            "summary": "未设置 DASHSCOPE_API_KEY，已跳过 AI 消息面评分",
            "drivers": [],
            "risks": [],
        }

    prompt = {
        "stock": {
            "ts_code": stock["ts_code"],
            "name": stock["name"],
            "trade_date": stock["latest_data"].get("trade_date", "N/A"),
            "close": _to_jsonable(stock["latest_data"].get("close")),
            "ma60": _to_jsonable(stock["latest_data"].get("ma60")),
            "k": _to_jsonable(stock["latest_data"].get("k")),
            "d": _to_jsonable(stock["latest_data"].get("d")),
            "j": _to_jsonable(stock["latest_data"].get("j")),
        },
        "research_context": context,
        "task": (
            f"请你作为A股研究员，结合公司最新财报摘要，并联网检索最近{AI_NEWS_LOOKBACK_DAYS}天与该股票、公司主体、核心业务相关的公开新闻、公告和市场信息，"
            "对该股票的消息面/基本面做0到100的打分。"
            "请重点考虑盈利能力、业绩预告/快报、最新新闻催化与风险。"
            "只返回JSON，格式为："
            '{"score": 0, "sentiment": "积极/中性/偏空", "summary": "一句话总结", '
            '"drivers": ["利好1", "利好2"], "risks": ["风险1", "风险2"]}'
        ),
    }
    base_payload = {
        "messages": [
            {"role": "system", "content": "你是严谨的A股研究助手，只输出合法JSON，不要输出额外解释。"},
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
        ],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    last_exc: Optional[Exception] = None

    for model_name in AI_QWEN_MODELS or [AI_QWEN_MODEL]:
        payload = {"model": model_name, **base_payload}
        response = None
        if AI_USE_QWEN_WEB_SEARCH:
            search_payload = dict(payload)
            search_payload["enable_search"] = True
            try:
                logger.info("qwen_try model=%s ts_code=%s enable_search=true", model_name, stock["ts_code"])
                response = requests.post(
                    AI_QWEN_API_URL,
                    headers=headers,
                    json=search_payload,
                    timeout=AI_QWEN_TIMEOUT_SEC,
                )
                response.raise_for_status()
            except Exception as e:
                last_exc = e
                logger.warning("qwen_web_search_failed model=%s ts_code=%s err=%s", model_name, stock["ts_code"], str(e))
                response = None
        if response is None:
            try:
                logger.info("qwen_try model=%s ts_code=%s enable_search=false", model_name, stock["ts_code"])
                response = requests.post(
                    AI_QWEN_API_URL,
                    headers=headers,
                    json=payload,
                    timeout=AI_QWEN_TIMEOUT_SEC,
                )
                response.raise_for_status()
            except Exception as e:
                last_exc = e
                logger.warning("qwen_model_failed model=%s ts_code=%s err=%s", model_name, stock["ts_code"], str(e))
                continue

        content = response.json()["choices"][0]["message"]["content"]
        parsed = _extract_json_block(content)
        logger.info("qwen_model_succeeded model=%s ts_code=%s", model_name, stock["ts_code"])
        return {
            "score": parsed.get("score"),
            "sentiment": parsed.get("sentiment", "中性"),
            "summary": parsed.get("summary", ""),
            "drivers": parsed.get("drivers", []),
            "risks": parsed.get("risks", []),
            "model": model_name,
        }

    raise last_exc if last_exc else RuntimeError(f"qwen_all_models_failed ts_code={stock['ts_code']}")


def enrich_selected_stocks_with_ai(selected_stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not AI_SCORE_ENABLED or not selected_stocks:
        return selected_stocks

    logger.info("开始执行 AI 消息面评分 count=%s models=%s", len(selected_stocks), AI_QWEN_MODELS)
    enriched: List[Dict[str, Any]] = []
    for stock in selected_stocks:
        stock_copy = dict(stock)
        try:
            context = fetch_stock_research_context(stock["ts_code"], stock["name"])
            ai_result = call_qwen_for_stock_score(stock_copy, context)
            stock_copy["ai_context"] = context
            stock_copy["ai_score"] = ai_result.get("score")
            stock_copy["ai_sentiment"] = ai_result.get("sentiment")
            stock_copy["ai_summary"] = ai_result.get("summary")
            stock_copy["ai_drivers"] = ai_result.get("drivers", [])
            stock_copy["ai_risks"] = ai_result.get("risks", [])
            stock_copy["ai_model"] = ai_result.get("model")
        except Exception as e:
            logger.warning("ai_score_failed ts_code=%s err=%s", stock["ts_code"], str(e))
            stock_copy["ai_score"] = None
            stock_copy["ai_sentiment"] = "评分失败"
            stock_copy["ai_summary"] = f"AI评分失败: {str(e)}"
            stock_copy["ai_drivers"] = []
            stock_copy["ai_risks"] = []
        enriched.append(stock_copy)
    return enriched


def resolve_stock_name(ts_code: str, fallback_name: Optional[str] = None) -> str:
    if fallback_name:
        return fallback_name
    try:
        stocks = get_stock_list()
        matched = stocks[stocks["ts_code"] == ts_code]
        if not matched.empty:
            return str(matched.iloc[0]["name"])
    except Exception as e:
        logger.warning("resolve_stock_name_failed ts_code=%s err=%s", ts_code, str(e))
    return fallback_name or ts_code


def score_single_stock_with_ai(ts_code: str, name: Optional[str] = None) -> Dict[str, Any]:
    """
    单独执行某只股票的 AI 消息面打分。
    优先读取本地最新 CSV 中的技术面快照，再抓取财报上下文并调用千问评分。
    """
    resolved_name = resolve_stock_name(ts_code, fallback_name=name)
    df = load_stock_data_from_csv(ts_code, resolved_name)
    if df is None or df.empty:
        raise ValueError(f"找不到股票 {ts_code} ({resolved_name}) 的本地数据文件，无法执行 AI 打分")

    df = validate_ohlcv_df(df, ts_code=ts_code, min_rows=1)
    df = _normalize_trade_date_to_yyyy_mm_dd(df)
    df = df.sort_values("trade_date").reset_index(drop=True)
    latest_data = df.iloc[-1]
    resolved_name = str(latest_data.get("name", resolved_name) or resolved_name)

    stock = {
        "ts_code": ts_code,
        "name": resolved_name,
        "latest_data": latest_data,
        "data_fetch_time": getattr(df, "attrs", {}).get("fetch_time"),
        "data_may_delay": bool(getattr(df, "attrs", {}).get("data_may_delay", False)),
        "delay_reason": getattr(df, "attrs", {}).get("delay_reason", ""),
    }
    context = fetch_stock_research_context(ts_code, resolved_name)
    ai_result = call_qwen_for_stock_score(stock, context)
    return {
        **stock,
        "ai_context": context,
        "ai_score": ai_result.get("score"),
        "ai_sentiment": ai_result.get("sentiment"),
        "ai_summary": ai_result.get("summary"),
        "ai_drivers": ai_result.get("drivers", []),
        "ai_risks": ai_result.get("risks", []),
    }


def print_single_stock_ai_score(result: Dict[str, Any]) -> None:
    latest_data = result["latest_data"]
    print("\n单股 AI 打分结果")
    print("=" * 80)
    print(f"股票: {result['ts_code']} {result['name']}")
    print(f"交易日期: {latest_data.get('trade_date', 'N/A')}")
    if "close" in latest_data:
        print(f"最新收盘价: {float(latest_data['close']):.2f}")
    if "ma60" in latest_data and pd.notna(latest_data["ma60"]):
        print(f"60日均线: {float(latest_data['ma60']):.2f}")
    if all(k in latest_data for k in ["k", "d", "j"]):
        print(f"KDJ指标 - K: {float(latest_data['k']):.2f}, D: {float(latest_data['d']):.2f}, J: {float(latest_data['j']):.2f}")
    if result.get("ai_model"):
        print(f"AI模型: {result['ai_model']}")
    print(f"AI消息面评分: {result.get('ai_score', 'N/A')} ({result.get('ai_sentiment', 'N/A')})")
    print(f"AI评分摘要: {result.get('ai_summary', 'N/A')}")
    if result.get("ai_drivers"):
        print(f"AI利好因素: {'; '.join(result['ai_drivers'])}")
    if result.get("ai_risks"):
        print(f"AI风险因素: {'; '.join(result['ai_risks'])}")
    print("=" * 80)


class StockSelectionStrategy:
    """
    股票选股策略基类
    """
    def apply(self, df):
        """
        应用选股策略
        :param df: 包含股票数据的DataFrame
        :return: 是否符合策略
        """
        raise NotImplementedError("子类必须实现apply方法")

class MACDGoldenCrossStrategy(StockSelectionStrategy):
    """
    MACD金叉策略：检测MACD即将金叉或已金叉的股票
    主要考虑：
    1. 最近三天的MACD趋势（排除下降趋势）
    2. DIF和DEA距离零轴的比例（相对于三天前）
    3. 排除三天内比例变化不大的假金叉
    """
    def __init__(self, days=2, min_change_threshold=0.02):
        """
        :param days: 检查最近几天的数据
        :param min_change_threshold: 最小变化阈值，用于排除假金叉
        """
        self.days = days
        self.min_change_threshold = min_change_threshold

    def apply(self, df):
        """
        检查MACD金叉信号
        """
        print("MACD金叉策略开始")

        if len(df) < self.days + 1:  # 需要至少days+1天数据来比较
            print(f"MACD策略数据不足: 需要 {self.days + 1} 天数据，但只有 {len(df)} 天")
            return False

        recent_data = df.tail(self.days + 1).reset_index(drop=True)

        # 检查必要的MACD列是否存在
        required_cols = ['dif', 'dea', 'macd']
        missing_cols = [col for col in required_cols if col not in recent_data.columns]
        if missing_cols:
            print(f"MACD策略错误: 缺少列 {missing_cols}")
            print(f"现有列名: {list(recent_data.columns)}")
            return False

        print(f"\n=== MACD金叉分析开始 ===")
        print(f"检查 {self.days} 个交易日的MACD指标")

        # 获取最近几天的DIF和DEA值
        dif_recent = recent_data['dif'].tail(self.days).values
        dea_recent = recent_data['dea'].tail(self.days).values
        macd_recent = recent_data['macd'].tail(self.days).values

        # 检查最近三天的DIF和DEA趋势
        dif_trend_up = all(dif_recent[i] >= dif_recent[i-1] for i in range(1, len(dif_recent)))
        dea_trend_up = all(dea_recent[i] >= dea_recent[i-1] for i in range(1, len(dea_recent)))
        macd_trend_up = all(macd_recent[i] >= macd_recent[i-1] for i in range(1, len(macd_recent)))

        # 检查DIF和DEA是否在向零轴靠近（绝对值减小）
        dif_abs_decrease = abs(dif_recent[-1]) < abs(dif_recent[0])
        dea_abs_decrease = abs(dea_recent[-1]) < abs(dea_recent[0])

        # 检查是否存在金叉或即将金叉的情况
        # 金叉条件：DIF上穿DEA，或两者接近（差值小于阈值）
        cross_condition = (dif_recent[-1] >= dea_recent[-1])  # 当前DIF >= DEA
        approaching_condition = abs(dif_recent[-1] - dea_recent[-1]) < abs(dif_recent[0] - dea_recent[0])  # 两者差距缩小

        # # 计算DIF和DEA相对三天前的变化幅度
        # dif_change_pct = abs((dif_recent[-1] - dif_recent[0]) / abs(dif_recent[0])) if abs(dif_recent[0]) > 0 else float('inf')
        # dea_change_pct = abs((dea_recent[-1] - dea_recent[0]) / abs(dea_recent[0])) if abs(dea_recent[0]) > 0 else float('inf')
        #
        # # 检查变化幅度是否大于最小阈值（排除假金叉）
        # significant_change = max(dif_change_pct, dea_change_pct) > self.min_change_threshold

        # 检查整体趋势（排除下降趋势）
        # overall_trend_positive = (dif_recent[-1] > dif_recent[0]) and (dea_recent[-1] > dea_recent[0])

        # print(f"DIF最近{self.days}天趋势: {'上升' if dif_trend_up else '非上升'}")
        print(f"MACD最近{self.days}天趋势: {'上升' if macd_trend_up else '非上升'}")
        # print(f"DEA最近{self.days}天趋势: {'上升' if dea_trend_up else '非上升'}")
        # print(f"DIF向零轴靠近: {'是' if dif_abs_decrease else '否'}")
        # print(f"DEA向零轴靠近: {'是' if dea_abs_decrease else '否'}")
        # print(f"金叉条件满足: {'是' if cross_condition else '否'}")
        # print(f"接近条件满足: {'是' if approaching_condition else '否'}")
        # print(f"变化幅度显著: {'是' if significant_change else '否'}")
        # print(f"整体趋势向上: {'是' if overall_trend_positive else '否'}")

        # 综合判断条件
        result = (
            # (cross_condition or approaching_condition) and  # 金叉或接近金叉
            # # significant_change and  # 变化幅度显著
            # (dif_trend_up or dea_trend_up) and  # 至少一个指标上升
            macd_trend_up  # MACD上升
            # overall_trend_positive  # 整体趋势向上
        )

        print(f"MACD金叉策略最终结果: {'通过' if result else '未通过'}")
        print(f"=== MACD金叉分析结束 ===\n")

        return result


class PriceAboveMaStrategy(StockSelectionStrategy):
    """
    收盘价在N日均线上方的策略
    """
    def __init__(self, ma_window=60, n_days=50):
        self.ma_window = ma_window
        self.n_days = n_days

    def apply(self, df):
        """
        检查最近n个交易日收盘价是否都在ma_window日均线上方
        """
        # if len(df) < self.n_days + self.ma_window:
        #     print(f"数据不足: 需要 {self.n_days + self.ma_window} 天数据，但只有 {len(df)} 天")
        #     return False

        recent_data = df.tail(self.n_days)
        ma_col = f'ma{self.ma_window}'

        if ma_col not in recent_data.columns:
            print(f"错误: {ma_col} 列不存在")
            print(f"现有列名: {list(recent_data.columns)}")
            return False

        # 添加详细调试信息
        print(f"\n=== 股票分析开始 ===")
        # print(f"检查 {self.n_days} 个交易日的数据")
        # print(f"均线窗口: {self.ma_window}")
        # print(f"数据长度: {len(recent_data)}")

        comparison_series = recent_data['close'] >= recent_data[ma_col]
        # print(f"收盘价与{self.ma_window}日均线比较结果:")
        for idx, (date, row) in enumerate(recent_data.iterrows()):
            close_val = row['close']
            ma_val = row[ma_col] if pd.notna(row[ma_col]) else 'NaN'
            is_above = close_val >= row[ma_col] if pd.notna(row[ma_col]) else False
            # print(f"  {idx+1:2d}. 日期: {row.get('trade_date', 'N/A')}, 收盘价: {close_val:.2f}, MA{self.ma_window}: {ma_val if isinstance(ma_val, str) or pd.isna(ma_val) else f'{ma_val:.2f}'}, 是否在均线上方: {is_above}")

        result = comparison_series.all()
        print(f"均线最终结果: {'通过' if result else '未通过'}")
        print(f"=== 均线分析结束 ===\n")

        return result

class KDJStrategy(StockSelectionStrategy):
    """
    KDJ指标筛选策略
    """
    def __init__(self, n_days=5):
        self.n_days = n_days

    def apply(self, df):
        """
        检查最近n个交易日的KDJ指标是否满足条件
        条件：K线在D线上方，且J值在合理范围内
        """
        print("KDJ策略开始")
        if len(df) < self.n_days:
            print(f"KDJ策略数据不足: 需要 {self.n_days} 天数据，但只有 {len(df)} 天")
            return False

        recent_data = df.tail(self.n_days)

        # 检查必要的KDJ列是否存在
        required_cols = ['k', 'd', 'j']
        missing_cols = [col for col in required_cols if col not in recent_data.columns]
        if missing_cols:
            print(f"KDJ策略错误: 缺少列 {missing_cols}")
            print(f"现有列名: {list(recent_data.columns)}")
            return False

        print(f"\n=== KDJ指标分析开始 ===")
        print(f"检查 {self.n_days} 个交易日的KDJ指标")

        # 检查每个交易日的KDJ条件：K > D 且 J在合理范围(通常0-100之间)
        conditions_met = []
        for idx, (date, row) in enumerate(recent_data.iterrows()):
            k_val = row['k']
            d_val = row['d']
            j_val = row['j']

            # K线在D线上方，且J值在合理范围
            condition = (k_val >= d_val) and (0 <= j_val <= 40)
            conditions_met.append(condition)

            print(f"  {idx+1:2d}. 日期: {row.get('trade_date', 'N/A')}, K: {k_val:.2f}, D: {d_val:.2f}, J: {j_val:.2f}, 条件满足: {condition}")

        result = all(conditions_met)
        print(f"KDJ策略最终结果: {'通过' if result else '未通过'}")
        print(f"=== KDJ指标分析结束 ===\n")

        return result

def get_stock_list():
    """
    获取A股股票列表，并过滤掉ST股票和非600、000开头的股票
    """
    # ts.set_token('qqpo836795038082a6484a0d1e43c54d3efc3efc8cd47131fd2d00ea')
    # pro = ts.pro_api()

    # 获取股票基本信息（走统一的限流/重试/日志）
    stock_basic, freshness = ts_client.call(
        "stock_basic",
        pro.stock_basic,
        exchange="",
        list_status="L",
        fields="ts_code,name",
    )
    logger.info(
        "stock_basic fetched_at=%s rows=%s may_delay=%s",
        freshness.fetch_time.strftime("%Y-%m-%d %H:%M:%S"),
        0 if stock_basic is None else len(stock_basic),
        freshness.data_may_delay,
    )

    # 过滤ST股票（名称中包含ST）
    non_st_stocks = stock_basic[~stock_basic['name'].str.contains('ST')]

    # 筛选以60或00开头的股票
    filtered_stocks = non_st_stocks[
        (non_st_stocks['ts_code'].str.startswith('60')) |
        (non_st_stocks['ts_code'].str.startswith('00'))
    ]

    # 测试模式：只保留指定股票，便于验证整条链路
    if TEST_MODE:
        filtered_stocks = filtered_stocks[filtered_stocks["ts_code"].isin(TEST_ONLY_TS_CODES)]
        logger.info("测试模式启用，仅处理股票: %s", sorted(TEST_ONLY_TS_CODES))


    return filtered_stocks

def calculate_ma(data, windows=None):
    """
    计算多个周期的移动平均线
    """
    if windows is None:
        windows = [5, 10, 20, 30, 60]
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
    filename = file_dir_path+f"stock_data_{ts_code}_{datetime.now().strftime('%Y%m%d')}.csv"
    # 保持 CSV schema 不变（不额外添加列），采集时间/延迟信息写入旁路 meta 文件
    df.to_csv(filename, index=False)
    meta_path = filename + ".meta.json"
    fetch_time = df.attrs.get("fetch_time")
    meta = {
        "ts_code": ts_code,
        "name": name,
        "csv_path": filename,
        "fetch_time": fetch_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(fetch_time, datetime) else None,
        "data_may_delay": bool(df.attrs.get("data_may_delay", False)),
        "delay_reason": df.attrs.get("delay_reason", ""),
    }
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("写入 meta 文件失败 path=%s err=%s", meta_path, str(e))
    print(f"股票 {ts_code} ({name}) 的数据已保存到 {filename}")

def load_stock_data_from_csv(ts_code, name):
    """
    从CSV文件加载股票数据
    """
    # 查找最新的数据文件，而不是限定特定日期
    import glob
    import os
    pattern = file_dir_path+f"stock_data_{ts_code}_*.csv"
    files = glob.glob(pattern)

    if not files:
        print(f"找不到股票 {ts_code} ({name}) 的数据文件")
        return None

    # 选择最新文件
    latest_file = max(files, key=os.path.getctime)
    try:
        df = pd.read_csv(latest_file)
        # 读取旁路 meta 文件（如果存在）
        meta_path = latest_file + ".meta.json"
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                ft = meta.get("fetch_time")
                parsed_ft = pd.to_datetime(ft, errors="coerce") if ft else pd.NaT
                if pd.notna(parsed_ft):
                    df.attrs["fetch_time"] = parsed_ft.to_pydatetime()
                df.attrs["data_may_delay"] = bool(meta.get("data_may_delay", False))
                df.attrs["delay_reason"] = meta.get("delay_reason", "")
            except Exception as e:
                logger.warning("读取 meta 文件失败 path=%s err=%s", meta_path, str(e))
        print(f"从文件 {latest_file} 加载股票 {ts_code} ({name}) 的数据")
        return df
    except FileNotFoundError:
        print(f"文件 {latest_file} 不存在")
        return None

def get_incremental_data(ts_code, name, start_date=None):
    """
    获取指定日期之后的增量数据
    """
    # 先尝试从本地CSV加载已有数据
    existing_df = load_stock_data_from_csv(ts_code, name)

    if existing_df is not None and not existing_df.empty:
        existing_df = _normalize_trade_date_to_yyyy_mm_dd(existing_df)
        # 确保数据按日期排序后再获取最大日期
        existing_df = existing_df.sort_values('trade_date').reset_index(drop=True)
        # 检查trade_date的类型，如果是字符串需要先转换为datetime
        last_date_raw = existing_df['trade_date'].max()
        print(f"已有数据最后日期: {last_date_raw}")
        # 检查trade_date的类型，如果是字符串需要先转换为datetime
        last_date_raw = existing_df['trade_date'].max()
        last_date = last_date_raw
        # 设置增量数据获取的起始日期（从已有数据的下一天开始）
        last_datetime = datetime.strptime(last_date, '%Y-%m-%d')
        incremental_start_date = (last_datetime + timedelta(days=1)).strftime('%Y%m%d')
        end_date = datetime.now().strftime('%Y%m%d')
        if incremental_start_date > end_date:
            print(f"无需更新: 增量起始日期 {incremental_start_date} 已超过当前日期 {end_date}")
            return None
    else:
        # 如果没有现有数据，则使用默认日期
        # start_date = (datetime.now() - timedelta(days=600)).strftime('%Y%m%d')
        incremental_start_date = None
        print(f"无现有数据")

    # 获取当前日期作为结束日期
    end_date = datetime.now().strftime('%Y-%m-%d')

    # # 设置token
    # ts.set_token('qqpo836795038082a6484a0d1e43c54d3efc3efc8cd47131fd2d00ea')
    # # 使用tushare API获取增量数据
    # pro = ts.pro_api()
    try:
        # pro.daily 的日期参数通常是 YYYYMMDD
        end_date = datetime.now().strftime("%Y%m%d")
        incremental_df, freshness = ts_client.call(
            "daily",
            pro.daily,
            ts_code=ts_code,
            start_date=incremental_start_date,
            end_date=end_date,
        )
        print(f"获取到 {len(incremental_df)} 条增量数据: {incremental_start_date} 到 {end_date}")

        # 盘中数据说明与处理：
        # pro.daily 是日线接口，盘中可能拿不到“当日”数据或存在延迟；因此记录采集时间并在日志中标注风险。
        current_time = datetime.now()
        today_yyyymmdd = current_time.strftime("%Y%m%d")
        today_yyyy_mm_dd = current_time.strftime("%Y-%m-%d")
        data_may_delay = False
        delay_reason = ""
        normalized_incremental_df = _normalize_trade_date_to_yyyy_mm_dd(incremental_df)
        has_today_data = (
            normalized_incremental_df is not None
            and not normalized_incremental_df.empty
            and "trade_date" in normalized_incremental_df.columns
            and today_yyyy_mm_dd in set(normalized_incremental_df["trade_date"].astype(str).tolist())
        )

        if current_time.hour < 15:
            # 交易时段更可能出现“当日尚未产出”或延迟
            data_may_delay = True
            delay_reason = "盘中时段，pro.daily 为日线数据，可能未包含当日最新数据或存在延迟"
            logger.warning(
                "intraday_notice ts_code=%s fetched_at=%s reason=%s",
                ts_code,
                freshness.fetch_time.strftime("%Y-%m-%d %H:%M:%S"),
                delay_reason,
            )

        # 如果首个 daily 结果已经包含当日数据，就不再额外查一次当日数据
        if not has_today_data:
            if current_time.hour < 15:
                # 盘中不再额外调用 daily_trade_date，优先走实时兜底，减少一次高概率无效的 daily 请求
                data_may_delay = True
                delay_reason = delay_reason or "盘中 pro.daily 未返回当日数据（可能尚未更新或有延迟）"
                logger.info(
                    "skip daily_trade_date in intraday ts_code=%s trade_date=%s mode=%s",
                    ts_code,
                    today_yyyymmdd,
                    INTRADAY_MODE,
                )
                if INTRADAY_MODE == "try_realtime_quotes":
                    realtime_row = try_get_realtime_quote_row(ts_code, name)
                    if realtime_row is not None and not realtime_row.empty:
                        incremental_df = pd.concat([incremental_df, realtime_row], ignore_index=True)
                        data_may_delay = True
                        delay_reason = "盘中 pro.daily 未返回当日数据，已使用 tushare 实时行情接口作为补充"
            else:
                try:
                    today_data, _ = ts_client.call(
                        "daily_trade_date",
                        pro.daily,
                        ts_code=ts_code,
                        trade_date=today_yyyymmdd,
                    )
                    if today_data is not None and not today_data.empty:
                        incremental_df = pd.concat([incremental_df, today_data], ignore_index=True)
                        logger.info("追加到日线当日数据 ts_code=%s trade_date=%s", ts_code, today_yyyymmdd)
                except Exception as e:
                    logger.warning("获取当日日线数据失败 ts_code=%s trade_date=%s err=%s", ts_code, today_yyyymmdd, str(e))
        if not incremental_df.empty:
            # 合并现有数据和增量数据
            if existing_df is not None and not existing_df.empty:
                # 确保增量数据的时间格式一致
                incremental_df = _normalize_trade_date_to_yyyy_mm_dd(incremental_df)
                incremental_df = incremental_df.sort_values('trade_date').reset_index(drop=True)
                combined_df = pd.concat([existing_df, incremental_df], ignore_index=True)

                # 重新计算所有技术指标
                combined_df = calculate_kdj(combined_df)
                combined_df = calculate_macd(combined_df)
                combined_df = calculate_ma(combined_df, windows=[5, 10, 20, 30, 60])

                combined_df.reset_index(drop=True, inplace=True)
            else:
                # 新文件trade_date格式修正为%Y-%m-%d
                incremental_df = _normalize_trade_date_to_yyyy_mm_dd(incremental_df)
                # 按时间正序排列
                incremental_df = incremental_df.sort_values('trade_date').reset_index(drop=True)
                # 对于全新的数据，也需要计算技术指标
                combined_df = incremental_df
                combined_df = calculate_kdj(combined_df)
                combined_df = calculate_macd(combined_df)
                combined_df = calculate_ma(combined_df, windows=[5, 10, 20, 30, 60])

            # 保存合并后的数据到CSV
            combined_df.attrs["fetch_time"] = freshness.fetch_time
            combined_df.attrs["data_may_delay"] = data_may_delay
            combined_df.attrs["delay_reason"] = delay_reason
            filename = file_dir_path+f"stock_data_{ts_code}_{datetime.now().strftime('%Y%m%d')}.csv"
            save_stock_data_to_csv(combined_df, ts_code, name)
            print(f"合并后数据已保存到 {filename}，共 {len(combined_df)} 条记录")

            return combined_df
        else:
            print("没有新的增量数据")
            return existing_df  # 返回现有数据

    except Exception as e:
        tr_bc = traceback.format_exc()
        print(f"获取增量数据时出错: {str(e)}, 回溯信息: {tr_bc}")
        return existing_df  # 返回现有数据

def update_all_stocks_incremental(max_workers=5):
    """
    对所有股票进行增量数据更新（简化版，使用for循环）
    """
    stocks = get_stock_list()
    print(f"开始对 {len(stocks)} 只股票进行增量数据更新")

    processed = 0
    updated = 0

    for i, (index, stock) in enumerate(stocks.iterrows()):
        ts_code = stock['ts_code']
        name = stock['name']

        try:
            result_df = get_incremental_data(ts_code, name)
            success = result_df is not None

            processed += 1
            if success:
                updated += 1

            status = "成功" if success else "无新数据"
            print(f"[{processed}/{len(stocks)}] 更新股票 {ts_code}: {status}")

        except Exception as e:
            tr_bc = traceback.format_exc()
            print(f"更新股票 {ts_code} 时出错: {str(tr_bc)}")
            processed += 1

    print(f"\n增量更新完成! 总共处理 {processed} 只股票，成功更新 {updated} 只股票")

def save_stock_data_with_indicators_to_csv(df, ts_code, name):
    """
    将包含技术指标的股票数据保存到CSV文件
    """
    # 创建副本以避免修改原始数据
    df_copy = df.copy()
    # 如果trade_date是datetime类型，转换回字符串格式
    if pd.api.types.is_datetime64_any_dtype(df_copy['trade_date']):
        df_copy['trade_date'] = df_copy['trade_date'].dt.strftime('%Y-%m-%d')

    filename = file_dir_path+f"stock_data_{ts_code}_{datetime.now().strftime('%Y%m%d')}_with_indicators.csv"
    df_copy.to_csv(filename, index=False)
    print(f"股票 {ts_code} ({name}) 的数据（含技术指标）已保存到 {filename}")

class BacktestEngine:
    """
    回测引擎，用于测试策略的历史表现
    """
    def __init__(self, strategy, lookback_days=60):
        """
        初始化回测引擎
        :param strategy: 选股策略实例
        :param lookback_days: 回测使用的回看天数
        """
        self.strategy = strategy
        self.lookback_days = lookback_days

    def backtest_stock(self, df, hold_days=5):
        """
        对单个股票进行回测
        :param df: 股票数据
        :param hold_days: 持有天数（默认5天）
        :return: 回测结果字典
        """
        results = []

        # 从回看天数开始遍历
        for i in range(self.lookback_days, len(df) - hold_days):
            # 获取当前时间点前的lookback_days天数据
            current_data = df.iloc[i-self.lookback_days:i].copy()

            # 应用策略
            if self.strategy.apply(current_data):
                # 如果策略通过，计算未来hold_days天的表现
                entry_price = df.iloc[i]['close']
                future_prices = df.iloc[i:i+hold_days]['close'].values

                # 计算每日收益率
                returns = []
                for j, price in enumerate(future_prices):
                    daily_return = (price - entry_price) / entry_price
                    returns.append(daily_return)

                # 计算5日内的最高收益和最终收益
                max_return = max(returns) if returns else 0
                final_return = returns[-1] if returns else 0
                win = final_return > 0  # 胜负判断

                results.append({
                    'entry_date': df.iloc[i]['trade_date'],
                    'entry_price': entry_price,
                    'final_return': final_return,
                    'max_return': max_return,
                    'win': win,
                    'returns': returns
                })

        if not results:
            return {
                'total_trades': 0,
                'win_rate': 0,
                'avg_return': 0,
                'max_avg_return': 0,
                'total_return': 0
            }

        total_trades = len(results)
        wins = sum(1 for r in results if r['win'])
        win_rate = wins / total_trades if total_trades > 0 else 0
        avg_return = sum(r['final_return'] for r in results) / total_trades if total_trades > 0 else 0
        max_avg_return = sum(r['max_return'] for r in results) / total_trades if total_trades > 0 else 0
        total_return = sum(r['final_return'] for r in results)

        return {
            'total_trades': total_trades,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'max_avg_return': max_avg_return,
            'total_return': total_return,
            'details': results
        }

def backtest_strategy_performance(stock_data, strategy, lookback_days=60, hold_days=5):
    """
    测试策略在给定股票上的历史表现
    :param stock_data: 股票数据DataFrame
    :param strategy: 选股策略实例
    :param lookback_days: 回测使用的回看天数
    :param hold_days: 持有天数（默认5天）
    :return: 回测结果
    """
    backtester = BacktestEngine(strategy, lookback_days)
    return backtester.backtest_stock(stock_data, hold_days)

class CombinedStrategy(StockSelectionStrategy):
    """
    组合策略：KDJ策略和均线策略同时生效
    """
    def __init__(self, ma_window=60, ma_n_days=40, kdj_n_days=1):
        self.ma_strategy = PriceAboveMaStrategy(ma_window=ma_window, n_days=ma_n_days)
        self.kdj_strategy = KDJStrategy(n_days=kdj_n_days)
        self.macd_strategy = MACDGoldenCrossStrategy(days=3, min_change_threshold=0.02)


    def apply(self, df):
        """
        检查数据是否同时满足均线策略和KDJ策略
        """
        print("组合策略开始")

        # 检查均线策略
        ma_result = self.ma_strategy.apply(df)
        print(f"均线策略结果: {'通过' if ma_result else '未通过'}")
        # 如果均线策略未通过，直接返回False
        if not ma_result:
            return False

        # 检查KDJ策略
        kdj_result = self.kdj_strategy.apply(df)
        print(f"KDJ策略结果: {'通过' if kdj_result else '未通过'}")
        # 如果KDJ策略未通过，直接返回False
        if not kdj_result:
            return False

        # 检查MACD策略
        macd_result = self.macd_strategy.apply(df)
        print(f"MACD策略结果: {'通过' if macd_result else '未通过'}")
        # 如果MACD策略未通过，直接返回False
        if not macd_result:
            return False

        return True

def batch_backtest_selected_stocks(selected_stocks=[], strategies=None, lookback_days=60, hold_days=3):
    """
    批量回测已选股票的策略表现
    :param selected_stocks: 已选股票列表
    :param strategies: 策略列表
    :param lookback_days: 回测使用的回看天数
    :param hold_days: 持有天数（默认5天）
    :return: 批量回测结果
    """
    if strategies is None:
        # 使用组合策略，即KDJ和均线策略同时生效
        strategies = [
            CombinedStrategy(ma_window=60, ma_n_days=40, kdj_n_days=1)
        ]

    batch_results = []

    selected_stocks = [
        {
            'ts_code': '600519.SH',
            'name': '贵州茅台'
        }
    ]

    for stock in selected_stocks:
        ts_code = stock['ts_code']
        name = stock['name']
        # 尝试从CSV加载完整数据
        stock_data = load_stock_data_from_csv(ts_code, name)
        if stock_data is None:
            print(f"无法加载股票 {ts_code} ({name}) 的数据，跳过回测")
            continue

        print(f"正在回测股票: {ts_code} ({name})")

        # 对每种策略进行回测
        stock_results = {'ts_code': ts_code, 'name': name, 'strategy_results': {}}

        for i, strategy in enumerate(strategies):
            strategy_name = f"Combined_Strategy"  # 修改策略名称以反映这是组合策略
            result = backtest_strategy_performance(
                stock_data,
                strategy,
                lookback_days=lookback_days,
                hold_days=hold_days
            )
            stock_results['strategy_results'][strategy_name] = result

            print(f"  {strategy_name} - 交易次数: {result['total_trades']}, "
                  f"胜率: {result['win_rate']:.2%}, 平均收益: {result['avg_return']:.2%}")

        batch_results.append(stock_results)

    return batch_results

def print_backtest_summary(batch_results):
    """
    打印回测总结
    :param batch_results: 批量回测结果
    """
    print("\n" + "="*80)
    print("回测总结报告")
    print("="*80)

    for stock_result in batch_results:
        print(f"\n股票: {stock_result['ts_code']} ({stock_result['name']})")
        for strategy_name, result in stock_result['strategy_results'].items():
            print(f"  {strategy_name}:")
            print(f"    交易次数: {result['total_trades']}")
            print(f"    胜率: {result['win_rate']:.2%}")
            print(f"    平均收益: {result['avg_return']:.2%}")
            print(f"    平均最大收益: {result['max_avg_return']:.2%}")
            print(f"    总收益: {result['total_return']:.2%}")

def pick_stocks(save_to_csv=True, strategies=None):
    """
    主要的选股函数
    """
    if strategies is None:
        # 默认使用收盘价在60日均线上方的策略、KDJ策略和MACD金叉策略
        strategies = [
            PriceAboveMaStrategy(ma_window=60, n_days=20),
            KDJStrategy(n_days=1),
            # MACDGoldenCrossStrategy(days=3, min_change_threshold=0.02)
        ]
    logger.info(
        "选股参数 strategy_count=%s ma_window=%s ma_n_days=%s kdj_n_days=%s macd_enabled=%s",
        len(strategies),
        60,
        40,
        1,
        False,
    )

    # 获取股票列表
    stocks = get_stock_list()
    print(f"总共有 {len(stocks)} 只符合条件的股票待检查")

    # 准备数据获取日期范围（最近60+个交易日的数据用于计算MA60）
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=200)).strftime('%Y%m%d')  # 获取更多历史数据确保有足够数据计算技术指标

    selected_stocks = []
    for i, (index, stock) in enumerate(stocks.iterrows()):
        ts_code = stock['ts_code']
        print(f"\n正在处理第 {i+1}/{len(stocks)} 只股票: {ts_code} ({stock['name']})")

        try:
            # 这里优先使用本地 CSV（避免对所有股票在盘中产生大量 API 调用），
            # 如需强制最新数据，可先运行 update_all_stocks_incremental() 更新缓存。
            df = load_stock_data_from_csv(ts_code, stock['name'])
            if df is None:
                print(f"  跳过: 文件不存在")
                continue  # 如果文件不存在，则跳过该股票

            if len(df) < 60:  # 如果数据不足60天，跳过这只股票
                print(f"  跳过: 数据不足60天，实际天数: {len(df)}")
                continue

            # 数据有效性验证 + 缺失/异常处理
            try:
                df = validate_ohlcv_df(df, ts_code=ts_code, min_rows=60)
            except Exception as ve:
                logger.warning("数据验证失败，跳过 ts_code=%s err=%s", ts_code, str(ve))
                continue

            # 只按当前策略依赖的核心列筛掉 NaN，避免被非核心列（如 name/realtime_time/data_source）清空整张表
            required_cols_for_strategies = ["trade_date", "close"]
            for strategy in strategies:
                if isinstance(strategy, PriceAboveMaStrategy):
                    required_cols_for_strategies.append(f"ma{strategy.ma_window}")
                elif isinstance(strategy, KDJStrategy):
                    required_cols_for_strategies.extend(["k", "d", "j"])
                elif isinstance(strategy, MACDGoldenCrossStrategy):
                    required_cols_for_strategies.extend(["dif", "dea", "macd"])

            required_cols_for_strategies = [
                col for col in dict.fromkeys(required_cols_for_strategies) if col in df.columns
            ]
            df = df.dropna(subset=required_cols_for_strategies)

            # 应用所有选股策略
            all_strategies_passed = True
            for strategy in strategies:
                if not strategy.apply(df):
                    all_strategies_passed = False
                    print(f"  策略未通过，跳过此股票")
                    break
                else:
                    print(f"  策略通过")

            if all_strategies_passed:
                print(f"  >>>> 选择股票: {ts_code} ({stock['name']})")
                # 添加最新数据到返回结果
                latest_data = df.iloc[-1]  # 获取最新一天的数据
                selected_stocks.append({
                    'ts_code': ts_code,
                    'name': stock['name'],
                    'latest_data': latest_data,  # 保存最新数据
                    'data_fetch_time': getattr(df, "attrs", {}).get("fetch_time"),
                    'data_may_delay': bool(getattr(df, "attrs", {}).get("data_may_delay", False)),
                    'delay_reason': getattr(df, "attrs", {}).get("delay_reason", ""),
                })
            else:
                print(f"  未选择股票: {ts_code} ({stock['name']})")

            # 显示进度
            if (i + 1) % 100 == 0:
                print(f"已处理 {i + 1}/{len(stocks)} 只股票")

        except Exception as e:
            tr_bc = traceback.format_exc()
            print(f"处理股票 {ts_code} 时出错: {str(e)}, 回溯信息: {tr_bc}")
            continue

    return selected_stocks

if __name__ == "__main__":
    # python filter_stock_by_index.py ai-score 000601.SZ
    if len(sys.argv) >= 3 and sys.argv[1] == "ai-score":
        single_ts_code = sys.argv[2]
        single_name = sys.argv[3] if len(sys.argv) >= 4 else None
        single_result = score_single_stock_with_ai(single_ts_code, single_name)
        print_single_stock_ai_score(single_result)
    else:
        update_all_stocks_incremental()
        result = pick_stocks()
        result = enrich_selected_stocks_with_ai(result)
        # batch_backtest_selected_stocks()
        print(f"\n符合条件的股票数量: {len(result)}")
        print(f"数据获取时间(脚本本地): {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if result:
            print("\n筛选结果（含数据时间与延迟标注）")
            print("=" * 80)
        for stock in result:
            print(f"{stock['ts_code']}: {stock['name']}")
            latest_data = stock['latest_data']
            fetch_time = stock.get("data_fetch_time")
            fetch_time_str = fetch_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(fetch_time, datetime) else "N/A"
            may_delay = stock.get("data_may_delay", False)
            delay_reason = stock.get("delay_reason", "")
            if may_delay:
                print(f"  数据延迟提示: 可能存在延迟（{delay_reason or '未知原因'}）")
            print(f"  数据采集时间: {fetch_time_str}")
            print(f"  交易日期: {latest_data.get('trade_date', 'N/A')}")
            # 打印各项数据（缺字段时做容错）
            if 'close' in latest_data: print(f"  最新收盘价: {float(latest_data['close']):.2f}")
            if 'open' in latest_data: print(f"  最新开盘价: {float(latest_data['open']):.2f}")
            if 'high' in latest_data: print(f"  最新最高价: {float(latest_data['high']):.2f}")
            if 'low' in latest_data: print(f"  最新最低价: {float(latest_data['low']):.2f}")
            if 'vol' in latest_data: print(f"  最新成交量: {latest_data['vol']}")
            print(f"  60日均线: {float(latest_data['ma60']):.2f}" if 'ma60' in latest_data else "  60日均线: N/A")
            if all(k in latest_data for k in ['k', 'd', 'j']):
                print(f"  KDJ指标 - K: {float(latest_data['k']):.2f}, D: {float(latest_data['d']):.2f}, J: {float(latest_data['j']):.2f}")
            if all(k in latest_data for k in ['dif', 'dea', 'macd']):
                print(f"  MACD指标 - DIF: {float(latest_data['dif']):.2f}, DEA: {float(latest_data['dea']):.2f}, MACD: {float(latest_data['macd']):.2f}")
            if "ai_score" in stock:
                if stock.get("ai_model"):
                    print(f"  AI模型: {stock.get('ai_model')}")
                print(f"  AI消息面评分: {stock.get('ai_score', 'N/A')} ({stock.get('ai_sentiment', 'N/A')})")
                print(f"  AI评分摘要: {stock.get('ai_summary', 'N/A')}")
                if stock.get("ai_drivers"):
                    print(f"  AI利好因素: {'; '.join(stock['ai_drivers'])}")
                if stock.get("ai_risks"):
                    print(f"  AI风险因素: {'; '.join(stock['ai_risks'])}")
            print("-" * 80)
