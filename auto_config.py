#!/usr/bin/env python3
"""
自动获取 Polymarket 加密货币 UP/DOWN 5 分钟市场配置
根据 slug 规则自动查询 token_id / condition_id
自动更新 market_config.json
slug 规则: {symbol_lower}-updown-5m-{window_start_timestamp}
"""
import json
import time
import requests
from typing import Dict, Optional, Tuple
from datetime import datetime


# 支持的币种列表
SYMBOLS = [
    "BTC",
    "ETH",
    "SOL",
    "XRP", 
    "BNB",
    "DOGE"
]

GAMMA_API_BASE = "https://gamma-api.polymarket.com"


def generate_slug(symbol: str, start_timestamp: int) -> str:
    """
    生成 slug
    格式: {币名小写}-updown-5m-{窗口开始时间戳(秒)}
    """
    return f"{symbol.lower()}-updown-5m-{start_timestamp}"


def get_market_by_slug(slug: str) -> Optional[Dict]:
    """
    通过 slug 查询市场信息
    GET https://gamma-api.polymarket.com/events/slug/{slug}
    """
    url = f"{GAMMA_API_BASE}/events/slug/{slug}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            print(f"[AUTO] 404 Not Found: {slug}")
            return None
        else:
            print(f"[AUTO] Request failed {resp.status_code}: {slug}")
            return None
    except Exception as e:
        print(f"[AUTO] Error querying {slug}: {e}")
        return None


def extract_tokens_from_event(event: Dict) -> Optional[Tuple[str, str, str]]:
    """
    从事件中提取 YES token ID, NO token ID, condition ID
    UP/DOWN 市场一般有两个 outcome: UP 和 DOWN
    UP -> YES token, DOWN -> NO token
    """
    if not event.get("markets"):
        return None
    
    # UP/DOWN 市场只有一个 market
    market = event["markets"][0]
    condition_id = market.get("conditionId")
    
    # 获取两个 outcome 的 token ID
    # 一般第一个是 UP (YES), 第二个是 DOWN (NO)
    clob_tokens = market.get("clobTokenIds", [])
    if len(clob_tokens) < 2:
        print(f"[AUTO] Not enough tokens for {event.get('slug')}")
        return None
    
    yes_token_id = clob_tokens[0]  # UP
    no_token_id = clob_tokens[1]   # DOWN
    
    return yes_token_id, no_token_id, condition_id


def get_current_window_start_timestamp() -> int:
    """
    获取当前 5 分钟窗口的开始时间戳（对齐到 5 分钟）
    Polymarket 5 分钟窗口是按整 5 分钟对齐的
    """
    now = int(time.time())
    # 对齐到最近的已经开始的 5 分钟窗口
    window_start = (now // 300) * 300
    return window_start


def auto_update_config(config_path: str = "market_config.json") -> None:
    """自动更新配置"""
    # 读取现有配置
    with open(config_path, "r") as f:
        config = json.load(f)
    
    current_window_start = get_current_window_start_timestamp()
    print(f"[AUTO] Current 5min window start: {current_window_start} "
          f"({datetime.fromtimestamp(current_window_start)})")
    print()
    
    updated = 0
    for symbol in SYMBOLS:
        binance_symbol = f"{symbol}USDT"
        slug = generate_slug(symbol, current_window_start)
        
        print(f"[{symbol}] Querying slug: {slug}")
        event = get_market_by_slug(slug)
        if not event:
            print(f"[{symbol}] ❌ Failed to get market data")
            continue
        
        tokens = extract_tokens_from_event(event)
        if not tokens:
            print(f"[{symbol}] ❌ Failed to extract tokens")
            continue
        
        yes_token, no_token, condition_id = tokens
        config[binance_symbol]["yes_token_id"] = yes_token
        config[binance_symbol]["no_token_id"] = no_token
        config[binance_symbol]["condition_id"] = condition_id
        
        print(f"[{symbol}] ✅ Updated:")
        print(f"       YES (UP) token_id: {yes_token}")
        print(f"       NO (DOWN) token_id: {no_token}")
        print(f"       condition_id: {condition_id}")
        updated += 1
        print()
        time.sleep(0.5)  # 避免 rate limit
    
    # 保存更新后的配置
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"\n[AUTO] Done. Updated {updated} markets. Config saved to {config_path}")


def query_single_slug(slug: str) -> None:
    """手动查询单个 slug"""
    event = get_market_by_slug(slug)
    if not event:
        print(f"Not found: {slug}")
        return
    
    tokens = extract_tokens_from_event(event)
    if not tokens:
        print(f"Failed to extract tokens from {slug}")
        print(json.dumps(event, indent=2))
        return
    
    yes_token, no_token, condition_id = tokens
    print(f"Slug: {slug}")
    print(f"YES (UP) token_id: {yes_token}")
    print(f"NO (DOWN) token_id: {no_token}")
    print(f"condition_id: {condition_id}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # 查询单个 slug
        query_single_slug(sys.argv[1])
    else:
        # 自动更新当前窗口所有币种
        auto_update_config()
