#!/usr/bin/env python3
"""
WebSocket 实时数据使用示例
演示:
1. 订阅市场价格实时变动
2. 订阅用户订单成交推送
3. 订阅 RTDS 加密货币/股票实时价格
"""
import os
import time
from dotenv import load_dotenv
from websocket_client import PolymarketWebsocketClient, PriceUpdate, OrderTradeUpdate

load_dotenv()


def handle_price_update(update: PriceUpdate):
    """价格变动回调"""
    print(f"\n[PRICE UPDATE] {update.token_id}:")
    print(f"  Best Bid: {update.best_bid}")
    print(f"  Best Ask: {update.best_ask}")
    if update.last_price:
        print(f"  Last Price: {update.last_price}")


def handle_trade(update: OrderTradeUpdate):
    """订单成交回调"""
    print(f"\n[TRADE EXECUTED] {update.order_id}:")
    print(f"  Token: {update.token_id}")
    print(f"  Side: {update.side}")
    print(f"  Size Filled: {update.size_filled}")
    print(f"  Price: {update.price}")
    print(f"  Status: {update.status}")


def handle_order_update(data: dict):
    """订单状态更新回调"""
    print(f"\n[ORDER UPDATE] {data.get('type')}: {data}")


def handle_rtds_price(data: dict):
    """RTDS 价格更新回调"""
    topic = data.get("topic")
    payload = data.get("payload", {})
    symbol = payload.get("symbol")
    price = payload.get("value")
    print(f"[RTDS {topic}] {symbol}: {price}")


def main():
    # 从环境变量读取API凭据
    api_key = os.getenv("CLOB_API_KEY")
    api_secret = os.getenv("CLOB_SECRET")
    api_passphrase = os.getenv("CLOB_PASS_PHRASE")

    # 创建 WebSocket 客户端
    ws_client = PolymarketWebsocketClient(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        on_price_change=handle_price_update,
        on_trade=handle_trade,
        on_order_update=handle_order_update,
        on_rtds_price=handle_rtds_price,
        auto_reconnect=True
    )

    # 启动心跳
    ws_client.start_heartbeat()

    # ============ 示例1: 订阅市场价格 ============
    # 这里填入你要订阅的代币ID
    tokens_to_subscribe = [
        "71321045679252212594626385532706912750332728571942532289631379312455583992563",
        # 添加更多代币ID...
    ]
    ws_client.subscribe_markets(tokens_to_subscribe)

    # ============ 示例2: 订阅用户订单更新 ============
    # 如果你有API凭据，可以订阅自己的订单成交推送
    if all([api_key, api_secret, api_passphrase]):
        # 如果想订阅特定市场，填入 condition ID 列表
        # 留空则接收所有市场的更新
        ws_client.subscribe_user(condition_ids=[])
    else:
        print("[WARN] No API credentials provided, user channel not connected")

    # ============ 示例3: RTDS 订阅加密货币价格 ============
    # 订阅 BTC/ETH 实时价格来自 Binance
    rtds_subs = [
        {
            "topic": "crypto_prices",
            "type": "update",
            "filters": "btcusdt,ethusdt"
        }
    ]
    # 可以再添加股票价格，比如 AAPL
    # rtds_subs.append({
    #     "topic": "equity_prices",
    #     "type": "update",
    #     "filters": '{"symbol":"AAPL"}'
    # })
    ws_client.subscribe_rtds(rtds_subs)

    print("\nWebSocket connected, waiting for updates... (Press Ctrl+C to exit)")
    print("-" * 60)

    # 保持运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting...")
        ws_client.close_all()


if __name__ == "__main__":
    main()
