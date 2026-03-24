#!/usr/bin/env python3
"""
Binance 5分钟K线订阅示例
适配 Polymarket 加密货币 UP/DOWN 5分钟市场
"""
import time
from binance_ws_client import BinanceFuturesWebsocketClient, KlineData


def handle_kline_update(kline: KlineData):
    """K线更新回调（每250ms触发一次，K线没收盘也会推最新价）"""
    if not kline.is_closed:
        print(f"[Kline UPDATE] {kline.symbol} {kline.interval} | O={kline.open:.2f} H={kline.high:.2f} L={kline.low:.2f} C={kline.close:.2f}")


def handle_kline_closed(kline: KlineData):
    """K线收盘回调（每5分钟触发一次，正好对应Polymarket 5m市场）"""
    print("\n" + "="*60)
    print(f"[Kline CLOSED] {kline.symbol} {kline.interval}")
    print(f"  Time: {kline.start_time} -> {kline.close_time}")
    print(f"  Open:   {kline.open:.2f}")
    print(f"  High:   {kline.high:.2f}")
    print(f"  Low:    {kline.low:.2f}")
    print(f"  Close:  {kline.close:.2f}")
    print(f"  Volume: {kline.volume:.2f} | Trades: {kline.number_of_trades}")
    print("="*60 + "\n")
    
    # 这里可以添加你的交易逻辑：
    # 根据刚刚收盘的5分钟K线判断涨跌，在 Polymarket 下单


def main():
    # 订阅多个交易对的5分钟K线
    # 根据你在 Polymarket 观察的市场添加对应交易对
    symbols_to_subscribe = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    
    client = BinanceFuturesWebsocketClient(
        on_kline_update=handle_kline_update,
        on_kline_closed=handle_kline_closed
    )
    
    # 批量订阅5分钟K线
    client.batch_subscribe_5m(symbols_to_subscribe)
    
    # 启动连接
    client.start()
    
    print(f"\nBinance WebSocket connected, subscribed to 5m klines: {symbols_to_subscribe}")
    print("Waiting for updates... (Press Ctrl+C to exit)\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting...")
        client.close()


if __name__ == "__main__":
    main()
