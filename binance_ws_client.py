"""
Binance 币安期货 WebSocket 客户端
专门用于获取实时K线数据（支持5分钟K线，适配Polymarket加密货币updown-5m市场）
文档: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
"""
import json
import threading
import time
import websocket
from typing import Callable, List, Optional, Dict
from dataclasses import dataclass


@dataclass
class KlineData:
    """K线数据结构"""
    symbol: str           # 交易对 (BTCUSDT)
    interval: str         # 时间间隔 (5m)
    start_time: int       # 开始时间 (ms)
    close_time: int       # 结束时间 (ms)
    open: float           # 开盘价
    high: float           # 最高价
    low: float            # 最低价
    close: float          # 收盘价
    volume: float         # 成交量
    number_of_trades: int # 成交笔数
    is_closed: bool       # 是否已经收盘

    def __repr__(self):
        return f"Kline({self.symbol}, {self.interval}, O={self.open}, H={self.high}, L={self.low}, C={self.close}, closed={self.is_closed})"


class BinanceFuturesWebsocketClient:
    """
    Binance 币安 USDT 合约 WebSocket 客户端
    支持订阅多个交易对的K线（蜡烛图）更新
    """
    # 主网
    WS_URL = "wss://fstream.binance.com/ws"
    
    def __init__(
        self,
        on_kline_update: Callable[[KlineData], None],
        on_kline_closed: Optional[Callable[[KlineData], None]] = None,
        auto_reconnect: bool = True,
        ping_interval: int = 30,
    ):
        """
        初始化
        :param on_kline_update: K线更新回调（每250ms推送一次）
        :param on_kline_closed: K线收盘回调（当这根K线结束时）
        :param auto_reconnect: 是否自动重连
        :param ping_interval: 心跳间隔，币安会自动发ping，客户端回复pong即可
        """
        self.on_kline_update = on_kline_update
        self.on_kline_closed = on_kline_closed
        self.auto_reconnect = auto_reconnect
        self.ping_interval = ping_interval
        
        # 订阅列表: [(symbol, interval), ...]
        self.subscriptions: List[tuple[str, str]] = []
        
        self.ws: Optional[websocket.WebSocketApp] = None
        self.connected = False
        self._thread: Optional[threading.Thread] = None
    
    def subscribe_kline(self, symbol: str, interval: str = "5m") -> None:
        """
        订阅单个交易对K线
        :param symbol: 交易对，例如 "btcusdt" (小写不敏感，会自动转换)
        :param interval: 时间间隔
            可选: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
        """
        symbol = symbol.lower()
        if (symbol, interval) not in self.subscriptions:
            self.subscriptions.append((symbol, interval))
            
            # 如果已经连接，动态订阅
            if self.connected and self.ws:
                self._send_subscribe()
    
    def unsubscribe_kline(self, symbol: str, interval: str = "5m") -> None:
        """取消订阅K线"""
        symbol = symbol.lower()
        if (symbol, interval) in self.subscriptions:
            self.subscriptions.remove((symbol, interval))
            
            if self.connected and self.ws:
                self._send_subscribe()
    
    def batch_subscribe_5m(self, symbols: List[str]) -> None:
        """批量订阅多个5分钟K线（便捷方法）"""
        for symbol in symbols:
            self.subscribe_kline(symbol, "5m")
    
    def _get_stream_name(self, symbol: str, interval: str) -> str:
        """获取流名称: <symbol>@kline_<interval>"""
        return f"{symbol}@kline_{interval}"
    
    def _send_subscribe(self) -> None:
        """发送订阅请求"""
        if not self.connected or not self.ws:
            return
        
        streams = [self._get_stream_name(s, i) for s, i in self.subscriptions]
        
        if len(streams) == 1:
            # 单个流直接连接，不需要订阅消息
            return
        
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": int(time.time() * 1000)
        }
        self.ws.send(json.dumps(subscribe_msg))
        print(f"[Binance WS] Subscribed: {streams}")
    
    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        """处理消息"""
        try:
            data = json.loads(message)
            
            # 响应订阅结果，忽略
            if "result" in data:
                return
            
            # PONG 回复
            if data.get("ping"):
                self.ws.send(json.dumps({"pong": data.get("ping")}))
                return
            
            # K线事件
            if data.get("e") == "kline":
                k = data.get("k", {})
                kline = KlineData(
                    symbol=k.get("s"),
                    interval=k.get("i"),
                    start_time=k.get("t"),
                    close_time=k.get("T"),
                    open=float(k.get("o")),
                    high=float(k.get("h")),
                    low=float(k.get("l")),
                    close=float(k.get("c")),
                    volume=float(k.get("v")),
                    number_of_trades=int(k.get("n")),
                    is_closed=bool(k.get("x"))
                )
                
                # 调用回调
                self.on_kline_update(kline)
                
                # 如果K线收盘，调用收盘回调
                if kline.is_closed and self.on_kline_closed:
                    self.on_kline_closed(kline)
        
        except Exception as e:
            print(f"[Binance WS] Error processing message: {e}")
    
    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        self.connected = True
        print("[Binance WS] Connected")
        
        # 如果有多个流，发送订阅
        if len(self.subscriptions) > 1:
            self._send_subscribe()
    
    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        print(f"[Binance WS] Error: {error}")
    
    def _on_close(self, ws: websocket.WebSocketApp) -> None:
        self.connected = False
        print("[Binance WS] Disconnected")
        
        if self.auto_reconnect:
            print("[Binance WS] Reconnecting...")
            time.sleep(1)
            self.start()
    
    def start(self) -> None:
        """启动 WebSocket 连接"""
        # 单流可以直接连接到流地址
        if len(self.subscriptions) == 1:
            symbol, interval = self.subscriptions[0]
            stream = self._get_stream_name(symbol, interval)
            url = f"{self.WS_URL}/{stream}"
        else:
            url = self.WS_URL
        
        def run():
            self.ws = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            self.ws.run_forever()
        
        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
    
    def close(self) -> None:
        """关闭连接"""
        if self.ws:
            self.ws.close()
        self.connected = False
        print("[Binance WS] Connection closed")
