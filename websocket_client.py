"""
Polymarket WebSocket 实时数据客户端
支持:
- Market Channel: 订阅市场价格变动、订单簿更新、最新成交价
- User Channel: 订阅用户订单成交/状态更新
- RTDS: 实时加密货币/股票价格数据流
"""
import json
import threading
import time
import websocket
from typing import Callable, List, Optional, Dict, Any
from dataclasses import dataclass
from py_clob_client.client import ClobClient


@dataclass
class PriceUpdate:
    token_id: str
    best_bid: float
    best_ask: float
    last_price: Optional[float] = None
    timestamp: int = 0


@dataclass
class OrderTradeUpdate:
    order_id: str
    status: str
    token_id: str
    side: str
    size_filled: float
    price: float
    timestamp: int = 0


class PolymarketWebsocketClient:
    # Endpoints
    MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
        on_price_change: Optional[Callable[[PriceUpdate], None]] = None,
        on_trade: Optional[Callable[[OrderTradeUpdate], None]] = None,
        on_order_update: Optional[Callable[[Dict], None]] = None,
        on_rtds_price: Optional[Callable[[Dict], None]] = None,
        auto_reconnect: bool = True,
        ping_interval: int = 10,
    ):
        """
        初始化 WebSocket 客户端
        :param api_key: CLOB API Key (用户频道需要)
        :param api_secret: CLOB API Secret (用户频道需要)
        :param api_passphrase: CLOB Passphrase (用户频道需要)
        :param on_price_change: 价格变动回调
        :param on_trade: 订单成交回调
        :param on_order_update: 订单状态更新回调
        :param on_rtds_price: RTDS 价格更新回调
        :param auto_reconnect: 是否自动重连
        :param ping_interval: 心跳间隔 (秒)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        
        self.on_price_change = on_price_change
        self.on_trade = on_trade
        self.on_order_update = on_order_update
        self.on_rtds_price = on_rtds_price
        
        self.auto_reconnect = auto_reconnect
        self.ping_interval = ping_interval
        
        # 订阅列表
        self.subscribed_tokens: List[str] = []
        self.subscribed_conditions: List[str] = []
        self.rtds_subscriptions: List[Dict] = []
        
        # 连接状态
        self.market_ws: Optional[websocket.WebSocketApp] = None
        self.user_ws: Optional[websocket.WebSocketApp] = None
        self.rtds_ws: Optional[websocket.WebSocketApp] = None
        
        self.market_connected = False
        self.user_connected = False
        self.rtds_connected = False
        
        self._threads: List[threading.Thread] = []
    
    # ============ Market Channel ============
    # 市场价格变动订阅
    
    def subscribe_markets(self, token_ids: List[str], custom_features: bool = True) -> None:
        """
        订阅多个代币市场数据
        :param token_ids: 代币ID列表
        :param custom_features: 是否启用自定义功能 (best_bid_ask, new_market, market_resolved)
        """
        if not self.market_connected:
            self._start_market_ws()
            time.sleep(0.5)
        
        if not self.market_ws:
            raise Exception("Market WebSocket not connected")
        
        # 如果已经连接，直接发送订阅
        subscribe_msg = {
            "assets_ids": token_ids,
            "type": "market",
            "custom_feature_enabled": custom_features
        }
        self.market_ws.send(json.dumps(subscribe_msg))
        self.subscribed_tokens.extend([t for t in token_ids if t not in self.subscribed_tokens])
        print(f"[Market WS] Subscribed to {len(token_ids)} tokens")
    
    def unsubscribe_markets(self, token_ids: List[str]) -> None:
        """取消订阅市场数据"""
        if not self.market_connected or not self.market_ws:
            return
        
        unsubscribe_msg = {
            "assets_ids": token_ids,
            "operation": "unsubscribe"
        }
        self.market_ws.send(json.dumps(unsubscribe_msg))
        self.subscribed_tokens = [t for t in self.subscribed_tokens if t not in token_ids]
        print(f"[Market WS] Unsubscribed from {len(token_ids)} tokens")
    
    def _on_market_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        """处理市场频道消息"""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "best_bid_ask":
                # 最优买卖价格更新
                token_id = data.get("asset_id")
                best_bid = float(data.get("best_bid", 0))
                best_ask = float(data.get("best_ask", 0))
                last_price = float(data.get("last_price", 0)) if data.get("last_price") else None
                timestamp = data.get("timestamp", int(time.time() * 1000))
                
                if self.on_price_change:
                    update = PriceUpdate(
                        token_id=token_id,
                        best_bid=best_bid,
                        best_ask=best_ask,
                        last_price=last_price,
                        timestamp=timestamp
                    )
                    self.on_price_change(update)
            
            elif msg_type == "last_trade_price":
                # 最新成交价格
                token_id = data.get("asset_id")
                price = float(data.get("price", 0))
                print(f"[Market WS] Last trade {token_id}: {price}")
            
            elif msg_type == "market_resolved":
                print(f"[Market WS] Market resolved: {data.get('condition_id')}")
            
            elif msg_type == "new_market":
                print(f"[Market WS] New market: {data.get('condition_id')}")
            
            else:
                # print(f"[Market WS] Unhandled message: {msg_type}")
                pass
        
        except Exception as e:
            print(f"[Market WS] Error processing message: {e}")
    
    def _on_market_open(self, ws: websocket.WebSocketApp) -> None:
        """市场频道连接打开"""
        self.market_connected = True
        print("[Market WS] Connected")
        
        # 如果已有订阅，发送订阅消息
        if self.subscribed_tokens:
            self.subscribe_markets(self.subscribed_tokens)
    
    def _on_market_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        print(f"[Market WS] Error: {error}")
    
    def _on_market_close(self, ws: websocket.WebSocketApp) -> None:
        self.market_connected = False
        print("[Market WS] Disconnected")
        
        if self.auto_reconnect:
            print("[Market WS] Reconnecting...")
            time.sleep(1)
            self._start_market_ws()
    
    def _start_market_ws(self) -> None:
        """启动市场频道 WebSocket"""
        def run():
            self.market_ws = websocket.WebSocketApp(
                self.MARKET_WS_URL,
                on_open=self._on_market_open,
                on_message=self._on_market_message,
                on_error=self._on_market_error,
                on_close=self._on_market_close
            )
            self.market_ws.run_forever()
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self._threads.append(thread)
    
    # ============ User Channel ============
    # 用户订单成交/状态更新订阅
    
    def subscribe_user(self, condition_ids: Optional[List[str]] = None) -> None:
        """
        订阅用户订单更新
        :param condition_ids: 可选，指定条件ID列表（市场ID），不填则接收所有市场更新
        """
        if not all([self.api_key, self.api_secret, self.api_passphrase]):
            raise Exception("API credentials required for user channel")
        
        if not self.user_connected:
            self._start_user_ws()
            time.sleep(0.5)
        
        if not self.user_ws:
            raise Exception("User WebSocket not connected")
        
        subscribe_msg = {
            "auth": {
                "apiKey": self.api_key,
                "secret": self.api_secret,
                "passphrase": self.api_passphrase
            },
            "markets": condition_ids or [],
            "type": "user"
        }
        self.user_ws.send(json.dumps(subscribe_msg))
        
        if condition_ids:
            self.subscribed_conditions.extend([
                c for c in condition_ids if c not in self.subscribed_conditions
            ])
        print(f"[User WS] Subscribed to user updates")
    
    def _on_user_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        """处理用户频道消息"""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "trade":
                # 订单成交更新
                order_id = data.get("order_id")
                status = data.get("status")
                token_id = data.get("asset_id")
                side = data.get("side")
                size_filled = float(data.get("size_filled", 0))
                price = float(data.get("price", 0))
                timestamp = data.get("timestamp", int(time.time() * 1000))
                
                if self.on_trade:
                    update = OrderTradeUpdate(
                        order_id=order_id,
                        status=status,
                        token_id=token_id,
                        side=side,
                        size_filled=size_filled,
                        price=price,
                        timestamp=timestamp
                    )
                    self.on_trade(update)
            
            elif msg_type == "order":
                # 订单状态更新（创建/取消等）
                if self.on_order_update:
                    self.on_order_update(data)
            
            else:
                print(f"[User WS] Unhandled message type: {msg_type}")
        
        except Exception as e:
            print(f"[User WS] Error processing message: {e}")
    
    def _on_user_open(self, ws: websocket.WebSocketApp) -> None:
        self.user_connected = True
        print("[User WS] Connected")
    
    def _on_user_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        print(f"[User WS] Error: {error}")
    
    def _on_user_close(self, ws: websocket.WebSocketApp) -> None:
        self.user_connected = False
        print("[User WS] Disconnected")
        
        if self.auto_reconnect:
            print("[User WS] Reconnecting...")
            time.sleep(1)
            self._start_user_ws()
    
    def _start_user_ws(self) -> None:
        """启动用户频道 WebSocket"""
        def run():
            self.user_ws = websocket.WebSocketApp(
                self.USER_WS_URL,
                on_open=self._on_user_open,
                on_message=self._on_user_message,
                on_error=self._on_user_error,
                on_close=self._on_user_close
            )
            self.user_ws.run_forever()
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self._threads.append(thread)
    
    # ============ RTDS (Real-Time Data Socket) ============
    # 实时加密货币/股票价格
    
    def subscribe_rtds(self, subscriptions: List[Dict]) -> None:
        """
        订阅 RTDS 数据流
        :param subscriptions: 订阅列表，每个元素格式:
            {
                "topic": "crypto_prices",
                "type": "update",
                "filters": "btcusdt,ethusdt"  # 逗号分隔
            }
        示例：
        - 加密货币: {"topic": "crypto_prices", "type": "update", "filters": "btcusdt,ethusdt"}
        - 股票: {"topic": "equity_prices", "type": "update", "filters": '{"symbol":"AAPL"}'}
        """
        if not self.rtds_connected:
            self._start_rtds_ws()
            time.sleep(0.5)
        
        if not self.rtds_ws:
            raise Exception("RTDS WebSocket not connected")
        
        subscribe_msg = {
            "action": "subscribe",
            "subscriptions": subscriptions
        }
        self.rtds_ws.send(json.dumps(subscribe_msg))
        self.rtds_subscriptions.extend(subscriptions)
        print(f"[RTDS] Subscribed to {len(subscriptions)} topics")
    
    def unsubscribe_rtds(self, subscriptions: List[Dict]) -> None:
        """取消 RTDS 订阅"""
        if not self.rtds_connected or not self.rtds_ws:
            return
        
        unsubscribe_msg = {
            "action": "unsubscribe",
            "subscriptions": subscriptions
        }
        self.rtds_ws.send(json.dumps(unsubscribe_msg))
        print(f"[RTDS] Unsubscribed from {len(subscriptions)} topics")
    
    def _on_rtds_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        """处理 RTDS 消息"""
        try:
            data = json.loads(message)
            
            if self.on_rtds_price:
                self.on_rtds_price(data)
            else:
                # 默认打印
                topic = data.get("topic")
                payload = data.get("payload", {})
                if topic in ["crypto_prices", "equity_prices", "crypto_prices_chainlink"]:
                    symbol = payload.get("symbol")
                    price = payload.get("value")
                    print(f"[RTDS] {topic} {symbol}: {price}")
        
        except Exception as e:
            print(f"[RTDS] Error processing message: {e}")
    
    def _on_rtds_open(self, ws: websocket.WebSocketApp) -> None:
        self.rtds_connected = True
        print("[RTDS] Connected")
        
        if self.rtds_subscriptions:
            self.subscribe_rtds(self.rtds_subscriptions)
    
    def _on_rtds_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        print(f"[RTDS] Error: {error}")
    
    def _on_rtds_close(self, ws: websocket.WebSocketApp) -> None:
        self.rtds_connected = False
        print("[RTDS] Disconnected")
        
        if self.auto_reconnect:
            print("[RTDS] Reconnecting...")
            time.sleep(1)
            self._start_rtds_ws()
    
    def _start_rtds_ws(self) -> None:
        """启动 RTDS WebSocket"""
        def run():
            self.rtds_ws = websocket.WebSocketApp(
                self.RTDS_WS_URL,
                on_open=self._on_rtds_open,
                on_message=self._on_rtds_message,
                on_error=self._on_rtds_error,
                on_close=self._on_rtds_close
            )
            self.rtds_ws.run_forever()
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self._threads.append(thread)
    
    # ============ Heartbeat ============
    
    def start_heartbeat(self) -> None:
        """启动心跳线程，定期发送 PING"""
        def heartbeat_loop():
            while True:
                time.sleep(self.ping_interval)
                
                # Market WS 需要客户端发送 PING
                if self.market_connected and self.market_ws:
                    try:
                        self.market_ws.send("PING")
                    except:
                        pass
                
                # User WS 同样需要客户端发送 PING
                if self.user_connected and self.user_ws:
                    try:
                        self.user_ws.send("PING")
                    except:
                        pass
                
                # RTDS 需要客户端发送 PING 每 5 秒
                if self.rtds_connected and self.rtds_ws and self.ping_interval >= 5:
                    try:
                        self.rtds_ws.send("PING")
                    except:
                        pass
        
        thread = threading.Thread(target=heartbeat_loop, daemon=True)
        thread.start()
        self._threads.append(thread)
    
    def close_all(self) -> None:
        """关闭所有连接"""
        if self.market_ws:
            self.market_ws.close()
        if self.user_ws:
            self.user_ws.close()
        if self.rtds_ws:
            self.rtds_ws.close()
        
        self.market_connected = False
        self.user_connected = False
        self.rtds_connected = False
        print("[WS] All connections closed")