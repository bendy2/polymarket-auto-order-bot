#!/usr/bin/env python3
"""
Polymarket 加密货币 UP/DOWN 自动交易策略
基于 Binance K线价差自动下单

策略逻辑:
1. 订阅 Binance WebSocket 获取所有交易对 5m + 15m Kline (一共 7 * 2 = 14 个流)
2. Kline新开窗口 → 自动查询 Polymarket Gamma API 获取市场信息 (token_id / condition_id)，每个窗口只查询一次
3. 窗口结束前 N 秒 → 计算价差比例 (current_price - open_price) / open_price
4. 如果价差比例绝对值 > 阈值 s → 加入待下单列表，批量提交
5. 使用 py-clob-client 批量提交 GTC 限价单，价格用当前中间价
"""
import os
import json
import time
import threading
import requests
from typing import Dict, Optional, List, Tuple
from dotenv import load_dotenv

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.constants import POLYGON
from py_clob_client.order_builder.constants import BUY

import websocket


class AutoTrader:
    # 配置
    SYMBOLS = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT", 
        "XRPUSDT",
        "DOGEUSDT",
        "BNBUSDT",
        "HYPERUSDT"  # HYPE
    ]
    INTERVALS = ["5m", "15m"]
    
    # 策略参数 (可以通过环境变量覆盖)
    SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "0.02"))  # 价差阈值 2%
    ORDER_AMOUNT_USD = float(os.getenv("ORDER_AMOUNT_USD", "10"))  # 每次下单金额 USD
    LAST_SECONDS_N = int(os.getenv("LAST_SECONDS_N", "60"))  # 窗口结束前 N 秒触发下单
    CHECK_INTERVAL = float(os.getenv("CHECK_INTERVAL", "0.1"))  # 主线程检查间隔 0.1 秒
    
    GAMMA_API_BASE = "https://gamma-api.polymarket.com"
    
    def __init__(self):
        load_dotenv()
        
        # 加载配置
        self.spread_threshold = float(os.getenv("SPREAD_THRESHOLD", "0.02"))
        self.order_amount = float(os.getenv("ORDER_AMOUNT_USD", "10"))
        self.trigger_before_seconds = int(os.getenv("LAST_SECONDS_N", "60"))
        self.check_interval = float(os.getenv("CHECK_INTERVAL", "0.1"))
        
        # 初始化 Polymarket 客户端
        self.host = os.getenv("CLOB_API_URL", "https://clob.polymarket.com")
        self.private_key = os.getenv("PK")
        self.chain_id = int(os.getenv("CHAIN_ID", POLYGON))
        self.api_key = os.getenv("CLOB_API_KEY")
        self.api_secret = os.getenv("CLOB_SECRET")
        self.api_passphrase = os.getenv("CLOB_PASS_PHRASE")
        
        self.client: Optional[ClobClient] = None
        self._init_clob()
        
        # Kline数据缓存: (symbol, interval) -> kline_data
        # kline_data: {
        #   "start_time": int,  # ms
        #   "end_time": int,    # ms
        #   "open": float,
        #   "current_close": float,
        #   "yes_token_id": str,
        #   "no_token_id": str,
        #   "condition_id": str,
        #   "slug": str,
        #   "triggered": bool  # 是否已下单
        # }
        self.kline_cache: Dict[tuple[str, str], Dict] = {}
        self._lock = threading.Lock()
        
        # WebSocket
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = True
    
    def _init_clob(self) -> None:
        """初始化 Polymarket 客户端"""
        from py_clob_client.clob_types import ApiCreds
        
        api_creds = None
        if all([self.api_key, self.api_secret, self.api_passphrase]):
            api_creds = ApiCreds(
                api_key=self.api_key,
                api_secret=self.api_secret,
                api_passphrase=self.api_passphrase
            )
        
        client_kwargs = {
            "host": self.host,
            "chain_id": self.chain_id
        }
        if self.private_key:
            client_kwargs["key"] = self.private_key
        if api_creds:
            client_kwargs["creds"] = api_creds
        
        self.client = ClobClient(**client_kwargs)
        
        if not api_creds and self.private_key:
            print("[TRADER] Deriving API credentials...")
            creds = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(creds)
            self.api_key = creds.api_key
            self.api_secret = creds.api_secret
            self.api_passphrase = creds.api_passphrase
        
        print("[TRADER] Polymarket client initialized")
    
    def _extract_token(self, t) -> Optional[str]:
        """提取token id，处理各种格式"""
        if isinstance(t, list):
            return self._extract_token(t[0]) if t else None
        return str(t).strip('"\' ')
    
    def _get_polymarket_market(self, symbol: str, interval: str, start_time_sec: int) -> Optional[Dict]:
        """通过slug获取Polymarket市场信息"""
        symbol_short = symbol.replace("USDT", "").lower()
        interval_min = interval.replace("m", "")
        slug = f"{symbol_short}-updown-{interval_min}m-{start_time_sec}"
        
        url = f"{self.GAMMA_API_BASE}/markets/slug/{slug}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                print(f"[TRADER] Failed to get market {slug}, status={resp.status_code}")
                return None
            
            data = resp.json()
            condition_id = data.get("conditionId")
            clob_tokens_raw = data.get("clobTokenIds", [])
            
            if isinstance(clob_tokens_raw, str):
                clob_tokens = json.loads(clob_tokens_raw)
            else:
                clob_tokens = clob_tokens_raw
            
            if len(clob_tokens) < 2:
                print(f"[TRADER] Not enough tokens for {slug}")
                return None
            
            yes_token_id = self._extract_token(clob_tokens[0])
            no_token_id = self._extract_token(clob_tokens[1])
            
            closed_time = data.get("closedTime", 0)
            
            print(f"[TRADER] Got market {slug}: YES={yes_token_id}, NO={no_token_id}, condition={condition_id}")
            
            return {
                "yes_token_id": yes_token_id,
                "no_token_id": no_token_id,
                "condition_id": condition_id,
                "slug": slug,
                "closed_time": closed_time
            }
        
        except Exception as e:
            print(f"[TRADER] Error getting market {slug}: {e}")
            return None
    
    def _on_kline_message(self, ws, message):
        """处理 Binance WebSocket 消息"""
        try:
            data = json.loads(message)
            if "data" not in data:
                return
            
            k = data["k"]
            symbol = k["s"]
            interval = k["i"]
            start_time = k["t"]  # 开始时间 ms
            end_time = k["T"]  # 结束时间 ms
            open_price = float(k["o"])
            close_price = float(k["c"])
            is_closed = bool(k["x"])
            
            interval_key = (symbol, interval)
            
            with self._lock:
                # 新开窗口
                if (symbol, interval) in self.kline_cache:
                    existing = self.kline_cache.get((symbol, interval))
                    if existing and existing["start_time"] != start_time:
                        # 窗口已关闭，新开窗口
                        start_time_sec = start_time // 1000
                        market = self._get_polymarket_market(symbol, interval, start_time_sec)
                        if market:
                            self.kline_cache[(symbol, interval)] = {
                                "start_time": start_time,
                                "end_time": end_time,
                                "open": open_price,
                                "current_close": close_price,
                                "triggered": False,
                                **market
                            }
                else:
                    # 第一个数据，初始化
                    start_time_sec = start_time // 1000
                    market = self._get_polymarket_market(symbol, interval, start_time_sec)
                    if market:
                        self.kline_cache[(symbol, interval)] = {
                            "start_time": start_time,
                            "end_time": end_time,
                            "open": open_price,
                            "current_close": close_price,
                            "triggered": False,
                            **market
                        }
            
                # 更新当前价格
                if (symbol, interval) in self.kline_cache:
                    self.kline_cache[(symbol, interval)]["current_close"] = close_price
        
        except Exception as e:
            print(f"[TRADER] Error processing kline: {e}")
    
    def _check_and_trade(self):
        """定时检查，满足条件收集订单，批量提交"""
        pending_orders: List[Tuple[str, float, float]] = []  # [(token_id, price, size), ...]
        
        with self._lock:
            now = int(time.time() * 1000)  # ms
            
            for (symbol, interval), kline in self.kline_cache.items():
                # 是否已经下单
                if kline.get("triggered", False):
                    continue
                
                end_time = kline["end_time"]
                trigger_before_ms = self.trigger_before_seconds * 1000
                time_to_end = end_time - now
                
                # 是否到触发时间：还剩 N 秒以内结束
                if 0 < time_to_end <= trigger_before_ms:
                    open_price = kline["open"]
                    current_close = kline["current_close"]
                    
                    if open_price == 0:
                        continue
                    
                    spread = (current_close - open_price) / open_price
                    spread_abs = abs(spread)
                    
                    print(f"[TRADER] Checking {symbol} {interval}: spread={spread:.4f} threshold={self.spread_threshold}")
                    
                    if spread_abs >= self.spread_threshold:
                        # 获取 token
                        token_id = None
                        if spread > 0:
                            # 上涨 → 买 YES (UP)
                            token_id = kline["yes_token_id"]
                        else:
                            # 下跌 → 买 NO (DOWN)
                            token_id = kline["no_token_id"]
                        
                        # 获取当前中间价
                        try:
                            mid_price = self.client.get_midpoint(token_id)
                            # 计算 size = order_amount / price  (Polymarket 计价方式：size = USD / price)
                            size = self.order_amount / mid_price
                            pending_orders.append((token_id, mid_price, size))
                            
                            direction = "UP (YES)" if spread > 0 else "DOWN (NO)"
                            print(f"[TRADER] Pending {symbol} {interval} → {direction} @ {mid_price:.3f} size={size:.2f}")
                        
                        except Exception as e:
                            print(f"[TRADER] Failed to get midprice for {token_id}: {e}")
                        
                        # 标记已下单
                        kline["triggered"] = True
        
        # 批量提交所有待下单
        if pending_orders:
            print(f"[TRADER] Batch placing {len(pending_orders)} GTC orders...")
            self._batch_place_gtc_orders(pending_orders)
    
    def _batch_place_gtc_orders(self, pending_orders: List[Tuple[str, float, float]]):
        """批量提交多个 GTC 限价单"""
        try:
            signed_orders = []
            for (token_id, price, size) in pending_orders:
                order_args = OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=size,
                    side=BUY
                )
                signed = self.client.create_order(order_args)
                signed_orders.append(signed)
            
            if not signed_orders:
                return []
            
            resp = self.client.post_orders(signed_orders, OrderType.GTC)
            for i, order_resp in enumerate(resp):
                token_id, price, size = pending_orders[i]
                print(f"[TRADER] GTC order placed: token={token_id} price={price:.3f} size={size:.2f} → {order_resp}")
            
            return resp
        
        except Exception as e:
            print(f"[TRADER] Error batch placing orders: {e}")
            return None
    
    def _on_open(self, ws):
        print("[BINANCE WS] Connected")
    
    def _on_error(self, ws, error):
        print(f"[BINANCE WS] Error: {error}")
    
    def _on_close(self, ws):
        print("[BINANCE WS] Disconnected")
        if self.running:
            print("[BINANCE WS] Reconnecting...")
            time.sleep(1)
            self.start()
    
    def start(self):
        """启动交易机器人"""
        # 构建 Binance WebSocket 端点
        # wss://stream.binance.com:9443/stream?streams=...
        streams = []
        for symbol in self.SYMBOLS:
            for interval in self.INTERVALS:
                streams.append(f"{symbol.lower()}@kline_{interval}")
        
        ws_url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
        print(f"[TRADER] Connecting to Binance WebSocket: {len(streams)} streams")
        print(f"[TRADER] Checking interval: {self.check_interval}s")
        
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self._on_open,
            on_message=self._on_kline_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        # 启动 WebSocket 线程
        ws_thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        ws_thread.start()
        
        # 主线程定时检查交易
        print(f"[TRADER] Started checking for trading opportunities every {self.check_interval} seconds")
        try:
            while self.running:
                time.sleep(self.check_interval)
                self._check_and_trade()
        except KeyboardInterrupt:
            print("\n[TRADER] Stopping...")
            self.stop()
    
    def stop(self):
        """停止机器人"""
        self.running = False
        if self.ws:
            self.ws.close()
        print("[TRADER] Stopped")


def main():
    trader = AutoTrader()
    trader.start()


if __name__ == "__main__":
    main()
