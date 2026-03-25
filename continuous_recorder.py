#!/usr/bin/env python3
"""
持续记录器 - 按照要求设计:
- Binance WebSocket 线程 → 更新内存币种价格缓存
- Polymarket WebSocket 线程 → 更新内存 YES/NO 价格缓存
- 主线程每隔 0.5 秒读取一次内存缓存 → 追加写入对应窗口文件

存储结构:
- data/{symbol}/ 每个币种单独目录
- data/{symbol}/{slug}.jsonl 每个窗口一个文件，slug = {symbol_short}-updown-5m-{start_timestamp}
每一行是一次采样（每隔 0.5 秒一行）
"""
import os
import json
import time
import threading
import requests
from typing import Dict, Optional
from dotenv import load_dotenv

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
from binance_ws_client import BinanceFuturesWebsocketClient, KlineData
from websocket_client import PolymarketWebsocketClient, PriceUpdate


# 支持的币种列表 (Binance 符号)
SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT", 
    "SOLUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "DOGEUSDT"
]


class ContinuousPriceRecorder:
    def __init__(
        self,
        data_root: str = "data",
        write_interval: float = 0.5,  # 写入间隔，秒
    ):
        load_dotenv()
        
        self.data_root = data_root
        self.write_interval = write_interval
        
        # 创建根数据目录
        os.makedirs(self.data_root, exist_ok=True)
        
        # 为每个币种创建目录
        for symbol in SYMBOLS:
            symbol_dir = os.path.join(self.data_root, symbol.lower())
            os.makedirs(symbol_dir, exist_ok=True)
        
        # 价格缓存 (内存共享，多个WS线程更新)
        # 结构: {
        #   "BTCUSDT": {
        #       "current_slug": "btc-updown-5m-1774402000",
        #       "binance_price": float,
        #       "yes_best_bid": float,
        #       "yes_best_ask": float,
        #       "yes_mid": float,
        #       "no_best_bid": float,
        #       "no_best_ask": float, 
        #       "no_mid": float,
        #       "yes_token_id": str,
        #       "no_token_id": str,
        #       "updated_at_ms": int
        #   }
        # }
        self.price_cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()  # 缓存读写锁
        
        # 初始化缓存
        for symbol in SYMBOLS:
            # 计算当前窗口slug
            current_window_start = (int(time.time()) // 300) * 300
            symbol_short = symbol.replace("USDT", "").lower()
            current_slug = f"{symbol_short}-updown-5m-{current_window_start}"
            
            self.price_cache[symbol] = {
                "current_slug": current_slug,
                "binance_price": None,
                "yes_best_bid": None,
                "yes_best_ask": None,
                "yes_mid": None,
                "no_best_bid": None,
                "no_best_ask": None,
                "no_mid": None,
                "yes_token_id": None,
                "no_token_id": None,
                "updated_at_ms": 0
            }
            
            # 预创建窗口文件
            symbol_dir = os.path.join(self.data_root, symbol.lower())
            output_path = os.path.join(symbol_dir, f"{current_slug}.jsonl")
            if not os.path.exists(output_path):
                open(output_path, "w").close()
        
        # 初始化 Polymarket 客户端
        self.host = os.getenv("CLOB_API_URL", "https://clob.polymarket.com")
        self.private_key = os.getenv("PK")
        self.chain_id = int(os.getenv("CHAIN_ID", POLYGON))
        self.api_key = os.getenv("CLOB_API_KEY")
        self.api_secret = os.getenv("CLOB_SECRET")
        self.api_passphrase = os.getenv("CLOB_PASS_PHRASE")
        
        self.clob_client: Optional[ClobClient] = None
        self._init_clob()
        
        # WebSocket 客户端
        self.binance_ws: Optional[BinanceFuturesWebsocketClient] = None
        self.polymarket_ws: Optional[PolymarketWebsocketClient] = None
        
        # token 映射: token_id -> (symbol, side)
        self._token_map: Dict[str, tuple[str, str]] = {}
        
        # 运行标志
        self.running = True
    
    def _init_clob(self) -> None:
        """初始化 Clob 客户端"""
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
        
        self.clob_client = ClobClient(**client_kwargs)
        
        if not api_creds and self.private_key:
            print("[RECORDER] Deriving API credentials...")
            creds = self.clob_client.create_or_derive_api_creds()
            self.clob_client.set_api_creds(creds)
            self.api_key = creds.api_key
            self.api_secret = creds.api_secret
            self.api_passphrase = creds.api_passphrase
        
        print("[RECORDER] Polymarket client initialized")
    
    def _bind_current_window_tokens(self) -> None:
        """绑定当前窗口token ID到缓存"""
        GAMMA_API_BASE = "https://gamma-api.polymarket.com"
        
        current_window_start = (int(time.time()) // 300) * 300
        
        for symbol in SYMBOLS:
            symbol_short = symbol.replace("USDT", "").lower()
            slug = f"{symbol_short}-updown-5m-{current_window_start}"
            
            try:
                url = f"{GAMMA_API_BASE}/events/slug/{slug}"
                resp = requests.get(url, timeout=10)
                if resp.status_code != 200:
                    print(f"[RECORDER] Failed to get {slug}, status={resp.status_code}")
                    continue
                
                data = resp.json()
                if not data.get("markets"):
                    print(f"[RECORDER] No markets for {slug}")
                    continue
                
                market = data["markets"][0]
                clob_tokens_raw = market.get("clobTokenIds", [])
                
                # 处理各种格式
                if isinstance(clob_tokens_raw, str):
                    clob_tokens = json.loads(clob_tokens_raw)
                else:
                    clob_tokens = clob_tokens_raw
                
                def extract_token(t):
                    if isinstance(t, list):
                        return extract_token(t[0]) if t else None
                    return str(t).strip('"\' ')
                
                yes_token = None
                no_token = None
                if len(clob_tokens) >= 2:
                    yes_token = extract_token(clob_tokens[0])
                    no_token = extract_token(clob_tokens[1])
                
                # 更新缓存
                with self._lock:
                    self.price_cache[symbol]["yes_token_id"] = yes_token
                    self.price_cache[symbol]["no_token_id"] = no_token
                    self.price_cache[symbol]["current_slug"] = slug
                
                # 映射token
                if yes_token:
                    self._token_map[yes_token] = (symbol, "yes")
                if no_token:
                    self._token_map[no_token] = (symbol, "no")
                
                # 创建新文件
                symbol_dir = os.path.join(self.data_root, symbol.lower())
                output_path = os.path.join(symbol_dir, f"{slug}.jsonl")
                if not os.path.exists(output_path):
                    open(output_path, "w").close()
                
                print(f"[RECORDER] {symbol}: new window {slug}, YES={yes_token}, NO={no_token}")
                
            except Exception as e:
                print(f"[RECORDER] Error processing {symbol} new window: {e}")
    
    def _on_binance_kline_update(self, kline: KlineData) -> None:
        """Binance K线更新 → 更新内存缓存，检查是否新开窗口"""
        symbol = kline.symbol
        current_time_sec = int(time.time())
        current_window_start = (current_time_sec // 300) * 300
        
        with self._lock:
            # 更新价格
            self.price_cache[symbol]["binance_price"] = kline.close
            self.price_cache[symbol]["updated_at_ms"] = int(time.time() * 1000)
            
            # 检查是否新开窗口
            cached_slug = self.price_cache[symbol]["current_slug"]
            cached_window_start = int(cached_slug.split("-")[-1])
            if current_window_start > cached_window_start:
                # 新开窗口，异步获取token
                print(f"[RECORDER] {symbol}: new window starting {current_window_start}")
                # 释放锁后再绑定
            pass
        
        # 新开窗口需要重新获取token，在主线程处理
        if current_window_start > int(cached_slug.split("-")[-1]):
            self._bind_current_window_tokens()
    
    def _handle_polymarket_update(self, update: PriceUpdate) -> None:
        """处理 Polymarket 价格更新 → 更新内存缓存"""
        token_id = update.token_id
        mapping = self._token_map.get(token_id)
        if not mapping:
            return
        
        symbol, side = mapping
        mid = (update.best_bid + update.best_ask) / 2
        
        with self._lock:
            if symbol in self.price_cache:
                if side == "yes":
                    self.price_cache[symbol]["yes_best_bid"] = update.best_bid
                    self.price_cache[symbol]["yes_best_ask"] = update.best_ask
                    self.price_cache[symbol]["yes_mid"] = mid
                else:
                    self.price_cache[symbol]["no_best_bid"] = update.best_bid
                    self.price_cache[symbol]["no_best_ask"] = update.best_ask
                    self.price_cache[symbol]["no_mid"] = mid
    
    def _start_ws(self) -> None:
        """启动两个 WebSocket"""
        # 1. 绑定当前窗口token
        self._bind_current_window_tokens()
        
        # 2. 启动 Binance WebSocket
        print("[RECORDER] Starting Binance WebSocket...")
        self.binance_ws = BinanceFuturesWebsocketClient(
            on_kline_update=self._on_binance_kline_update,
            on_kline_closed=lambda k: None
        )
        self.binance_ws.batch_subscribe_5m(SYMBOLS)
        self.binance_ws.start()
        
        # 3. 启动 Polymarket WebSocket
        print("[RECORDER] Starting Polymarket WebSocket...")
        all_tokens = list(self._token_map.keys())
        self.polymarket_ws = PolymarketWebsocketClient(
            api_key=self.api_key,
            api_secret=self.api_secret,
            api_passphrase=self.api_passphrase,
            on_price_change=self._handle_polymarket_update
        )
        if all_tokens:
            self.polymarket_ws.subscribe_markets(all_tokens)
        self.polymarket_ws.start_heartbeat()
        
        print("[RECORDER] Both WebSocket connected")
    
    def start(self) -> None:
        """启动记录器"""
        # 启动 WebSocket
        self._start_ws()
        
        print(f"[RECORDER] Started continuous recording, root: {self.data_root}")
        print(f"[RECORDER] Writing every {self.write_interval} seconds to window files")
        
        # 主线程循环写入
        while self.running:
            time.sleep(self.write_interval)
            
            # 按币种写入各自窗口文件
            with self._lock:
                for symbol, cache in self.price_cache.items():
                    slug = cache["current_slug"]
                    symbol_dir = os.path.join(self.data_root, symbol.lower())
                    output_path = os.path.join(symbol_dir, f"{slug}.jsonl")
                    
                    # 创建采样记录
                    sample = {
                        "timestamp_ms": int(time.time() * 1000),
                        "binance_price": cache["binance_price"],
                        "yes_best_bid": cache["yes_best_bid"],
                        "yes_best_ask": cache["yes_best_ask"],
                        "yes_mid": cache["yes_mid"],
                        "no_best_bid": cache["no_best_bid"],
                        "no_best_ask": cache["no_best_ask"],
                        "no_mid": cache["no_mid"],
                    }
                    
                    # 追加写入
                    try:
                        with open(output_path, "a", encoding="utf-8") as f:
                            f.write(json.dumps(sample) + "\n")
                    except Exception as e:
                        print(f"[RECORDER] Write error for {symbol}/{slug}: {e}")
    
    def stop(self) -> None:
        """停止记录器"""
        self.running = False
        if self.binance_ws:
            self.binance_ws.close()
        if self.polymarket_ws:
            self.polymarket_ws.close_all()
        print("[RECORDER] Stopped")


def main():
    recorder = ContinuousPriceRecorder()
    recorder.start()
    
    try:
        while recorder.running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[RECORDER] Stopping...")
        recorder.stop()


if __name__ == "__main__":
    main()
