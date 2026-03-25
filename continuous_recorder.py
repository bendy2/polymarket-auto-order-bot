#!/usr/bin/env python3
"""
持续记录器 - 按照要求设计:
- Binance WebSocket 线程 → 更新内存币种价格缓存
- Polymarket WebSocket 线程 → 更新内存 YES/NO 价格缓存
- 主线程每隔 0.5 秒读取一次内存缓存 → 写入一条记录到 JSONL

每条记录包含:
- 时间戳
- 各个币种 Binance 实时价格
- 对应 Polymarket YES/NO 实时价格 (best_bid/best_ask/mid)
"""
import os
import json
import time
import threading
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
        output_file: str = "data/continuous_prices.jsonl",
        write_interval: float = 0.5,  # 写入间隔，秒
    ):
        load_dotenv()
        
        self.output_file = output_file
        self.write_interval = write_interval
        
        # 创建输出目录
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # 价格缓存 (内存共享，多个WS线程更新)
        # 结构: {
        #   "BTCUSDT": {
        #       "binance_price": float,
        #       "yes_best_bid": float,
        #       "yes_best_ask": float,
        #       "yes_mid": float,
        #       "no_best_bid": float,
        #       "no_best_ask": float, 
        #       "no_mid": float,
        #       "updated_at": int
        #   }
        # }
        self.price_cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()  # 缓存读写锁
        
        # 初始化缓存
        for symbol in SYMBOLS:
            self.price_cache[symbol] = {
                "binance_price": None,
                "yes_best_bid": None,
                "yes_best_ask": None,
                "yes_mid": None,
                "no_best_bid": None,
                "no_best_ask": None,
                "no_mid": None,
                "updated_at_ms": 0
            }
        
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
        
        # token 映射: token_id -> symbol
        self.token_to_symbol: Dict[str, str] = {}
        
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
    
    def _auto_get_tokens(self) -> None:
        """自动获取当前窗口所有币种的 token ID"""
        import requests
        GAMMA_API_BASE = "https://gamma-api.polymarket.com"
        
        current_window_start = (int(time.time()) // 300) * 300
        
        for symbol in SYMBOLS:
            symbol_short = symbol.replace("USDT", "").lower()
            slug = f"{symbol_short}-updown-5m-{current_window_start}"
            
            url = f"{GAMMA_API_BASE}/events/slug/{slug}"
            try:
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
                
                yes_token = extract_token(clob_tokens[0]) if len(clob_tokens) >= 2 else None
                no_token = extract_token(clob_tokens[1]) if len(clob_tokens) >= 2 else None
                
                if yes_token:
                    self.token_to_symbol[yes_token] = symbol
                if no_token:
                    self.token_to_symbol[no_token] = symbol
                
                print(f"[RECORDER] {symbol}: YES={yes_token}, NO={no_token}")
                
            except Exception as e:
                print(f"[RECORDER] Error getting token for {symbol}: {e}")
    
    def _on_binance_kline_update(self, kline: KlineData) -> None:
        """Binance K线更新 → 更新内存缓存"""
        symbol = kline.symbol
        with self._lock:
            if symbol in self.price_cache:
                self.price_cache[symbol]["binance_price"] = kline.close
                self.price_cache[symbol]["updated_at_ms"] = int(time.time() * 1000)
    
    def _on_polymarket_price_update(self, update: PriceUpdate) -> None:
        """Polymarket 价格更新 → 更新内存缓存"""
        token_id = update.token_id
        
        # 查找对应币种
        symbol = self.token_to_symbol.get(token_id)
        if not symbol:
            return
        
        mid = (update.best_bid + update.best_ask) / 2
        
        with self._lock:
            # 判断是 YES 还是 NO
            # 需要在启动时绑定token，这里通过当前窗口判断，第一个token是YES，第二个NO
            # 实际已经在auto_get_tokens完成映射，这里只需要更新
            if symbol in self.price_cache:
                # 通过token_id判断是YES还是NO
                # 我们在auto_get_tokens里已经建立token_to_symbol，但是没法区分YES/NO
                # 这里走一次查询确认
                for sym, info in self.price_cache.items():
                    pass
                # 直接更新对应字段：我们需要在token_to_symbol保存更多信息
                # 重新处理
                pass
    
    def _start_ws(self) -> None:
        """启动两个 WebSocket 线程"""
        # 1. 自动获取当前窗口 token ID
        print("[RECORDER] Fetching current window token IDs...")
        self._auto_get_tokens()
        
        # 重建 token_to_symbol 包含 YES/NO 信息
        # 我们需要重新映射 token -> (symbol, side)
        self._token_map: Dict[str, tuple[str, str]] = {}  # token_id -> (symbol, "yes"/"no")
        
        current_window_start = (int(time.time()) // 300) * 300
        import requests
        GAMMA_API_BASE = "https://gamma-api.polymarket.com"
        
        for symbol in SYMBOLS:
            symbol_short = symbol.replace("USDT", "").lower()
            slug = f"{symbol_short}-updown-5m-{current_window_start}"
            
            try:
                url = f"{GAMMA_API_BASE}/events/slug/{slug}"
                resp = requests.get(url, timeout=10)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                if not data.get("markets"):
                    continue
                
                market = data["markets"][0]
                clob_tokens_raw = market.get("clobTokenIds", [])
                
                if isinstance(clob_tokens_raw, str):
                    clob_tokens = json.loads(clob_tokens_raw)
                else:
                    clob_tokens = clob_tokens_raw
                
                def extract_token(t):
                    if isinstance(t, list):
                        return extract_token(t[0]) if t else None
                    return str(t).strip('"\' ')
                
                if len(clob_tokens) >= 2:
                    yes_token = extract_token(clob_tokens[0])
                    no_token = extract_token(clob_tokens[1])
                    if yes_token:
                        self._token_map[yes_token] = (symbol, "yes")
                    if no_token:
                        self._token_map[no_token] = (symbol, "no")
                    print(f"[RECORDER] Mapped {symbol}: YES={yes_token}, NO={no_token}")
                    
            except Exception as e:
                print(f"[RECORDER] Error mapping {symbol} tokens: {e}")
        
        # 2. 启动 Binance WebSocket
        print("[RECORDER] Starting Binance WebSocket...")
        self.binance_ws = BinanceFuturesWebsocketClient(
            on_kline_update=self._on_binance_kline_update,
            on_kline_closed=lambda k: None  # 不需要处理收盘
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
    
    def _handle_polymarket_update(self, update: PriceUpdate) -> None:
        """处理 Polymarket 价格更新"""
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
    
    def start(self) -> None:
        """启动记录器"""
        # 启动 WebSocket 线程
        self._start_ws()
        
        # 打开输出文件
        with open(self.output_file, "a", encoding="utf-8") as f:
            print(f"[RECORDER] Started continuous recording, output: {self.output_file}")
            print(f"[RECORDER] Writing every {self.write_interval} seconds")
            
            # 主线程循环写入
            while self.running:
                time.sleep(self.write_interval)
                
                # 拷贝缓存快照
                with self._lock:
                    snapshot = {
                        "timestamp_ms": int(time.time() * 1000),
                        "data": {
                            symbol: data.copy()
                            for symbol, data in self.price_cache.items()
                        }
                    }
                
                # 写入 JSONL 一行
                try:
                    f.write(json.dumps(snapshot) + "\n")
                    f.flush()
                except Exception as e:
                    print(f"[RECORDER] Write error: {e}")
    
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
