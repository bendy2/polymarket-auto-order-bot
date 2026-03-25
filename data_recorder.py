"""
Polymarket 加密货币 5 分钟 UP/DOWN 市场数据记录器
用于记录每个周期的市场数据供日后回测策略使用

记录内容:
- 市场窗口开始/结束时间
- 窗口行权价格（窗口开始时币种价格）
- YES/NO 代币实时中间价（定时采样）
- 对应币种实时 Binance 价格（定时采样）
- 最终结果（UP/DOWN）

自动处理:
- 每个 5 分钟窗口新开自动生成 slug 查询 Gamma API 获取 token_id
- 不需要手动更新配置文件，自动切换周期
- 每个币种所有窗口保存在一个 JSONL 文件，每个窗口一行
"""
import os
import json
import time
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
from binance_ws_client import BinanceFuturesWebsocketClient, KlineData
from websocket_client import PolymarketWebsocketClient, PriceUpdate


GAMMA_API_BASE = "https://gamma-api.polymarket.com"


class MarketDataRecorder:
    """
    5分钟 UP/DOWN 市场数据记录器
    对应 Binance 5分钟K线收盘，记录 Polymarket 数据和最终结果
    自动获取每个新窗口的 slug/token_id/condition_id
    每个币种保存为单独的 JSONL 文件，每个窗口一行
    """
    
    # 币种列表 (不含USDT后缀)
    SYMBOLS = [
        "BTC", "ETH", "SOL", "XRP", "BNB", "DOGE"
    ]

    def __init__(
        self,
        data_dir: str = "data",
        sample_interval: int = 60,  # 采样间隔（秒），每个5分钟窗口采样多次
    ):
        load_dotenv()
        
        self.data_dir = data_dir
        self.sample_interval = sample_interval
        
        # 创建数据目录
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 初始化 Polymarket 客户端
        self.host = os.getenv("CLOB_API_URL", "https://clob.polymarket.com")
        self.private_key = os.getenv("PK")
        self.chain_id = int(os.getenv("CHAIN_ID", POLYGON))
        self.api_key = os.getenv("CLOB_API_KEY")
        self.api_secret = os.getenv("CLOB_SECRET")
        self.api_passphrase = os.getenv("CLOB_PASS_PHRASE")
        
        self.clob_client: Optional[ClobClient] = None
        self._init_clob()
        
        # 当前活跃窗口数据
        self.active_windows: Dict[str, Dict] = {}
        
        # Binance WS 客户端
        self.binance_client: Optional[BinanceFuturesWebsocketClient] = None
        
        # Polymarket WS 客户端
        self.polymarket_ws: Optional[PolymarketWebsocketClient] = None
        
        # 最新价格缓存
        self.latest_binance_prices: Dict[str, float] = {}
        self.latest_yes_prices: Dict[str, float] = {}
        self.latest_no_prices: Dict[str, float] = {}
    
    # ============ 自动获取市场信息 ============
    
    def generate_slug(self, symbol: str, start_timestamp: int) -> str:
        """生成 slug: {币名小写}-updown-5m-{开始时间戳(秒)}"""
        symbol_short = symbol.replace("USDT", "").lower()
        return f"{symbol_short}-updown-5m-{start_timestamp}"
    
    def get_market_info_by_slug(self, slug: str) -> Optional[Dict]:
        """通过slug查询市场信息，获取token_id"""
        url = f"{GAMMA_API_BASE}/events/slug/{slug}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                print(f"[RECORDER] Failed to get market {slug}, status={resp.status_code}")
                return None
            
            data = resp.json()
            if not data.get("markets"):
                print(f"[RECORDER] No markets found for {slug}")
                return None
            
            # 提取token id
            market = data["markets"][0]
            condition_id = market.get("conditionId")
            clob_tokens = market.get("clobTokenIds", [])
            
            # 处理Gamma API返回的各种格式
            # 格式1: ["id1", "id2"]
            # 格式2: [["id1"], ["id2"]]
            # 格式3: 直接字符串
            def extract_token(t):
                if isinstance(t, list):
                    return extract_token(t[0]) if t else None
                return str(t)
            
            if len(clob_tokens) >= 2:
                yes_token_id = extract_token(clob_tokens[0])
                no_token_id = extract_token(clob_tokens[1])
            else:
                print(f"[RECORDER] Not enough tokens for {slug}, got {len(clob_tokens)}")
                print(f"[RECORDER] Raw clob_tokens: {clob_tokens}")
                return None
            
            if not yes_token_id or not no_token_id:
                print(f"[RECORDER] Failed to extract token IDs for {slug}")
                return None
            
            # 第一个token是 UP (YES), 第二个是 DOWN (NO)
            return {
                "yes_token_id": yes_token_id,
                "no_token_id": no_token_id,
                "condition_id": condition_id,
                "event_slug": slug
            }
            
        except Exception as e:
            print(f"[RECORDER] Error querying market {slug}: {e}")
            return None
    
    def _get_output_path(self, symbol: str) -> str:
        """获取对应币种的JSONL输出路径"""
        return os.path.join(self.data_dir, f"{symbol.lower()}_windows.jsonl")
    
    def _append_window_to_jsonl(self, window_data: Dict) -> None:
        """将完整窗口追加到JSONL文件，每个窗口一行"""
        symbol = window_data["symbol"]
        path = self._get_output_path(symbol)
        
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(window_data) + "\n")
        
        print(f"[RECORDER] Window saved to {path}")
    
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
    
    def _on_binance_kline_closed(self, kline: KlineData) -> None:
        """Binance K线收盘，处理 Polymarket 窗口结束"""
        symbol = kline.symbol
        # 获取当前活跃窗口
        active_window = self.active_windows.get(symbol)
        if not active_window:
            print(f"[RECORDER] No active window for {symbol}, skipping")
            return
        
        # 窗口结束，计算最终结果
        # UP = 收盘价 > 行权价
        # DOWN = 收盘价 <= 行权价
        final_result = "UP" if kline.close > active_window["strike_price"] else "DOWN"
        
        # 组装完整窗口数据
        window_data = {
            "symbol": symbol,
            "condition_id": active_window.get("condition_id"),
            "yes_token_id": active_window.get("yes_token_id"),
            "no_token_id": active_window.get("no_token_id"),
            "slug": active_window.get("slug"),
            "start_time_ms": active_window["start_time"],
            "end_time_ms": kline.close_time,
            "strike_price": active_window["strike_price"],
            "final_close_price": kline.close,
            "final_result": final_result,
            "samples": active_window.get("samples", []),
            "created_at_ms": int(time.time() * 1000)
        }
        
        # 保存到 JSONL
        self._append_window_to_jsonl(window_data)
        
        print(f"[RECORDER] Window closed for {symbol}: {final_result} | "
              f"strike={active_window['strike_price']:.2f} close={kline.close:.2f}")
        
        # 清除活跃窗口，下一个K线会开始新窗口
        del self.active_windows[symbol]
    
    def _on_binance_kline_update(self, kline: KlineData) -> None:
        """Binance K线更新，更新最新价格缓存"""
        symbol = kline.symbol
        self.latest_binance_prices[symbol] = kline.close
        
        # 如果是新窗口第一个更新，启动窗口 (Kline.start_time 是对齐过的整5分钟)
        current_time_sec = int(time.time())
        if not self.active_windows.get(symbol) and kline.start_time == (current_time_sec // 300) * 300 * 1000:
            self._start_new_window(symbol, kline.start_time, kline.open)
    
    def _start_new_window(self, symbol: str, start_time_ms: int, strike_price: float) -> None:
        """开始一个新的5分钟窗口，自动查询slug获取token信息"""
        start_time_sec = int(start_time_ms / 1000)
        slug = self.generate_slug(symbol, start_time_sec)
        
        print(f"[RECORDER] New window for {symbol}, querying {slug}...")
        market_info = self.get_market_info_by_slug(slug)
        
        if not market_info:
            print(f"[RECORDER] Failed to get market info for {symbol}, skipping window")
            return
        
        # 获取token，订阅Polymarket价格更新
        yes_token = market_info["yes_token_id"]
        no_token = market_info["no_token_id"]
        
        # 如果Polymarket WS已经连接，订阅新token
        if self.polymarket_ws and self.polymarket_ws.market_connected:
            self.polymarket_ws.subscribe_markets([yes_token, no_token])
        
        self.active_windows[symbol] = {
            "start_time": start_time_ms,
            "strike_price": strike_price,
            "yes_token_id": yes_token,
            "no_token_id": no_token,
            "condition_id": market_info["condition_id"],
            "slug": slug,
            "samples": []
        }
        
        print(f"[RECORDER] New window started for {symbol}:")
        print(f"  start={datetime.fromtimestamp(start_time_ms/1000)} UTC")
        print(f"  strike={strike_price:.2f}")
        print(f"  YES token={str(yes_token)}")
        print(f"  NO token={str(no_token)}")
    
    def _on_polymarket_price_update(self, update: PriceUpdate) -> None:
        """Polymarket 价格更新"""
        token_id = update.token_id
        
        # 更新价格缓存
        mid_price = (update.best_bid + update.best_ask) / 2
        # 查找当前活跃窗口
        for symbol, active_window in self.active_windows.items():
            if active_window.get("yes_token_id") == token_id:
                self.latest_yes_prices[token_id] = mid_price
                return
            if active_window.get("no_token_id") == token_id:
                self.latest_no_prices[token_id] = mid_price
                return
    
    def _sample_loop(self) -> None:
        """定时采样循环"""
        while True:
            # 对每个活跃窗口进行采样
            for symbol, active_window in list(self.active_windows.items()):
                yes_token = active_window.get("yes_token_id")
                no_token = active_window.get("no_token_id")
                
                binance_price = self.latest_binance_prices.get(symbol)
                if binance_price is None:
                    continue
                
                # 获取最新价格
                sample = {
                    "timestamp_ms": int(time.time() * 1000),
                    "binance_price": binance_price,
                    "yes_mid_price": None,
                    "no_mid_price": None,
                    "yes_best_bid": None,
                    "yes_best_ask": None,
                    "no_best_bid": None,
                    "no_best_ask": None
                }
                
                # 如果 WebSocket 没有更新，通过 REST API 获取
                try:
                    if yes_token:
                        if yes_token in self.latest_yes_prices:
                            sample["yes_mid_price"] = self.latest_yes_prices[yes_token]
                        else:
                            sample["yes_mid_price"] = self.clob_client.get_midpoint(yes_token)
                        
                        price_resp = self.clob_client.get_price(yes_token, "BUY")
                        sample["yes_best_ask"] = float(price_resp)
                        price_resp = self.clob_client.get_price(yes_token, "SELL")
                        sample["yes_best_bid"] = float(price_resp)
                except Exception as e:
                    print(f"[RECORDER] Error fetching {symbol} YES price: {e}")
                
                try:
                    if no_token:
                        if no_token in self.latest_no_prices:
                            sample["no_mid_price"] = self.latest_no_prices[no_token]
                        else:
                            sample["no_mid_price"] = self.clob_client.get_midpoint(no_token)
                        
                        price_resp = self.clob_client.get_price(no_token, "BUY")
                        sample["no_best_ask"] = float(price_resp)
                        price_resp = self.clob_client.get_price(no_token, "SELL")
                        sample["no_best_bid"] = float(price_resp)
                except Exception as e:
                    print(f"[RECORDER] Error fetching {symbol} NO price: {e}")
                
                # 添加采样到当前窗口
                active_window["samples"].append(sample)
            
            time.sleep(self.sample_interval)
    
    def start(self) -> None:
        """启动记录器"""
        # 启动 Binance WebSocket
        symbols_to_subscribe = [f"{s}USDT" for s in self.SYMBOLS]
        print(f"[RECORDER] Starting Binance WebSocket for: {symbols_to_subscribe}")
        
        self.binance_client = BinanceFuturesWebsocketClient(
            on_kline_update=self._on_binance_kline_update,
            on_kline_closed=self._on_binance_kline_closed
        )
        self.binance_client.batch_subscribe_5m(symbols_to_subscribe)
        self.binance_client.start()
        
        # 启动 Polymarket WebSocket 订阅价格
        print("[RECORDER] Starting Polymarket WebSocket for YES/NO prices")
        self.polymarket_ws = PolymarketWebsocketClient(
            api_key=self.api_key,
            api_secret=self.api_secret,
            api_passphrase=self.api_passphrase,
            on_price_change=self._on_polymarket_price_update
        )
        # 新token会动态订阅，启动时不用订阅
        self.polymarket_ws.start_heartbeat()
        
        # 启动采样循环
        print("[RECORDER] Starting sampling...")
        import threading
        sample_thread = threading.Thread(target=self._sample_loop, daemon=True)
        sample_thread.start()
        
        print("\n[RECORDER] All started! Recording data...")
        print(f"[RECORDER] Data will be saved to {self.data_dir}/<symbol>_windows.jsonl")


def main():
    recorder = MarketDataRecorder()
    recorder.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[RECORDER] Stopped")


if __name__ == "__main__":
    main()
