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
- 不需要手动更新配置文件，自动换周��
"""
import os
import json
import time
import sqlite3
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
    """
    
    # 币种列表 (不含USDT后缀)
    SYMBOLS = [
        "BTC", "ETH", "SOL", "XRP", "BNB", "DOGE"
    ]

    def __init__(
        self,
        db_path: str = "polymarket_data.db",
        sample_interval: int = 60,  # 采样间隔（秒），每个5分钟窗口采样多次
    ):
        load_dotenv()
        
        self.db_path = db_path
        self.sample_interval = sample_interval
        
        # 初始化数据库
        self._init_db()
        
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
            
            if len(clob_tokens) < 2:
                print(f"[RECORDER] Not enough tokens for {slug}")
                return None
            
            # 第一个token是 UP (YES), 第二个是 DOWN (NO)
            return {
                "yes_token_id": clob_tokens[0],
                "no_token_id": clob_tokens[1],
                "condition_id": condition_id,
                "event_slug": slug
            }
            
        except Exception as e:
            print(f"[RECORDER] Error querying market {slug}: {e}")
            return None
    
    def _init_db(self) -> None:
        """初始化 SQLite 数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建窗口表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_windows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                condition_id TEXT,
                yes_token_id TEXT,
                no_token_id TEXT,
                start_time INTEGER NOT NULL,
                end_time INTEGER NOT NULL,
                strike_price REAL NOT NULL,
                final_result TEXT,
                created_at INTEGER NOT NULL,
                UNIQUE(symbol, start_time)
            )
        """)
        
        # 创建采样数据表（每个窗口多次采样）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                binance_price REAL NOT NULL,
                yes_mid_price REAL,
                no_mid_price REAL,
                yes_best_bid REAL,
                yes_best_ask REAL,
                no_best_bid REAL,
                no_best_ask REAL,
                FOREIGN KEY(window_id) REFERENCES market_windows(id)
            )
        """)
        
        conn.commit()
        conn.close()
        print(f"[RECORDER] Database initialized at {self.db_path}")
    
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
        
        # 保存到数据库
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO market_windows
                (symbol, condition_id, yes_token_id, no_token_id, start_time, end_time, strike_price, final_result, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                active_window.get("condition_id"),
                active_window.get("yes_token_id"),
                active_window.get("no_token_id"),
                active_window["start_time"],
                kline.close_time,
                active_window["strike_price"],
                final_result,
                int(time.time())
            ))
            
            window_id = cursor.lastrowid
            conn.commit()
            
            print(f"[RECORDER] Window closed for {symbol}: {final_result} | strike={active_window['strike_price']:.2f} close={kline.close:.2f} (window_id={window_id})")
        
        except Exception as e:
            print(f"[RECORDER] Error saving window: {e}")
            conn.rollback()
        
        conn.close()
        
        # 清除活跃窗口，下一个K线会开始新窗口
        del self.active_windows[symbol]
    
    def _on_binance_kline_update(self, kline: KlineData) -> None:
        """Binance K线更新，更新最新价格缓存"""
        symbol = kline.symbol
        self.latest_binance_prices[symbol] = kline.close
        
        # 如果是新窗口第一个更新，启动窗口
        if not self.active_windows.get(symbol) and kline.start_time == kline.timestamp // 1000 * 1000:
            self._start_new_window(symbol, kline.start_time * 1000, kline.open)
    
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
            "slug": slug
        }
        
        print(f"[RECORDER] New window started for {symbol}:")
        print(f"  start={datetime.fromtimestamp(start_time_ms/1000)}")
        print(f"  strike={strike_price:.2f}")
        print(f"  YES token={yes_token}")
        print(f"  NO token={no_token}")
    
    def _on_polymarket_price_update(self, update: PriceUpdate) -> None:
        """Polymarket 价格更新"""
        token_id = update.token_id
        
        # 找出属于哪个币种对
        symbol = None
        side = None
        for sym, cfg in self.market_config.items():
            if cfg.get("yes_token_id") == token_id:
                symbol = sym
                side = "yes"
                break
            if cfg.get("no_token_id") == token_id:
                symbol = sym
                side = "no"
                break
        
        if not symbol:
            return
        
        if side == "yes":
            self.latest_yes_prices[token_id] = (update.best_bid + update.best_ask) / 2
        else:
            self.latest_no_prices[token_id] = (update.best_bid + update.best_ask) / 2
    
    def _sample_loop(self) -> None:
        """定时采样循环"""
        while True:
            # 对每个活跃窗口进行采样
            for symbol, active_window in self.active_windows.items():
                yes_token = active_window.get("yes_token_id")
                no_token = active_window.get("no_token_id")
                
                binance_price = self.latest_binance_prices.get(symbol)
                if binance_price is None:
                    continue
                
                # 获取最新价格
                yes_mid = None
                no_mid = None
                yes_bid = None
                yes_ask = None
                no_bid = None
                no_ask = None
                
                # 如果 WebSocket 没有更新，通过 REST API 获取
                try:
                    if yes_token:
                        if yes_token in self.latest_yes_prices:
                            yes_mid = self.latest_yes_prices[yes_token]
                        else:
                            yes_mid = self.clob_client.get_midpoint(yes_token)
                        
                        price_resp = self.clob_client.get_price(yes_token, "BUY")
                        yes_ask = float(price_resp)
                        price_resp = self.clob_client.get_price(yes_token, "SELL")
                        yes_bid = float(price_resp)
                except Exception as e:
                    print(f"[RECORDER] Error fetching {symbol} YES price: {e}")
                
                try:
                    if no_token:
                        if no_token in self.latest_no_prices:
                            no_mid = self.latest_no_prices[no_token]
                        else:
                            no_mid = self.clob_client.get_midpoint(no_token)
                        
                        price_resp = self.clob_client.get_price(no_token, "BUY")
                        no_ask = float(price_resp)
                        price_resp = self.clob_client.get_price(no_token, "SELL")
                        no_bid = float(price_resp)
                except Exception as e:
                    print(f"[RECORDER] Error fetching {symbol} NO price: {e}")
                
                # 保存采样到数据库
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # 先获取 window_id
                cursor.execute("""
                    SELECT id FROM market_windows WHERE symbol=? AND start_time=?
                """, (symbol, active_window["start_time"]))
                res = cursor.fetchone()
                if res:
                    window_id = res[0]
                else:
                    # 插入新窗口
                    cursor.execute("""
                        INSERT INTO market_windows
                        (symbol, condition_id, yes_token_id, no_token_id, start_time, end_time, strike_price, final_result, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol,
                        active_window.get("condition_id"),
                        yes_token,
                        no_token,
                        active_window["start_time"],
                        0,
                        active_window["strike_price"],
                        None,
                        int(time.time())
                    ))
                    window_id = cursor.lastrowid
                    conn.commit()
                
                # 插入采样
                try:
                    cursor.execute("""
                        INSERT INTO samples
                        (window_id, timestamp, binance_price, yes_mid_price, no_mid_price, yes_best_bid, yes_best_ask, no_best_bid, no_best_ask)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        window_id,
                        int(time.time() * 1000),
                        binance_price,
                        yes_mid,
                        no_mid,
                        yes_bid,
                        yes_ask,
                        no_bid,
                        no_ask
                    ))
                    conn.commit()
                except Exception as e:
                    print(f"[RECORDER] Error inserting sample: {e}")
                    conn.rollback()
                
                conn.close()
            
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
        self.polymarket_ws.subscribe_markets(all_tokens)
        self.polymarket_ws.start_heartbeat()
        
        # 启动采样循环
        print("[RECORDER] Starting sampling...")
        import threading
        sample_thread = threading.Thread(target=self._sample_loop, daemon=True)
        sample_thread.start()
        
        print("\n[RECORDER] All started! Recording data...")
    
    def export_to_csv(self, output_path: str = "polymarket_data_export.csv") -> None:
        """导出所有数据到CSV文件"""
        import csv
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 连接窗口和采样数据
        cursor.execute("""
            SELECT 
                mw.id, mw.symbol, mw.start_time, mw.end_time, mw.strike_price, 
                mw.final_result, s.timestamp, s.binance_price, 
                s.yes_mid_price, s.no_mid_price, 
                s.yes_best_bid, s.yes_best_ask, s.no_best_bid, s.no_best_ask
            FROM market_windows mw
            LEFT JOIN samples s ON mw.id = s.window_id
            ORDER BY mw.start_time DESC, s.timestamp ASC
        """)
        
        columns = [desc[0] for desc in cursor.description]
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(cursor)
        
        conn.close()
        print(f"[RECORDER] Data exported to {output_path}")


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