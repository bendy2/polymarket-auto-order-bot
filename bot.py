"""
Polymarket 自动下单机器人
支持：市价单/限价单、自动配置API密钥、订单管理、行情查询
"""
import os
import sys
import time
from typing import List, Optional, Dict
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    MarketOrderArgs, OrderArgs, OrderType, OpenOrderParams, ApiCreds
)
from py_clob_client.constants import POLYGON, AMOY
from py_clob_client.order_builder.constants import BUY, SELL


class PolymarketAutoBot:
    def __init__(self, env_file: str = ".env"):
        """
        初始化机器人
        :param env_file: 环境变量文件路径
        """
        load_dotenv(env_file)
        self.host = os.getenv("CLOB_API_URL", "https://clob.polymarket.com")
        self.private_key = os.getenv("PK")
        self.chain_id = int(os.getenv("CHAIN_ID", POLYGON))
        self.funder = os.getenv("FUNDER")
        self.signature_type = int(os.getenv("SIGNATURE_TYPE", 0))
        
        # API 凭据
        self.api_key = os.getenv("CLOB_API_KEY")
        self.api_secret = os.getenv("CLOB_SECRET")
        self.api_passphrase = os.getenv("CLOB_PASS_PHRASE")
        
        self.client = None
        self._init_client()
    
    def _init_client(self) -> None:
        """初始化 Clob 客户端"""
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
        if self.funder:
            client_kwargs["funder"] = self.funder
        if self.signature_type:
            client_kwargs["signature_type"] = self.signature_type
        
        self.client = ClobClient(**client_kwargs)
        
        # 如果没有预生成 API 凭据，自动创建/派生
        if not api_creds and self.private_key:
            print("[INFO] 正在创建/派生 API 凭据...")
            creds = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(creds)
            print(f"[INFO] API 凭据已设置: {creds.api_key}")
    
    def check_connection(self) -> bool:
        """检查服务器连接"""
        try:
            ok = self.client.get_ok()
            server_time = self.client.get_server_time()
            print(f"[INFO] 服务器状态: {ok}, 服务器时间: {server_time}")
            return ok
        except Exception as e:
            print(f"[ERROR] 连接失败: {e}")
            return False
    
    def get_market_price(self, token_id: str, side: str = BUY) -> float:
        """
        获取市场最优价格
        :param token_id: 代币ID
        :param side: BUY/SELL
        :return: 价格
        """
        try:
            price = self.client.get_price(token_id, side)
            print(f"[INFO] {token_id} {side} 价格: {price}")
            return float(price)
        except Exception as e:
            print(f"[ERROR] 获取价格失败: {e}")
            return 0.0
    
    def get_midpoint(self, token_id: str) -> float:
        """获取中间价"""
        try:
            mid = self.client.get_midpoint(token_id)
            print(f"[INFO] {token_id} 中间价: {mid}")
            return float(mid)
        except Exception as e:
            print(f"[ERROR] 获取中间价失败: {e}")
            return 0.0
    
    def place_market_order(
        self,
        token_id: str,
        amount_usd: float,
        side: str = BUY,
        order_type: str = OrderType.FOK
    ) -> Dict:
        """
        下市价单
        :param token_id: 代币ID
        :param amount_usd: 美元金额
        :param side: BUY/SELL
        :param order_type: FOK/IOC
        :return: 响应结果
        """
        try:
            print(f"[INFO] 发送市价单: {side} {amount_usd} USD of {token_id}")
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount_usd,
                side=side,
                order_type=order_type
            )
            signed_order = self.client.create_market_order(order_args)
            resp = self.client.post_order(signed_order, orderType=order_type)
            print(f"[INFO] 下单成功: {resp}")
            return resp
        except Exception as e:
            print(f"[ERROR] 下单失败: {e}")
            return {"error": str(e)}
    
    def place_limit_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str = BUY,
        order_type: str = OrderType.GTC
    ) -> Dict:
        """
        下限价单
        :param token_id: 代币ID
        :param price: 价格 (0-1)
        :param size: 份额数量
        :param side: BUY/SELL
        :param order_type: GTC/IOC/FOK
        :return: 响应结果
        """
        try:
            print(f"[INFO] 发送限价单: {side} {size} shares of {token_id} @ {price}")
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side
            )
            signed_order = self.client.create_order(order_args)
            resp = self.client.post_order(signed_order, orderType=order_type)
            print(f"[INFO] 下单成功: {resp}")
            return resp
        except Exception as e:
            print(f"[ERROR] 下单失败: {e}")
            return {"error": str(e)}
    
    def get_open_orders(self) -> List[Dict]:
        """获取当前未成交订单"""
        try:
            orders = self.client.get_orders(OpenOrderParams())
            print(f"[INFO] 当前未成交订单: {len(orders)}")
            return orders
        except Exception as e:
            print(f"[ERROR] 获取订单失败: {e}")
            return []
    
    def cancel_order(self, order_id: str) -> Dict:
        """取消单个订单"""
        try:
            resp = self.client.cancel(order_id)
            print(f"[INFO] 取消订单 {order_id}: {resp}")
            return resp
        except Exception as e:
            print(f"[ERROR] 取消订单失败: {e}")
            return {"error": str(e)}
    
    def cancel_all_orders(self) -> Dict:
        """取消所有订单"""
        try:
            resp = self.client.cancel_all()
            print(f"[INFO] 取消所有订单: {resp}")
            return resp
        except Exception as e:
            print(f"[ERROR] 取消所有订单失败: {e}")
            return {"error": str(e)}
    
    def get_my_trades(self) -> List[Dict]:
        """获取历史成交"""
        try:
            trades = self.client.get_trades()
            print(f"[INFO] 获取历史成交: {len(trades)}")
            return trades
        except Exception as e:
            print(f"[ERROR] 获取历史成交失败: {e}")
            return []
    
    def get_order_book(self, token_id: str) -> Dict:
        """获取订单簿"""
        try:
            book = self.client.get_order_book(token_id)
            return book
        except Exception as e:
            print(f"[ERROR] 获取订单簿失败: {e}")
            return {}


def main():
    """示例运行"""
    bot = PolymarketAutoBot()
    if not bot.check_connection():
        print("连接失败，请检查配置")
        sys.exit(1)
    
    print("\n=== 当前未成交订单 ===")
    orders = bot.get_open_orders()
    for o in orders[:5]:
        print(f"  {o.get('id')}: {o.get('tokenId')} {o.get('side')} {o.get('price')}")


if __name__ == "__main__":
    main()
