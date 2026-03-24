# Polymarket 自动下单机器人

基于官方 [py-clob-client](https://github.com/Polymarket/py-clob-client) 开发的 Polymarket 自动交易机器人，支持市价单、限价单、订单管理等功能。

## 功能

- ✅ 自动初始化API凭据（无需手动创建）
- ✅ 市价下单（按美元金额买入/卖出）
- ✅ 限价下单（按价格+数量下单）
- ✅ 查询订单、持仓、历史成交
- ✅ 取消单个/所有订单
- ✅ 查询行情、中间价、订单簿

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置

1. 复制 `.env.example` 为 `.env`:
   ```bash
   cp .env.example .env
   ```

2. 编辑 `.env`，填入你的私钥和配置：
   - `PK`: 你的钱包私钥（不带 `0x` 前缀）
   - `CHAIN_ID`: `137` 主网 / `80002` 测试网
   - `SIGNATURE_TYPE`: 
     - `0` 标准EOA钱包 (MetaMask等)
     - `1` Email/Magic钱包 
     - `2` 浏览器代理钱包
   - 如果使用代理钱包，需要额外配置 `FUNDER` 为实际持有资金的地址

程序会自动创建/派生API凭据，如果你有现成的，可以手动填写到环境变量中。

## 使用示例

### 基本使用

```python
from bot import PolymarketAutoBot
from py_clob_client.order_builder.constants import BUY, SELL

# 初始化机器人
bot = PolymarketAutoBot()

# 检查连接
bot.check_connection()

# 市价买入 10 USDC
result = bot.place_market_order(
    token_id="你的代币ID",
    amount_usd=10,
    side=BUY
)
print(result)

# 限价买入 5 份，价格 0.45
result = bot.place_limit_order(
    token_id="你的代币ID",
    price=0.45,
    size=5,
    side=BUY
)
print(result)

# 获取未成交订单
orders = bot.get_open_orders()

# 取消所有订单
bot.cancel_all_orders()
```

### 命令行快速测试

```bash
python bot.py
```

会输出连接状态和当前未成交订单。

### 如何获取 token_id ?

token_id 是 Polymarket 市场中每个 outcome 的唯一ID，可以通过 [Gamma API](https://docs.polymarket.com/api-reference/markets/get-markets) 查询所有市场及其 outcome。

## 重要提示

1. **授权**: 如果你使用EOA/MetaMask钱包，首次使用前需要授权 USDC 和 Conditional Tokens 给兑换合约，参考 [官方说明](https://github.com/Polymarket/py-clob-client?tab=readme-ov-file#important-token-allowances-for-metamaskeoa-users)
2. **费率**: Polymarket 收取交易手续费，请参考 [官方文档](https://docs.polymarket.com/trading/fees)
3. **心跳**: 如果需要长时间保持订单，需要定期发送心跳，否则服务器会自动取消所有订单，机器人目前没有内置心跳，需要你自己实现。

## 官方参考

- Polymarket 官方文档: https://docs.polymarket.com/
- py-clob-client GitHub: https://github.com/Polymarket/py-clob-client
- API 参考: https://docs.polymarket.com/api-reference/introduction
