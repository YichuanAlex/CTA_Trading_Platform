class bar_type(object):
    """
    行情 bar 数据对象
    字段说明：
    - symbol: 合约代码，例如 'CU2409.SHFE'
    - product_id: 品种/标的 ID
    - mkt_time: 行情时间（HH:MM:SS）
    - trading_day: 交易日（YYYY-MM-DD）
    - open/high/low/close: 开高低收
    - volume/amount: 成交量/成交额
    - vwap: 加权成交价（成交额/成交量）
    - underlying_close/underlying_pre_close: 标的收盘价/前收盘价（期权用）
    - delta/gamma/vega/theta/imp_vol: 期权希腊与隐含波动率
    - pre_imp_vol: 前一时点隐波（期权用）
    - rt_time: 分钟线时间戳（YYYY-MM-DD HH:MM:SS）
    """
    def __init__(self):
        self.symbol = ""
        self.product_id = ""
        self.mkt_time = ""
        self.trading_day = ""
        self.open = 0.0
        self.high = 0.0
        self.low = 0.0
        self.close = 0.0
        self.volume = 0.0
        self.amount = 0.0
        self.vwap = 0.0
        self.underlying_close = 0.0
        self.delta = 0.0
        self.gamma = 0.0
        self.vega = 0.0
        self.theta = 0.0
        self.imp_vol = 0.0
        self.underlying_pre_close = 0.0
        self.pre_imp_vol = 0.0
        self.rt_time = ""
