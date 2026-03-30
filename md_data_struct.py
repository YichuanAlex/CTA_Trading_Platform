# encoding=utf8
import struct
from ctypes import Structure, c_char, c_double
import zmq

class origin_minute_bar_field(Structure):
    """
    原始分钟行情结构（未复权）
    字段说明：
    - symbol: 合约代码
    - trading_day: 业务日期（交易日）
    - date_time: 日期时间（YYYY-MM-DD HH:MM:SS）
    - open_price/high_price/low_price/close_price: 开/高/低/收
    - volume: 成交量
    - turnover: 成交额
    - open_interest: 持仓量
    - bid1_price/ask1_price: 买一价/卖一价
    - avg_price: 成交均价
    - volume_fi/turnover_fi/open_interest_fi: 品种总成交量/成交额/持仓量
    """
    _fields_ = [
        ("symbol", c_char * 16),
        #合约代码
        ("trading_day", c_char * 16),
        # 业务日期
        ("date_time", c_char * 32),
        #日期时间
        ("open_price", c_double),
        # 开盘价
        ("high_price", c_double),
        #最高价
        ("low_price", c_double),
        #最低价
        ("close_price", c_double),
        #收盘价
        ("volume", c_double),
        #成交量
        ("turnover", c_double),
        #成交额
        ("open_interest", c_double),
        # 持仓量
        ("bid1_price", c_double),
        #买一价
        ("ask1_price", c_double),
        #卖一价
        ("avg_price", c_double),
        #成交均价
        ("volume_fi", c_double),
        # 品种总成交量
        ("turnover_fi", c_double),
        #品种总成交额
        ("open_interest_fi", c_double)
        #品种总持仓量
    ]

    def __str__(self):
        """
        以逗号分隔的字符串形式输出，数值统一保留6位小数
        """
        head = [
            str(self.symbol, encoding='utf-8'),
            str(self.trading_day, encoding='utf-8'),
            str(self.date_time, encoding='utf-8'),
        ]
        vals = [
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.volume,
            self.turnover,
            self.open_interest,
            self.bid1_price,
            self.ask1_price,
            self.avg_price,
            self.volume_fi,
            self.turnover_fi,
            self.open_interest_fi,
        ]
        return ",".join(head + [format(v, ".6f") for v in vals])

class md_origin_minute_bar_parser:
    """
    原始分钟行情解析器
    - 接收二进制消息并解析为 origin_minute_bar_field
    """
    def __init__(self, listener):
        self._listener = listener
        return
    def on_bar(self, msg):
        """
        行情回调入口
        :param msg: 二进制消息
        """
        if self._listener is not None:
            self._listener(self.decode(msg))
        return
    def decode(self, msg):
        """
        解码二进制消息
        :param msg: 二进制消息
        :return: origin_minute_bar_field
        """
        bar = origin_minute_bar_field()
        (
            symbol,
            msg_type,
            msg_len,
            bar.symbol,
            bar.trading_day,
            bar.date_time,
            bar.open_price,
            bar.high_price,
            bar.low_price,
            bar.close_price,
            bar.volume,
            bar.turnover,
            bar.open_interest,
            bar.bid1_price,
            bar.ask1_price,
            bar.avg_price,
            bar.volume_fi,
            bar.turnover_fi,
            bar.open_interest_fi,
        ) = struct.unpack("<16s2i16s16s32s13d", msg)
        return bar

class adj_factor_minute_bar_field(Structure):
    """
    复权分钟行情结构（含复权因子）
    在原始结构基础上增加：
    - adj_factor: 复权因子
    """
    _fields_ = [
        ("symbol", c_char * 16),
        #合约代码
        ("trading_day", c_char * 16),
        # 业务日期
        ("date_time", c_char * 32),
        #日期时间
        ("open_price", c_double),
        #开盘价
        ("high_price", c_double),
        #最高价
        ("low_price", c_double),
        #最低价
        ("close_price", c_double),
        #收盘价
        ("volume", c_double),
        #成交量
        ("turnover", c_double),
        #成交量
        ("open_interest", c_double),
        # 持仓量
        ("bid1_price", c_double),
        #买价
        ("ask1_price", c_double),
        #卖一价
        ("avg_price", c_double),
        #成交均价
        ("volume_fi", c_double),
        #品种总成交量
        ("turnover_fi", c_double),
        #品种总成交额
        ("open_interest_fi", c_double),
        #品种总持仓量
        ("adj_factor", c_double)
        #复权因子
    ]

    def __str__(self):
        """
        以逗号分隔的字符串形式输出，数值统一保留6位小数（含复权因子）
        """
        head = [
            str(self.symbol, encoding='utf-8'),
            str(self.trading_day, encoding='utf-8'),
            str(self.date_time, encoding='utf-8'),
        ]
        vals = [
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.volume,
            self.turnover,
            self.open_interest,
            self.bid1_price,
            self.ask1_price,
            self.avg_price,
            self.volume_fi,
            self.turnover_fi,
            self.open_interest_fi,
            self.adj_factor,
        ]
        return ",".join(head + [format(v, ".6f") for v in vals])

class md_adj_factor_minute_bar_parser:
    """
    复权分钟行情解析器
    - 接收二进制消息并解析为 adj_factor_minute_bar_field
    """
    def __init__(self, listener):
        self._listener = listener
        return
    def on_bar(self, msg):
        """
        行情回调入口
        :param msg: 二进制消息
        """
        if self._listener is not None:
            self._listener(self.decode(msg))
        return
    def decode(self, msg):
        """
        解码二进制消息
        :param msg: 二进制消息
        :return: adj_factor_minute_bar_field
        """
        bar = adj_factor_minute_bar_field()
        (
            symbol,
            msg_type,
            msg_len,
            bar.symbol,
            bar.trading_day,
            bar.date_time,
            bar.open_price,
            bar.high_price,
            bar.low_price,
            bar.close_price,
            bar.volume,
            bar.turnover,
            bar.open_interest,
            bar.bid1_price,
            bar.ask1_price,
            bar.avg_price,
            bar.volume_fi,
            bar.turnover_fi,
            bar.open_interest_fi,
            bar.adj_factor,
        ) = struct.unpack("<16s2i16s16s32s14d", msg)
        return bar

class MarketDataRecv(object):
    """
    市场数据订阅与分发
    - 连接 zmq，订阅分钟bar，按消息类型分发到对应解析器
    """
    def __init__(self) -> None:
        self._is_running = True
        return
    def stop(self):
        self._is_running = False
    def recv_bar(self, addr, origin_bar_listener, adj_factor_bar_listener):
        """
        接收分钟bar数据
        :param addr: 订阅地址（zmq）
        :param origin_bar_listener: 原始bar回调
        :param adj_factor_bar_listener: 复权bar回调
        """
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect(addr)
        socket.setsockopt(zmq.SUBSCRIBE, ''.encode('utf-8'))

        md_origin_bar = md_origin_minute_bar_parser(origin_bar_listener)
        md_adj_factor_bar = md_adj_factor_minute_bar_parser(adj_factor_bar_listener)

        while self._is_running:
            response = socket.recv_multipart()
            if not response:
                continue
            msg = response[0]
            recv_len = len(msg)
            if recv_len < 24:
                continue
            _, msg_type, _ = struct.unpack("<16s2i", msg[0:24])
            if msg_type == 1:
                md_origin_bar.on_bar(msg)
            elif msg_type == 2:
                md_adj_factor_bar.on_bar(msg)
        try:
            socket.close(0)
        except Exception:
            pass
        try:
            context.term()
        except Exception:
            pass
        return
