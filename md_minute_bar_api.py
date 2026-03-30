import md_data_struct as mds
import threading
import redis
import pandas as pd
import numpy as np

class OriginBar1M(object):
    def __init__(self) -> None:
        """
        原始1分钟bar数据对象
        """
        self.symbol = ""
        self.trading_day = ""
        self.date_time = ""
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.volume = 0
        self.turnover = 0
        self.open_interest = 0
        self.bid1 = 0
        self.ask1 = 0
        self.avg_price = 0
        self.volume_fi = 0
        self.turnover_fi = 0
        self.open_interest_fi = 0
        return

    def convert(self, bar):
        """
        从结构体对象转换
        :param bar: mds.origin_minute_bar_field
        """
        self.symbol = str(bar.symbol, encoding='utf-8')
        self.trading_day = str(bar.trading_day, encoding='utf-8')
        self.date_time = str(bar.date_time, encoding='utf-8') + ":00"
        self.open = bar.open_price
        self.high = bar.high_price
        self.low = bar.low_price
        self.close = bar.close_price
        self.volume = bar.volume
        self.turnover = bar.turnover
        self.open_interest = bar.open_interest
        self.bid1 = bar.bid1_price
        self.ask1 = bar.ask1_price
        self.avg_price = bar.avg_price
        self.volume_fi = bar.volume_fi
        self.turnover_fi = bar.turnover_fi
        self.open_interest_fi = bar.open_interest_fi
        return

    def decode(self, msg):
        """
        从字符串解码（Redis存储的逗号分隔值）
        :param msg: 形如 "code,trading_day,date_time,open,high,low,..." 的字符串
        """
        data = msg.split(',')
        if len(data) >= 16:
            self.symbol = data[0]
            self.trading_day = str(data[1])
            self.date_time = data[2] + ":00"
            self.open = float(data[3])
            if self.open == 0.0:
                self.open = np.nan
            self.high = float(data[4])
            if self.high == 0.0:
                self.high = np.nan
            self.low = float(data[5])
            if self.low == 0.0:
                self.low = np.nan
            self.close = float(data[6])
            if self.close == 0.0:
                self.close = np.nan
            self.volume = float(data[7])
            self.turnover = float(data[8])
            self.open_interest = float(data[9])
            if self.open_interest == 0.0:
                self.open_interest = np.nan
            self.bid1 = float(data[10])
            self.ask1 = float(data[11])
            self.avg_price = float(data[12])
            if self.avg_price == 0.0:
                self.avg_price = np.nan
            self.volume_fi = float(data[13])
            if self.volume_fi == 0.0:
                self.volume_fi = np.nan
            self.turnover_fi = float(data[14])
            if self.turnover_fi == 0.0:
                self.turnover_fi = np.nan
            self.open_interest_fi = float(data[15])
            if self.open_interest_fi == 0.0:
                self.open_interest_fi = np.nan
        return

    def to_tuple(self):
        return (self.symbol, self.date_time, self.trading_day, self.open, self.high, self.low, 
                self.close, self.volume, self.turnover, self.open_interest, self.bid1, self.ask1, self.avg_price, self.volume_fi, 
                self.turnover_fi, self.open_interest_fi)

    def to_df(self, list_bar):
        """
        转为DataFrame，列名统一采用首字母大写风格
        """
        df = pd.DataFrame(list_bar, columns=["Code", "DateTime", "TradingDay", "Open", "High", "Low",
                                           "Close", "Volume", "Turnover", "OpenInterest", "Bid1", "Ask1", "AvgPrice", "Volume_FI",
                                           "Turnover_FI", "OpenInterest_FI"])
        return df

class AdjFactorBar1M(object):
    def __init__(self) -> None:
        """
        复权1分钟bar数据对象
        """
        self.symbol = ""
        self.trading_day = ""
        self.date_time = ""
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.volume = 0
        self.turnover = 0
        self.open_interest = 0
        self.bid1 = 0
        self.ask1 = 0
        self.avg_price = 0
        self.volume_fi = 0
        self.turnover_fi = 0
        self.open_interest_fi = 0
        self.adj_factor = 0
        return

    def convert(self, bar):
        """
        从结构体对象转换
        :param bar: mds.adj_factor_minute_bar_field
        """
        self.symbol = str(bar.symbol, encoding='utf-8')
        self.trading_day = str(bar.trading_day, encoding='utf-8')
        self.date_time = str(bar.date_time, encoding='utf-8') + ":00"
        self.open = bar.open_price
        self.high = bar.high_price
        self.low = bar.low_price
        self.close = bar.close_price
        self.volume = bar.volume
        self.turnover = bar.turnover
        self.open_interest = bar.open_interest
        self.bid1 = bar.bid1_price
        self.ask1 = bar.ask1_price
        self.avg_price = bar.avg_price
        self.volume_fi = bar.volume_fi
        self.turnover_fi = bar.turnover_fi
        self.open_interest_fi = bar.open_interest_fi
        self.adj_factor = bar.adj_factor
        return

    def decode(self, msg):
        """
        从字符串解码（Redis存储的逗号分隔值，含复权因子）
        :param msg: 形如 "...,OpenInterest_FI,AdjFactor" 的字符串
        """
        data = msg.split(',')
        if len(data) >= 17:
            self.symbol = data[0]
            self.trading_day = str(data[1])
            self.date_time = data[2] + ":00"
            self.open = float(data[3])
            if self.open == 0.0:
                self.open = np.nan
            self.high = float(data[4])
            if self.high == 0.0:
                self.high = np.nan
            self.low = float(data[5])
            if self.low == 0.0:
                self.low = np.nan
            self.close = float(data[6])
            if self.close == 0.0:
                self.close = np.nan
            self.volume = float(data[7])
            self.turnover = float(data[8])
            self.open_interest = float(data[9])
            if self.open_interest == 0.0:
                self.open_interest = np.nan
            self.bid1 = float(data[10])
            self.ask1 = float(data[11])
            self.avg_price = float(data[12])
            if self.avg_price == 0.0:
                self.avg_price = np.nan
            self.volume_fi = float(data[13])
            if self.volume_fi == 0.0:
                self.volume_fi = np.nan
            self.turnover_fi = float(data[14])
            if self.turnover_fi == 0.0:
                self.turnover_fi = np.nan
            self.open_interest_fi = float(data[15])
            if self.open_interest_fi == 0.0:
                self.open_interest_fi = np.nan
            self.adj_factor = float(data[16])
            if self.adj_factor == 0.0:
                self.adj_factor = np.nan
        return

    def to_tuple(self):
        return (self.symbol, self.date_time, self.trading_day, self.open, self.high, self.low, 
                self.close, self.volume, self.turnover, self.open_interest, self.bid1, self.ask1, self.avg_price, self.volume_fi, 
                self.turnover_fi, self.open_interest_fi, self.adj_factor)

    def to_df(self, list_bar):
        """
        转为DataFrame，列名统一采用首字母大写风格
        """
        df = pd.DataFrame(list_bar, columns=["Code", "DateTime", "TradingDay", "Open", "High", "Low",
                                           "Close", "Volume", "Turnover", "OpenInterest", "Bid1", "Ask1", "AvgPrice", "Volume_FI",
                                           "Turnover_FI", "OpenInterest_FI", "AdjFactor"])
        return df

class MdMinuteBarApi(object):
    def __init__(self, origin_1m_bar_listener = None, adj_factor_1m_bar_listener = None) -> None:
        """
        1分钟bar行情API
        - 支持ZMQ推送监听与Redis读取
        """
        self._th = 0
        self._origin_1m_bar_listener = origin_1m_bar_listener
        self._adj_factor_1m_bar_listener = adj_factor_1m_bar_listener
        self._mds = mds.MarketDataRecv()
        return

    def init(self, addr, redis_host, redis_port):
        """
        初始化订阅与Redis连接
        :param addr: ZMQ地址，例如 'tcp://host:port'
        :param redis_host: Redis主机
        :param redis_port: Redis端口
        """
        if self._origin_1m_bar_listener is not None and self._adj_factor_1m_bar_listener is not None:
            self._th = threading.Thread(target=self._mds.recv_bar, args=(addr, self.on_origin_1m_bar, self.on_adj_factor_1m_bar))
            self._th.start()
        self.__redis_pool = redis.ConnectionPool(host=redis_host, port=redis_port, decode_responses=True)
        self.__redis = redis.Redis(connection_pool=self.__redis_pool)
        return

    def get_redis_inst(self):
        return self.__redis

    #获取原始分钟数据
    # symbol 合约，例如：‘IC2312.CFFEX"
    #begin_date_time 开始时间，例如：20231129 9:30:00
    #end_date_time 结束时间，例如："2023112914:30:00
    # 返回DataFrame
    def get_origin_1m_bar(self, symbol, begin_date_time, end_date_time):
        key = symbol + "_origin_bar"
        min_score = begin_date_time.replace(' ', '').replace(':', '')[:14]
        max_score = end_date_time.replace(' ', '').replace(':', '')[:14]
        data = self.__redis.zrangebyscore(key, min_score, max_score)
        list_bar = []
        for msg in data:
            origin_1m_bar = OriginBar1M()
            origin_1m_bar.decode(msg)
            list_bar.append(origin_1m_bar.to_tuple())
        tmp_bar = OriginBar1M()
        ret = tmp_bar.to_df(list_bar)
        ret.ffill(inplace=True)
        ret.bfill(inplace=True)
        return ret

    #获取复权分钟数据
    # symbol 合约，例如：‘IC2312.CFFEX'
    #begin_date_time 开始时间，例如：20231129 9:30:00
    # end_date_time 结束时间，例如：'20231129 14:30:00
    # 返回DataFrame
    def get_adj_factor_1m_bar(self, symbol, begin_date_time, end_date_time):
        key = symbol + "_adj_factor_bar"
        min_score = begin_date_time.replace(' ', '').replace(':', '')[:14]
        max_score = end_date_time.replace(' ', '').replace(':', '')[:14]
        data = self.__redis.zrangebyscore(key, min_score, max_score)
        list_bar = []
        for msg in data:
            adj_factor_1m_bar = AdjFactorBar1M()
            adj_factor_1m_bar.decode(msg)
            list_bar.append(adj_factor_1m_bar.to_tuple())
        tmp_bar = AdjFactorBar1M()
        ret = tmp_bar.to_df(list_bar)
        ret.ffill(inplace=True)
        ret.bfill(inplace=True)
        return ret

    def get_main_symbol(self, symbol, tag):
        """
        获取主力合约代码
        :param symbol: 品种+交易所，例如 'IC.CFFEX' 或 'CU.SHFE'
        :param tag: 追加的redis键后缀，例如 '_origin_bar' 或 '_adj_factor_bar'
        :return: 主力合约全代码，例如 'IC2406,CFFEX'
        """
        list_symbol_info = symbol.split('.')
        if len(list_symbol_info) < 2:
            return ""
        main_symbol = ""
        key = ""
        if "SHFE" == list_symbol_info[-1] or "DCE" == list_symbol_info[-1] or "INE" == list_symbol_info[-1] or \
           "GFEX" == list_symbol_info[-1]:
            key = list_symbol_info[0].lower() + "," + list_symbol_info[-1] + "_main" + tag
        else:
            key = list_symbol_info[0].upper() + "," + list_symbol_info[-1] + "_main" + tag
        data = self.__redis.zrange(key, 0, -1)
        for msg in data:
            main_symbol = msg + "," + list_symbol_info[-1]
        return main_symbol

    def get_origin_data(self, list_symbol, begin_date_time, end_date_time, columns, type="1m"):
        """
        批量获取原始分钟数据并拼接
        :param list_symbol: 品种列表
        :param begin_date_time: 开始时间
        :param end_date_time: 结束时间
        :param columns: 需要的列（将自动补充 'DateTime' 与 'Code'）
        :param type: 分钟类型（预留）
        """
        ret_columns = list(columns)
        if "DateTime" not in ret_columns:
            ret_columns.append("DateTime")
        if "Code" not in ret_columns:
            ret_columns.append("Code")
        ret = pd.DataFrame([], columns=ret_columns)
        for symbol in list_symbol:
            symbol = self.get_main_symbol(symbol, "_origin_bar")
            df = self.get_origin_1m_bar(symbol, begin_date_time, end_date_time)
            df = df[ret_columns]
            ret = pd.concat([ret, df])
        ret.reset_index(inplace=True, drop=True)
        return ret

    def get_adj_factor_data(self, list_symbol, begin_date_time, end_date_time, columns, type="1m"):
        """
        批量获取复权分钟数据并拼接
        :param list_symbol: 品种列表
        :param begin_date_time: 开始时间
        :param end_date_time: 结束时间
        :param columns: 需要的列（将自动补充 'DateTime' 与 'Code'）
        :param type: 分钟类型（预留）
        """
        ret_columns = list(columns)
        if "DateTime" not in ret_columns:
            ret_columns.append("DateTime")
        if "Code" not in ret_columns:
            ret_columns.append("Code")
        ret = pd.DataFrame([], columns=ret_columns)
        for symbol in list_symbol:
            symbol = self.get_main_symbol(symbol, "_adj_factor_bar")
            df = self.get_adj_factor_1m_bar(symbol, begin_date_time, end_date_time)
            df = df[ret_columns]
            ret = pd.concat([ret, df])
        ret.reset_index(inplace=True, drop=True)
        return ret

    #原始分钟数据回调 (推送)
    def on_origin_1m_bar(self, bar):
        ori_1m_bar = OriginBar1M()
        ori_1m_bar.convert(bar)
        if self._origin_1m_bar_listener is not None:
            self._origin_1m_bar_listener(ori_1m_bar)
        return

    #复权分钟数据回调 (推送)
    def on_adj_factor_1m_bar(self, bar):
        adj_factor_1m_bar = AdjFactorBar1M()
        adj_factor_1m_bar.convert(bar)
        if self._adj_factor_1m_bar_listener is not None:
            self._adj_factor_1m_bar_listener(adj_factor_1m_bar)
        return

    def close(self):
        self._mds.stop()
        return
