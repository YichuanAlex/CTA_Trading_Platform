import traceback
import pdb
import datetime
import queue

class MA(object):
    def __init__(self, size) -> None:
        self.que_size = size
        self.que = queue.Queue(size)
        self.sum = 0.0
    
    def size(self):
        return self.que.qsize()
    
    def push(self, price):
        if self.size() >= self.que_size:
            v = self.que.get()
            self.sum -= v
        self.que.put(price)
        self.sum += price
        return
    
    # 高效计算法
    def get_ma(self):
        if self.size() >= self.que_size:
            return self.sum / self.que_size
        else:
            return 999999
    
    # 低效遍历
    def get_dummy_ma(self):
        if self.size() >= self.que_size:
            sum = 0
            for i in range(self.size()):
                sum += self.que[i]
            return sum / self.size()
        else:
            return 999999

class test_sample_strategy():
    def __init__(self) -> None:
        self.dict_target_position = {}
        self.dict_ma = {}
    
    # 策略必须实现此函数
    # api 是由平台提供的接口对象
    def init(self, api):
        self.api = api
        
        # 此函数以下的代码为：通过接口获取数据的示例用法
        
        # 获取交易日列表，返回DataFrame
        df = api.get_trading_day()
        print(df)
        
        # 获取每个交易日的合约信息
        # df = api.get_daily_symbol()
        # print(df)
        
        # 获取bar数据
        df = api.get_future_bar("20240531", "1m", "SHFE", "CU2409.SHFE")
        print(df)
        
        df = api.get_future_bar("20240531", "1d", "SHFE", "CU2409.SHFE")
        print(df)
        
        # 获取原始日线数据
        df = api.get_future_origin_daily_bar("cu.SHFE", "20210101", "20240601", "main")
        print(df)
        
        # 获取复权日线数据
        df = api.get_future_spec_daily_bar("cu.SHFE", "20210101", "20240601", "v3")
        print(df)
        
        # 获取原始分钟线数据
        # df = api.get_future_origin_minute_bar("cu.SHFE", "20210101 09:00:00", "20240601 15:00:00", "main")
        # print(df)
        
        # 获取复权分钟线数据
        # df = api.get_future_adj_factor_minute_bar("cu.SHFE", "20210101 09:00:00", "20240601 15:00:00")
        # print(df)
        
        # 获取主力合约表
        df = api.get_main_symbol_info("CU.SHFE")
        print(df)
        
        # 获取主力复权因子表
        df = api.get_main_symbol_adj_factor_info("CU.SHFE")
        print(df)
        
        return 0
    
    # 子策略必须实现此函数
    # 返回目标仓位dict，仓位是否有变化
    def calculate_target_position(self, list_bar):
        
        if len(list_bar) == 0:
            return self.dict_target_position
        
        for bar in list_bar:
            symbol = getattr(bar, "symbol")
            self.dict_target_position[symbol] = 1
        
        # return self.dict_target_position, False
        
        is_pos_changed = False
        
        try:
            if len(list_bar) == 0:
                return self.dict_target_position, False
            
            for bar in list_bar:
                symbol = getattr(bar, "symbol")
                if symbol not in self.dict_ma.keys():
                    self.dict_ma[symbol] = MA(120)
                close_price = float(getattr(bar, "close"))
                self.dict_ma[symbol].push(close_price)
                cur_ma = self.dict_ma[symbol].get_ma()
                if close_price < cur_ma:
                    if symbol not in self.dict_target_position.keys() or abs(self.dict_target_position[symbol] + 0.1) > 0.000001:
                        is_pos_changed = True
                        self.dict_target_position[symbol] = -1000
                else:
                    if symbol not in self.dict_target_position.keys() or abs(self.dict_target_position[symbol]) > 0.000001:
                        is_pos_changed = True
                        self.dict_target_position[symbol] = 0
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())
            pdb.set_trace()
        
        return self.dict_target_position, is_pos_changed