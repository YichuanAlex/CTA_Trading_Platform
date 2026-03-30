class cta_platform_api(object):
    def __init__(self) -> None:
        self._api = None
        pass
    def __getattr__(self, name):
        api = object.__getattribute__(self, "_api")
        return getattr(api, name)
    def __setattr__(self, name, value):
        if name in ("_api",):
            object.__setattr__(self, name, value)
            return
        api = None
        try:
            api = object.__getattribute__(self, "_api")
        except Exception:
            api = None
        if api is not None and hasattr(api, name):
            setattr(api, name, value)
        else:
            object.__setattr__(self, name, value)
        return
    def get_account(self):
        try:
            avail = float(self._api.get_available())
        except Exception:
            avail = 0.0
        class _Acct:
            pass
        acct = _Acct()
        acct.available = avail
        return acct

    # 初始化
    def init(self, api):
        self._api = api
        return

    def get_pm(self, symbol):
        pm = self._api.get_pm()
        if symbol in pm.dict_positions.keys():
            return pm.dict_positions[symbol]
        return None

    # 获取交易日列表
    # 返回 DataFrame
    def get_trading_day(self):
        return self._api.get_trading_day()

    # 获取每日合约列表
    # 返回 DataFrame
    def get_daily_symbol(self):
        return self._api.get_daily_symbol()

    # 获取每日期权合约信息列表
    # 返回 DataFrame
    def get_daily_option_info(self):
        return self._api.get_option_info()

    # 获取期货分钟或日数据
    # date 数据日期，例如："20240531"
    # data_type 数据类型："1m" 分钟；"1d" 日
    # exchange 交易所："CFFEX" 中金所；"SHFE" 上期所；"DCE" 大商所；"CZCE" 郑商所
    # symbol 合约；为空返回当天所有合约
    # 返回 DataFrame
    def get_future_bar(self, date, data_type, exchange, symbol):
        return self._api.get_future_bar(date, data_type, exchange, symbol)

    # 获取期货原始分钟数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_datetime 开始时间，例如："20200101 09:00:00"
    # end_datetime 结束时间，例如："20240601 15:00:00"
    # data_type 数据类型，例如："main"、"sec"、"sec_fwd"
    # 返回 DataFrame
    def get_future_origin_minute_bar(self, symbol_root, start_datetime, end_datetime, data_type):
        return self._api.get_future_origin_minute_bar(symbol_root, start_datetime, end_datetime, data_type)

    # 获取期货原始日数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_date 开始日期，例如："20200101"
    # end_date 结束日期，例如："20240601"
    # data_type 数据类型，例如："main"、"sec"、"sec_fwd"
    # 返回 DataFrame
    def get_future_origin_daily_bar(self, symbol_root, start_date, end_date, data_type):
        return self._api.get_future_origin_daily_bar(symbol_root, start_date, end_date, data_type)

    # 获取期货特殊日数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_date 开始日期，例如："20200101"
    # end_date 结束日期，例如："20240601"
    # data_type 数据类型，例如："v3"
    # 返回 DataFrame
    def get_future_spec_daily_bar(self, symbol_root, start_date, end_date, data_type):
        return self._api.get_future_spec_daily_bar(symbol_root, start_date, end_date, data_type)

    # 获取期货特殊复权日数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_date 开始日期，例如："20200101"
    # end_date 结束日期，例如："20240601"
    # data_type 数据类型，例如："v3"
    # 返回 DataFrame
    def get_future_spec_adj_factor_daily_bar(self, symbol_root, start_date, end_date, data_type):
        return self._api.get_future_spec_adj_factor_daily_bar(symbol_root, start_date, end_date, data_type)

    # 获取期货复权分钟数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_datetime 开始时间，例如："20200101 09:00:00"
    # end_datetime 结束时间，例如："20240601 15:00:00"
    # 返回 DataFrame
    def get_future_adj_factor_minute_bar(self, symbol_root, start_datetime, end_datetime):
        return self._api.get_future_adj_factor_minute_bar(symbol_root, start_datetime, end_datetime)

    # 获取期货主力合约表
    # symbol_root 品种，例如："A.DCE"
    # 返回 DataFrame
    def get_main_symbol_info(self, symbol_root):
        return self._api.get_main_symbol_info(symbol_root)

    # 获取期货主力复权因子表
    # symbol_root 品种，例如："A.DCE"
    # 返回 DataFrame
    def get_main_symbol_adj_factor_info(self, symbol_root):
        return self._api.get_main_symbol_adj_factor_info(symbol_root)

    # 获取当天实时期货分钟线原始数据
    # symbol_root 品种列表，例如：["A.DCE","CU.SHFE","IC.CFFEX","SC.INE","AP.CZCE","SI.GFEX"]
    # start_datetime 开始时间，例如："20240715 09:05:00"
    # end_datetime 结束时间，例如："20240715 15:00:00"
    # columns 列名，例如：["open","close"]
    # 返回 DataFrame
    def get_origin_data(self, symbol_root, start_datetime, end_datetime, columns):
        return self._api.get_live_1m_data(symbol_root, start_datetime, end_datetime, columns)

    # 获取当天实时期货分钟线复权数据
    # symbol_root 品种列表，例如：["A.DCE","CU.SHFE","IC.CFFEX","SC.INE","AP.CZCE","SI.GFEX"]
    # start_datetime 开始时间，例如："20240715 09:05:00"
    # end_datetime 结束时间，例如："20240715 15:00:00"
    # columns 列名，例如：["open","close"]
    # 返回 DataFrame
    def get_adj_factor_data(self, symbol_root, start_datetime, end_datetime, columns):
        return self._api.get_live_1m_adj_data(symbol_root, start_datetime, end_datetime, columns)
