import pdb

class date_time_util(object):
    def __init__(self):
        # 交易日映射：支持 "YYYYMMDD" 与 "YYYY-MM-DD" 两种键形式
        self._dict_pre_trading_day = {}
        self._dict_trading_day = {}
        self._dict_next_trading_day = {}
        self._is_inited_ = False

    def _to_hyphen(self, s):
        s = str(s).strip().split(' ')[0]
        s = s.replace('_', '-')
        if len(s) == 8 and s.isdigit():
            return s[0:4] + '-' + s[4:6] + '-' + s[6:8]
        return s

    def _to_digits(self, s):
        s = ''.join(ch for ch in str(s).strip().split(' ')[0] if ch.isdigit())
        return s if len(s) == 8 else None

    # 初始化交易日序列，用于前一/后一交易日查询
    # list_trading_day 列表，元素为 "YYYYMMDD" 或 "YYYY-MM-DD"
    def init(self, list_trading_day):
        pre_hyphen = ""
        pre_digits = ""
        for trading_day in list_trading_day:
            t_hyphen = self._to_hyphen(trading_day)
            t_digits = self._to_digits(trading_day) or ''.join(ch for ch in t_hyphen if ch.isdigit())
            # 记录存在的交易日（以连字符格式为准）
            self._dict_trading_day[t_hyphen] = t_hyphen
            if t_digits:
                self._dict_trading_day[t_digits] = t_hyphen
            # 前一交易日映射（保持输入格式）
            self._dict_pre_trading_day[t_hyphen] = pre_hyphen
            if t_digits:
                self._dict_pre_trading_day[t_digits] = pre_digits
            # 后一交易日映射（保持输入格式）
            if len(pre_hyphen) > 0:
                self._dict_next_trading_day[pre_hyphen] = t_hyphen
            if len(pre_digits) > 0 and t_digits:
                self._dict_next_trading_day[pre_digits] = t_digits
            pre_hyphen = t_hyphen
            pre_digits = t_digits or pre_digits
        self._is_inited_ = True
        return

    # 返回是否完成初始化
    def is_init(self):
        return self._is_inited_

    # 获取所有已知交易日（返回标准 "YYYY-MM-DD" 列表）
    def get_trading_day(self):
        return [d for d in self._dict_trading_day.values() if '-' in str(d)]

    # 获取前一交易日（输入支持两种格式，返回与输入格式一致；不存在时返回 None）
    def get_pre_trading_day(self, trading_day):
        if trading_day in self._dict_pre_trading_day.keys():
            v = self._dict_pre_trading_day[trading_day]
            return v if len(str(v)) > 0 else None
        return None

    # 获取后一交易日（输入支持两种格式，返回与输入格式一致；不存在时返回 None）
    def get_next_trading_day(self, trading_day):
        if trading_day in self._dict_next_trading_day.keys():
            v = self._dict_next_trading_day[trading_day]
            return v if len(str(v)) > 0 else None
        return None
