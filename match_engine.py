import order_manager
import order
import order_event
import math
import pdb
import logging

class match_engine(object):
    def __init__(self) -> None:
        """
        撮合引擎
        负责订单生命周期管理（下单、确认、成交、撤单）、成交价/量计算，以及期权到期处理。
        """
        self.om = order_manager.order_manager()
        self.dict_all_orders = {}
        self.dict_pending_ack_orders = {}
        self.dict_pending_conf_orders = {}
        self.dict_opened_orders = {}
        self.dict_pending_cancel_orders = {}
        self.func_on_ack = None
        self.func_on_conf = None
        self.func_on_fill = None
        self.func_on_cxl = None
        self.func_on_cxl_rej = None
        self.func_on_rej = None
        self.data_type = "im"
        self.deal_type = "close"
        self.max_deal_pct = 1.0
        self.is_check_market_volume = True
        self.dict_last_bar = {}
        self.dict_last_tick = {}
        self.dict_expired_date = {}
        self.dict_call_or_put = {}
        self.dict_daily_strike_price = {}
        self.is_option = False
        self.logger = logging.getLogger("match_engine")
        self._log_sample_n = 100
        self._debug_counter = 0
        self._bar_total = 0
        pass
    def _norm_day(self, s):
        try:
            s = str(s).strip()
            if len(s) == 8 and s.isdigit():
                return s[0:4] + "-" + s[4:6] + "-" + s[6:8]
            return s.replace("_", "-")
        except Exception:
            return str(s)
    def _tm_to_sec(self, t):
        try:
            t = str(t).strip()
            if len(t) == 8 and ":" in t:
                hh, mm, ss = t.split(":")
                return int(hh) * 3600 + int(mm) * 60 + int(ss)
        except Exception:
            pass
        return -1
    def set_log_config(self, level=None, sample_n=None):
        try:
            if level:
                lv = getattr(logging, str(level).upper(), logging.INFO)
                self.logger.setLevel(lv)
            if sample_n:
                n = int(sample_n)
                if n > 0:
                    self._log_sample_n = n
        except Exception:
            pass
        return
    def _log_debug(self, msg, *args):
        try:
            self._debug_counter += 1
            if self._debug_counter % self._log_sample_n == 0:
                self.logger.debug(msg, *args)
        except Exception:
            pass
        return
    def init(self):
        """
        初始化内部组件
        """
        self.om.init()
        return
    def register_on_ack_cb(self, cb):
        """
        注册订单确认回调（ack）
        :param cb: 回调函数，签名为 cb(order, event)
        """
        self.func_on_ack = cb
        return
    def register_on_conf_cb(self, cb):
        """
        注册订单生效回调（conf）
        :param cb: 回调函数，签名为 cb(order, event)
        """
        self.func_on_conf = cb
        return
    def register_on_fill_cb(self, cb):
        """
        注册订单成交回调（fill）
        :param cb: 回调函数，签名为 cb(order, event)
        """
        self.func_on_fill = cb
        return
    def register_on_cxl_cb(self, cb):
        """
        注册订单撤单完成回调
        :param cb: 回调函数，签名为 cb(order, event)
        """
        self.func_on_cxl = cb
        return
    def register_on_cxl_rej_cb(self, cb):
        """
        注册订单撤单拒绝回调
        :param cb: 回调函数，签名为 cb(order, event)
        """
        self.func_on_cxl_rej = cb
        return
    def register_on_rej_cb(self, cb):
        """
        注册订单拒绝回调（下单被拒绝）
        :param cb: 回调函数，签名为 cb(order, event)
        """
        self.func_on_rej = cb
        return
    def set_expired_date(self, expired_date):
        """
        设置期权到期日字典
        :param expired_date: dict，键为合约代码，值为到期交易日
        """
        self.dict_expired_date = expired_date
        return
    def set_call_or_put_info(self, call_or_put_info):
        """
        设置期权类型字典（call/put）
        :param call_or_put_info: dict，键为合约代码，值为 'call' 或 'put'
        """
        self.dict_call_or_put = call_or_put_info
        return
    def set_daily_strike_price(self, daily_strike_price):
        """
        设置每日行权价字典
        :param daily_strike_price: dict，键为交易日，值为 {symbol: strike_price}
        """
        self.dict_daily_strike_price = daily_strike_price
        return
    def set_data_type(self, data_type):
        """
        设置数据类型
        :param data_type: 'tick' 表示逐笔，否则按日/分钟bar处理
        """
        self.data_type = data_type
        return
    def set_deal_type(self, type):
        """
        设置成交价选择类型
        :param type: 'close' 使用收盘价，'open' 使用开盘价，其它使用 vwap
        """
        self.deal_type = type
        return
    def set_max_deal_pct(self, pct):
        """
        设置最大可成交比例
        :param pct: 0.0~1.0，成交量不超过市场成交量乘以此比例
        """
        try:
            p = float(pct)
        except Exception:
            p = 1.0
        if p < 0.0:
            p = 0.0
        if p > 1.0:
            p = 1.0
        self.max_deal_pct = p
        try:
            if self.max_deal_pct >= 1.0:
                # 满比例成交，禁用成交量门控以保证全部目标成交
                self.is_check_market_volume = False
                self.logger.info("[match_engine][cfg] max_deal_pct=%.2f -> disable volume gate", float(self.max_deal_pct))
            else:
                self.logger.info("[match_engine][cfg] max_deal_pct=%.2f volume_gate=%s", float(self.max_deal_pct), str(bool(self.is_check_market_volume)))
        except Exception:
            pass
        return
    def set_check_market_volume(self, flag):
        """
        设置是否检查市场成交量
        :param flag: True 检查，False 不检查
        """
        self.is_check_market_volume = flag
        return
    def set_is_option(self, is_option):
        """
        设置是否按照期权逻辑处理
        :param is_option: True 为期权，False 为期货/现货
        """
        self.is_option = is_option
        return
    def get_position_manager(self):
        """
        获取持仓管理器
        :return: position_manager 实例
        """
        return self.om.get_position_manager()
    def bod(self, trading_day):
        """
        盘前初始化
        :param trading_day: 交易日字符串
        """
        self.dict_all_orders.clear()
        self.dict_pending_ack_orders.clear()
        self.dict_pending_conf_orders.clear()
        self.dict_opened_orders.clear()
        self.dict_pending_cancel_orders.clear()
        self.om.on_bod(trading_day)
        self._log_debug("[match_engine][bod] trading_day=%s", trading_day)
        return
    #判断期权类型是实值期权-正数，平值期权-零，还是虚值期权-负数
    def get_option_type(self, symbol, trading_day):
        """
        判断期权类型（实值/平值/虚值）
        :param symbol: 期权合约代码
        :param trading_day: 交易日
        :return: 1 实值，0 平值，-1 虚值
        """
        #实值期权In-the-Money（ITM）：行权价格小于标的资产当前价格的看涨期权，或行权价格大于标的资产当前价格的看跌期权。
        #平值期权At-the-Money（ATM）：行权价格等于标的资产当前价格的看涨期权和看跌期权。
        #虚值期权out-of-the-Money（OTM)：行权价格大于标的资产当前价格的看涨期权，或行权价格小于标的资产当前价格的看跌期权。
        op_type = 0
        if symbol in self.dict_call_or_put.keys():
            call_or_put = self.dict_call_or_put[symbol]
            strike_price = self.dict_daily_strike_price[trading_day][symbol]
            underlying_price = 0.0
            if self.data_type == "tick":
                # 优先使用标的价格字段，不存在则回退到 last_price
                underlying_price = getattr(self.dict_last_tick[symbol], "underlying_last_price", self.dict_last_tick[symbol].last_price)
            else:
                # 优先使用标的收盘价，不存在则回退到 close
                underlying_price = getattr(self.dict_last_bar[symbol], "underlying_close", getattr(self.dict_last_bar[symbol], "close"))
            if (call_or_put == "call" and strike_price < underlying_price) or (call_or_put == "put" and strike_price > underlying_price):
                op_type = 1
            if (call_or_put == "call" and strike_price > underlying_price) or (call_or_put == "put" and strike_price < underlying_price):
                op_type = -1
        return op_type
    def check_to_close_position(self, trading_day):
        """
        到期处理未平仓期权
        - 虚值：按价格0平仓（自动补交易记录）
        - 实值：按收盘价强制平仓
        :param trading_day: 交易日
        """
        pm = self.get_position_manager()
        for symbol, pos in pm.dict_positions.items():
            if type(symbol) == type(b'bytes'):
                symbol = symbol.decode()
            exp_raw = self.dict_expired_date.get(symbol, None)
            exp_day = self._norm_day(exp_raw) if exp_raw is not None else None
            cur_day = str(trading_day)
            need_close = False
            if exp_day is not None and cur_day >= str(exp_day):
                need_close = True
            else:
                try:
                    base = symbol.split('.')[0]
                    digits = "".join(ch for ch in base if ch.isdigit())
                    if len(digits) >= 4:
                        yy = 2000 + int(digits[0:2])
                        mm = int(digits[2:4])
                        sym_ym = yy * 100 + mm
                        cur_ym = int(cur_day[0:4]) * 100 + int(cur_day[5:7])
                        if cur_ym >= sym_ym:
                            need_close = True
                except Exception:
                    need_close = False
            if need_close:
                net_pos = pos.long - pos.short
                if abs(net_pos - 0.0) > 0.0000001:
                    op_type = self.get_option_type(symbol, trading_day)
                    fill_px = 0.0
                    if op_type < 0:
                        # 虚值期权，按价格0平仓
                        fill_px = 0.0
                    else:
                        # 实值期权，按收盘价平仓
                        if self.data_type == "tick":
                            fill_px = self.dict_last_tick[symbol].last_price
                        else:
                            fill_px = float(getattr(self.dict_last_bar[symbol], "option_close", getattr(self.dict_last_bar[symbol], "close", 0.0)))
                    ord = self.om.create_order()
                    side = 0
                    if net_pos > 0:
                        side = 1
                    ord.init(symbol, abs(net_pos), side, 1, ord.order_id, "16:00:00", 0.)
                    ord.open_close = 1
                    self.om.on_place_order(ord)
                    ord_evt = order_event.order_filled_evt()
                    ord_evt.order_id = ord.order_id
                    ord_evt.filled_px = fill_px
                    ord_evt.filled_sz = abs(net_pos)
                    ord.status.status = ord.on_order_evt(ord_evt)
                    self.om.on_fill(ord_evt)
                    self.func_on_fill(ord, ord_evt)
                    self.dict_all_orders[ord.order_id] = ord
        return
    def eod(self, trading_day):
        """
        收盘处理
        - 将缓存的最后一根bar触发处理
        - 期权则执行到期检查
        :param trading_day: 交易日
        """
        for symbol, bar in self.dict_last_bar.items():
            self.process_by_bar(bar)
        self.check_to_close_position(trading_day)
        self.dict_last_bar.clear()
        try:
            self.logger.info("[match_engine][bar_stat] day=%s total=%d", trading_day, self._bar_total)
            self._bar_total = 0
        except Exception:
            pass
        return
    def force_close_unavailable_futures(self, available_set, account_mgr, trading_day):
        """
        对于当日不可交易（无数据）的期货合约，强制以上一有效价格平仓，避免持仓在无行情下持续占用资金与导致价格不更新
        :param available_set: 当日可交易符号集合（str）
        :param account_mgr: 账户管理器，用于读取上一有效bar价格
        :param trading_day: 交易日（str）
        """
        try:
            pm = self.get_position_manager()
            for symbol_b, pos in pm.dict_positions.items():
                try:
                    symbol = symbol_b.decode() if hasattr(symbol_b, "decode") else str(symbol_b)
                except Exception:
                    symbol = str(symbol_b)
                net_pos = pos.long - pos.short
                if abs(net_pos) <= 0.0000001:
                    continue
                if symbol in available_set:
                    continue
                fill_px = 0.0
                try:
                    bar = account_mgr.dict_last_bar.get(symbol, None)
                    if self.is_option:
                        fill_px = float(getattr(bar, "option_close", getattr(bar, "option_pre_close", 0.0)) if bar is not None else 0.0)
                    else:
                        fill_px = float(getattr(bar, "close", getattr(bar, "pre_close", 0.0)) if bar is not None else 0.0)
                except Exception:
                    fill_px = 0.0
                ord = self.om.create_order()
                side = 1 if net_pos > 0 else 0
                sym_bytes = symbol.encode() if isinstance(symbol, str) else symbol
                price_type = 1
                place_time = "09:00:00"
                try:
                    ord.init(sym_bytes, abs(net_pos), side, price_type, ord.order_id, place_time, fill_px)
                    ord.open_close = 1
                except Exception:
                    continue
                self.om.on_place_order(ord)
                ord_evt = order_event.order_filled_evt()
                ord_evt.order_id = ord.order_id
                ord_evt.filled_px = fill_px
                ord_evt.filled_sz = abs(net_pos)
                ord.status.status = ord.on_order_evt(ord_evt)
                self.om.on_fill(ord_evt)
                self.func_on_fill(ord, ord_evt)
                self.dict_all_orders[ord.order_id] = ord
                try:
                    self.logger.warning("[match_engine][force_close_unavailable] day=%s symbol=%s net=%f px=%.6f", trading_day, symbol, net_pos, fill_px)
                except Exception:
                    pass
        except Exception:
            try:
                self.logger.warning("[match_engine][force_close_unavailable][error] day=%s", trading_day)
            except Exception:
                pass
        return
    def force_close_stale_price_futures(self, current_time, account_mgr, trading_day):
        try:
            pm = self.get_position_manager()
            for symbol_b, pos in pm.dict_positions.items():
                try:
                    symbol = symbol_b.decode() if hasattr(symbol_b, "decode") else str(symbol_b)
                except Exception:
                    symbol = str(symbol_b)
                net_pos = pos.long - pos.short
                if abs(net_pos) <= 0.0000001:
                    continue
                bar = account_mgr.dict_last_bar.get(symbol, None)
                btday = ""
                btime = ""
                if bar is not None:
                    try:
                        btday = str(getattr(bar, "trading_day"))
                        btime = str(getattr(bar, "date_time")).split(' ')[-1]
                    except Exception:
                        btday = ""
                        btime = ""
                try:
                    td_norm = self._norm_day(trading_day)
                except Exception:
                    td_norm = str(trading_day)
                btday_norm = self._norm_day(btday)
                cur_sec = self._tm_to_sec(current_time)
                bar_sec = self._tm_to_sec(btime)
                lag_ok = (cur_sec >= 0 and bar_sec >= 0 and (cur_sec >= bar_sec) and (cur_sec - bar_sec) <= 18000)
                if (bar is None) or (btday_norm != td_norm) or (not lag_ok):
                    fill_px = 0.0
                    try:
                        if self.is_option:
                            fill_px = float(getattr(bar, "option_close", getattr(bar, "option_pre_close", 0.0)) if bar is not None else 0.0)
                        else:
                            fill_px = float(getattr(bar, "close", getattr(bar, "pre_close", 0.0)) if bar is not None else 0.0)
                    except Exception:
                        fill_px = 0.0
                    ord = self.om.create_order()
                    side = 1 if net_pos > 0 else 0
                    sym_bytes = symbol.encode() if isinstance(symbol, str) else symbol
                    price_type = 1
                    place_time = current_time or "09:00:00"
                    try:
                        ord.init(sym_bytes, abs(net_pos), side, price_type, ord.order_id, place_time, fill_px)
                        ord.open_close = 1
                    except Exception:
                        continue
                    self.om.on_place_order(ord)
                    ord_evt = order_event.order_filled_evt()
                    ord_evt.order_id = ord.order_id
                    ord_evt.filled_px = fill_px
                    ord_evt.filled_sz = abs(net_pos)
                    ord.status.status = ord.on_order_evt(ord_evt)
                    self.om.on_fill(ord_evt)
                    self.func_on_fill(ord, ord_evt)
                    self.dict_all_orders[ord.order_id] = ord
                    try:
                        self.logger.warning("[match_engine][force_close_stale] day=%s time=%s symbol=%s net=%f last_time=%s bar_day=%s bar_day_norm=%s lag_sec=%s px=%.6f", trading_day, current_time, symbol, net_pos, btime, btday, btday_norm, str((cur_sec - bar_sec) if (cur_sec>=0 and bar_sec>=0) else "NA"), fill_px)
                    except Exception:
                        pass
        except Exception:
            try:
                self.logger.warning("[match_engine][force_close_stale][error] day=%s time=%s", trading_day, current_time)
            except Exception:
                pass
        return
    def place_order(self, symbol, size, side, price = 0.0):
        """
        下单
        :param symbol: 合约代码（str 或 bytes）
        :param size: 下单手数/数量
        :param side: 方向，0 买，1 卖
        :param price: 限价价格，0/默认表示市价
        :return: 订单号
        """
        if type(symbol) == type("string"):
            symbol = symbol.encode()
        ord = self.om.create_order()
        price_type = 0
        if abs(price - 0.0) < 0.0000001:
            price_type = 1
        place_time = ""
        ord.init(symbol, size, side, price_type, ord.order_id, place_time, price)
        self.dict_all_orders[ord.order_id] = ord
        self.om.on_place_order(ord)
        self.dict_pending_ack_orders[ord.order_id] = ord
        self._log_debug("[match_engine][place_order] oid=%d symbol=%s size=%f side=%d price_type=%d price=%f", ord.order_id, ord.symbol.decode(), size, side, price_type, price)
        return ord.order_id
    def cancel_order(self, order_id):
        """
        撤单
        :param order_id: 订单号
        """
        self.dict_pending_cancel_orders[order_id] = order_id
        return
    def get_order(self, order_id):
        """
        获取订单
        :param order_id: 订单号
        :return: 订单对象或 None
        """
        if order_id not in self.dict_all_orders.keys():
            return None
        return self.dict_all_orders[order_id]
    def process_pending_ack_orders(self):
        """
        处理待确认订单（ack）
        - 设置下单时间
        - 进入待生效队列
        """
        for oid, ord in self.dict_pending_ack_orders.items():
            ord_evt = order_event.order_ack_evt()
            ord_evt.order_id = oid
            ord.status.status = ord.on_order_evt(ord_evt)
            place_time = ""
            if self.data_type == "tick":
                sym = ord.symbol.decode()
                if sym in self.dict_last_tick:
                    place_time = getattr(self.dict_last_tick[sym], "mkt_time", "09:00:00")
                else:
                    place_time = "09:00:00"
            else:
                sym = ord.symbol.decode()
                if sym in self.dict_last_bar:
                    dt = getattr(self.dict_last_bar[sym], "date_time", None)
                    if dt:
                        place_time = dt.split(' ')[-1]
                    else:
                        place_time = getattr(self.dict_last_bar[sym], "mkt_time", "09:00:00")
                else:
                    place_time = "09:00:00"
            ord.set_place_time(place_time)
            self.om.on_ack(ord_evt)
            self.func_on_ack(ord, ord_evt)
            self.dict_pending_conf_orders[oid] = ord
            self.dict_all_orders[oid] = ord
        if len(self.dict_pending_ack_orders) > 0:
            self._log_debug("[match_engine][ack] moved=%d", len(self.dict_pending_ack_orders))
        self.dict_pending_ack_orders.clear()
        return
    def process_pending_conf_orders(self):
        """
        处理待生效订单（conf）
        - 将订单移入已生效队列
        """
        for oid, ord in self.dict_pending_conf_orders.items():
            ord_evt = order_event.order_conf_evt()
            ord_evt.order_id = oid
            ord.status.status = ord.on_order_evt(ord_evt)
            self.om.on_conf(ord_evt)
            self.func_on_conf(ord, ord_evt)
            self.dict_opened_orders[oid] = ord
            self.dict_all_orders[oid] = ord
        if len(self.dict_pending_conf_orders) > 0:
            self._log_debug("[match_engine][conf] moved=%d", len(self.dict_pending_conf_orders))
        self.dict_pending_conf_orders.clear()
        return
    def calculate_filled_px(self, symbol):
        """
        计算成交价格
        :param symbol: 合约代码（str）
        :return: 成交价（float）
        """
        if self.deal_type == "close":
            if self.data_type == "tick":
                return float(self.dict_last_tick[symbol].last_price)
            else:
                if self.is_option:
                    return float(getattr(self.dict_last_bar[symbol], "option_close", getattr(self.dict_last_bar[symbol], "close", 0.0)))
                else:
                    return float(getattr(self.dict_last_bar[symbol], "close"))
        elif self.deal_type == "open":
            if self.data_type == "tick":
                return float(self.dict_last_tick[symbol].last_price)
            else:
                if self.is_option:
                    return float(getattr(self.dict_last_bar[symbol], "option_open", getattr(self.dict_last_bar[symbol], "open", 0.0)))
                else:
                    return float(getattr(self.dict_last_bar[symbol], "open"))
        else:
            if self.data_type == "tick":
                return self.dict_last_tick[symbol].vwap
            else:
                return getattr(self.dict_last_bar[symbol], "vwap")
        return 0.0
    def calculate_filled_sz(self, symbol, order_sz):
        """
        计算可成交数量
        :param symbol: 合约代码（str）
        :param order_sz: 订单数量
        :return: 实际成交数量（向下取整）
        """
        if (self.max_deal_pct >= 1.0) or (not self.is_check_market_volume):
            try:
                self.logger.info("[match_engine][filled_sz] symbol=%s order_sz=%f check_vol=%s max_pct=%.2f result_sz=%f", symbol, float(order_sz), str(bool(self.is_check_market_volume)), float(self.max_deal_pct), float(order_sz))
            except Exception:
                pass
            return order_sz
        vol = 0.0
        if self.data_type == "tick":
            try:
                vol = float(getattr(self.dict_last_tick[symbol], "delta_volume"))
            except Exception:
                vol = 0.0
            res = math.floor(min(order_sz, vol * self.max_deal_pct))
        else:
            if self.is_option:
                try:
                    vol = float(getattr(self.dict_last_bar[symbol], "option_volume", getattr(self.dict_last_bar[symbol], "volume", 0.0)))
                except Exception:
                    vol = 0.0
                res = math.floor(min(order_sz, vol * self.max_deal_pct))
            else:
                try:
                    vol = float(getattr(self.dict_last_bar[symbol], "volume"))
                except Exception:
                    vol = 0.0
                res = math.floor(min(order_sz, vol * self.max_deal_pct))
        try:
            self.logger.info("[match_engine][filled_sz] symbol=%s order_sz=%f mkt_vol=%.2f max_pct=%.2f result_sz=%f", symbol, float(order_sz), float(vol), float(self.max_deal_pct), float(res))
        except Exception:
            pass
        return res
    def process_open_orders(self, symbol):
        """
        处理已生效订单
        - 市价单按当前行情成交并自动对剩余部分撤单
        - 限价单暂未实现
        :param symbol: 合约代码（str）
        """
        list_oid_done = []
        for oid, ord in self.dict_opened_orders.items():
            if symbol != ord.symbol.decode():
                continue
            if ord.price_type == 1:
                #市价单
                filled_sz = self.calculate_filled_sz(symbol, ord.size)
                if filled_sz >= 1.0:
                    ord_evt = order_event.order_filled_evt()
                    ord_evt.order_id = oid
                    ord_evt.filled_px = self.calculate_filled_px(symbol)
                    ord_evt.filled_sz = filled_sz
                    ord.status.status = ord.on_order_evt(ord_evt)
                    self.om.on_fill(ord_evt)
                    self.func_on_fill(ord, ord_evt)
                    self.dict_all_orders[oid] = ord
                    if ord.status.status >= 3:
                        list_oid_done.append(oid)
                    self._log_debug("[match_engine][fill] oid=%d symbol=%s sz=%f px=%f", oid, symbol, filled_sz, ord_evt.filled_px)
                else:
                    try:
                        self.logger.info("[match_engine][no_fill] oid=%d symbol=%s filled_sz=%f (mkt_vol不足或max_pct限制)", oid, symbol, float(filled_sz))
                    except Exception:
                        pass
                #剩余未成交部分，自动撤单
                self.dict_pending_cancel_orders[oid] = oid
            else:
                #限价单
                # todo
                pass
        for oid in list_oid_done:
            del self.dict_opened_orders[oid]
        return
    def process_pending_cancel_orders(self):
        """
        处理撤单队列
        - 已生效订单按剩余数量撤单
        - 未生效订单生成撤单拒绝事件
        """
        list_oid_done = []
        for oid in self.dict_pending_cancel_orders.keys():
            if oid in self.dict_opened_orders.keys():
                ord_evt = order_event.order_cxlled_evt()
                ord_evt.order_id = oid
                ord = self.dict_opened_orders[oid]
                ord_evt.cxl_sz = ord.size - ord.filled_so_far
                ord.status.status = ord.on_order_evt(ord_evt)
                self.om.on_cxl(ord_evt)
                self.func_on_cxl(ord, ord_evt)
                self.dict_all_orders[oid] = ord
                if self.dict_opened_orders[oid].status.status >= 3:
                    list_oid_done.append(oid)
            else:
                ord_evt = order_event.order_cxl_rej_evt()
                ord_evt.order_id = oid
                self.om.on_cxl_rej(ord_evt)
                ord = self.dict_all_orders[oid]
                self.func_on_cxl_rej(ord, ord_evt)
                if oid in self.dict_all_orders.keys():
                    ord = self.dict_all_orders[oid]
                    ord.status.status = ord.on_order_evt(ord_evt)
                    self.dict_all_orders[oid] = ord
        if len(self.dict_pending_cancel_orders) > 0:
            self._log_debug("[match_engine][cancel] count=%d", len(self.dict_pending_cancel_orders))
        self.dict_pending_cancel_orders.clear()
        for oid in list_oid_done:
            del self.dict_opened_orders[oid]
        return

    def process_by_bar(self, bar):
        """
        按bar事件处理
        :param bar: 行情bar对象
        """
        symbol = ""
        if self.is_option:
            symbol = getattr(bar, "option_code", getattr(bar, "symbol", ""))
        else:
            symbol = getattr(bar, "symbol")
        if type(symbol) == type(b'bytes'):
            symbol = symbol.decode()
        self.dict_last_bar[symbol] = bar
        try:
            self._bar_total += 1
            if self._bar_total % self._log_sample_n == 0:
                self.logger.info("[match_engine][bar_stat] count=%d symbol=%s", self._bar_total, symbol)
        except Exception:
            pass
        self.process_pending_ack_orders()
        self.process_pending_conf_orders()
        self.process_open_orders(symbol)
        self.process_pending_cancel_orders()
        return
        
    def on_bar(self, bar):
        """
        行情bar回调入口
        :param bar: 行情bar对象
        """
        self.process_by_bar(bar)
        return
