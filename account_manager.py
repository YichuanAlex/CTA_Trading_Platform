import order
import position
import position_manager
import pandas as pd
import pdb
import datetime
import evaluating_indicator
import return_split
import logging

class account_manager(object):
    def __init__(self):
        self.init_balance = 0
        self.total_balance = 0
        self.available_balance = 0
        self.cost_basis = 0.0
        self.margin_rate = 1.0
        self.slippage_type = "pct"
        self.slippage_value = 0.0
        self.buy_fee_type = "pct"
        self.buy_fee_value = 0.0
        self.sell_fee_type = "pct"
        self.sell_fee_value = 0.0
        self.deal_type = "close"
        self.data_type = "1m"
        self.dict_all_orders = {}
        self.dict_last_bar = {}
        self.dict_last_tick = {}
        self.dict_daily_multiplier = {}
        self.dict_symbol_multiplier = {}
        self.df_daily_net_value = pd.DataFrame()
        self.cur_trading_day = ""
        self.df_all_trade_order = pd.DataFrame([], columns=["order_id","symbol", "side", "price", "size", "trade_day", "time","open_close"])
        self.annualized_return = 0.0
        self.max_drawdown = 0.0
        self.annualized_volatility = 0.0
        self.sharp_rate = 0.0
        self.daily_win_rate = 0.0
        self.monthly_win_rate = 0.0
        self.daily_win_loss_rate = 0.0
        self.monthly_win_loss_rate = 0.0
        #self.return_split = return_split.return_split()
        self.dict_product_id = {}
        self.is_option = False
        self.logger = logging.getLogger("account_manager")
        self._log_sample_n = 100
        self._debug_counter = 0
        self._bar_total = 0
        pass
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
        return

    def set_is_option(self, is_option):
        self.is_option = is_option
        return

    def set_product_id(self, dict_product_id):
        self.dict_product_id = dict_product_id
        return

    def set_daily_multiplier(self, daily_multiplier):
        self.dict_daily_multiplier = daily_multiplier
        try:
            for day, mp in self.dict_daily_multiplier.items():
                for sym, m in mp.items():
                    try:
                        mv = float(m)
                        if (mv != mv) or (mv <= 0.0) or (abs(mv) < 1e-6) or (abs(mv) > 1e6):
                            continue
                        self.dict_symbol_multiplier[sym] = mv
                    except Exception:
                        continue
        except Exception:
            pass
        return

    def set_balance(self, balance):
        self.init_balance = balance
        self.total_balance = balance
        self.available_balance = balance
        return

    def set_margin_rate(self, rate):
        self.margin_rate = rate
        return

    def set_slippage_type(self, type):
        #pct':按成交额比例，'fix'：固定值(元)
        self.slippage_type = type
        return

    def set_slippage_value(self, value):
        self.slippage_value = value
        return

    def set_buy_fee_type(self, type):
        #‘pct':按成交额比例，fix'：固定值(元)
        self.buy_fee_type = type
        return

    def set_buy_fee_value(self, value):
        #pct':按成交额比例，'fix'：固定值(元)
        self.buy_fee_value = value
        return

    def set_sell_fee_type(self, type):
        #pct'：按成交额比例，'fix'：固定值(元)
        self.sell_fee_type = type
        return

    def set_sell_fee_value(self, value):
        #‘pct':按成交额比例，‘fix'：固定值(元)
        self.sell_fee_value = value
        return

    def set_deal_type(self, type):
        #‘close'：以bar收盘价成交，
        #vwap'：以bar内"成交额/成交量”成交
        self.deal_type = type
        return

    def set_data_type(self, data_type):
        self.data_type = data_type
        return

    def get_multiplier(self, symbol):
        sym = symbol.decode() if type(symbol) == type(b'bytes') else str(symbol)
        m = None
        if sym in self.dict_symbol_multiplier:
            m = self.dict_symbol_multiplier.get(sym, None)
        td = str(self.cur_trading_day or "")
        if m is None and td in self.dict_daily_multiplier and sym in self.dict_daily_multiplier.get(td, {}):
            m = self.dict_daily_multiplier[td][sym]
        if m is None:
            td2 = td.replace('_', '-')
            if td2 in self.dict_daily_multiplier and sym in self.dict_daily_multiplier.get(td2, {}):
                m = self.dict_daily_multiplier[td2][sym]
        if m is None:
            td3 = td.replace('-', '')
            if td3 in self.dict_daily_multiplier and sym in self.dict_daily_multiplier.get(td3, {}):
                m = self.dict_daily_multiplier[td3][sym]
        if m is None:
            try:
                keys = sorted(list(self.dict_daily_multiplier.keys()))
                for day in reversed(keys):
                    mp = self.dict_daily_multiplier.get(day, {})
                    if sym in mp:
                        m = mp[sym]
                        break
            except Exception:
                m = None
        try:
            mv = float(m) if m is not None else 1.0
            if (mv != mv) or (mv <= 0.0) or (abs(mv) < 1e-6) or (abs(mv) > 1e6):
                mv = 1.0
        except Exception:
            mv = 1.0
        try:
            self.dict_symbol_multiplier[sym] = mv
        except Exception:
            pass
        return mv

    def calculate_fee_cost(self, mkt_value, side, symbol, filled_size):
        cost = 0.0
        multiplier = self.get_multiplier(symbol)
        if side == 0:
            if self.buy_fee_type == "pct":
                cost += mkt_value * multiplier * self.buy_fee_value
            else:
                cost += self.buy_fee_value * filled_size
        else:
            if self.sell_fee_type == "pct":
                cost += mkt_value * multiplier * self.sell_fee_value
            else:
                cost += self.sell_fee_value * filled_size
        
        if self.slippage_type == "pct":
            cost += mkt_value * multiplier * self.slippage_value
        else:
            cost += self.slippage_value * multiplier * filled_size
        return cost

    def calculate_order_margin_cost(self, symbol, side, size, price):
        #对于期货来说，占用资金即：价格*乘数*数量*保证金比例
        mkt_value = price * size
        multiplier = self.get_multiplier(symbol)
        order_cost = mkt_value * multiplier * self.margin_rate
        return order_cost

    def risk_check(self, symbol, side, size, price, net_pos):
        try:
            px = price
            if abs(px - 0.0) < 0.0000001:
                if self.data_type == "tick":
                    try:
                        px = self.dict_last_tick[symbol].last_price
                    except Exception:
                        px = 0.0
                else:
                    if self.deal_type == "close":
                        if self.is_option:
                            try:
                                px = getattr(self.dict_last_bar[symbol], "option_close")
                            except Exception:
                                try:
                                    px = getattr(self.dict_last_bar[symbol], "option_pre_close")
                                except Exception:
                                    px = 0.0
                        else:
                            try:
                                px = getattr(self.dict_last_bar[symbol], "close")
                            except Exception:
                                try:
                                    px = getattr(self.dict_last_bar[symbol], "pre_close")
                                except Exception:
                                    px = 0.0
                    else:
                        if self.is_option:
                            try:
                                px = getattr(self.dict_last_bar[symbol], "option_open")
                            except Exception:
                                try:
                                    px = getattr(self.dict_last_bar[symbol], "option_pre_close")
                                except Exception:
                                    px = 0.0
                        else:
                            try:
                                px = getattr(self.dict_last_bar[symbol], "open")
                            except Exception:
                                try:
                                    px = getattr(self.dict_last_bar[symbol], "pre_close")
                                except Exception:
                                    px = 0.0
            try:
                px = float(px)
                if px != px or px == float("inf") or px == float("-inf") or px <= 0.0:
                    px = float(price) if abs(float(price)) > 0.0 else 0.0
            except Exception:
                px = float(price) if abs(float(price)) > 0.0 else 0.0
            opening = False
            if net_pos >= 0 and side == 0:
                opening = True
            if net_pos <= 0 and side == 1:
                opening = True
            if opening:
                need = self.calculate_order_margin_cost(symbol, side, size, px)
                if self.available_balance < need:
                    try:
                        self.logger.warning("[risk_check][deny] symbol=%s side=%d size=%f px=%.6f need=%.2f avail=%.2f net_pos=%.2f", symbol, side, size, px, need, self.available_balance, net_pos)
                    except Exception:
                        pass
                    return False
            try:
                need2 = self.calculate_order_margin_cost(symbol, side, size, px)
                self.logger.debug("[risk_check][pass] symbol=%s side=%d size=%f px=%.6f open=%s avail=%.2f need=%.2f", symbol, side, size, px, str(opening), self.available_balance, need2)
            except Exception:
                pass
        except Exception:
            try:
                self.logger.warning("[risk_check][error] symbol=%s side=%d size=%f price=%.6f net_pos=%.2f", symbol, side, size, price, net_pos)
            except Exception:
                pass
        return True

    def get_available(self):
        return self.available_balance

    def on_bod(self, trading_day):
        self.cur_trading_day = trading_day
        self.dict_all_orders.clear()
        self._log_debug("[account_manager][bod] trading_day=%s", trading_day)
        return

    def on_place_order(self, ord, net_pos):
        symbol = ord.symbol.decode()
        price = ord.price
        if abs(ord.price - 0.0) < 0.0000001:
            if self.data_type == "tick":
                try:
                    price = self.dict_last_tick[symbol].last_price
                except Exception:
                    price = 0.0
            else:
                if self.deal_type == "close":
                    if self.is_option:
                        try:
                            price = getattr(self.dict_last_bar[symbol], "option_close")
                        except Exception:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "option_pre_close")
                            except Exception:
                                price = 0.0
                    else:
                        try:
                            price = getattr(self.dict_last_bar[symbol], "close")
                        except Exception:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "pre_close")
                            except Exception:
                                price = 0.0
                else:
                    if self.is_option:
                        try:
                            price = getattr(self.dict_last_bar[symbol], "option_open")
                        except Exception:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "option_pre_close")
                            except Exception:
                                price = 0.0
                    else:
                        try:
                            price = getattr(self.dict_last_bar[symbol], "open")
                        except Exception:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "pre_close")
                            except Exception:
                                price = 0.0
        
        mkt_value = float(price) * ord.size
        opening = (ord.side == 0 and net_pos >= 0) or (ord.side == 1 and net_pos <= 0)
        ord.open_close = 0 if opening else 1
        order_margin_cost = self.calculate_order_margin_cost(symbol, ord.side, ord.size, price)
        if ord.open_close == 0:
            self.available_balance -= order_margin_cost
        
        if abs(ord.price - 0.0) < 0.0000001:
            if self.data_type == "tick":
                try:
                    ord.price = self.dict_last_tick[symbol].last_price
                except Exception:
                    ord.price = 0.0
            else:
                if self.deal_type == "close":
                    if self.is_option:
                        try:
                            ord.price = getattr(self.dict_last_bar[symbol], "option_close")
                        except Exception:
                            try:
                                ord.price = getattr(self.dict_last_bar[symbol], "option_pre_close")
                            except Exception:
                                ord.price = 0.0
                    else:
                        try:
                            ord.price = getattr(self.dict_last_bar[symbol], "close")
                        except Exception:
                            try:
                                ord.price = getattr(self.dict_last_bar[symbol], "pre_close")
                            except Exception:
                                ord.price = 0.0
                else:
                    if self.is_option:
                        try:
                            ord.price = getattr(self.dict_last_bar[symbol], "option_open")
                        except Exception:
                            try:
                                ord.price = getattr(self.dict_last_bar[symbol], "option_pre_close")
                            except Exception:
                                ord.price = 0.0
                    else:
                        try:
                            ord.price = getattr(self.dict_last_bar[symbol], "open")
                        except Exception:
                            try:
                                ord.price = getattr(self.dict_last_bar[symbol], "pre_close")
                            except Exception:
                                ord.price = 0.0
        
        self.dict_all_orders[ord.order_id] = ord
        self._log_debug("[account_manager][place_order] oid=%d symbol=%s side=%d size=%f price=%f net_pos=%f oc=%d avail=%.2f", ord.order_id, symbol, ord.side, ord.size, ord.price, net_pos, ord.open_close, self.available_balance)
        return

    def store_trade_order(self, ord, fill_detail):
        sym = ord.symbol.decode()
        side = 1 if ord.side == 0 else -1
        px = fill_detail.filled_px
        sz = fill_detail.filled_sz
        oid = int(getattr(ord, "order_id", 0))
        tday = datetime.datetime.strptime(self.cur_trading_day, "%Y-%m-%d")
        try:
            tm = ord.place_time.decode() if hasattr(ord.place_time, "decode") else str(ord.place_time)
        except Exception:
            tm = str(ord.place_time)
        oc = int(getattr(ord, "open_close", 0))
        try:
            if not isinstance(self.df_all_trade_order, pd.DataFrame) or set(self.df_all_trade_order.columns) != {"order_id","symbol","side","price","size","trade_day","time","open_close"}:
                self.df_all_trade_order = pd.DataFrame([], columns=["order_id","symbol","side","price","size","trade_day","time","open_close"])
            self.df_all_trade_order.loc[self.df_all_trade_order.shape[0]] = [oid, sym, side, float(px), float(sz), tday, str(tm), oc]
        except Exception:
            pass
        return

    def on_fill(self, ord, evt):
        mkt_value = evt.filled_px * evt.filled_sz
        symbol = ord.symbol.decode()
        fee_cost = self.calculate_fee_cost(mkt_value, ord.side, symbol, evt.filled_sz)
        self.available_balance -= fee_cost
        self.total_balance -= fee_cost
        if ord.side == 0:
            self.cost_basis -= mkt_value * self.get_multiplier(symbol)
        else:
            self.cost_basis += mkt_value * self.get_multiplier(symbol)
        if ord.open_close == 1 and evt.filled_sz and evt.filled_sz > 0:
            self.available_balance += self.calculate_order_margin_cost(symbol, ord.side, evt.filled_sz, evt.filled_px)
        self.store_trade_order(ord, evt)
        self._log_debug("[account_manager][fill] oid=%d symbol=%s side=%d sz=%f px=%f fee=%.2f avail=%.2f total=%.2f cost_basis=%.2f", ord.order_id, symbol, ord.side, evt.filled_sz, evt.filled_px, fee_cost, self.available_balance, self.total_balance, self.cost_basis)
        #self.return_split.on_fill(ord, evt, self.get_multiplier(symbol), self.dict_product_id[symbol])
        return

    def on_cxl(self, evt):
        ord = self.dict_all_orders[evt.order_id]
        symbol = ord.symbol.decode()
        price = ord.price
        if abs(price - 0.0) < 0.0000001:
            if self.data_type == "tick":
                try:
                    price = self.dict_last_tick[symbol].last_price
                except Exception:
                    price = 0.0
            else:
                if self.deal_type == "close":
                    if self.is_option:
                        try:
                            price = getattr(self.dict_last_bar[symbol], "option_close")
                        except Exception:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "option_pre_close")
                            except Exception:
                                price = 0.0
                    else:
                        try:
                            price = getattr(self.dict_last_bar[symbol], "close")
                        except Exception:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "pre_close")
                            except Exception:
                                price = 0.0
                else:
                    if self.is_option:
                        try:
                            price = getattr(self.dict_last_bar[symbol], "option_open")
                        except Exception:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "option_pre_close")
                            except Exception:
                                price = 0.0
                    else:
                        try:
                            price = getattr(self.dict_last_bar[symbol], "open")
                        except Exception:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "pre_close")
                            except Exception:
                                price = 0.0
        if ord.open_close == 0 and evt.cxl_sz and evt.cxl_sz > 0:
            try:
                self.available_balance += self.calculate_order_margin_cost(symbol, ord.side, evt.cxl_sz, price)
            except Exception:
                pass
        ord.cxl_sz = evt.cxl_sz
        self.dict_all_orders[evt.order_id] = ord
        self._log_debug("[account_manager][cancel] oid=%d symbol=%s cxl_sz=%f avail=%.2f", evt.order_id, symbol, evt.cxl_sz, self.available_balance)
        return

    def on_rej(self, evt):
        try:
            ord = self.dict_all_orders.get(evt.order_id, None)
            if ord is None:
                return
            symbol = ord.symbol.decode()
            price = ord.price
            if abs(price - 0.0) < 0.0000001:
                if self.data_type == "tick":
                    try:
                        price = self.dict_last_tick[symbol].last_price
                    except Exception:
                        price = 0.0
                else:
                    if self.deal_type == "close":
                        if self.is_option:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "option_close")
                            except Exception:
                                try:
                                    price = getattr(self.dict_last_bar[symbol], "option_pre_close")
                                except Exception:
                                    price = 0.0
                        else:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "close")
                            except Exception:
                                try:
                                    price = getattr(self.dict_last_bar[symbol], "pre_close")
                                except Exception:
                                    price = 0.0
                    else:
                        if self.is_option:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "option_open")
                            except Exception:
                                try:
                                    price = getattr(self.dict_last_bar[symbol], "option_pre_close")
                                except Exception:
                                    price = 0.0
                        else:
                            try:
                                price = getattr(self.dict_last_bar[symbol], "open")
                            except Exception:
                                try:
                                    price = getattr(self.dict_last_bar[symbol], "pre_close")
                                except Exception:
                                    price = 0.0
            if ord.open_close == 0 and ord.size and ord.size > 0:
                self.available_balance += self.calculate_order_margin_cost(symbol, ord.side, ord.size, price)
        except Exception:
            pass
        return

    def on_bar(self, bar):
        symbol = ""
        if self.is_option:
            symbol = getattr(bar, "option_code")
        else:
            symbol = getattr(bar, "symbol")
        self.dict_last_bar[symbol] = bar
        try:
            self._bar_total += 1
            if self._bar_total % self._log_sample_n == 0:
                self.logger.info("[account_manager][bar_stat] count=%d symbol=%s", self._bar_total, symbol)
        except Exception:
            pass
        return

    def calculate_pnl(self, pm):
        all_pos = pm.get_all_pos()
        cur_balance = self.total_balance + self.cost_basis
        for symbol, pos in all_pos.items():
            if type(symbol) == type(b'bytes'):
                symbol = symbol.decode()
            net_pos = pos.long - pos.short
            if abs(net_pos - 0.0) > 0.0000001:
                px = 0.0
                try:
                    if self.data_type == "tick":
                        px = float(getattr(self.dict_last_tick.get(symbol, object()), "last_price", 0.0))
                    else:
                        bar = self.dict_last_bar.get(symbol, None)
                        if self.is_option:
                            px = float(getattr(bar, "option_close", getattr(bar, "option_pre_close", 0.0)) if bar is not None else 0.0)
                        else:
                            px = float(getattr(bar, "close", getattr(bar, "pre_close", 0.0)) if bar is not None else 0.0)
                except Exception:
                    px = 0.0
                try:
                    if px != px or px == float("inf") or px == float("-inf") or px < 0.0:
                        px = 0.0
                except Exception:
                    px = 0.0
                cur_balance += px * net_pos * self.get_multiplier(symbol)
        return cur_balance

    def on_eod(self, trading_day, pm):
        self.calculate_net_value(trading_day, pm)
        return

    def get_all_trade_order(self):
        return self.df_all_trade_order

    # 计算每日净值
    def calculate_net_value(self, trading_day, pm):
        cur_balance = float(self.calculate_pnl(pm))
        base = float(self.init_balance) if abs(self.init_balance) > 0.0000001 else 1.0
        nv = cur_balance / base
        try:
            import logging
            logging.info("[net_value] tday=%s base=%.4f total=%.4f cost_basis=%.4f cur=%.4f nv=%.6f", trading_day, base, self.total_balance, self.cost_basis, cur_balance, nv)
        except Exception:
            pass
        if nv != nv or nv == float("inf") or nv == float("-inf"):
            if self.df_daily_net_value.shape[0] > 0 and "net_value" in self.df_daily_net_value.columns:
                try:
                    nv = float(self.df_daily_net_value.iloc[-1]["net_value"])
                except Exception:
                    nv = 1.0
            else:
                nv = 1.0
        row = [datetime.datetime.strptime(trading_day, "%Y-%m-%d"), nv, base, cur_balance]
        self.df_daily_net_value = pd.concat([self.df_daily_net_value, pd.DataFrame([row], columns=["trading_day","net_value","init_balance","cur_balance"])])
        return

    def on_done(self, pm):
        balance = float(self.calculate_pnl(pm))
        base = float(self.init_balance) if abs(self.init_balance) > 0.0000001 else 1.0
        nav = [base, balance]
        import pandas as pd
        df = self.df_daily_net_value.copy()
        td_cnt = 0 if df is None else int(df.shape[0])
        if td_cnt <= 0:
            s = pd.Series([1.0])
        else:
            if "net_value" in df.columns:
                s = pd.to_numeric(df["net_value"], errors="coerce").ffill().fillna(1.0)
            elif "cur_balance" in df.columns and "init_balance" in df.columns:
                cur = pd.to_numeric(df["cur_balance"], errors="coerce")
                initb = pd.to_numeric(df["init_balance"], errors="coerce")
                s = (cur / initb).replace([float("inf"), float("-inf")], pd.NA).ffill().fillna(1.0)
            else:
                s = pd.Series([1.0] * td_cnt)
        self.annualized_return = evaluating_indicator.calculate_annualized_return(nav, td_cnt if td_cnt > 0 else 1)
        self.max_drawdown = evaluating_indicator.calculate_max_drawdown(list(s))
        try:
            s_ret = s.pct_change().replace([float("inf"), float("-inf")], pd.NA).dropna()
        except Exception:
            s_ret = s.diff()
        self.annualized_volatility = evaluating_indicator.calculate_annualized_volatility(s_ret)
        self.sharp_rate = evaluating_indicator.calculate_sharp_rate(self.annualized_return, self.annualized_volatility)
        try:
            list_daily = list(s.pct_change().replace([float("inf"), float("-inf")], pd.NA).dropna())
        except Exception:
            list_daily = []
        self.daily_win_rate = evaluating_indicator.calculate_win_rate(list_daily) if list_daily else None
        try:
            if "trading_day" in df.columns:
                df2 = pd.DataFrame({"trading_day": pd.to_datetime(df["trading_day"]), "net_value": list(s)})
                df2["month"] = df2["trading_day"].dt.to_period("M")
                grp = df2.groupby("month")
                monthly_ret = []
                for _, g in grp:
                    try:
                        first_nv = float(g["net_value"].iloc[0])
                        last_nv = float(g["net_value"].iloc[-1])
                        if first_nv > 0.0 and last_nv > 0.0:
                            monthly_ret.append(last_nv / first_nv - 1.0)
                    except Exception:
                        pass
                list_diff = monthly_ret
            else:
                list_diff = []
        except Exception:
            list_diff = []
        self.monthly_win_rate = evaluating_indicator.calculate_win_rate(list_diff) if list_diff else None
        self.daily_win_loss_rate = evaluating_indicator.calculate_win_loss_rate(list_daily) if list_daily else None
        self.monthly_win_loss_rate = evaluating_indicator.calculate_win_loss_rate(list_diff) if list_diff else None
        try:
            if (not list_daily) and s.shape[0] >= 1:
                base_nv = float(s.iloc[0])
                last_nv = float(s.iloc[-1])
                if base_nv > 0.0 and last_nv > 0.0:
                    ret1 = last_nv / base_nv - 1.0
                    self.daily_win_rate = 1.0 if ret1 > 0.0 else 0.0
                    self.daily_win_loss_rate = 1.0 if ret1 > 0.0 else 0.0
        except Exception:
            pass
        try:
            if (not list_diff) and s.shape[0] >= 1:
                base_nv_m = float(s.iloc[0])
                last_nv_m = float(s.iloc[-1])
                if base_nv_m > 0.0 and last_nv_m > 0.0:
                    ret_m = last_nv_m / base_nv_m - 1.0
                    self.monthly_win_rate = 1.0 if ret_m > 0.0 else 0.0
                    self.monthly_win_loss_rate = 1.0 if ret_m > 0.0 else 0.0
        except Exception:
            pass
        return

    def get_result(self):
        try:
            import pandas as pd
            import datetime
            df = self.df_daily_net_value.copy()
            if df is None or df.shape[0] == 0:
                df = pd.DataFrame([[datetime.datetime.now().strftime("%Y-%m-%d"), 1.0, self.init_balance, self.init_balance]],
                                  columns=["trading_day","net_value","init_balance","cur_balance"])
            if "trading_day" not in df.columns:
                if "date_time" in df.columns:
                    df["trading_day"] = df["date_time"].astype(str).str.split().str[0]
                else:
                    df["trading_day"] = datetime.datetime.now().strftime("%Y-%m-%d")
            if "net_value" not in df.columns:
                if "cur_balance" in df.columns and "init_balance" in df.columns:
                    cur = pd.to_numeric(df["cur_balance"], errors="coerce")
                    initb = pd.to_numeric(df["init_balance"], errors="coerce")
                    df["net_value"] = (cur / initb).replace([float("inf"), float("-inf")], pd.NA)
                else:
                    df["net_value"] = 1.0
            df = df.drop_duplicates(subset=["trading_day"], keep="last")
            df["trading_day"] = pd.to_datetime(df["trading_day"], errors="coerce")
            df["net_value"] = pd.to_numeric(df["net_value"], errors="coerce")
            df = df.sort_values("trading_day")
            df["net_value"] = df["net_value"].ffill().fillna(1.0)
            self.df_daily_net_value = df
        except Exception:
            try:
                import pandas as pd
                if "trading_day" in self.df_daily_net_value.columns:
                    self.df_daily_net_value["trading_day"] = pd.to_datetime(self.df_daily_net_value["trading_day"], errors="coerce")
                    self.df_daily_net_value = self.df_daily_net_value.sort_values("trading_day")
            except Exception:
                pass
        try:
            ret_df = self.df_daily_net_value.set_index("trading_day")[["net_value"]]
        except Exception:
            if "trading_day" in self.df_daily_net_value.columns and "net_value" in self.df_daily_net_value.columns:
                ret_df = self.df_daily_net_value[["trading_day","net_value"]]
            else:
                import pandas as pd
                import datetime
                ret_df = pd.DataFrame([[datetime.datetime.now(), 1.0]], columns=["trading_day","net_value"]).set_index("trading_day")
        return (ret_df,self.df_all_trade_order,self.annualized_return,self.max_drawdown,self.annualized_volatility,self.sharp_rate,self.daily_win_rate, self.monthly_win_rate, self.daily_win_loss_rate, self.monthly_win_loss_rate)
