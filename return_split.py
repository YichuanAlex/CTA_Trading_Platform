import position_manager as pm
import pandas as pd
import datetime
import pdb
import os

class return_split(object):
    def __init__(self) -> None:
        """
        收益拆分
        - _pm_: 持仓管理器
        - _dict_return_detail_: 分产品/合约的收益明细
        - _dict_last_bar_: 合约上一根bar，用于增量计算
        - trading_day: 当前交易日（yyyyMMdd）
        """
        self._pm_ = pm.position_manager()
        self._dict_return_detail_ = {}
        self._dict_last_bar_ = {}
        self.trading_day = ""
        pass

    def delta_return(self, start_price, end_price, delta, multi, size):
        return (end_price - start_price) * delta * multi * size

    def gamma_return(self, start_price, end_price, gamma, multi, size):
        return 1.0 / 2.0 * (end_price - start_price) * (end_price - start_price) * gamma * multi * size

    def vega_return(self, start_imp_vol, end_imp_vol, vega, multi, size):
        return (end_imp_vol - start_imp_vol) * 100 * vega * multi * size

    def theta_return(self, days, theta, multi, size):
        return days * theta * multi * size

    def other_return(self, total_return, delta_rtn, gamma_rtn, vega_rtn, theta_rtn):
        return total_return - delta_rtn - gamma_rtn - vega_rtn - theta_rtn

    def on_bod(self, tday):
        self.trading_day = tday.replace('-', '')
        return

    def on_place_order(self, ord, product_id):
        symbol = ord.symbol.decode()
        self._pm_.on_place_order(ord.symbol.decode(), ord.size, ord.side)
        symbol_root = product_id
        if symbol_root not in self._dict_return_detail_.keys():
            self._dict_return_detail_[symbol_root] = {}
        if symbol not in self._dict_return_detail_[symbol_root].keys():
            self._dict_return_detail_[symbol_root][symbol] = pd.DataFrame([], columns=["symbol", "trading_day", "time", "return",
                                                                                      "delta_rtn", "gamma_rtn", "vega_rtn", "theta_rtn", "other_rtn"])
        return

    def on_fill(self, ord, evt, multi, product_id):
        symbol = ord.symbol.decode()
        symbol_root = product_id
        side = ord.side
        filled_px = evt.filled_px
        filled_sz = evt.filled_sz
        self._pm_.on_fill(symbol, filled_sz, filled_px, side)
        net_pos = self._pm_.get_net_pos(symbol)
        pos = self._pm_.get_pos(symbol)
        pnl = 0
        if net_pos == 0:
            pnl = (pos.short_cost_basis - pos.long_cost_basis) * multi
            self._pm_.reset(symbol)
        else:
            pnl = (pos.short_cost_basis - pos.long_cost_basis) * multi + \
                  float(getattr(self._dict_last_bar_.get(symbol, pos), "option_close", 0.0)) * net_pos * multi
        idx = 0
        if self._dict_return_detail_[symbol_root][symbol].shape[0] > 0:
            idx = self._dict_return_detail_[symbol_root][symbol].shape[0] - 1
        if symbol in self._dict_last_bar_.keys():
            bar = self._dict_last_bar_[symbol]
            if str(getattr(bar, "trade_day")) != str(self.trading_day) and idx > 0:
                idx = idx + 1
        self._dict_return_detail_[symbol_root][symbol].loc[idx, "symbol"] = symbol
        self._dict_return_detail_[symbol_root][symbol].loc[idx, "trading_day"] = self.trading_day
        # 优先使用上一根bar的rt_time；若不可用则回退到订单下单时间
        rt_time = ""
        if symbol in self._dict_last_bar_.keys():
            rt_time = getattr(self._dict_last_bar_[symbol], "rt_time")
        else:
            try:
                rt_time = ord.place_time.decode()
            except Exception:
                rt_time = str(ord.place_time)
        self._dict_return_detail_[symbol_root][symbol].loc[idx, "time"] = rt_time
        end_idx = idx
        if idx > 0:
            end_idx = idx - 1
        pre_pnl = self._dict_return_detail_[symbol_root][symbol]["return"][0:end_idx].sum()
        self._dict_return_detail_[symbol_root][symbol].loc[idx, "return"] = pnl - pre_pnl
        delta_rtn = self._dict_return_detail_[symbol_root][symbol].loc[idx, "delta_rtn"]
        gamma_rtn = self._dict_return_detail_[symbol_root][symbol].loc[idx, "gamma_rtn"]
        vega_rtn = self._dict_return_detail_[symbol_root][symbol].loc[idx, "vega_rtn"]
        theta_rtn = self._dict_return_detail_[symbol_root][symbol].loc[idx, "theta_rtn"]
        self._dict_return_detail_[symbol_root][symbol].loc[idx, "other_rtn"] = self.other_return(pnl - pre_pnl, delta_rtn, gamma_rtn, 
                                                                                                  vega_rtn, theta_rtn)
        return

    def on_bar(self, bar, multi):
        symbol = getattr(bar, "option_code")
        net_pos = self._pm_.get_net_pos(symbol)
        if net_pos != 0:
            pre_delta = 0.0
            pre_gamma = 0.0
            pre_vega = 0.0
            pre_theta = 0.0
            if symbol in self._dict_last_bar_.keys():
                last_bar = self._dict_last_bar_[symbol]
                pre_delta = float(getattr(last_bar, "delta"))
                pre_gamma = float(getattr(last_bar, "gamma"))
                pre_vega = float(getattr(last_bar, "vega"))
                pre_theta = float(getattr(last_bar, "theta"))
            symbol_root = getattr(bar, "product_id")
            if symbol_root not in self._dict_return_detail_.keys():
                self._dict_return_detail_[symbol_root] = {}
            if symbol not in self._dict_return_detail_[symbol_root].keys():
                self._dict_return_detail_[symbol_root][symbol] = pd.DataFrame([], columns=["symbol", "trading_day", "time", "return", 
                                                                                          "delta_rtn", "gamma_rtn", "vega_rtn", "theta_rtn", "other_rtn"])
            trade_day = getattr(bar, "trade_day")
            rt_time = getattr(bar, "rt_time")
            idx = self._dict_return_detail_[symbol_root][symbol].shape[0]
            self._dict_return_detail_[symbol_root][symbol].loc[idx, "symbol"] = symbol
            self._dict_return_detail_[symbol_root][symbol].loc[idx, "trading_day"] = trade_day
            self._dict_return_detail_[symbol_root][symbol].loc[idx, "time"] = rt_time
            option_close = getattr(bar, "option_close")
            if option_close == 0.0 or str(option_close) == 'nan':
                if symbol in self._dict_last_bar_.keys():
                    option_close = getattr(self._dict_last_bar_[symbol], "option_close")
                option_close = float(option_close)
            pos = self._pm_.get_pos(symbol)
            pnl = (pos.short_cost_basis - pos.long_cost_basis) * multi + option_close * net_pos * multi
            pre_pnl = self._dict_return_detail_[symbol_root][symbol]["return"].sum()
            self._dict_return_detail_[symbol_root][symbol].loc[idx, "return"] = pnl - pre_pnl
            size = net_pos
            underlying_pre_close = float(getattr(bar, "underlying_pre_close"))
            if symbol in self._dict_last_bar_.keys():
                underlying_pre_close = float(getattr(self._dict_last_bar_[symbol], "underlying_close"))
            underlying_close = float(getattr(bar, "underlying_close"))
            delta_rtn = self.delta_return(underlying_pre_close, underlying_close, pre_delta, multi, size)
            self._dict_return_detail_[symbol_root][symbol].loc[idx, "delta_rtn"] = delta_rtn
            gamma_rtn = self.gamma_return(underlying_pre_close, underlying_close, pre_gamma, multi, size)
            self._dict_return_detail_[symbol_root][symbol].loc[idx, "gamma_rtn"] = gamma_rtn
            pre_imp_vol = getattr(bar, "pre_imp_vol")
            imp_vol = getattr(bar, "imp_vol")
            vega_rtn = self.vega_return(float(pre_imp_vol), float(imp_vol), pre_vega, multi, size)
            self._dict_return_detail_[symbol_root][symbol].loc[idx, "vega_rtn"] = vega_rtn
            days = 0
            if symbol in self._dict_last_bar_.keys():
                tday = str(trade_day).replace('-', '')
                if '.' in tday:
                    tday = tday.split('.')[0]
                cur_dt = datetime.datetime.strptime(tday + str(rt_time).split(' ')[-1], "%Y%m%d%H:%M:%S")
                last_tday = str(getattr(self._dict_last_bar_[symbol], "trade_day")).replace('-', '')
                if '.' in last_tday:
                    last_tday = last_tday.split('.')[0]
                pre_dt = datetime.datetime.strptime(last_tday + str(getattr(self._dict_last_bar_[symbol], "rt_time")).split(' ')[-1], "%Y%m%d%H:%M:%S")
                days = (cur_dt - pre_dt).total_seconds() / 60 / 60 / 24
            theta_rtn = self.theta_return(days, pre_theta, multi, size)
            self._dict_return_detail_[symbol_root][symbol].loc[idx, "theta_rtn"] = theta_rtn
            self._dict_return_detail_[symbol_root][symbol].loc[idx, "other_rtn"] = self.other_return(pnl - pre_pnl, delta_rtn, gamma_rtn, 
                                                                                                      vega_rtn, theta_rtn)
            self._dict_last_bar_[symbol] = bar
        return

    def on_done(self):
        """
        输出收益拆分明细到 CSV
        - 路径：result/return_detail/{product_id}_rtn_detail.csv
        - 按 trading_day 排序
        """
        output_path = os.path.join(".", "result", "return_detail")
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        for symbol_root, symbol_df in self._dict_return_detail_.items():
            filename = os.path.join(output_path, symbol_root + "_rtn_detail.csv")
            df = pd.DataFrame([], columns=["symbol", "trading_day", "time", "return",
                                           "delta_rtn", "gamma_rtn", "vega_rtn", "theta_rtn", "other_rtn"])
            for symbol, d in symbol_df.items():
                df = pd.concat([df, d], axis=0)
            # df.dropna(inplace=True)
            df["trading_day"] = df["trading_day"].astype(str)
            df.sort_values(by="trading_day", inplace=True)
            df.to_csv(filename, index=False)
        return
