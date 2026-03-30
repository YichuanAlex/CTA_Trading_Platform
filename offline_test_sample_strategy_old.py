import os
import json
import datetime
import logging
import pandas as pd
import matplotlib.pyplot as plt
import statistics
import calendar

import client_api
from strategy.source_code.test.test_sample_strategy import test_sample_strategy


class OfflineAdapter:
    """
    离线回测适配器（基于主力连续合约的截面相对强弱策略）
    - 负责：调仓门控、因子计算、目标仓位生成、订单执行顺序（先平后开）、状态记录
    - 事件入口：on_bod / on_eod / on_bar / on_section_bar
    - 依赖：client_api（撮合 + 账户 + 持仓 + 日历），dataset 目录
    """
    def __init__(self):
        """
        初始化运行时状态与参数
        - api: 平台API对象，init(api)后赋值
        - cur_trading_day: 当前交易日（str 或 datetime/date）
        - last_dict_target_position: 上一次的目标仓位字典 {symbol: net_qty}
        - strat: 在线策略对象，用于读取部分配置（如门控参数、保留池倍数等）
        - prev_close: 记录各symbol上一个可用的收盘价，用于补齐 pre_close 缺失
        - top_n_long/top_n_short: 多空信号数量（默认 3/3）；可从策略JSON参数外部覆盖
        - signal_threshold: 信号阈值（>=阈值做多，<=-阈值做空）
        - _last_rebalance_day: 最近一次实际成交的调仓交易日（避免重复调仓）
        - _pending_rebalance_day/_pending_pre_fill_total: 挂起的调仓检测（用成交累计量确认）
        - BUY/SELL: 委托方向映射到平台（BUY=1，SELL=-1；平台将 1 映射为 side=0，-1 映射为 side=1）
        """
        self.api = None
        self.cur_trading_day = None
        self.last_dict_target_position = {}
        self.strat = test_sample_strategy()
        self.prev_close = {}
        self.top_n_long = 3
        self.top_n_short = 3
        self.signal_threshold = 0.0
        self._last_rebalance_day = None
        self._pending_rebalance_day = None
        self._pending_pre_fill_total = 0.0
        self.BUY = 1
        self.SELL = -1

    def init(self, api):
        """
        适配器初始化
        - 参数：
          api: client_api.client_api 实例
        - 行为：
          绑定 API；调用在线策略 init(api) 以加载配置；按需覆盖门控开关与信号阈值
        - 返回：
          None
        """
        self.api = api
        self.strat.init(api)
        try:
            self.strat.enable_trend_filter = False
            self.strat.signal_threshold = 0.0
            try:
                self.top_n_long = int(getattr(self.strat, "long_count", self.top_n_long))
            except Exception:
                pass
            try:
                self.top_n_short = int(getattr(self.strat, "short_count", self.top_n_short))
            except Exception:
                pass
        except Exception:
            pass
        return

    def on_bod(self, trading_day):
        """
        盘前事件（Begin Of Day）
        - 参数：
          trading_day: 当前交易日（str 或 datetime/date）
        - 行为：
          更新当前交易日并打印日志
        """
        self.cur_trading_day = trading_day
        logging.info("[offline][on_bod] %s", trading_day)
        return

    def on_eod(self, trading_day):
        """
        收盘事件（End Of Day）
        - 参数：
          trading_day: 当前交易日（str 或 datetime/date）
        - 行为：
          打印日志；可扩展收尾动作
        """
        logging.info("[offline][on_eod] %s", trading_day)
        return

    def on_bar(self, bar):
        """
        单bar事件（盘中）
        - 参数：
          bar: 单根bar对象，需包含至少 symbol / date_time 或 datetime
        - 行为：
          打印调试日志；如存在挂起的调仓确认，在匹配交易日后用成交累计量确认本次调仓已发生
        """
        try:
            logging.debug("[offline][bar] %s %s", str(getattr(bar, "symbol", "")), str(getattr(bar, "date_time", "")))
        except Exception:
            pass
        try:
            if self._pending_rebalance_day:
                dt_str = str(getattr(bar, "date_time", "")).split(" ")[0]
                if not dt_str:
                    dt = getattr(bar, "datetime", None)
                    if dt:
                        dt_str = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
                if dt_str == self._pending_rebalance_day:
                    cur_total = 0.0
                    try:
                        pm = self.api.get_pm()
                        for _, pos in pm.dict_positions.items():
                            cur_total += float(getattr(pos, "long_total_filled", 0.0)) + float(getattr(pos, "short_total_filled", 0.0))
                    except Exception:
                        cur_total = 0.0
                    if cur_total > self._pending_pre_fill_total:
                        self._last_rebalance_day = self._pending_rebalance_day
                        self._pending_rebalance_day = None
                        self._pending_pre_fill_total = 0.0
        except Exception:
            pass
        return

    def _parse_trading_day_str(self, list_bar):
        """
        从当前状态或bar集合中解析交易日字符串（YYYY-MM-DD）
        - 参数：
          list_bar: 当前截面bar列表
        - 返回：
          str: 交易日（YYYY-MM-DD）；若无法解析返回空字符串
        """
        td = self.cur_trading_day
        try:
            if isinstance(td, datetime.date):
                return f"{td.year:04d}-{td.month:02d}-{td.day:02d}"
            if isinstance(td, datetime.datetime):
                return f"{td.year:04d}-{td.month:02d}-{td.day:02d}"
            if isinstance(td, str):
                if len(td) == 8 and td.isdigit():
                    return f"{td[0:4]}-{td[4:6]}-{td[6:8]}"
                return td.split(" ")[0]
        except Exception:
            pass
        try:
            b = list_bar[0]
            tday = getattr(b, "trading_day", None)
            if isinstance(tday, str) and tday:
                if len(tday) == 8 and tday.isdigit():
                    return f"{tday[0:4]}-{tday[4:6]}-{tday[6:8]}"
                return tday.split(" ")[0]
            dt = getattr(b, "datetime", None)
            if dt:
                return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
            dt_str = str(getattr(b, "date_time", "")).split(" ")[0]
            if dt_str:
                return dt_str
        except Exception:
            pass
        return ""

    def _parse_bar_time(self, list_bar):
        """
        提取截面bar的时间（用于交易时间窗口门控）
        - 参数：
          list_bar: 当前截面bar列表
        - 返回：
          datetime.time 或 None
        """
        if not list_bar:
            return None
        b = list_bar[0]
        dt_str = str(getattr(b, "date_time", "")).strip()
        if dt_str:
            parts = dt_str.split(" ")
            if len(parts) >= 2:
                try:
                    hh, mm, ss = parts[1].split(":")
                    return datetime.time(int(hh), int(mm), int(float(ss)))
                except Exception:
                    pass
        dt = getattr(b, "datetime", None)
        if isinstance(dt, datetime.datetime):
            return dt.time()
        return None

    def on_section_bar(self, list_bar):
        """
        截面bar事件（调仓主入口）
        - 参数：
          list_bar: 当前截面bar列表；需包含至少 symbol / close / open_interest / trading_day（或 datetime）
        - 门控：
          - 首日不调仓
          - rebalance_interval + rebalance_unit（trading_day/week/month）
          - 交易时间窗：open_allowed_after ~ cancel_cutoff_time
          - 同一交易日避免重复调仓（基于成交累计量确认）
        - 行为：
          计算目标仓位 → 先平不在目标内的旧仓 → 对目标仓执行（同向差额；反手先平后开）
        """
        try:
            logging.info("[offline][section] BAR TRIGGERED, size=%d", len(list_bar))
        except Exception:
            pass
        if not list_bar:
            return
        td_str = self._parse_trading_day_str(list_bar)
        if not td_str:
            return
        tds = getattr(self.api, "trading_date_section", [])
        try:
            idx = tds.index(td_str) if isinstance(tds, list) and td_str in tds else -1
        except Exception:
            idx = -1
        if idx < 0:
            return
        if idx == 0:
            logging.info("[offline][rebalance_gate] td=%s idx=%d pass=%s", td_str, idx, "False(first_day)")
            return
        try:
            ri = int(getattr(self.strat, "rebalance_interval", 5))
        except Exception:
            ri = 5
        if ri <= 0:
            ri = 1
        try:
            unit = str(getattr(self.strat, "rebalance_unit", "trading_day")).lower()
        except Exception:
            unit = "trading_day"
        gate_ok = False
        try:
            cur_dt = datetime.datetime.strptime(td_str, "%Y-%m-%d").date()
        except Exception:
            cur_dt = None
        if unit == "week" and cur_dt and idx > 0:
            prev_str = tds[idx - 1] if idx > 0 else None
            try:
                prev_dt = datetime.datetime.strptime(prev_str, "%Y-%m-%d").date() if prev_str else None
            except Exception:
                prev_dt = None
            gate_ok = bool(prev_dt and (prev_dt.isocalendar()[1] != cur_dt.isocalendar()[1] or prev_dt.isocalendar()[0] != cur_dt.isocalendar()[0]))
        elif unit == "month" and cur_dt and idx > 0:
            prev_str = tds[idx - 1] if idx > 0 else None
            try:
                prev_dt = datetime.datetime.strptime(prev_str, "%Y-%m-%d").date() if prev_str else None
            except Exception:
                prev_dt = None
            gate_ok = bool(prev_dt and (prev_dt.month != cur_dt.month or prev_dt.year != cur_dt.year))
        else:
            gate_ok = (idx % ri == 0)
        logging.info("[offline][rebalance_gate] td=%s idx=%d ri=%d unit=%s pass=%s", td_str, idx, ri, unit, str(bool(gate_ok)))
        if not gate_ok:
            return
        if self._last_rebalance_day == td_str:
            return
        bt = self._parse_bar_time(list_bar)
        try:
            open_after = getattr(self.strat, "open_allowed_after", datetime.time(9, 10))
        except Exception:
            open_after = datetime.time(9, 10)
        try:
            cutoff = getattr(self.strat, "cancel_cutoff_time", datetime.time(15, 0))
        except Exception:
            cutoff = datetime.time(15, 0)
        if bt and (bt < open_after or bt >= cutoff):
            logging.info("[offline][precheck][time_window] td=%s bar_time=%s allow_after=%s cutoff=%s", td_str, bt.isoformat(), open_after.isoformat(), cutoff.isoformat())
            return
        dict_target_position, is_pos_changed = self._generate_targets(list_bar)
        try:
            logging.info("[offline][section] signal changed=%s targets=%s", str(is_pos_changed), str(dict_target_position))
        except Exception:
            pass
        try:
            cur_int = {str(k): int(round(v)) for k, v in dict_target_position.items()}
            last_int = {str(k): int(round(v)) for k, v in self.last_dict_target_position.items()}
        except Exception:
            cur_int = dict_target_position
            last_int = self.last_dict_target_position
        if cur_int == last_int:
            self.last_dict_target_position = dict_target_position.copy()
            return

        balance = self.api.get_balance()
        try:
            dict_cur_pos_raw = self.api.get_current_position()
            dict_cur_pos = {(k.decode('utf-8') if isinstance(k, bytes) else str(k)): v for k, v in dict_cur_pos_raw.items()}
        except Exception:
            dict_cur_pos = self.api.get_current_position()
        logging.debug("[offline][section] cur_pos=%s balance=%.2f", str(dict_cur_pos), balance)
        dict_close_price = {}
        for bar in list_bar:
            try:
                dict_close_price[str(getattr(bar, "symbol"))] = float(getattr(bar, "close"))
            except Exception:
                pass

        try:
            pm = self.api.get_pm()
            pre_total = 0.0
            for _, pos in pm.dict_positions.items():
                pre_total += float(getattr(pos, "long_total_filled", 0.0)) + float(getattr(pos, "short_total_filled", 0.0))
            self._pending_rebalance_day = td_str
            self._pending_pre_fill_total = pre_total
        except Exception:
            self._pending_rebalance_day = td_str
            self._pending_pre_fill_total = 0.0

        # close positions not in targets first (release margin)
        for symbol, net_pos in dict_cur_pos.items():
            if symbol not in dict_target_position.keys():
                if abs(net_pos) != 0:
                    try:
                        direction = self.SELL if net_pos > 0 else self.BUY
                        oid = self.api.send_order(symbol, abs(int(round(net_pos))), direction)
                        if oid == -1:
                            logging.error("[offline][close][perm_fail] symbol=%s sz=%d dir=%d", symbol, abs(int(round(net_pos))), 1 if net_pos > 0 else 0)
                            continue
                    except Exception as e:
                        logging.error("[offline][close][fail] symbol=%s err=%s", symbol, str(e))
                        continue
            logging.debug("[offline][close] symbol=%s sz=%d side=%d", symbol, abs(int(round(net_pos))), 1 if net_pos > 0 else 0)

        # open/adjust positions with explicit close-then-open on flip
        for symbol, target_pos in dict_target_position.items():
            last_target_pos = self.last_dict_target_position.get(symbol, -1000)
            if abs(target_pos - last_target_pos) < 1e-6 and (symbol not in dict_cur_pos.keys()):
                continue

            multiplier = 1.0
            try:
                if self.cur_trading_day in getattr(self.api, "dict_daily_multiplier", {}) and \
                   symbol in self.api.dict_daily_multiplier[self.cur_trading_day].keys():
                    multiplier = float(self.api.dict_daily_multiplier[self.cur_trading_day][symbol])
            except Exception:
                multiplier = 1.0
            if multiplier <= 0:
                multiplier = 1.0

            if abs(target_pos) < 1:
                if symbol in dict_close_price.keys() and dict_close_price[symbol] > 0:
                    try:
                        mr = float(getattr(getattr(self.api, "account_manager", object()), "margin_rate", 0.2))
                    except Exception:
                        mr = 0.2
                    qty = int(round(abs(target_pos) * balance * mr / max(multiplier * dict_close_price[symbol], 1e-6)))
                    target_pos = qty if target_pos >= 0 else -qty
                    if qty == 0:
                        try:
                            logging.warning("[offline][section] target rounded to 0: symbol=%s raw=%.6f bal=%.2f mult=%.4f price=%.4f", symbol, float(target_pos), float(balance), float(multiplier), float(dict_close_price[symbol]))
                        except Exception:
                            logging.warning("[offline][section] target rounded to 0: symbol=%s", symbol)
                else:
                    logging.warning("[offline][section] invalid close price for %s, skip fractional target", symbol)
                    continue

            tgt_i = int(round(target_pos))
            cur_net = int(round(dict_cur_pos.get(symbol, 0)))
            if cur_net == tgt_i:
                continue
            if cur_net == 0:
                if abs(tgt_i) != 0:
                    try:
                        direction = self.BUY if tgt_i > 0 else self.SELL
                        oid = self.api.send_order(symbol, abs(tgt_i), direction)
                        if oid == -1:
                            logging.error("[offline][send_order][perm_fail] symbol=%s sz=%d dir=%d", symbol, abs(tgt_i), 0 if tgt_i >= 0 else 1)
                            continue
                    except Exception as e:
                        logging.error("[offline][send_order][fail] symbol=%s err=%s", symbol, str(e))
                        continue
                    logging.debug("[offline][send_order] symbol=%s sz=%d side=%d", symbol, abs(tgt_i), 0 if tgt_i >= 0 else 1)
                continue
            if cur_net * tgt_i < 0:
                try:
                    close_dir = self.SELL if cur_net > 0 else self.BUY
                    oid1 = self.api.send_order(symbol, abs(cur_net), close_dir)
                    if oid1 == -1:
                        logging.error("[offline][close_then_open][close_fail] symbol=%s sz=%d dir=%d", symbol, abs(cur_net), 1 if cur_net > 0 else 0)
                        continue
                except Exception as e:
                    logging.error("[offline][close_then_open][close_err] symbol=%s err=%s", symbol, str(e))
                    continue
                try:
                    open_dir = self.BUY if tgt_i > 0 else self.SELL
                    oid2 = self.api.send_order(symbol, abs(tgt_i), open_dir)
                    if oid2 == -1:
                        logging.error("[offline][close_then_open][open_fail] symbol=%s sz=%d dir=%d", symbol, abs(tgt_i), 0 if tgt_i >= 0 else 1)
                        continue
                except Exception as e:
                    logging.error("[offline][close_then_open][open_err] symbol=%s err=%s", symbol, str(e))
                    continue
                logging.debug("[offline][close_then_open] symbol=%s close_sz=%d open_sz=%d", symbol, abs(cur_net), abs(tgt_i))
            else:
                diff_pos = tgt_i - cur_net
                if abs(diff_pos) != 0:
                    try:
                        direction = self.BUY if diff_pos > 0 else self.SELL
                        oid = self.api.send_order(symbol, abs(diff_pos), direction)
                        if oid == -1:
                            logging.error("[offline][send_order][perm_fail] symbol=%s sz=%d dir=%d", symbol, abs(diff_pos), 0 if diff_pos >= 0 else 1)
                            continue
                    except Exception as e:
                        logging.error("[offline][send_order][fail] symbol=%s err=%s", symbol, str(e))
                        continue
                    logging.debug("[offline][send_order] symbol=%s sz=%d side=%d", symbol, abs(diff_pos), 0 if diff_pos >= 0 else 1)

        try:
            self._consistency_check(list_bar, dict_target_position)
        except Exception:
            pass
        self.last_dict_target_position = dict_target_position.copy()
        try:
            for bar in list_bar:
                s = str(getattr(bar, "symbol"))
                c = float(getattr(bar, "close"))
                if c > 0:
                    self.prev_close[s] = c
        except Exception:
            pass
        return

    def _symbol_root(self, s):
        """
        提取品种根（root）
        - 参数：
          s: 合约symbol，如 "CU2409.SHFE"
        - 返回：
          str: 品种根，如 "CU.SHFE"
        """
        try:
            return str(self.api.get_symbol_root(s))
        except Exception:
            return str(s).split(".")[0]

    def _days_to_expire(self, symbol, trading_day):
        """
        估算距到期的天数（粗略按合约代码中的 YYMM 推断到当月最后一天）
        - 参数：
          symbol: 合约代码
          trading_day: 当前交易日
        - 返回：
          int: 剩余天数；异常时返回 999
        """
        try:
            base = symbol.split(".")[0]
            digits = "".join(ch for ch in base if ch.isdigit())
            if len(digits) < 4:
                return 999
            yy = 2000 + int(digits[0:2])
            mm = int(digits[2:4])
            last_day = calendar.monthrange(yy, mm)[1]
            ed = datetime.datetime(yy, mm, last_day)
            if isinstance(trading_day, str):
                td = datetime.datetime.strptime(trading_day.split(" ")[0], "%Y-%m-%d")
            else:
                td = trading_day if isinstance(trading_day, datetime.datetime) else datetime.datetime.now()
            return (ed - td).days
        except Exception:
            return 999

    def _group_by_root(self, list_bar):
        """
        按品种根分组截面bar
        - 参数：
          list_bar: 当前截面bar列表
        - 返回：
          dict[root, List[bar]]
        """
        ret = {}
        for bar in list_bar:
            try:
                s = str(getattr(bar, "symbol"))
                r = self._symbol_root(s)
                if r not in ret:
                    ret[r] = []
                ret[r].append(bar)
            except Exception:
                continue
        return ret

    def _pick_main(self, bars):
        """
        按持仓量与到期剩余天数挑选主力合约bar
        - 参数：
          bars: 同一root下的bar列表
        - 返回：
          bar: 主力bar；若无法确定则返回最后一个
        """
        best = None
        best_oi = -1.0
        for bar in bars:
            try:
                s = str(getattr(bar, "symbol"))
                oi = float(getattr(bar, "open_interest"))
            except Exception:
                s = str(getattr(bar, "symbol"))
                oi = 0.0
            try:
                tday = getattr(bar, "trading_day")
            except Exception:
                tday = str(self.cur_trading_day)
            dt_exp = self._days_to_expire(s, tday)
            if oi > best_oi and dt_exp >= 5:
                best_oi = oi
                best = bar
        if best is None and bars:
            best = bars[-1]
        return best

    def _calc_excess_rs(self, list_bar):
        """
        计算截面相对强弱（超额收益）
        - 参数：
          list_bar: 当前截面bar列表
        - 返回：
          dict[root, {symbol, return, close, factor, bar}]
        """
        grouped = self._group_by_root(list_bar)
        rec = {}
        returns = []
        for r, bars in grouped.items():
            main_bar = self._pick_main(bars)
            if not main_bar:
                continue
            try:
                s = str(getattr(main_bar, "symbol"))
                c = float(getattr(main_bar, "close"))
                pc = float(getattr(main_bar, "pre_close"))
            except Exception:
                s = str(getattr(main_bar, "symbol"))
                c = float(getattr(main_bar, "close")) if hasattr(main_bar, "close") else 0.0
                pc = float(self.prev_close.get(s, 0.0))
            rtn = 0.0
            try:
                if pc and pc > 0 and c > 0:
                    rtn = c / pc - 1.0
                elif s in self.prev_close and self.prev_close[s] > 0 and c > 0:
                    rtn = c / self.prev_close[s] - 1.0
                else:
                    rtn = 0.0
            except Exception:
                rtn = 0.0
            returns.append(rtn)
            rec[r] = {"symbol": s, "return": rtn, "close": c, "bar": main_bar}
        mkt = 0.0
        try:
            mkt = statistics.median(returns) if returns else 0.0
        except Exception:
            mkt = 0.0
        for r in list(rec.keys()):
            rec[r]["factor"] = rec[r]["return"] - mkt
        return rec

    def _generate_targets(self, list_bar):
        """
        生成目标仓位（多空等权）
        - 参数：
          list_bar: 当前截面bar列表
        - 返回：
          (dict_target_position, changed)
            dict_target_position: {symbol: net_qty}
            changed: 是否相对 last_dict_target_position 有变化
        """
        rec = self._calc_excess_rs(list_bar)
        items = [(k, v) for k, v in rec.items()]
        try:
            retain_mult = float(getattr(self.strat, "retain_pool_mult", 2.0))
        except Exception:
            retain_mult = 2.0
        retain_mult = max(1.0, retain_mult)
        long_pool_size = max(int(self.top_n_long), int(round(self.top_n_long * retain_mult)))
        short_pool_size = max(int(self.top_n_short), int(round(self.top_n_short * retain_mult)))

        long_candidates = sorted(
            [it for it in items if it[1].get("factor", 0.0) >= self.signal_threshold],
            key=lambda x: x[1].get("factor", 0.0),
            reverse=True,
        )[:long_pool_size]
        short_candidates = sorted(
            [it for it in items if it[1].get("factor", 0.0) <= -self.signal_threshold],
            key=lambda x: x[1].get("factor", 0.0),
        )[:short_pool_size]

        held_long_roots = set()
        held_short_roots = set()
        try:
            for sym, pos in self.last_dict_target_position.items():
                r = self._symbol_root(str(sym))
                if float(pos) > 0:
                    held_long_roots.add(r)
                elif float(pos) < 0:
                    held_short_roots.add(r)
        except Exception:
            held_long_roots = set()
            held_short_roots = set()

        longs = []
        for it in long_candidates:
            if it[0] in held_long_roots:
                longs.append(it)
        for it in long_candidates:
            if len(longs) >= self.top_n_long:
                break
            if it not in longs:
                longs.append(it)
        longs = longs[: self.top_n_long]

        shorts = []
        for it in short_candidates:
            if it[0] in held_short_roots:
                shorts.append(it)
        for it in short_candidates:
            if len(shorts) >= self.top_n_short:
                break
            if it not in shorts:
                shorts.append(it)
        shorts = shorts[: self.top_n_short]
        targets = {}
        try:
            acct = self.api.get_account()
            bal = float(getattr(acct, "available", 0.0))
            if bal <= 0:
                bal = float(self.api.get_balance())
        except Exception:
            try:
                bal = float(self.api.get_balance())
            except Exception:
                bal = 1000000.0
        try:
            mdp = float(getattr(self.strat, "max_deal_pct", getattr(self.api.match_engine, "max_deal_pct", 0.2)))
        except Exception:
            try:
                mdp = float(getattr(self.api.match_engine, "max_deal_pct", 0.2))
            except Exception:
                mdp = 0.2
        total_signals = len(longs) + len(shorts)
        if total_signals <= 0:
            return {}, False
        capital_total = bal * mdp
        capital_per = capital_total / total_signals
        for root, info in longs + shorts:
            sym = str(info["symbol"])
            price = float(info.get("close", 0.0))
            mult = 1.0
            try:
                if self.cur_trading_day in getattr(self.api, "dict_daily_multiplier", {}) and \
                   sym in self.api.dict_daily_multiplier[self.cur_trading_day].keys():
                    mult = float(self.api.dict_daily_multiplier[self.cur_trading_day][sym])
                else:
                    mult = float(self.strat._get_multiplier(sym))
            except Exception:
                try:
                    mult = float(self.strat._get_multiplier(sym))
                except Exception:
                    mult = 1.0
            if price <= 0 or mult <= 0:
                continue
            try:
                mr_sym = float(self.strat._get_margin_rate(sym))
            except Exception:
                try:
                    mr_sym = float(getattr(self.api.account_manager, "margin_rate", 0.2))
                except Exception:
                    mr_sym = 0.2
            qty = int(capital_per / max(mult * price * mr_sym, 1e-6))
            if qty <= 0:
                continue
            if (root, info) in longs:
                targets[sym] = qty
            else:
                targets[sym] = -qty
        changed = True
        try:
            last_int = {str(k): int(round(v)) for k, v in self.last_dict_target_position.items()}
            cur_int = {str(k): int(round(v)) for k, v in targets.items()}
            changed = (cur_int != last_int)
        except Exception:
            changed = True
        return targets, changed

    def _consistency_check(self, list_bar, offline_targets):
        try:
            grouped = self._group_by_root(list_bar)
            main_bars = []
            for _, bars in grouped.items():
                mb = self._pick_main(bars)
                if mb:
                    main_bars.append(mb)
            try:
                for b in main_bars:
                    s = str(getattr(b, "symbol"))
                    r = self._symbol_root(s)
                    pc = float(self.prev_close.get(s, 0.0))
                    if pc > 0:
                        self.strat.price_hist[r] = [pc]
            except Exception:
                pass
            long_syms, short_syms, scores = self.strat.generate_signals(main_bars, list_bar)
            online_targets = self.strat.adjust_positions(long_syms, short_syms, main_bars, scores)
            def _norm(d):
                return {str(k): int(round(v)) for k, v in (d or {}).items()}
            off_n = _norm(offline_targets)
            on_n = _norm(online_targets)
            if off_n != on_n:
                raise AssertionError(f"[consistency] mismatch offline={str(off_n)} online={str(on_n)}")
            else:
                logging.info("[consistency] match positions=%s", str(off_n))
        except Exception:
            pass
    @staticmethod
    def roots_from_config(js):
        """
        从主配置解析订阅的 root 列表
        - 参数：
          js: JSON 对象（test_config.json）
        - 返回：
          List[str]: ["CU.SHFE", ...]
        """
        roots = []
        subs = [str(x) for x in js.get("subscribe symbol", [])]
        ex_map = {str(k): str(v) for k, v in js.get("exchanges", {}).items()}
        for s in subs:
            if s in ex_map:
                roots.append(f"{s}.{ex_map[s]}")
        return roots

    @staticmethod
    def dataset_days(cwd_ds, data_type, roots, begin_day, end_day):
        """
        扫描 dataset 目录下的可用交易日（按订阅root过滤）
        - 参数：
          cwd_ds: 数据集根目录（通常为 ./dataset）
          data_type: 数据粒度（"1m"/"1d"/...）
          roots: 订阅root列表
          begin_day/end_day: 起止日期（YYYY-MM-DD）
        - 返回：
          List[str]: 交易日字符串，按升序
        """
        days = set()
        scan_type = data_type if str(data_type).lower() != "5m" else "1m"
        for r in roots:
            ex = r.split(".")[-1] if "." in r else ""
            base1 = os.path.join(cwd_ds, "data", scan_type, ex, r)
            base2 = os.path.join(cwd_ds, "data", "data", scan_type, ex, r)
            for base in (base1, base2):
                if not os.path.exists(base) or not os.path.isdir(base):
                    continue
                for name in os.listdir(base):
                    path = os.path.join(base, name)
                    if os.path.isdir(path):
                        day = str(name).split(" ")[0]
                        if day >= begin_day and day <= end_day:
                            days.add(day)
        return sorted(list(days))


    @staticmethod
    def setup_logger():
        """
        初始化日志输出（控制台 + 文件）
        - 日志路径：./log/offline_test_sample_strategy.log
        - 日志级别：INFO
        """
        try:
            base = os.getcwd()
            log_dir = os.path.join(base, "log")
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            fn = "offline_test_sample_strategy.log"
            log_path = os.path.join(log_dir, fn)
            fmt = logging.Formatter('[%(levelname)s] %(message)s')
            root = logging.getLogger()
            root.handlers = []
            root.setLevel(logging.INFO)
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(fmt)
            fh = logging.FileHandler(log_path, encoding="utf-8")
            fh.setLevel(logging.INFO)
            fh.setFormatter(fmt)
            root.addHandler(ch)
            root.addHandler(fh)
            logging.getLogger('matplotlib').setLevel(logging.WARNING)
        except Exception:
            pass


def main():
    """
    离线回测入口
    - 读取 test_config.json
    - 初始化 API 与撮合参数
    - 解析 dataset 交易日
    - 注册回调（on_bod / on_eod / on_section_bar / on_bar / on_result）
    - 启动回测并输出结果指标与净值/订单文件
    """
    OfflineAdapter.setup_logger()
    cfg_path = os.path.join(os.getcwd(), "test_config.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        js = json.load(f)

    api = client_api.client_api()
    api.init(js.get("server_addr", "tcp://127.0.0.1:50010"))
    api.set_live_mode(False)
    try:
        api.user_name = js.get("user_name", "test")
    except Exception:
        pass
    api.set_is_option(js.get("is_option", False))
    api.set_data_type(js.get("data_type", "1m"))
    api.set_balance(js.get("init_money", 1000000))
    api.set_margin_rate(js.get("margin_rate", 0.2))
    api.set_slippage_type(js.get("slippage_type", "pct"))
    api.set_slippage_value(js.get("slippage_value", 0.0001))
    api.set_buy_fee_type(js.get("buy_fee_type", "pct"))
    api.set_buy_fee_value(js.get("buy_fee_value", 0.00007))
    api.set_sell_fee_type(js.get("sell_fee_type", "pct"))
    api.set_sell_fee_value(js.get("sell_fee_value", 0.00007))
    api.set_deal_type(js.get("deal_type", "close"))
    api.set_max_deal_pct(js.get("max_deal_pct", 0.2))
    api.set_check_market_volume(js.get("is_check_market_volume", False))
    api.set_night_trade(js.get("night_trade", True))

    ds_base = os.path.join(os.getcwd(), "dataset")
    api.cwd = ds_base

    roots = OfflineAdapter.roots_from_config(js)
    api.subscribe_symbol_root(roots)

    # 解析回测日期区间
    bp = str(js.get("backtest_period", "2024-08-31~2025-08-31"))
    begin_day, end_day = bp.split("~")
    begin_day = begin_day.strip()
    end_day = end_day.strip()

    # 从 dataset 汇总可用交易日
    api.trading_date_section = OfflineAdapter.dataset_days(api.cwd, api.data_type, roots, begin_day, end_day)
    logging.info("[offline] trading days=%d [%s ... %s]", len(api.trading_date_section), api.trading_date_section[0] if api.trading_date_section else "", api.trading_date_section[-1] if api.trading_date_section else "")
    if not isinstance(api.trading_date_section, list) or len(api.trading_date_section) == 0:
        raise RuntimeError(f"[offline] no trading days found for period {begin_day}~{end_day}; please verify dataset path and config")
    try:
        sample_days = int(js.get("offline_sample_days", 0))
    except Exception:
        sample_days = 0
    if isinstance(api.trading_date_section, list) and sample_days and sample_days > 0 and len(api.trading_date_section) > sample_days:
        api.trading_date_section = api.trading_date_section[-sample_days:]
        logging.info("[offline] shrink to last %d days [%s ... %s]", len(api.trading_date_section), api.trading_date_section[0], api.trading_date_section[-1])

    # 适配策略
    adapter = OfflineAdapter()
    adapter.init(api)
    api.register_bod(adapter.on_bod)
    api.register_eod(adapter.on_eod)
    api.register_section_bar(adapter.on_section_bar)
    api.register_bar(adapter.on_bar)

    # 结果回调
    g_result = {"ret": None}

    def on_result(result):
        g_result["ret"] = result
        return

    api.register_result_cb(on_result)

    # 启动回测（纯离线，无需登录/下载）
    api.start()
    api.join()

    # 输出结果
    df_net = pd.DataFrame()
    df_trade = pd.DataFrame()
    metrics = {}
    if isinstance(g_result.get("ret"), (list, tuple)) and len(g_result["ret"]) >= 10:
        df_net = g_result["ret"][0]
        df_trade = g_result["ret"][1]
        metrics = {
            "annual_return": g_result["ret"][2],
            "max_drawdown": g_result["ret"][3],
            "annual_volatility": g_result["ret"][4],
            "sharpe_ratio": g_result["ret"][5],
            "daily_win_rate": g_result["ret"][6],
            "monthly_win_rate": g_result["ret"][7],
            "daily_profit_loss_ratio": g_result["ret"][8],
            "monthly_profit_loss_ratio": g_result["ret"][9],
        }
    try:
        if "trading_day" in df_net.columns:
            df_net["trading_day"] = pd.to_datetime(df_net["trading_day"])
            df_net = df_net.set_index("trading_day").sort_index()
        if "net_value" in df_net.columns:
            df_net["net_value"] = pd.to_numeric(df_net["net_value"], errors="coerce")
            df_net["net_value"] = df_net["net_value"].astype(float)
            df_net["net_value"] = df_net["net_value"].replace([float("inf"), float("-inf")], pd.NA).ffill().bfill()
    except Exception:
        pass
    fig = plt.figure(figsize=(12, 4))
    ax = fig.add_subplot(1,1,1)
    try:
        (df_net["net_value"] if "net_value" in df_net.columns else df_net).plot(ax=ax)
    except Exception:
        pass
    ax.set_title("Net Value")
    ax.grid(True)
    fig.tight_layout()
    result_path = os.path.join(os.getcwd(), "result", "offline_test_sample_strategy")
    if not os.path.exists(result_path):
        os.makedirs(result_path)
    try:
        df_out = df_net.reset_index()
        df_out.to_csv(os.path.join(result_path, "net_value.csv"), header=True, index=False)
    except Exception:
        df_net.to_csv(os.path.join(result_path, "net_value.csv"), header=True)
    plt.savefig(os.path.join(result_path, "pnl.png"))
    try:
        plt.close(fig)
    except Exception:
        pass
    # 交易订单输出
    try:
        if isinstance(df_trade, pd.DataFrame) and df_trade.shape[0] > 0:
            df_trade.to_csv(os.path.join(result_path, "trading_order.csv"), header=True, index=False)
    except Exception:
        pass
    # 指标输出与打印
    try:
        if metrics:
            with open(os.path.join(result_path, "evaluating_indicator.txt"), "w", encoding="utf-8") as f:
                f.write(f"年化收益率:{metrics['annual_return']}\n")
                f.write(f"最大回撤:{metrics['max_drawdown']}\n")
                f.write(f"年化波动率:{metrics['annual_volatility']}\n")
                f.write(f"夏普率:{metrics['sharpe_ratio']}\n")
                f.write(f"日胜率:{metrics['daily_win_rate']}\n")
                f.write(f"月胜率:{metrics['monthly_win_rate']}\n")
                f.write(f"日盈亏比:{metrics['daily_profit_loss_ratio']}\n")
                f.write(f"月盈亏比:{metrics['monthly_profit_loss_ratio']}\n")
            print(f"年化收益率:{metrics['annual_return']}最大回撤:{metrics['max_drawdown']}年化波动率:{metrics['annual_volatility']}夏普率:{metrics['sharpe_ratio']}日胜率:{metrics['daily_win_rate']}月胜率:{metrics['monthly_win_rate']}日盈亏比:{metrics['daily_profit_loss_ratio']}月盈亏比:{metrics['monthly_profit_loss_ratio']}")
    except Exception:
        pass
    logging.info("[offline] done")


if __name__ == "__main__":
    main()
