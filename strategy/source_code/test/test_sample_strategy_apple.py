# test_sample_strategy.py
# Apple (AP) breakout strategy (5-minute) for CZCE with:
# - NextBarOpen execution model
# - Main contract auto-selection by OI and monthly rollover with dynamic adjustment factors (复权)
# - 2000-bar channel / min ready bars configurable
# - ATR(14) on 5m, Stop = ATR*10, trailing Exit as described
# - Robust bar parsing (pandas Series / dict / object)
# - Respect config parameters (slippage, fees, margin_rate, night_trade, is_main_symbol_only, max_deal_pct)
# - Detailed debug prints (to help integration with your platform)
#
# Save as test_sample_strategy.py — platform must import module and call test_sample_strategy() (exposed at bottom).
#
# 注意：此文件实现了策略核心逻辑与主力合约换月复权流程。你需要在平台侧确保传入的 bar
# 包含字段（其中之一可用）: symbol / instrument / close / high / low / open / datetime or trading_day/time /
# volume / open_interest (或 oi). 如果没有 open_interest, 会用 volume 作为替代指标（弱化判断）。
#
# 配置（可在 init(api, **kwargs) 传入覆盖）:
#  - channel_bars (default 2000)
#  - min_channel_bars (default 200)
#  - atr_period (default 14)
#  - stop_atr_mult (default 10.0)
#  - profit_atr_mult (default 3.0)
#  - open_allowed_after (default 09:10)  # bar.time >= 09:10 才开仓
#  - cancel_cutoff_time (default 14:57)   # >=14:57 撤单/禁止新单
#  - contract_multiplier (default 10)
#  - account_capital (default 1_000_000)
#  - risk_pct_per_trade (default 0.02)
#  - slippage_type ('pct' or 'abs'), slippage_value
#  - buy_fee_type/sell_fee_type ('pct' or 'abs'), buy_fee_value/sell_fee_value
#  - margin_rate, max_deal_pct, is_main_symbol_only, night_trade
#
# 接口：
#  - init(api, **kwargs)
#  - calculate_target_position(list_bar) -> (dict_pos, changed)
#
# 暴露对象: test_sample_strategy = TestSampleStrategy
# ------------------------------------------------------------------------------

from collections import deque
from datetime import datetime, time, timedelta, timezone
import math
import traceback
import logging
import sys
import threading
import json

OI_SWITCH_THRESHOLD = 0.9
FORCE_ROLLOVER_DAYS = 60
CHANNEL_BARS_DEFAULT = 2000
MIN_CHANNEL_BARS_DEFAULT = 200
ATR_PERIOD_DEFAULT = 14
STOP_ATR_MULT_DEFAULT = 10.0
PROFIT_ATR_MULT_DEFAULT = 3.0
OPEN_ALLOWED_AFTER_DEFAULT = time(9,10)
CANCEL_CUTOFF_DEFAULT = time(14,57)
CONSECUTIVE_STOP_LOSS_THRESHOLD = 3
HALT_SCORE_THRESHOLD = 7.0
AVG_SLIPPAGE_MULT_THRESHOLD = 3.0
HOLDING_TIME_MULT = 2
PRICE_JUMP_THRESHOLD = 0.05
CAPITAL_DD_THRESHOLD = 0.5

# ------------------ 小工具：字段提取 & 时间解析 ------------------
def get_field(bar, *names):
    """
    尝试从 bar 中按顺序提取字段。
    支持：object.attr, dict[key], pandas Series-like: bar[name]
    返回第一个找到的值，找不到返回 None。
    """
    for name in names:
        try:
            # object attribute
            if hasattr(bar, name):
                val = getattr(bar, name)
                if val is not None:
                    return val
        except Exception:
            pass
        try:
            # dict-like access
            if isinstance(bar, dict) and name in bar:
                return bar[name]
        except Exception:
            pass
        try:
            # mapping/Series-like access
            if hasattr(bar, "__getitem__"):
                v = bar[name]
                if v is not None:
                    return v
        except Exception:
            pass
    return None

def parse_datetime_maybe(dt_raw):
    """解析可能的 datetime 字段，兼容：
       - datetime.datetime
       - 'YYYY-MM-DD HH:MM:SS'
       - 'YYYYMMDD' (-> assign 15:00)
       - int 20220104 (-> assign 15:00)
       - time-only '09:05:00' with trading_day separately handled in caller
    """
    if dt_raw is None:
        return None
    if isinstance(dt_raw, datetime):
        try:
            if dt_raw.tzinfo is not None:
                return dt_raw
        except Exception:
            pass
        return dt_raw
    try:
        if isinstance(dt_raw, int):
            s = str(dt_raw)
            if len(s) == 8:
                return datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]), 15, 0, 0)
        if isinstance(dt_raw, str):
            s = dt_raw.strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(s)
            except Exception:
                pass
            # yyyymmdd
            if len(s) == 8 and s.isdigit():
                return datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]), 15, 0, 0)
            # y-m-d
            try:
                if ' ' in s:
                    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                if '-' in s and len(s.split('-')[0]) == 4:
                    return datetime.strptime(s, "%Y-%m-%d")
            except Exception:
                pass
    except Exception:
        pass
    return None

# ------------------ 指标：ATR 与 滑动高低 ------------------
class ATR:
    def __init__(self, period=14):
        self.period = int(period)
        self.trs = deque(maxlen=self.period)
        self.prev_close = None

    def push(self, high, low, close):
        try:
            if self.prev_close is None:
                tr = max(0.0, float(high) - float(low))
            else:
                tr = max(float(high) - float(low),
                         abs(float(high) - float(self.prev_close)),
                         abs(float(low) - float(self.prev_close)))
        except Exception:
            tr = 0.0
        if len(self.trs) == self.period:
            # deque will pop left automatically on append
            pass
        self.trs.append(tr)
        self.prev_close = close

    def get(self):
        if len(self.trs) < self.period:
            return None
        return sum(self.trs) / len(self.trs)

class RollingHighLow:
    """维护窗口内 high/low（简单实现，保留历史以便换月复权重算）"""
    def __init__(self, maxlen):
        self.maxlen = int(maxlen)
        self.items = deque(maxlen=self.maxlen)  # store (dt, high, low, open, close, volume)
    def push(self, bar_entry):
        self.items.append(bar_entry)
    def highest(self):
        if len(self.items) == 0:
            return None
        return max(x['high'] for x in self.items)
    def lowest(self):
        if len(self.items) == 0:
            return None
        return min(x['low'] for x in self.items)
    def size(self):
        return len(self.items)
    def clear(self):
        self.items.clear()
    def highest_ago(self, ago):
        n = len(self.items)
        if n <= ago:
            return None
        window = list(self.items)[:n-ago]
        if not window:
            return None
        return max(x['high'] for x in window)
    def lowest_ago(self, ago):
        n = len(self.items)
        if n <= ago:
            return None
        window = list(self.items)[:n-ago]
        if not window:
            return None
        return min(x['low'] for x in window)

# ------------------ 持仓对象 ------------------
class Position:
    def __init__(self, side, qty, entry_price, entry_time, adj_factor=1.0):
        self.side = int(side)      # 1 多, -1 空
        self.qty = int(qty)
        self.entry_price = float(entry_price)      # 入场价格（复权后的）
        self.entry_time = entry_time
        self.adj_factor = float(adj_factor)        # 当时所用的复权因子（用于换月后回推）
        self.max_profit_point = 0.0
        self.current_profit_point = 0.0

    def update_pnl_points(self, current_price):
        if self.side == 1:
            diff = current_price - self.entry_price
        else:
            diff = self.entry_price - current_price
        self.current_profit_point = diff
        if diff > self.max_profit_point:
            self.max_profit_point = diff

# ------------------ 策略主体 ------------------
class TestSampleStrategy:
    def __init__(self):
        # 参数（可由 init 覆盖）
        self.channel_bars = CHANNEL_BARS_DEFAULT
        self.min_channel_bars = MIN_CHANNEL_BARS_DEFAULT
        self.atr_period = ATR_PERIOD_DEFAULT
        self.stop_atr_mult = STOP_ATR_MULT_DEFAULT
        self.profit_atr_mult = PROFIT_ATR_MULT_DEFAULT
        self.open_allowed_after = OPEN_ALLOWED_AFTER_DEFAULT
        self.cancel_cutoff_time = CANCEL_CUTOFF_DEFAULT
        self.contract_multiplier = 10
        self.account_capital = 1_000_000.0
        self.risk_pct_per_trade = 0.02
        self.timezone_offset_minutes = 480
        # 费用与滑点
        self.slippage_type = 'pct'   # 'pct' or 'abs'
        self.slippage_value = 0.0001
        self.buy_fee_type = 'pct'
        self.buy_fee_value = 0.00007
        self.sell_fee_type = 'pct'
        self.sell_fee_value = 0.00007
        # other cfg
        self.margin_rate = 0.2
        self.max_deal_pct = 0.1
        self.is_main_symbol_only = True
        self.night_trade = False
        self.is_live_mode = False
        # halt & rule thresholds (configurable)
        self.halt_score_threshold = HALT_SCORE_THRESHOLD
        self.consecutive_stop_loss_threshold = CONSECUTIVE_STOP_LOSS_THRESHOLD
        self.oi_missing_threshold = 3
        self.enforce_full_channel = True
        self.avg_slippage_mult_threshold = AVG_SLIPPAGE_MULT_THRESHOLD
        self.holding_time_mult = HOLDING_TIME_MULT
        self.price_jump_threshold = PRICE_JUMP_THRESHOLD
        self.capital_dd_threshold = CAPITAL_DD_THRESHOLD
        self.slippage_tolerance_pct = 0.25
        # runtime flags/state
        self._halted = False
        self._oi_missing_days = set()

        # 状态
        self.atr_map = {}            # sym -> ATR
        self.hl_map = {}             # sym -> RollingHighLow for raw bars (un-adjusted)
        self.hist_map = {}           # sym -> list of raw bar dicts (for adj)
        self.pending_orders = {}     # sym -> {'side', 'qty', 'created_at', 'price','type'}
        self.positions = {}          # sym -> Position
        self.target_positions = {}   # sym -> qty (int)
        self.changed = False
        self.aggr_map = {}           # sym -> current 1m-to-5m aggregator
        self.last_bod_day = None
        self._eod_oi_fetched_day = None
        self._last_midday_checked_day = None
        self.sym_adj_map = {}
        self.alerts = []

        # 主力合约管理
        self.current_main = None              # symbol string
        self.current_main_adj = 1.0           # current main's cumulative adj factor
        self.prev_main_close = None           # close price of previous main at switch time
        self.oi_map = {}                      # trading_day -> {sym: oi}
        self.expire_date_map = {}             # sym -> datetime (expire)
        self.log_level = 'INFO'
        self.trade_log = []
        print("[test_sample_strategy] init complete")

    def init(self, api=None, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                try:
                    logging.warning(f"Unknown parameter {k}, skip")
                except Exception:
                    pass
            else:
                setattr(self, k, v)
        self.api = api
        try:
            lvl = getattr(logging, str(self.log_level).upper(), logging.INFO)
            logging.getLogger().setLevel(lvl)
        except Exception:
            pass
        try:
            if self.min_channel_bars < 200:
                self.min_channel_bars = 200
            if self.open_allowed_after < time(9,10):
                self.open_allowed_after = time(9,10)
            if self.cancel_cutoff_time > time(14,57):
                self.cancel_cutoff_time = time(14,57)
        except Exception:
            pass
        print("[test_sample_strategy] API init complete. Params:")
        print({
            "channel_bars": self.channel_bars,
            "min_channel_bars": self.min_channel_bars,
            "atr_period": self.atr_period,
            "stop_atr_mult": self.stop_atr_mult,
            "open_allowed_after": self.open_allowed_after,
            "cancel_cutoff_time": self.cancel_cutoff_time,
            "slippage_type": self.slippage_type,
            "slippage_value": self.slippage_value,
            "buy_fee": (self.buy_fee_type, self.buy_fee_value),
            "sell_fee": (self.sell_fee_type, self.sell_fee_value),
            "is_main_symbol_only": self.is_main_symbol_only,
            "night_trade": self.night_trade,
            "timezone_offset_minutes": self.timezone_offset_minutes
        })
        try:
            self.auditor = TradeAuditor(self)
        except Exception:
            self.auditor = None
        try:
            self.circuit_breaker = ComplianceCircuitBreaker(self)
        except Exception:
            self.circuit_breaker = None
        return

    def _snapshot_halt_context(self):
        try:
            return {
                'positions': {k: {'side': v.side, 'qty': v.qty, 'entry_price': v.entry_price} for k, v in self.positions.items()},
                'pending_orders': self.pending_orders,
                'current_main': self.current_main,
                'violations': getattr(self.auditor, 'daily_violations', [])
            }
        except Exception:
            return {}
    def _graceful_shutdown(self, reason):
        self._halted = True
        try:
            print(f"\n{'='*50}\nFATAL: Trading halted due to violations\n{reason}\n{'='*50}")
        except Exception:
            pass
        try:
            logging.error(f"[HALT] {reason}")
            logging.error(f"[HALT] context={self._snapshot_halt_context()}")
        except Exception:
            pass
        try:
            if hasattr(self, 'api') and self.api:
                self.api.running = False
        except Exception:
            pass

    # ---------------- bar parsing helpers ----------------
    def _extract_bar_fields(self, bar):
        """
        Robust extraction: support object.attr, dict, pandas Series-like.
        Returns dict with keys: symbol, open, high, low, close, datetime (datetime obj), volume, oi
        """
        try:
            sym = get_field(bar, 'symbol', 'instrument', 'instrument_id', 'inst', 'InstrumentId')
            o = get_field(bar, 'open', 'Open', 'o', 'open_price')
            h = get_field(bar, 'high', 'High', 'h', 'high_price')
            l = get_field(bar, 'low', 'Low', 'l', 'low_price')
            c = get_field(bar, 'close', 'Close', 'c', 'last_price', 'settlement_price')
            dt_raw = get_field(bar, 'datetime', 'date_time', 'trade_time', 'trading_day', 'date')
            vol = get_field(bar, 'volume', 'Volume', 'vol')
            oi = get_field(bar, 'open_interest', 'oi', 'OpenInterest', 'open_interest')
        except Exception as e:
            try:
                logging.error(f"Critical error in bar parsing: {e}")
            except Exception:
                pass
            try:
                traceback.print_exc()
            except Exception:
                pass
            return None

        dt = parse_datetime_maybe(dt_raw)
        # if dt is None but trading_day exists as e.g. '20220104', parse_datetime_maybe will return 15:00
        # For 5m data dt should have time component; if not, it's daily -> treat as daybar
        b = {
            'symbol': sym,
            'open': float(o) if o is not None else None,
            'high': float(h) if h is not None else None,
            'low': float(l) if l is not None else None,
            'close': float(c) if c is not None else None,
            'datetime': dt,
            'raw_datetime': dt_raw,
            'volume': float(vol) if vol is not None else 0.0,
            'oi': float(oi) if oi is not None else None
        }
        assert b['close'] is not None, f"{sym} close is None"
        return b

    def _bucket_start(self, dt):
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute - dt.minute % 5, 0)

    def _update_agg_5m(self, sym, dt, o, h, l, c, vol, oi):
        if dt is None:
            return None
        bucket = self._bucket_start(dt)
        cur = self.aggr_map.get(sym)
        finalized = None
        if cur and cur['bucket'] != bucket:
            finalized = {
                'dt': cur['last_dt'],
                'open': cur['open'],
                'high': cur['high'],
                'low': cur['low'],
                'close': cur['close'],
                'volume': cur['vol'],
                'oi': cur.get('oi', None)
            }
            cur = None
        if cur is None:
            self.aggr_map[sym] = {
                'bucket': bucket,
                'open': o if o is not None else c,
                'high': h if h is not None else c,
                'low': l if l is not None else c,
                'close': c,
                'vol': vol or 0.0,
                'start_dt': dt,
                'last_dt': dt,
                'oi': oi
            }
        else:
            cur['high'] = max(cur['high'], h if h is not None else c)
            cur['low'] = min(cur['low'], l if l is not None else c)
            cur['close'] = c
            cur['vol'] = (cur['vol'] or 0.0) + (vol or 0.0)
            cur['last_dt'] = dt
            if oi is not None:
                cur['oi'] = oi
        return finalized

    def _finalize_and_process_5m(self, sym, dt, o, h, l, c, vol, oi):
        ndt = self._normalize_dt(dt) if dt is not None else None
        if ndt is not None and ndt.time() < self.open_allowed_after:
            print(f"[TIME] skip recording before {self.open_allowed_after} for {sym} at {dt}")
            self._execute_pending_if_ready(sym, dt, o)
            if ndt.time().hour == 15 and ndt.time().minute == 0:
                trading_day = ndt.strftime("%Y%m%d")
            return
        entry = {'datetime': dt, 'open': o, 'high': h, 'low': l, 'close': c, 'volume': vol}
        self.hist_map[sym].append(entry)
        self.hl_map[sym].push(entry)
        self.atr_map[sym].push(entry['high'], entry['low'], entry['close'])
        executed = False
        executed = self._execute_pending_if_ready(sym, dt, o)
        atr_val = self.atr_map[sym].get()
        ch_ready = self.hl_map[sym].size() >= self.min_channel_bars
        open_ready = self._ready_for_open(sym)
        ch_high = self.hl_map[sym].highest()
        ch_low = self.hl_map[sym].lowest()
        print(f"[IND] {sym} ATR={atr_val} channel_size={self.hl_map[sym].size()} ch_high={ch_high} ch_low={ch_low} ready={ch_ready}")
        if atr_val is None:
            print(f"[IND] {sym} ATR not ready, stop/size disabled")
        try:
            if ndt is not None and ndt.time().hour == 15 and ndt.time().minute == 0:
                trading_day = ndt.strftime("%Y%m%d")
                if self._eod_oi_fetched_day != trading_day:
                    self._record_oi(trading_day, sym, oi)
                    self._fill_day_oi_and_roll(trading_day, ndt.strftime("%Y-%m-%d"))
                    self._eod_oi_fetched_day = trading_day
        except Exception as e:
            print("[ERROR] rollover check failed:", e)
        # midday check moved to calculate_target_position
        allow_open = self._check_open_time(ndt) if ndt is not None else False
        if ndt is not None and ndt.time() >= self.cancel_cutoff_time:
            allow_open = False
            if sym in self.pending_orders:
                self.pending_orders.pop(sym, None)
                logging.info(f"[ORDER] canceled pending for {sym} due to cutoff")
        print(f"[TIME] allow_open={allow_open}")
        pos = self.positions.get(sym)
        pos_qty = pos.qty if pos else 0
        entry_price = pos.entry_price if pos else None
        print(f"[POS] {sym} pos_qty={pos_qty} entry_price={entry_price}")
        if pos:
            pos.update_pnl_points(c)
            stop_v = self.stop_atr_mult * (atr_val or 0.0)
            if pos.current_profit_point <= -stop_v:
                qty = abs(pos.qty)
                self._create_pending_order(sym, -pos.side, qty, dt, price=c, reason='stop')
                print(f"[SIGNAL-STOP] {sym} stoploss triggered cur_pnl_point={pos.current_profit_point:.2f} stop_v={stop_v:.2f}")
                return
            if pos.max_profit_point > 0 and atr_val:
                MaxTarget = stop_v
                ExitPoint = (pos.max_profit_point / MaxTarget) * stop_v if MaxTarget > 0 else 0.0
                AllowDropDown = stop_v - ExitPoint
                Drop = (pos.max_profit_point - pos.current_profit_point)
                print(f"[TRAIL] pos.max={pos.max_profit_point:.2f} cur={pos.current_profit_point:.2f} ExitPoint={ExitPoint:.2f} AllowDropDown={AllowDropDown:.2f} Drop={Drop:.2f}")
                if Drop > AllowDropDown:
                    qty = abs(pos.qty)
                    self._create_pending_order(sym, -pos.side, qty, dt, price=c, reason='take')
                    print(f"[SIGNAL-TAKE] {sym} trailing exit created")
                    return
        if (pos is None or pos.qty == 0) and sym not in self.pending_orders:
            if not open_ready:
                print(f"[DEBUG] {sym} not ready for entry (ch_ready={ch_ready}, atr={atr_val})")
            elif not allow_open:
                print(f"[DEBUG] {sym} not within open allowed time, skip entry")
            else:
                prev_close = None
                if len(self.hist_map[sym]) >= 2:
                    prev_close = self.hist_map[sym][-2]['close']
                ch_high_current = self.hl_map[sym].highest_ago(5)
                ch_low_current = self.hl_map[sym].lowest_ago(5)
                if prev_close is not None and ch_high_current is not None and prev_close < ch_high_current and c >= ch_high_current:
                    stop_v = self.stop_atr_mult * atr_val
                    try:
                        tday = ndt.strftime("%Y-%m-%d") if ndt is not None else None
                        mult = None
                        if tday and hasattr(self, 'api') and self.api and tday in getattr(self.api, 'dict_daily_multiplier', {}):
                            mult = self.api.dict_daily_multiplier[tday].get(sym, None)
                        qty = self._calc_position_size(atr_val, stop_v, mult, sym)
                    except Exception:
                        qty = self._calc_position_size(atr_val, stop_v, None, sym)
                    if qty > 0:
                        self._create_pending_order(sym, 1, qty, dt, price=c, reason='open_long')
                        print(f"[SIGNAL-OPEN-LONG] {sym} breakout up at {ch_high_current:.2f} -> pending long qty={qty}")
                    else:
                        print(f"[DEBUG] {sym} qty calc 0")
                    return
                if prev_close is not None and ch_low_current is not None and prev_close > ch_low_current and c <= ch_low_current:
                    stop_v = self.stop_atr_mult * atr_val
                    try:
                        tday = ndt.strftime("%Y-%m-%d") if ndt is not None else None
                        mult = None
                        if tday and hasattr(self, 'api') and self.api and tday in getattr(self.api, 'dict_daily_multiplier', {}):
                            mult = self.api.dict_daily_multiplier[tday].get(sym, None)
                        qty = self._calc_position_size(atr_val, stop_v, mult, sym)
                    except Exception:
                        qty = self._calc_position_size(atr_val, stop_v, None, sym)
                    if qty > 0:
                        self._create_pending_order(sym, -1, qty, dt, price=c, reason='open_short')
                        print(f"[SIGNAL-OPEN-SHORT] {sym} breakout down at {ch_low_current:.2f} -> pending short qty={qty}")
                    else:
                        print(f"[DEBUG] {sym} qty calc 0")
                    return
                print(f"[DEBUG] {sym} no breakout condition met (prev_close={prev_close})")
        if pos:
            prev_close = None
            if len(self.hist_map[sym]) >= 2:
                prev_close = self.hist_map[sym][-2]['close']
                ch_high_current = self.hl_map[sym].highest_ago(5)
                ch_low_current = self.hl_map[sym].lowest_ago(5)
                if pos.side == 1 and prev_close is not None and ch_low_current is not None and prev_close > ch_low_current and c <= ch_low_current:
                    qty = abs(pos.qty)
                    self._create_pending_order(sym, -1, qty, dt, price=c, reason='reversal_long_close')
                    print(f"[SIGNAL-REVERSAL] {sym} long hit lower breakout -> pending close/reverse qty={qty}")
                elif pos.side == -1 and prev_close is not None and ch_high_current is not None and prev_close < ch_high_current and c >= ch_high_current:
                    qty = abs(pos.qty)
                    self._create_pending_order(sym, 1, qty, dt, price=c, reason='reversal_short_close')
                    print(f"[SIGNAL-REVERSAL] {sym} short hit upper breakout -> pending close/reverse qty={qty}")
        if sym in self.positions:
            self.target_positions[sym] = self.positions[sym].side * self.positions[sym].qty
    # ---------------- main contract selection & adj factor ----------------
    def _record_oi(self, trading_day, sym, oi):
        """记录当日每合约 OI，用于日终选择主力"""
        if trading_day not in self.oi_map:
            self.oi_map[trading_day] = {}
        pre = self.oi_map[trading_day].get(sym, 0.0)
        cur = oi if oi is not None else pre
        self.oi_map[trading_day][sym] = max(pre, cur)
        try:
            if oi is not None:
                self._oi_missing_days.discard(trading_day)
            elif sym == self.current_main:
                self._oi_missing_days.add(trading_day)
            if len(self._oi_missing_days) > 30:
                try:
                    keep = sorted(self._oi_missing_days)[-30:]
                    remove = set(self._oi_missing_days) - set(keep)
                    if remove:
                        self._oi_missing_days.difference_update(remove)
                except Exception:
                    pass
        except Exception:
            pass

    def _select_main_by_oi(self, trading_day):
        """在交易日结束后调用（或每天10:30检查），选择当日 OI 最大的合约为主力"""
        day_map = self.oi_map.get(trading_day, {})
        if not day_map:
            return None
        # choose max OI
        main = max(day_map.items(), key=lambda x: x[1])[0]
        return main

    def _parse_delivery_month(self, sym):
        try:
            base = str(sym).split('.')[0]
            digits = ''.join(ch for ch in base if ch.isdigit())
            if len(digits) >= 4:
                # assume yymm or yyyymm
                if len(digits) >= 6:
                    return int(digits[-6:])
                return int(digits[-4:])
        except Exception:
            pass
        return None

    def _channel_high_low_ago(self, sym, ago=5):
        if sym not in self.hist_map:
            return None, None
        h = self.hl_map[sym].highest_ago(ago)
        l = self.hl_map[sym].lowest_ago(ago)
        return h, l

    def _ensure_symbol(self, sym):
        if sym not in self.hist_map:
            self.hist_map[sym] = []    # raw bars list for that symbol (dicts)
            self.atr_map[sym] = ATR(self.atr_period)
            self.hl_map[sym] = RollingHighLow(self.channel_bars)
            print(f"[DEBUG] init symbol state for {sym}")

    def _apply_adj_to_history(self, sym, new_factor, prev_factor):
        """
        当换月时，把已有历史（所有 sym 的 hist_map 条目）乘以因子比率
        For main contract rollback: we reconstruct unified adjusted series for main symbol:
          newAdjFactor = prev_main_close * prev_main_factor / new_main_close
        Then for each historical bar in the unified series we multiply by ratio = newAdjFactor / oldAdj
        We'll implement per-bar scaling for all recorded bars of the previous main to align to new main.
        """
        try:
            # We apply adjustment to *all* stored bars in hist_map[sym] by factor_ratio = new_factor / prev_factor
            if sym not in self.hist_map:
                return
            ratio = 1.0
            if prev_factor and prev_factor != 0:
                ratio = new_factor / prev_factor
            if ratio <= 0:
                ratio = abs(ratio)
            ratio = max(ratio, 1e-9)
            if abs(ratio - 1.0) < 1e-12:
                logging.debug(f"[DEBUG] adj ratio ~1.0, skip re-scaling for {sym}")
                return
            logging.info(f"[DEBUG] Applying adj ratio {ratio:.8f} to {len(self.hist_map[sym])} bars of {sym}")
            # scale stored bars (open/high/low/close)
            for entry in self.hist_map[sym]:
                entry['open'] = max(entry['open'] * ratio, 0.0)
                entry['high'] = max(entry['high'] * ratio, 0.0)
                entry['low'] = max(entry['low'] * ratio, 0.0)
                entry['close'] = max(entry['close'] * ratio, 0.0)
            # Rebuild RollingHighLow and ATR for sym from adjusted history
            rh = RollingHighLow(self.channel_bars)
            atr = ATR(self.atr_period)
            for entry in self.hist_map[sym]:
                rh.push(entry)
                atr.push(entry['high'], entry['low'], entry['close'])
            self.hl_map[sym] = rh
            self.atr_map[sym] = atr
            self.sym_adj_map[sym] = new_factor
            logging.info(f"[DEBUG] Rebuilt RollingHighLow/ATR for {sym} after adj")
        except Exception as e:
            logging.error(f"[ERROR] _apply_adj_to_history failed: {e}")
            try:
                traceback.print_exc()
            except Exception:
                pass

    def _roll_main_contract_if_needed(self, current_day):
        """
        每日检查主力合约（例如 10:30），如果发现新的主力满足换月条件（OI_new > OI_main*0.9）则换月。
        主力不能后退。
        换月步骤：
          - 计算 newAdjFactor = (old_close * old_adj_factor) / new_close
          - 对历史进行复权（按 ratio = newAdjFactor / old_adj_factor）
          - 更新 current_main, current_main_adj, prev_main_close
          - 重新计算 hl_map[current_main]（we call _apply_adj_to_history）
        备注：此函数假定在调用前已填充 oi_map[current_day]
        """
        try:
            if getattr(self, '_halted', False):
                return
            self._rolling_over = True
            try:
                if current_day not in self.oi_map:
                    return
                day_oi = self.oi_map[current_day]
                if not day_oi:
                    return
                new_main = max(day_oi.items(), key=lambda x: x[1])[0]
                new_main_oi = day_oi[new_main]
                old_main = self.current_main
                if old_main is None:
                    self.current_main = new_main
                    self.current_main_adj = 1.0
                    if new_main in self.hist_map and len(self.hist_map[new_main])>0:
                        self.prev_main_close = self.hist_map[new_main][-1]['close']
                    logging.info(f"[ROLLOVER] initial main set to {new_main}")
                    return
                if new_main == old_main:
                    return
                old_oi = day_oi.get(old_main, 0.0)
                force_switch = False
                try:
                    exp = self.expire_date_map.get(old_main)
                    if exp is not None:
                        cur_dt = datetime.strptime(current_day, "%Y%m%d")
                        if (exp - cur_dt).days <= 60:
                            force_switch = True
                except Exception:
                    pass
                if new_main_oi > old_oi * OI_SWITCH_THRESHOLD or force_switch:
                    old_m = self._parse_delivery_month(old_main)
                    new_m = self._parse_delivery_month(new_main)
                    if old_m is not None and new_m is not None and new_m < old_m:
                        logging.info(f"[ROLLOVER] candidate {new_main} delivery {new_m} < old {old_m}, skip")
                        return
                    logging.info(f"[ROLLOVER] candidate new main {new_main} (oi={new_main_oi}) vs old {old_main} (oi={old_oi}) -> will rollover")
                    if old_main in self.hist_map and len(self.hist_map[old_main])>0:
                        old_close = self.hist_map[old_main][-1]['close']
                    else:
                        old_close = self.prev_main_close or 1.0
                    if new_main in self.hist_map and len(self.hist_map[new_main])>0:
                        new_close = self.hist_map[new_main][-1]['close']
                    else:
                        new_close = 1.0
                        logging.warning("[ROLLOVER] missing close for new_main, fallback new_close=1.0 and will ensure history")
                    old_adj = self.sym_adj_map.get(old_main, self.current_main_adj or 1.0)
                    new_adj = (abs(old_close) * max(old_adj, 1e-9)) / max(abs(new_close), 1e-6)
                    if not (1e-6 <= new_adj <= 1e6):
                        print(f"[WARN] Adj factor out of bounds: {new_adj}, reset to 1.0")
                        new_adj = 1.0
                    try:
                        if old_main in self.pending_orders:
                            self.pending_orders.pop(old_main, None)
                            logging.info(f"[ROLLOVER] Canceled pending orders for {old_main} due to rollover")
                    except Exception:
                        pass
                    logging.info(f"[ROLLOVER] computing new_adj={new_adj:.8f} (old_close={old_close}, new_close={new_close}, old_adj={old_adj})")
                    self._apply_adj_to_history(old_main, new_adj, old_adj)
                    if new_main in self.hist_map:
                        prev_new_factor = self.sym_adj_map.get(new_main, 1.0)
                        self._apply_adj_to_history(new_main, new_adj, prev_new_factor)
                    self.current_main = new_main
                    self.current_main_adj = new_adj
                    self.prev_main_close = new_close
                    self._adjust_positions_after_rollover(old_main, new_main, new_adj)
                    try:
                        ref_dt = None
                        if new_main in self.hist_map and len(self.hist_map[new_main])>0:
                            ref_dt = self.hist_map[new_main][-1]['datetime']
                        else:
                            ref_dt = datetime.strptime(current_day, "%Y%m%d")
                        self._ensure_min_history_for_symbol(new_main, ref_dt, self.channel_bars)
                        try:
                            self._try_generate_signal_for_symbol(new_main, ref_dt)
                        except Exception:
                            pass
                    except Exception as e:
                        print("[WARN] ensure_min_history_for_symbol failed:", e)
                    self.atr_map[new_main] = ATR(self.atr_period)
                    self.hl_map[new_main] = RollingHighLow(self.channel_bars)
                    if new_main in self.hist_map:
                        for entry in self.hist_map[new_main]:
                            self.hl_map[new_main].push(entry)
                            self.atr_map[new_main].push(entry['high'], entry['low'], entry['close'])
                    self._cleanup_old_contracts(keep_days=5)
                else:
                    logging.info("[ROLLOVER] no rollover condition met")
            finally:
                self._rolling_over = False
        except Exception as e:
            print("[ERROR] _roll_main_contract_if_needed failed:", e)
            traceback.print_exc()

    def _adjust_positions_after_rollover(self, old_main, new_main, new_adj):
        """
        换月后需要把持仓按照复权因子反推历史价格并更新持仓 entry_price（文档要求）。
        实现策略：
            - 对所有持仓，如果持仓基于 old_main（sym equals old_main），则 entry_price *= (new_adj / old_adj)
        Note: We store position.adj_factor to account for its original basis.
        """
        try:
            for sym, pos in list(self.positions.items()):
                if sym == old_main:
                    old_adj = pos.adj_factor or 1.0
                    ratio = new_adj / old_adj if old_adj != 0 else 1.0
                    old_entry = pos.entry_price
                    pos.entry_price = pos.entry_price * ratio
                    pos.adj_factor = new_adj
                    try:
                        pos.max_profit_point = pos.max_profit_point * ratio
                        pos.current_profit_point = pos.current_profit_point * ratio
                    except Exception:
                        pass
                    self.positions[new_main] = pos
                    try:
                        del self.positions[old_main]
                    except Exception:
                        pass
                    print(f"[ROLLOVER-POS] Migrated pos {old_main} -> {new_main}: entry {old_entry:.2f} -> {pos.entry_price:.2f} (ratio {ratio:.8f})")
        except Exception as e:
            print("[ERROR] _adjust_positions_after_rollover failed:", e)
            traceback.print_exc()
    def _cleanup_old_contracts(self, keep_days=5):
        try:
            base_dt = None
            if self.current_main in self.hist_map and self.hist_map[self.current_main]:
                base_dt = self.hist_map[self.current_main][-1]['datetime']
            if base_dt is None:
                return
            cutoff = base_dt - timedelta(days=keep_days)
            for sym in list(self.hist_map.keys()):
                if sym == self.current_main:
                    continue
                arr = self.hist_map[sym]
                if not arr:
                    continue
                last_dt = arr[-1]['datetime']
                if last_dt and last_dt < cutoff:
                    print(f"[CLEANUP] remove old contract {sym}")
                    try:
                        del self.hist_map[sym]
                    except Exception:
                        pass
                    try:
                        del self.atr_map[sym]
                    except Exception:
                        pass
                    try:
                        del self.hl_map[sym]
                    except Exception:
                        pass
        except Exception as e:
            print("[ERROR] cleanup failed:", e)

    # ------------------ 实用：计算仓位大小（基于账户风险） ------------------
    def _calc_position_size(self, atr, stop_v, multiplier=None, sym=None):
        assert self.account_capital is not None and self.account_capital > 0, "account_capital not set"
        if self.account_capital is None or self.account_capital <= 0:
            return 0
        if atr is None or atr <= 0:
            return 0
        m = float(multiplier) if multiplier and multiplier>0 else float(self.contract_multiplier)
        risk_per_contract = stop_v * m
        if risk_per_contract <= 0:
            return 0
        pos = (self.account_capital * self.risk_pct_per_trade) / risk_per_contract
        # apply max_deal_pct cap
        max_position = int((self.account_capital * self.max_deal_pct) / (stop_v * m)) if self.max_deal_pct and self.max_deal_pct>0 else None
        q = max(1, int(pos))
        if max_position is not None:
            q = min(q, max_position)
        try:
            last_close = None
            if sym:
                arr = self.hist_map.get(sym, [])
                if arr:
                    last_close = arr[-1].get('close')
            if last_close and self.margin_rate and self.margin_rate>0:
                cap_by_margin = int((self.account_capital * self.max_deal_pct) / (last_close * m * self.margin_rate))
                if cap_by_margin > 0:
                    q = min(q, cap_by_margin)
        except Exception:
            pass
        return q

    def _normalize_dt(self, dt):
        try:
            if dt is None:
                return None
            tz = getattr(dt, 'tzinfo', None)
            if tz is not None:
                try:
                    utc_dt = dt.astimezone(timezone.utc)
                    local_dt = utc_dt + timedelta(minutes=int(self.timezone_offset_minutes or 0))
                    return local_dt.replace(tzinfo=None)
                except Exception:
                    return dt.replace(tzinfo=None)
        except Exception:
            pass
        return dt

    def _check_open_time(self, dt):
        if dt is None:
            return False
        try:
            t = self._normalize_dt(dt).time()
            if (t >= self.open_allowed_after and (t < time(15,0))):
                return True
            if self.night_trade and ((t >= time(21,0) and t <= time(23,30)) or (t >= time(0,0) and t <= time(2,30))):
                return True
            return False
        except Exception:
            return False

    def _ready_for_open(self, sym):
        try:
            atr = self.atr_map.get(sym)
            hl = self.hl_map.get(sym)
            return (atr and atr.get() is not None) and (hl and hl.size() >= self.min_channel_bars)
        except Exception:
            return False

    # ------------------ 执行/挂单/撮合（NextBarOpen） ------------------
    def _create_pending_order(self, sym, side, qty, created_at, price=None, order_type='market', reason=None):
        if getattr(self, '_halted', False):
            closing_reasons = {'stop', 'take', 'reversal_long_close', 'reversal_short_close'}
            if str(reason) not in closing_reasons:
                logging.warning(f"[ORDER] halted, skip creating pending order {sym}")
                return None
        try:
            if str(reason).startswith('open_'):
                hl = self.hl_map.get(sym)
                ok = bool(hl and hl.size() >= self.min_channel_bars)
                if not ok:
                    try:
                        if hasattr(self, 'auditor') and self.auditor:
                            self.auditor.daily_violations.append(("channel_insufficient", {'symbol': sym}))
                    except Exception:
                        pass
                    return None
        except Exception:
            pass
        pend = {
            'side': int(side),
            'qty': int(qty),
            'created_at': created_at,
            'price': price,
            'type': order_type,
            'reason': reason
        }
        self.pending_orders[sym] = pend
        logging.info(f"[ORDER] Created pending order for {sym}: side={side} qty={qty} at {created_at} price={price}")
        return pend

    def _execute_pending_if_ready(self, sym, bar_dt, exec_price):
        pend = self.pending_orders.get(sym)
        if not pend:
            return False
        try:
            if getattr(self, '_halted', False):
                return False
            def bucket_base(dt):
                return dt.replace(minute=dt.minute - (dt.minute % 5), second=0, microsecond=0)
            created = pend['created_at']
            next_bucket = bucket_base(created) + timedelta(minutes=5)
            if getattr(self, '_rolling_over', False):
                return False
            if bar_dt >= next_bucket:
                side = pend['side']
                qty = pend['qty']
                base_price = exec_price
                price = exec_price
                if self.slippage_type == 'pct':
                    if side == 1:
                        price = price * (1.0 + self.slippage_value)
                    else:
                        price = price * (1.0 - self.slippage_value)
                else:
                    price = price + (self.slippage_value if side==1 else -self.slippage_value)
                prev = self.positions.get(sym)
                self.positions[sym] = Position(side, qty, price, bar_dt, adj_factor=self.current_main_adj)
                self.target_positions[sym] = side * qty
                self.pending_orders.pop(sym, None)
                self.changed = True
                logging.info(f"[EXECUTE] {sym} executed: side={side} qty={qty} price={price:.2f} at {bar_dt}")
                realized_pnl_pts = None
                if prev is not None:
                    if prev.side == 1:
                        realized_pnl_pts = price - prev.entry_price
                    else:
                        realized_pnl_pts = prev.entry_price - price
                try:
                    ind_atr = None
                    ind_ch_high = None
                    ind_ch_low = None
                    prev_close = None
                    current_close = None
                    try:
                        ind_atr = self.atr_map.get(sym).get()
                    except Exception:
                        ind_atr = None
                    try:
                        ind_ch_high = self.hl_map.get(sym).highest_ago(5)
                        ind_ch_low = self.hl_map.get(sym).lowest_ago(5)
                    except Exception:
                        pass
                    try:
                        arr = self.hist_map.get(sym, [])
                        if arr and arr[-1].get('datetime') == bar_dt:
                            current_close = arr[-1].get('close')
                            if len(arr) >= 2:
                                prev_close = arr[-2].get('close')
                    except Exception:
                        pass
                    cutoff_violation = False
                    try:
                        t = bar_dt.time()
                        if t >= self.cancel_cutoff_time and pend.get('reason','').startswith('open_'):
                            cutoff_violation = True
                    except Exception:
                        cutoff_violation = False
                    is_main_trade = True
                    try:
                        if self.is_main_symbol_only and self.current_main and sym != self.current_main:
                            is_main_trade = False
                    except Exception:
                        pass
                    self.trade_log.append({
                        'symbol': sym,
                        'dt': bar_dt,
                        'side': side,
                        'qty': qty,
                        'price': price,
                        'base_price': base_price,
                        'reason': pend.get('reason'),
                        'realized_pnl_pts': realized_pnl_pts,
                        'ind_atr': ind_atr,
                        'ind_ch_high': ind_ch_high,
                        'ind_ch_low': ind_ch_low,
                        'prev_close': prev_close,
                        'current_close': current_close,
                        'cutoff_violation': cutoff_violation,
                        'is_main_trade': is_main_trade
                    })
                    try:
                        if hasattr(self, 'auditor') and self.auditor:
                            halt, reason = self.auditor.should_halt_trading()
                            if halt:
                                self._graceful_shutdown(reason)
                    except Exception:
                        pass
                except Exception:
                    pass
                return True
            else:
                return False
        except Exception as e:
            print("[ERROR] execute_pending failed:", e)
            try:
                self.alerts.append(f"EXECUTE_FAIL {sym} {bar_dt} {e}")
            except Exception:
                pass
            return False

    # ------------------ 主回调：接收多根 bar（通常一个时间点的若干合约） ------------------
    def calculate_target_position(self, list_bar):
        """
        list_bar: list of bar objects (pandas row / dict / object)
        Return: (target_positions dict, changed bool)
        """
        print("\n" + "="*80)
        print(f"[DEBUG] receive {len(list_bar)} bars")
        self.changed = False
        if getattr(self, '_rolling_over', False) or getattr(self, '_halted', False):
            return self.target_positions, False
        try:
            self._sanity_check()
        except Exception as e:
            print("[WARN] sanity_check failed:", e)

        if not list_bar:
            print("[DEBUG] empty bar list")
            return self.target_positions, False

        # sort by datetime for deterministic behavior
        extracted = []
        for bar in list_bar:
            b = self._extract_bar_fields(bar)
            if b is None:
                print("[WARN] bar extraction returned None, skipping bar")
                continue
            # normalize symbol string
            try:
                b['symbol'] = str(b['symbol']).strip()
            except Exception:
                pass
            try:
                raw = b.get('raw_datetime')
                if isinstance(raw, str):
                    s = raw.strip()
                    if len(s) == 8 and s.isdigit() and ' ' not in s and ':' not in s:
                        print(f"[TIME] invalid datetime granularity for {b['symbol']} raw={s}, skip 5m decisions")
                        # keep for EOD/BOD OI syncing but skip decision path by not appending
                        continue
            except Exception:
                pass
            extracted.append(b)

        # if nothing extracted
        if not extracted:
            print("[DEBUG] no valid bars after extraction")
            return self.target_positions, False

        # print first bar keys for debugging
        print("[DEBUG] sample extracted bar keys:", list(extracted[0].keys()))

        # group by symbol - but we want chronological order
        extracted_sorted = sorted(extracted, key=lambda x: (x['symbol'], x['datetime'] or datetime.min))

        # OI 在收盘时记录，移至 _finalize_and_process_5m

        # BOD self-protection: ensure expire map sync once per new day
        try:
            first_dt = None
            for b in extracted:
                if b['datetime'] is not None:
                    first_dt = b['datetime']
                    break
            if first_dt is not None:
                tday = first_dt.strftime("%Y%m%d")
                if tday != self.last_bod_day:
                    self.on_bod(tday)
                    self.last_bod_day = tday
        except Exception as e:
            print("[WARN] bod self-protect failed:", e)
        try:
            dts = [b['datetime'] for b in extracted if b['datetime'] is not None]
            if dts:
                md = max(dts)
                tday_md = md.strftime("%Y%m%d")
                if md.time() >= time(10,30) and self._last_midday_checked_day != tday_md:
                    self._fill_day_oi_and_roll(tday_md, md.strftime("%Y-%m-%d"))
                    self._last_midday_checked_day = tday_md
        except Exception as e:
            print("[WARN] midday self-check failed:", e)

        # process bars in time order (by datetime)
        for b in sorted(extracted, key=lambda x: x['datetime'] or datetime.min):
            sym = b['symbol']
            dt = b['datetime']
            close = b['close']
            high = b['high']
            low = b['low']
            vol = b['volume']
            oi = b['oi']

            print(f"\n[BAR] {sym} dt={b['raw_datetime']} parsed_dt={dt} close={close} high={high} low={low} vol={vol} oi={oi}")

            # ensure symbol state
            self._ensure_symbol(sym)

            # fully skip pre 09:10 bars to avoid inconsistent state
            if dt is not None and dt.time() < self.open_allowed_after:
                print(f"[TIME] skip pre-{self.open_allowed_after} bar {sym} {dt}")
                continue

            finalized = self._update_agg_5m(sym, dt, b['open'] or close, high or close, low or close, close, vol, oi)
            if finalized:
                self._finalize_and_process_5m(sym, finalized['dt'], finalized['open'], finalized['high'], finalized['low'], finalized['close'], finalized['volume'], finalized.get('oi', None))
                continue

            # if not finalized, skip decision until bucket closes

            # decision deferred until 5m bar finalization

        # end for bars

        # summary
        logging.debug(f"[DEBUG] Final target_positions: {self.target_positions}")
        print("[DEBUG] Pending orders:", self.pending_orders)
        print("[DEBUG] Open positions:", {k: (v.side, v.qty, v.entry_price) for k,v in self.positions.items()})
        logging.debug(f"[DEBUG] changed: {self.changed}")
        print("="*80)
        try:
            if self.changed and hasattr(self, 'auditor') and self.auditor:
                halt, reason = self.auditor.should_halt_trading()
                if halt:
                    self._graceful_shutdown(reason)
        except Exception as e:
            logging.warning(f"[AUDIT] should_halt_trading failed: {e}")
        return self.target_positions, self.changed

    # BOD/EOD hooks to sync expire dates and trigger EOD rollover
    def on_bod(self, tday):
        try:
            if hasattr(self, 'circuit_breaker') and self.circuit_breaker:
                self.circuit_breaker.on_bod(tday)
        except Exception:
            pass
        try:
            df = self.api.get_daily_symbol()
            if df is not None:
                for tup in df.itertuples():
                    sym = getattr(tup, "symbol")
                    exp = getattr(tup, "expire_date", None)
                    if exp is not None:
                        s = str(exp)
                        try:
                            if len(s) == 8 and s.isdigit():
                                self.expire_date_map[sym] = datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                        except Exception:
                            pass
        except Exception as e:
            print("[ERROR] on_bod expire map failed:", e)
        print(f"[BOD] {tday} expire_map size={len(self.expire_date_map)}")

    def on_eod(self, tday):
        try:
            self._roll_main_contract_if_needed(tday)
        except Exception as e:
            print("[ERROR] on_eod rollover failed:", e)
        try:
            if hasattr(self, 'auditor') and self.auditor:
                self.auditor.audit_daily_trades(tday)
        except Exception as e:
            print("[WARN] audit_daily_trades failed:", e)
        try:
            if hasattr(self, 'circuit_breaker') and self.circuit_breaker:
                self.circuit_breaker.on_eod(tday)
        except Exception:
            pass
        self.last_bod_day = None

    def _get_daily_oi_from_api(self, sym, dt):
        try:
            if not hasattr(self, 'api') or self.api is None:
                return None
            date_str = dt.strftime("%Y-%m-%d")
            exch = str(sym).split('.')[-1] if '.' in str(sym) else ""
            df = None
            try:
                df = self.api.get_future_bar(date_str, "1d", exch, sym)
            except Exception as e:
                print("[WARN] get_future_bar failed:", e)
            if df is not None and df.shape[0] > 0:
                for col in ["open_interest", "oi", "OpenInterest"]:
                    if col in df.columns:
                        try:
                            val = float(df.iloc[-1][col])
                            return val
                        except Exception:
                            pass
                print("[WARN] OI missing in daily data, skip")
        except Exception as e:
            print("[WARN] _get_daily_oi_from_api failed:", e)
        return None
    def _ensure_min_history_for_symbol(self, sym, until_dt, target_count):
        try:
            exch = str(sym).split('.')[-1] if '.' in str(sym) else ""
            if sym not in self.hist_map:
                self.hist_map[sym] = []
            existing = set([x['datetime'] for x in self.hist_map[sym] if x.get('datetime') is not None])
            count = len(self.hist_map[sym])
            if count >= target_count:
                return
            day_str_until = until_dt.strftime("%Y-%m-%d")
            tdays = []
            try:
                df_t = self.api.get_trading_day()
                arr = []
                try:
                    arr = list(df_t.itertuples())
                except Exception:
                    arr = []
                for tup in arr:
                    try:
                        tday = str(getattr(tup, "trade_day"))
                    except Exception:
                        try:
                            tday = str(getattr(tup, "TradingDay"))
                        except Exception:
                            tday = None
                    if not tday:
                        continue
                    if '-' in tday:
                        tday_n = tday
                    elif len(tday) == 8 and tday.isdigit():
                        tday_n = tday[0:4] + "-" + tday[4:6] + "-" + tday[6:8]
                    else:
                        continue
                    if tday_n <= day_str_until:
                        tdays.append(tday_n)
                if len(tdays) > 120:
                    tdays = tdays[-120:]
            except Exception:
                pass
            steps = 0
            for day_str in reversed(tdays):
                df = None
                try:
                    df = self.api.get_future_bar(day_str, "1m", exch, sym)
                except Exception:
                    df = None
                if df is None or not hasattr(df, 'shape') or df.shape[0] == 0:
                    steps += 1
                    continue
                rows = []
                try:
                    rows = list(df.itertuples())
                except Exception:
                    rows = []
                for tup in rows:
                    try:
                        o = float(getattr(tup, "open"))
                    except Exception:
                        try:
                            o = float(getattr(tup, "Open"))
                        except Exception:
                            o = None
                    try:
                        h = float(getattr(tup, "high"))
                    except Exception:
                        try:
                            h = float(getattr(tup, "High"))
                        except Exception:
                            h = None
                    try:
                        l = float(getattr(tup, "low"))
                    except Exception:
                        try:
                            l = float(getattr(tup, "Low"))
                        except Exception:
                            l = None
                    try:
                        c = float(getattr(tup, "close"))
                    except Exception:
                        try:
                            c = float(getattr(tup, "Close"))
                        except Exception:
                            c = None
                    try:
                        tm = str(getattr(tup, "date_time"))
                    except Exception:
                        try:
                            tm = str(getattr(tup, "rt_time"))
                        except Exception:
                            tm = None
                    dtv = None
                    try:
                        dtv = datetime.strptime(day_str + " " + tm, "%Y-%m-%d %H:%M:%S") if tm else None
                    except Exception:
                        dtv = None
                    if dtv is None:
                        continue
                    final_5m = self._update_agg_5m(sym, dtv, o if o is not None else c, h if h is not None else c, l if l is not None else c, c, 0.0, None)
                    if final_5m:
                        fdt = final_5m['dt']
                        if fdt in existing:
                            continue
                        entry = {'datetime': final_5m['dt'], 'open': final_5m['open'], 'high': final_5m['high'], 'low': final_5m['low'], 'close': final_5m['close'], 'volume': final_5m['volume']}
                        self.hist_map[sym].append(entry)
                        self.hl_map[sym].push(entry)
                        self.atr_map[sym].push(entry['high'], entry['low'], entry['close'])
                        existing.add(fdt)
                        count += 1
                        if count >= target_count:
                            break
                steps += 1
                if count >= target_count:
                    break
        except Exception as e:
            print("[WARN] _ensure_min_history_for_symbol failed:", e)
    def _recalc_position_metrics(self, pos, sym):
        try:
            if sym not in self.hist_map or len(self.hist_map[sym]) == 0:
                pos.max_profit_point = 0.0
                pos.current_profit_point = 0.0
                return
            metrics = []
            et = pos.entry_time
            for entry in self.hist_map[sym]:
                dtv = entry.get('datetime')
                if et is not None and dtv is not None and dtv < et:
                    continue
                price = entry['close']
                diff = price - pos.entry_price if pos.side == 1 else pos.entry_price - price
                metrics.append(diff)
            if metrics:
                pos.max_profit_point = max(metrics)
                last_diff = metrics[-1]
                pos.current_profit_point = last_diff
            else:
                pos.max_profit_point = 0.0
                pos.current_profit_point = 0.0
        except Exception as e:
            print("[WARN] _recalc_position_metrics failed:", e)
    def _try_generate_signal_for_symbol(self, sym, dt):
        try:
            if sym in self.pending_orders or (sym in self.positions and self.positions[sym].qty):
                return
            if sym not in self.hl_map or sym not in self.atr_map or sym not in self.hist_map:
                return
            atr_val = self.atr_map[sym].get()
            ch_ready = self.hl_map[sym].size() >= self.min_channel_bars
            if atr_val is None or not ch_ready:
                return
            if not self._check_open_time(dt):
                return
            c = self.hist_map[sym][-1]['close'] if len(self.hist_map[sym])>0 else None
            prev_close = self.hist_map[sym][-2]['close'] if len(self.hist_map[sym])>=2 else None
            ch_high_current = self.hl_map[sym].highest_ago(5)
            ch_low_current = self.hl_map[sym].lowest_ago(5)
            if c is None or prev_close is None:
                return
            if prev_close < ch_high_current and c >= ch_high_current:
                stop_v = self.stop_atr_mult * atr_val
                qty = self._calc_position_size(atr_val, stop_v, None, sym)
                if qty > 0:
                    self._create_pending_order(sym, 1, qty, dt, price=c, reason='open_long')
                return
            if prev_close > ch_low_current and c <= ch_low_current:
                stop_v = self.stop_atr_mult * atr_val
                qty = self._calc_position_size(atr_val, stop_v, None, sym)
                if qty > 0:
                    self._create_pending_order(sym, -1, qty, dt, price=c, reason='open_short')
                return
        except Exception:
            pass
    def _fill_day_oi_and_roll(self, trading_day, date_str):
        try:
            fetched_all = False
            try:
                if hasattr(self.api, "get_all_symbols_oi"):
                    df_all = self.api.get_all_symbols_oi(trading_day)
                    if df_all is not None:
                        items = df_all.items() if hasattr(df_all, "items") else []
                        for sym_all, oi_val in items:
                            self._record_oi(trading_day, sym_all, oi_val)
                            print(f"[OI-DEBUG] {trading_day} {sym_all} OI={oi_val} current_main={self.current_main}")
                        fetched_all = True
            except Exception as e:
                print("[WARN] get_all_symbols_oi failed:", e)
            if not fetched_all:
                syms = []
                try:
                    syms = self.api.get_daily_symbols(date_str)
                except Exception:
                    syms = []
                for s in syms:
                    exch_s = str(s).split('.')[-1] if '.' in str(s) else ""
                    df_s = None
                    try:
                        df_s = self.api.get_future_bar(date_str, "1d", exch_s, s)
                    except Exception:
                        df_s = None
                    oi_s = None
                    if df_s is not None and hasattr(df_s, 'shape') and df_s.shape[0] > 0:
                        for col in ["open_interest", "oi", "OpenInterest"]:
                            if col in df_s.columns:
                                try:
                                    oi_s = float(df_s.iloc[-1][col])
                                except Exception:
                                    pass
                    self._record_oi(trading_day, s, oi_s)
                    print(f"[OI-DEBUG] {trading_day} {s} OI={oi_s} current_main={self.current_main}")
            self._roll_main_contract_if_needed(trading_day)
        except Exception as e:
            print("[ERROR] _fill_day_oi_and_roll failed:", e)
            
    def _sanity_check(self):
        try:
            cm = self.current_main
            if cm is None:
                return
            atr = self.atr_map.get(cm)
            hl = self.hl_map.get(cm)
            if atr is None or atr.get() is None:
                raise RuntimeError("ATR not ready for current_main")
            if hl is None or hl.size() < self.min_channel_bars:
                raise RuntimeError("Channel not ready for current_main")
        except Exception as e:
            logging.warning(f"[SANITY] {e}")

# ---------------- 审计与熔断 ----------------
class TradeAuditor:
    def __init__(self, strategy_instance):
        self.strategy = strategy_instance
        self.daily_violations = []
        self._daily_scores = {}
        self._last_audit_key = None
        self._lock = threading.RLock()
    def _get_day_trades(self, trading_day):
        trades = []
        for t in self.strategy.trade_log:
            try:
                if t['dt'].strftime("%Y-%m-%d") == trading_day:
                    trades.append(t)
            except Exception:
                pass
        try:
            if hasattr(self.strategy.api, "get_trade_log"):
                df = self.strategy.api.get_trade_log(trading_day)
                if df is not None:
                    for tup in df.itertuples():
                        try:
                            trades.append({
                                'symbol': getattr(tup, "symbol"),
                                'dt': getattr(tup, "dt"),
                                'side': getattr(tup, "side"),
                                'qty': getattr(tup, "qty"),
                                'price': getattr(tup, "price"),
                                'base_price': getattr(tup, "base_price", None),
                                'reason': getattr(tup, "reason", None),
                                'realized_pnl_pts': getattr(tup, "pnl_pts", None)
                            })
                        except Exception:
                            pass
        except Exception:
            pass
        return trades
    def audit_daily_trades(self, trading_day):
        with self._lock:
            self.daily_violations = []
        trades = self._get_day_trades(trading_day)
        total_slip = 0.0
        slip_cnt = 0
        total_loss = 0.0
        try:
            dcur = datetime.strptime(trading_day, "%Y-%m-%d")
        except Exception:
            dcur = None
        for t in trades:
            try:
                sym = t['symbol']
                dt = t['dt']
                price = t['price']
                base_price = t.get('base_price', None)
                if base_price is not None:
                    if self.strategy.slippage_type == 'pct':
                        exp = abs(base_price * self.strategy.slippage_value)
                    else:
                        exp = abs(self.strategy.slippage_value)
                    dev = abs(price - base_price)
                    total_slip += dev
                    slip_cnt += 1
                    if dev > 2.0 * exp:
                        self.daily_violations.append(("price_deviation", t))
                if not self.strategy._check_open_time(dt):
                    self.daily_violations.append(("time_violation", t))
                atr_val = t.get('ind_atr', None)
                if atr_val is None:
                    try:
                        atr_val = self.strategy.atr_map.get(sym).get()
                    except Exception:
                        atr_val = None
                stop_v = (self.strategy.stop_atr_mult * atr_val) if atr_val else None
                pnl_pts = t.get('realized_pnl_pts', None)
                if pnl_pts is not None and stop_v is not None and pnl_pts < -2.0 * stop_v:
                    self.daily_violations.append(("single_loss_too_large", t))
                try:
                    if pnl_pts is not None:
                        m = float(self.strategy.contract_multiplier)
                        total_loss += (-min(pnl_pts, 0.0)) * m * float(t.get('qty', 0))
                except Exception:
                    logging.warning("[AUDIT] error while processing trade", exc_info=True)
                if t.get('cutoff_violation', False):
                    self.daily_violations.append(("late_entry_after_cutoff", t))
                if self.strategy.is_main_symbol_only and not t.get('is_main_trade', True):
                    self.daily_violations.append(("non_main_trade", t))
                try:
                    if t.get('reason') in ('open_long', 'open_short'):
                        hl = self.strategy.hl_map.get(sym)
                        if not (hl and hl.size() >= self.strategy.min_channel_bars):
                            self.daily_violations.append(("channel_insufficient", t))
                        # bar range check
                        bar = None
                        arr = self.strategy.hist_map.get(sym, [])
                        for entry in reversed(arr):
                            if entry.get('datetime') == dt:
                                bar = entry
                                break
                        if bar and not (bar.get('low') is None or bar.get('high') is None):
                            if not (bar['low'] <= price <= bar['high']):
                                self.daily_violations.append(("price_out_of_bar_range", t))
                except Exception:
                    logging.warning("[AUDIT] error price range check", exc_info=True)
                try:
                    if t.get('reason') in ('open_long', 'open_short'):
                        base = t['base_price'] if 'base_price' in t else t.get('base_price', None)
                        price = t['price'] if 'price' in t else t.get('price', None)
                        if base is not None and price is not None:
                            if self.strategy.slippage_type == 'pct':
                                exp_dev = abs(base * self.strategy.slippage_value)
                                tol_pct = getattr(self.strategy, 'slippage_tolerance_pct', 0.25)
                                tol = max(1e-6, exp_dev * tol_pct)
                                if abs(price - base) > exp_dev + tol:
                                    self.daily_violations.append(("price_mismatch_nextbaropen", t))
                            else:
                                exp_dev = abs(self.strategy.slippage_value)
                                tol_pct = getattr(self.strategy, 'slippage_tolerance_pct', 0.25)
                                tol = max(1e-6, exp_dev * tol_pct)
                                if abs(price - base) > exp_dev + tol:
                                    self.daily_violations.append(("price_mismatch_nextbaropen", t))
                except (KeyError, AttributeError) as e:
                    logging.error(f"[AUDIT_CRIT] Programming error: {e}", exc_info=True)
                    raise
                except Exception:
                    logging.warning("[AUDIT] error nextbaropen price check", exc_info=True)
                if t.get('reason') == 'open_long':
                    prev_close = t.get('prev_close')
                    ch_high = t.get('ind_ch_high')
                    cur_close = t.get('current_close')
                    if prev_close is None or ch_high is None or cur_close is None or not (prev_close < ch_high and cur_close >= ch_high):
                        self.daily_violations.append(("signal_mismatch_long", t))
                if t.get('reason') == 'open_short':
                    prev_close = t.get('prev_close')
                    ch_low = t.get('ind_ch_low')
                    cur_close = t.get('current_close')
                    if prev_close is None or ch_low is None or cur_close is None or not (prev_close > ch_low and cur_close <= ch_low):
                        self.daily_violations.append(("signal_mismatch_short", t))
                if t.get('reason') in ('take', 'stop'):
                    if atr_val:
                        stop_v = self.strategy.stop_atr_mult * atr_val
                        max_p = t.get('realized_pnl_pts', None)
                        cur_p = t.get('realized_pnl_pts', None)
                        if t.get('reason') == 'take':
                            try:
                                maxp = self.strategy.positions.get(sym).max_profit_point if sym in self.strategy.positions else None
                                curp = self.strategy.positions.get(sym).current_profit_point if sym in self.strategy.positions else None
                            except Exception:
                                maxp = None
                                curp = None
                            if maxp is not None and curp is not None:
                                exit_point = (maxp / stop_v) * stop_v if stop_v > 0 else 0.0
                                allow_drop = stop_v - exit_point
                                drop = maxp - curp
                                if not (drop > allow_drop):
                                    self.daily_violations.append(("exit_rule_mismatch_take", t))
                        if t.get('reason') == 'stop':
                            curp = self.strategy.positions.get(sym).current_profit_point if sym in self.strategy.positions else None
                            if curp is not None and not (curp <= -stop_v):
                                self.daily_violations.append(("exit_rule_mismatch_stop", t))
            except Exception:
                pass
        try:
            pend = self.strategy.pending_orders
            for sym, p in list(pend.items()):
                try:
                    created = p['created_at']
                    if created is None:
                        continue
                    if created.time() < time(14,57) and sym in self.strategy.pending_orders:
                        self.daily_violations.append(("pending_not_canceled", {'symbol': sym, 'dt': created, 'reason': 'pending'}))
                    if created.time() < self.strategy.open_allowed_after and str(p.get('reason','')).startswith('open_'):
                        self.daily_violations.append(("pending_before_open", {'symbol': sym, 'dt': created, 'reason': p.get('reason')}))
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if slip_cnt > 0:
                avg_slip = total_slip / slip_cnt
                if self.strategy.slippage_type == 'pct':
                    exp = getattr(self.strategy, 'avg_slippage_mult_threshold', AVG_SLIPPAGE_MULT_THRESHOLD) * abs(self.strategy.slippage_value)
                    if abs(avg_slip / max(1e-9, (trades[0].get('base_price') or avg_slip))) > exp:
                        self.daily_violations.append(("avg_slippage_excessive", {'avg_slip': avg_slip}))
                else:
                    exp = getattr(self.strategy, 'avg_slippage_mult_threshold', AVG_SLIPPAGE_MULT_THRESHOLD) * abs(self.strategy.slippage_value)
                    if avg_slip > exp:
                        self.daily_violations.append(("avg_slippage_excessive", {'avg_slip': avg_slip}))
        except Exception:
            pass
        try:
            for sym, pos in self.strategy.positions.items():
                et = pos.entry_time
                if dcur and et:
                    delta = dcur - et
                    limit_min = self.strategy.channel_bars * 5 * getattr(self.strategy, 'holding_time_mult', HOLDING_TIME_MULT)
                    if delta.total_seconds() >= limit_min * 60:
                        self.daily_violations.append(("holding_time_excessive", {'symbol': sym, 'dt': dcur, 'entry_time': et}))
        except Exception:
            pass
        try:
            cons_stop = 0
            for t in trades:
                if t.get('reason') == 'stop':
                    cons_stop += 1
                    threshold = getattr(self.strategy, 'consecutive_stop_loss_threshold', CONSECUTIVE_STOP_LOSS_THRESHOLD)
                    if cons_stop >= threshold:
                        self.daily_violations.append(("consecutive_stops", t))
                        break
                else:
                    cons_stop = 0
        except Exception:
            pass
        try:
            freq = len(trades)
            exp_freq = 1
            if freq > exp_freq * 3:
                self.daily_violations.append(("too_many_trades", {'dt': trading_day, 'count': freq}))
        except Exception:
            pass
        try:
            tday = trading_day.replace('-', '')
            oi_map_day = self.strategy.oi_map.get(tday, {})
            if oi_map_day:
                old_main = self.strategy.current_main
                new_main = max(oi_map_day.items(), key=lambda x: x[1])[0]
                old_oi = oi_map_day.get(old_main, 0.0)
                new_oi = oi_map_day.get(new_main, 0.0)
                exp = self.strategy.expire_date_map.get(old_main)
                need_force = False
                if exp is not None:
                    try:
                        dcur = datetime.strptime(trading_day, "%Y-%m-%d")
                        if (exp - dcur).days <= FORCE_ROLLOVER_DAYS:
                            need_force = True
                    except Exception:
                        pass
                if (new_oi > old_oi * OI_SWITCH_THRESHOLD or need_force) and new_main != old_main:
                    self.daily_violations.append(("rollover_missing", {'old_main': old_main, 'new_main': new_main, 'oi_old': old_oi, 'oi_new': new_oi}))
                try:
                    if self.strategy._last_midday_checked_day != tday:
                        self.daily_violations.append(("midday_check_missing", {'day': trading_day}))
                except Exception:
                    pass
            try:
                cm = self.strategy.current_main
                if oi_map_day and cm and oi_map_day.get(cm, None) is None:
                    self.daily_violations.append(("oi_missing_for_main_day", {'day': trading_day, 'main': cm}))
            except Exception:
                pass
        except Exception:
            pass
        try:
            # OI quality degraded: last N days missing
            n = getattr(self.strategy, 'oi_missing_threshold', 3)
            df_t = self.strategy.api.get_trading_day()
            days = []
            if df_t is not None:
                # collect last n trading days up to current
                all_days = [str(getattr(tup, "trade_day")).replace('_','-') for tup in df_t.itertuples()]
                all_days = [d for d in all_days if d <= trading_day]
                days = all_days[-n:] if len(all_days) >= n else all_days
            recent_days = set([d.replace('-','') for d in days])
            miss_set = set([d.replace('-','') for d in getattr(self.strategy, '_oi_missing_days', set())]) & recent_days
            if days and all(d.replace('-','') in miss_set for d in days):
                self.daily_violations.append(("oi_quality_degraded", {'days': days}))
        except Exception:
            logging.warning("[AUDIT] oi quality check failed", exc_info=True)
        try:
            keys = list(self.strategy.positions.keys())
            if self.strategy.current_main and any(k != self.strategy.current_main for k in keys):
                self.daily_violations.append(("position_not_migrated_on_rollover", {'positions': keys, 'current_main': self.strategy.current_main}))
        except Exception:
            pass
        try:
            old_main = None
            new_main = self.strategy.current_main
            if new_main and new_main in self.strategy.hist_map and len(self.strategy.hist_map[new_main])>0:
                new_close = self.strategy.hist_map[new_main][-1]['close']
            else:
                new_close = None
            # attempt previous main close and adj
            for k in self.strategy.sym_adj_map.keys():
                if k != new_main and k in self.strategy.hist_map and len(self.strategy.hist_map[k])>0:
                    old_main = k
                    break
            if old_main:
                old_close = self.strategy.hist_map[old_main][-1]['close']
                old_adj = self.strategy.sym_adj_map.get(old_main, 1.0)
                new_adj = self.strategy.sym_adj_map.get(new_main, 1.0)
                if old_close and new_close and old_adj and new_adj:
                    scaled_new = new_close * (old_adj / new_adj)
                    jump = abs(scaled_new - old_close) / max(1e-9, abs(old_close))
                    if jump > getattr(self.strategy, 'price_jump_threshold', PRICE_JUMP_THRESHOLD):
                        self.daily_violations.append(("rollover_price_jump", {'old_main': old_main, 'new_main': new_main, 'jump': jump}))
        except Exception:
            pass
        try:
            if total_loss > self.strategy.account_capital * getattr(self.strategy, 'capital_dd_threshold', CAPITAL_DD_THRESHOLD):
                self.daily_violations.append(("capital_drawdown_excessive", {'loss_abs': total_loss}))
        except Exception:
            pass
        return self.daily_violations
    def calculate_violation_score(self) -> float:
        score = 0.0
        for v in self.daily_violations:
            tag = v[0]
            if tag == "price_deviation":
                score += 3.0
            elif tag == "time_violation":
                score += 3.0
            elif tag == "consecutive_stops":
                score += 2.0
            elif tag == "single_loss_too_large":
                score += 2.0
            elif tag == "too_many_trades":
                score += 2.0
            elif tag == "pending_delay":
                score += 1.0
            elif tag == "price_mismatch_nextbaropen":
                score += 2.0
            elif tag == "channel_insufficient":
                score += 3.0
            elif tag == "oi_quality_degraded":
                score += 3.0
            elif tag == "rollover_missing":
                score += 2.0
            elif tag == "position_not_migrated_on_rollover":
                score += 2.0
        return score
    def should_halt_trading(self) -> (bool, str):
        cur_day = None
        try:
            dts = []
            for t in self.strategy.trade_log:
                try:
                    dts.append(t['dt'])
                except Exception:
                    pass
            if dts:
                cur_day = max(dts).strftime("%Y-%m-%d")
        except Exception:
            cur_day = None
        # debounce: avoid repeated full audit if nothing changed
        trades = []
        try:
            trades = self._get_day_trades(cur_day) if cur_day else []
        except Exception:
            trades = []
        audit_key = None
        try:
            if cur_day:
                last_dt = trades[-1]['dt'] if trades else None
                pending_count = 0
                try:
                    pending_count = len(self.strategy.pending_orders)
                except Exception:
                    pending_count = 0
                try:
                    with self._lock:
                        v_snapshot = list(self.daily_violations)
                except Exception:
                    v_snapshot = []
                try:
                    violation_hash = hash(json.dumps(v_snapshot, sort_keys=True, default=str))
                except Exception:
                    violation_hash = hash(str(v_snapshot))
                audit_key = (cur_day, len(trades), last_dt, pending_count, violation_hash)
        except Exception:
            try:
                pending_count = len(self.strategy.pending_orders)
            except Exception:
                pending_count = 0
            try:
                with self._lock:
                    v_snapshot = list(self.daily_violations)
            except Exception:
                v_snapshot = []
            try:
                violation_hash = hash(json.dumps(v_snapshot, sort_keys=True, default=str))
            except Exception:
                violation_hash = hash(str(v_snapshot))
            audit_key = (cur_day, len(trades), None, pending_count, violation_hash)
        try:
            with self._lock:
                if cur_day and audit_key != self._last_audit_key:
                    self.audit_daily_trades(cur_day)
                    self._last_audit_key = audit_key
        except Exception:
            logging.warning("[AUDIT] audit_daily_trades failed", exc_info=True)
        try:
            score = self.calculate_violation_score()
        except Exception:
            score = 0.0
        threshold = getattr(self.strategy, 'halt_score_threshold', HALT_SCORE_THRESHOLD)
        if score >= threshold and self.daily_violations:
            first = self.daily_violations[0]
            tag = first[0]
            info = first[1]
            try:
                if isinstance(info, dict):
                    detail = info
                else:
                    detail = {
                        'symbol': info.get('symbol'),
                        'dt': info.get('dt'),
                        'side': info.get('side'),
                        'qty': info.get('qty'),
                        'price': info.get('price'),
                        'base_price': info.get('base_price'),
                        'reason': info.get('reason'),
                        'prev_close': info.get('prev_close'),
                        'current_close': info.get('current_close'),
                        'ind_ch_high': info.get('ind_ch_high'),
                        'ind_ch_low': info.get('ind_ch_low'),
                        'ind_atr': info.get('ind_atr')
                    }
            except Exception:
                detail = {}
            reason = f"score={score:.2f} threshold={threshold} type={tag} detail={detail}"
            return True, reason
        return False, ""

class ComplianceCircuitBreaker:
    def __init__(self, strategy_instance):
        self.strategy = strategy_instance
        self.block_next_day = False
        self._last_eod_day = None
        self._halt_reason = ""
    def on_eod(self, trading_day):
        try:
            self._last_eod_day = trading_day
            v = []
            try:
                v = list(getattr(self.strategy.auditor, 'daily_violations', []))
            except Exception:
                v = []
            if v:
                self.block_next_day = True
                try:
                    _, reason = self.strategy.auditor.should_halt_trading()
                    self._halt_reason = reason
                except Exception:
                    self._halt_reason = ""
            else:
                self.block_next_day = False
                self._halt_reason = ""
        except Exception:
            pass
    def on_bod(self, trading_day):
        try:
            if self.block_next_day:
                reason = self._halt_reason or f"violations on {self._last_eod_day}"
                try:
                    self.strategy._graceful_shutdown(reason)
                except Exception:
                    pass
        except Exception:
            pass

# ---------------- export compatibility ----------------
# expose common names for platform loader
test_sample_strategy = TestSampleStrategy
TestSampleStrategy = TestSampleStrategy

# If platform expects a factory:
def create_strategy(*args, **kwargs):
    return TestSampleStrategy(*args, **kwargs)
