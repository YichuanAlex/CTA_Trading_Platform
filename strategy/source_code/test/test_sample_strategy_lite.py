from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Dict, List, Tuple, Any, Optional
import logging
import os
import json
import calendar


class test_sample_strategy:
    """
    多因子截面调仓策略（轻量化优化版）
    - 因子：相对强弱（超额收益，主力近月相对截面中位数）
    - 优化点：缓存预计算、减少重复遍历、简化日志、批量处理
    - 合约到期：到期前5天或进入交割月前平仓
    """

    def __init__(self) -> None:
        # ===== 可配置参数 =====
        self.rebalance_interval: int = 5
        self.rebalance_unit: str = "trading_day"
        self.factor_window: int = 20
        self.momentum_window: int = 20
        self.oi_change_window: int = 20
        self.ts_kind: str = "ratio"
        self.long_count: int = 3
        self.short_count: int = 3
        self.open_allowed_after: time = time(9, 10)
        self.cancel_cutoff_time: time = time(15, 0)
        self.max_deal_pct: float = 0.2

        # 因子权重
        self.momentum_weight: float = 0.3
        self.term_structure_weight: float = 0.5
        self.oi_weight: float = 0.2

        # ===== 运行时状态 =====
        self.api: Any = None
        self.enable_trend_filter: bool = False
        self.trend_filter_window: int = 60
        self.signal_threshold: float = 0.0
        self.margin_rates: Dict[str, float] = {}
        self.commission_per_lot: float = 5.0
        self.slippage_per_lot: float = 2.0
        self.impact_cost_pct: float = 0.0001
        self.enable_strategy_cost: bool = False
        self.night_start: time = time(21, 0)
        self.night_end: time = time(23, 59)
        self.stop_loss_pct: float = 0.05
        self.enable_stop_loss: bool = True
        self.retain_pool_mult: float = 2.0
        self.entry_price_map: Dict[str, float] = {}
        self.enable_market_volume_check: bool = False
        self.market_volume_cap_pct: float = 0.1
        self.dict_target_position: Dict[str, int] = {}
        self.price_hist: Dict[str, List[float]] = {}
        self.oi_hist: Dict[str, List[float]] = {}
        self.prev_close: Dict[str, float] = {}
        self.last_rebalance_day: datetime | None = None
        self._bars_since_last_rebalance: int = 0
        self._prev_main_bars = None
        self._prev_all_bars = None
        self.prev_main_symbol: Dict[str, str] = {}
        self.root_adj_factor: Dict[str, float] = {}
        self.cache_expire_days: int = 0
        self.min_hold_days: int = 5
        self._last_change_day_map: Dict[str, datetime] = {}
        self._last_scores: Dict[str, float] = {}
        self._main_symbol_map: Dict[str, str] = {}
        self.fixed_budget_base: float = 0.0
        self.sizing_mode: str = "nominal"
        self.multiplier_cache: Dict[str, float] = {}

        # ===== 预计算缓存 =====
        self._symbol_root_cache: Dict[str, str] = {}
        self._margin_rate_cache: Dict[str, float] = {}
        self._contract_info_cache: Dict[str, Any] = {}
        self._trading_days_list: List[str] = []
        self._main_contracts: Dict[str, Tuple[str, float]] = {}
        self._last_main_update_day: datetime | None = None
        self._roots_subscribed: List[str] = []
        self._config_loaded: bool = False

    # ==================================================
    # 平台初始化
    # ==================================================
    def init(self, api, **kwargs) -> int:
        """策略初始化：加载参数、预计算缓存"""
        self.api = api
        
        # 1) 从 kwargs 注入
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        
        # 2) 加载 JSON 配置（一次性）
        self._load_config()
        
        # 3) 设置固定预算
        self._setup_fixed_budget()
        
        # 4) 订阅品种根并设置数据集
        self._setup_subscription()
        
        # 5) 预计算 factor_window
        self.factor_window = max(self.momentum_window, self.oi_change_window)
        
        # 6) 禁用在线模式的复杂功能
        self._disable_complex_features()
        
        logging.info(
            "[test_sample_strategy_lite][init] rebalance=%d window=%d long=%d short=%d",
            self.rebalance_interval, self.factor_window, self.long_count, self.short_count
        )
        
        self.on_init()
        return 0

    def _load_config(self) -> None:
        """一次性加载配置文件"""
        try:
            user = getattr(self.api, "user_name", "test")
            cfg_path = os.path.join(os.getcwd(), "strategy", "config", user, "test_sample_strategy.json")
            if not os.path.exists(cfg_path):
                return
            
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            
            params = cfg.get("params", {})
            
            # 批量设置参数
            int_params = ["rebalance_interval", "factor_window", "long_count", "short_count", "min_hold_days", "trend_filter_window"]
            float_params = ["max_deal_pct", "momentum_weight", "term_structure_weight", "oi_weight", "signal_threshold", "stop_loss_pct", "retain_pool_mult", "market_volume_cap_pct", "commission_per_lot", "slippage_per_lot", "impact_cost_pct"]
            str_params = ["rebalance_unit", "sizing_mode", "ts_kind"]
            bool_params = ["enable_trend_filter", "enable_strategy_cost", "enable_stop_loss", "enable_market_volume_check"]
            
            for p in int_params:
                v = params.get(p, cfg.get(p))
                if v is not None:
                    setattr(self, p, int(v))
            
            for p in float_params:
                v = params.get(p, cfg.get(p))
                if v is not None:
                    setattr(self, p, float(v))
            
            for p in str_params:
                v = params.get(p, cfg.get(p))
                if v is not None:
                    setattr(self, p, str(v))
            
            for p in bool_params:
                v = params.get(p, cfg.get(p))
                if v is not None:
                    setattr(self, p, bool(v))
            
            # 成本设置
            costs = params.get("costs", cfg.get("costs", {})) or {}
            if costs.get("commission_per_lot") is not None:
                self.commission_per_lot = float(costs["commission_per_lot"])
            if costs.get("slippage_per_lot") is not None:
                self.slippage_per_lot = float(costs["slippage_per_lot"])
            if costs.get("impact_cost_pct") is not None:
                self.impact_cost_pct = float(costs["impact_cost_pct"])
            
            # 保证金率
            mr = cfg.get("margin_rates", {})
            if isinstance(mr, dict):
                self.margin_rates = {str(k): float(v) for k, v in mr.items()}
            
            # 时间转换
            def _to_time(s: str) -> time:
                hh, mm = s.split(":")[0:2]
                return time(int(hh), int(mm))
            
            oa = params.get("open_allowed_after", cfg.get("open_allowed_after"))
            ct = params.get("cancel_cutoff_time", cfg.get("cancel_cutoff_time"))
            ns = params.get("night_start", cfg.get("night_start"))
            ne = params.get("night_end", cfg.get("night_end"))
            if isinstance(oa, str):
                self.open_allowed_after = _to_time(oa)
            if isinstance(ct, str):
                self.cancel_cutoff_time = _to_time(ct)
            if isinstance(ns, str):
                self.night_start = _to_time(ns)
            if isinstance(ne, str):
                self.night_end = _to_time(ne)
            
            self._config_loaded = True
        except Exception as e:
            logging.warning("[test_sample_strategy_lite][init] load json params fail: %s", str(e))

    def _setup_fixed_budget(self) -> None:
        """设置固定预算"""
        try:
            base_bal = 0.0
            try:
                base_bal = float(getattr(getattr(self.api, "account_manager", object()), "init_balance", 0.0))
            except Exception:
                pass
            if base_bal <= 0:
                try:
                    base_bal = float(self.api.get_balance())
                except Exception:
                    pass
            self.fixed_budget_base = base_bal if base_bal > 0 else 0.0
        except Exception:
            pass

    def _setup_subscription(self) -> None:
        """设置订阅和数据集"""
        try:
            cfg_main = os.path.join(os.getcwd(), "test_config.json")
            if not os.path.exists(cfg_main):
                return
            
            with open(cfg_main, "r", encoding="utf-8") as f:
                js_main = json.load(f)
            
            subs = [str(x) for x in js_main.get("subscribe symbol", [])]
            ex_map = {str(k): str(v) for k, v in js_main.get("exchanges", {}).items()}
            
            try:
                ced = int(js_main.get("cache_expire_days", 0))
                if ced >= 0:
                    self.cache_expire_days = ced
            except Exception:
                pass
            
            self._roots_subscribed = [f"{s}.{ex_map.get(s, '')}".strip(".") for s in subs if s in ex_map]
            
            # 获取回测区间
            begin_day, end_day = None, None
            try:
                bp = js_main.get("backtest_period", "")
                if isinstance(bp, str) and "~" in bp:
                    begin_day, end_day = bp.split("~")[0].strip(), bp.split("~")[1].strip()
            except Exception:
                pass
            
            # 订阅品种根
            if self._roots_subscribed:
                try:
                    self.api.subscribe_symbol_root(self._roots_subscribed)
                except Exception:
                    pass
                try:
                    setattr(self.api, "list_sub_symbol_root", self._roots_subscribed)
                except Exception:
                    pass
            
            # 设置交易日序列
            self._setup_trading_days(begin_day, end_day)
        except Exception:
            pass

    def _setup_trading_days(self, begin_day: str | None, end_day: str | None) -> None:
        """设置交易日序列"""
        try:
            remote_ok = bool(getattr(self.api, "remote_enabled", True))
            ds_base = os.path.join(os.getcwd(), "dataset")
            
            if not remote_ok:
                ready = self._dataset_cache_ready_for_period()
                if ready:
                    try:
                        setattr(self.api, "cwd", ds_base)
                        raw = getattr(self.api, "_api", None)
                        if raw:
                            setattr(raw, "cwd", ds_base)
                    except Exception:
                        pass
                    try:
                        setattr(self.api, "dataset_allow_download", False)
                        raw = getattr(self.api, "_api", None)
                        if raw and hasattr(raw, "dataset_allow_download"):
                            setattr(raw, "dataset_allow_download", False)
                    except Exception:
                        pass
            
            # 获取或生成交易日序列
            tds = list(getattr(self.api, "trading_date_section", []))
            if not tds and begin_day and end_day:
                try:
                    b = datetime.strptime(begin_day, "%Y-%m-%d")
                    e = datetime.strptime(end_day, "%Y-%m-%d")
                    days = []
                    cur = b
                    while cur <= e:
                        days.append(cur.strftime("%Y-%m-%d"))
                        cur += timedelta(days=1)
                    tds = days
                    setattr(self.api, "trading_date_section", days)
                    raw = getattr(self.api, "_api", None)
                    if raw:
                        setattr(raw, "trading_date_section", days)
                except Exception:
                    pass
            
            # 缓存交易日列表
            self._trading_days_list = [str(d).split(" ")[0] for d in tds]
        except Exception:
            pass

    def _disable_complex_features(self) -> None:
        """禁用在线模式的复杂功能"""
        self.enable_trend_filter = False
        self.signal_threshold = 0.0
        self.enable_strategy_cost = False
        self.enable_stop_loss = False
        self.enable_market_volume_check = False

    # ==================================================
    # 生命周期方法
    # ==================================================
    def on_init(self) -> None:
        """初始化运行时状态"""
        self.dict_target_position.clear()
        self.price_hist.clear()
        self.oi_hist.clear()
        self.prev_close.clear()
        self.last_rebalance_day = None
        self._symbol_root_cache.clear()
        self._margin_rate_cache.clear()
        self._contract_info_cache.clear()
        
        # 初始化主力合约映射（与原始版本逻辑一致）
        try:
            df_daily = None
            if bool(getattr(self.api, "remote_enabled", True)):
                try:
                    df_daily = self.api.get_daily_symbol()
                except Exception:
                    pass
            
            if df_daily is not None and hasattr(df_daily, "itertuples"):
                for row in df_daily.itertuples():
                    try:
                        symbol = getattr(row, "symbol")
                        product_id = self._symbol_root(symbol)
                        oi = float(getattr(row, "open_interest", 0.0))
                        if product_id not in self._main_contracts or oi > self._main_contracts[product_id][1]:
                            self._main_contracts[product_id] = (symbol, oi)
                    except Exception:
                        continue
        except Exception:
            pass
        
        logging.info("[test_sample_strategy_lite][on_init] ready, main_contracts=%d", len(self._main_contracts))

    def on_bar(self, bar: Any) -> None:
        """单根 bar 处理（仅维护主力的价格与持仓量历史）"""
        try:
            symbol = getattr(bar, "symbol")
            if not self._is_main_contract(symbol):
                return
            close = float(getattr(bar, "close"))
            oi = float(getattr(bar, "open_interest", 0.0))
            self._push_hist(symbol, close, oi)
        except Exception:
            pass

    def on_cycle(self, list_bar: List[Any]) -> Tuple[Dict[str, int], bool]:
        """截面周期调仓入口：门控 → 信号 → 目标仓位"""
        if not list_bar:
            return self.dict_target_position, False
        
        trading_day = self._get_trading_day(list_bar)
        
        # 更新主力合约（每日一次）
        if self._last_main_update_day != trading_day:
            self._update_main_contracts(trading_day)
            self._last_main_update_day = trading_day
        
        # 过滤主力合约 bar
        main_bars = [b for b in list_bar if self._is_main_contract(getattr(b, "symbol", ""))]
        if not main_bars:
            return self.dict_target_position, False
        
        # 应用复权调整
        self._apply_adj_on_rollover(main_bars)
        
        # 更新 prev_close（关键：无论是否调仓都要更新，且只在交易窗口内更新，与原始版本一致）
        self._update_prev_close(list_bar)
        
        # 更新 price_hist（只在交易窗口内）
        self._update_price_history(main_bars)
        
        # 检查是否需要调仓
        if not self._need_rebalance(trading_day, main_bars):
            self._prev_main_bars = main_bars
            self._prev_all_bars = list_bar
            return self.dict_target_position, False
        
        # 生成信号并调整仓位
        long_syms, short_syms, scores = self.generate_signals(main_bars, list_bar)
        new_target = self.adjust_positions(long_syms, short_syms, main_bars, scores)
        
        # 检测仓位变化
        changed = self._detect_position_change(new_target)
        
        if changed:
            self._update_entry_prices(new_target, trading_day, list_bar)
            self.dict_target_position = new_target
        
        # 注意：last_rebalance_day 不由策略层更新，由底层框架控制
        # 但 bar 计数器需要重置
        if self.rebalance_unit.lower() == "bar":
            self._bars_since_last_rebalance = 0
        
        self._prev_main_bars = main_bars
        self._prev_all_bars = list_bar
        
        return self.dict_target_position, changed

    def _update_prev_close(self, all_bars: List[Any]) -> None:
        """更新 prev_close（只在交易窗口内，与原始版本保持一致）"""
        try:
            for b in all_bars:
                dt_raw = getattr(b, "datetime", None) or getattr(b, "date_time", None)
                dt_val = self._parse_datetime(dt_raw)
                # 关键修复：只在交易窗口内更新，与原始版本一致
                if dt_val and self._is_within_trading_window(dt_val):
                    s = str(getattr(b, "symbol"))
                    c = float(getattr(b, "close"))
                    if c > 0:
                        self.prev_close[s] = c
        except Exception:
            pass

    def _update_price_history(self, main_bars: List[Any]) -> None:
        """更新 price_hist（只在交易窗口内）"""
        try:
            for bar in main_bars:
                dt_raw = getattr(bar, "datetime", None) or getattr(bar, "date_time", None)
                dt_val = self._parse_datetime(dt_raw)
                if dt_val and self._is_within_trading_window(dt_val):
                    symbol = getattr(bar, "symbol")
                    close = float(getattr(bar, "close"))
                    oi = float(getattr(bar, "open_interest", 0.0))
                    self._push_hist(symbol, close, oi)
        except Exception:
            pass

    def _parse_datetime(self, raw: Any) -> datetime | None:
        """解析 datetime 字段"""
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, str) and " " in raw:
            try:
                return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None
        return None

    def _detect_position_change(self, new_target: Dict[str, int]) -> bool:
        """检测仓位是否变化（整数手数对比）"""
        try:
            last_int = {k: int(round(v)) for k, v in self.dict_target_position.items()}
            cur_int = {k: int(round(v)) for k, v in new_target.items()}
            return cur_int != last_int
        except Exception:
            return not self._dict_almost_equal(new_target, self.dict_target_position)

    def _update_entry_prices(self, new_target: Dict[str, int], trading_day: datetime, list_bar: List[Any]) -> None:
        """更新入场价格映射"""
        try:
            prev_side_map = {self._symbol_root(sym): (1 if pos > 0 else -1 if pos < 0 else 0) 
                            for sym, pos in self.dict_target_position.items()}
            new_side_map = {self._symbol_root(sym): (1 if pos > 0 else -1 if pos < 0 else 0) 
                           for sym, pos in new_target.items()}
            
            all_roots = set(prev_side_map.keys()) | set(new_side_map.keys())
            
            for r in all_roots:
                prev_s = prev_side_map.get(r, 0)
                new_s = new_side_map.get(r, 0)
                
                if prev_s != new_s:
                    self._last_change_day_map[r] = trading_day
                
                if new_s == 0:
                    self.entry_price_map.pop(r, None)
                elif prev_s != new_s or self.entry_price_map.get(r, 0.0) <= 0.0:
                    # 查找对应的主力合约价格
                    for s in new_target.keys():
                        if self._symbol_root(s) == r:
                            cp = self._get_current_price(s, list_bar)
                            if cp > 0:
                                self.entry_price_map[r] = cp
                            break
        except Exception:
            pass

    # ==================================================
    # 信号与仓位
    # ==================================================
    def generate_signals(self, main_bars: List[Any], all_bars: List[Any]) -> Tuple[List[str], List[str], Dict[str, float]]:
        """生成相对强弱信号：计算当日收益相对截面中位数的超额收益"""
        # 按品种根分组
        grouped: Dict[str, List[Any]] = {}
        for b in all_bars:
            try:
                s = str(getattr(b, "symbol"))
                r = self._symbol_root(s)
                grouped.setdefault(r, []).append(b)
            except Exception:
                continue
        
        rec: Dict[str, Dict[str, Any]] = {}
        returns: List[float] = []
        
        for r, bars in grouped.items():
            mb = self._select_main_bar(r, bars)
            if not mb:
                continue
            
            try:
                s = str(getattr(mb, "symbol"))
                c = float(getattr(mb, "close"))
                pc = self._get_pre_close(mb, s)
                
                rtn = c / pc - 1.0 if pc > 0 and c > 0 else 0.0
                returns.append(rtn)
                rec[r] = {"symbol": s, "return": rtn, "close": c}
            except Exception:
                continue
        
        # 计算截面中位数
        mkt = self._calc_median(returns)
        
        # 计算超额收益得分
        score = {r: info["return"] - mkt for r, info in rec.items()}
        self._last_scores = score
        self._main_symbol_map = {r: info["symbol"] for r, info in rec.items()}
        
        # 选择多空标的
        thr = self.signal_threshold
        scale = 10000.0
        thr_scaled = thr * scale
        
        items = list(rec.items())
        pool_mult = self.retain_pool_mult
        long_pool_size = max(self.long_count, int(round(self.long_count * pool_mult)))
        short_pool_size = max(self.short_count, int(round(self.short_count * pool_mult)))
        
        # 多头候选
        long_candidates = sorted(
            [it for it in items if score.get(it[0], 0.0) * scale > thr_scaled],
            key=lambda x: score.get(x[0], 0.0),
            reverse=True
        )[:long_pool_size]
        
        # 空头候选
        short_candidates = sorted(
            [it for it in items if score.get(it[0], 0.0) * scale < -thr_scaled],
            key=lambda x: score.get(x[0], 0.0)
        )[:short_pool_size]
        
        # 优先保留已持仓
        held_long_roots = {self._symbol_root(sym) for sym, pos in self.dict_target_position.items() if pos > 0}
        held_short_roots = {self._symbol_root(sym) for sym, pos in self.dict_target_position.items() if pos < 0}
        
        longs = self._select_with_priority(long_candidates, held_long_roots, self.long_count)
        shorts = self._select_with_priority(short_candidates, held_short_roots, self.short_count, reverse=True)
        
        long_syms = [info["symbol"] for _, info in longs]
        short_syms = [info["symbol"] for _, info in shorts]
        
        final_scores = {s: score.get(self._symbol_root(s), 0.0) for s in long_syms + short_syms}
        
        return long_syms, short_syms, final_scores

    def _select_main_bar(self, root: str, bars: List[Any]) -> Any | None:
        """选择主力合约 bar（与原始版本逻辑完全一致）"""
        # 优先使用已缓存的主力合约
        main_sym = self._main_contracts.get(root, (None, 0))[0]
        if main_sym:
            for bar in bars:
                if str(getattr(bar, "symbol")) == main_sym:
                    return bar
        
        # fallback: 选择持仓量最大且未临近到期的合约（与原始版本一致）
        best, best_oi = None, -1.0
        for bar in bars:
            try:
                s = str(getattr(bar, "symbol"))
                oi = float(getattr(bar, "open_interest", 0.0))
                tday = getattr(bar, "trading_day", None)
                if self._is_near_expire(s, tday):
                    continue
                if oi > best_oi:
                    best_oi = oi
                    best = bar
            except Exception:
                continue
        
        return best if best else (bars[-1] if bars else None)

    def _is_near_expire(self, symbol: str, trading_day: Any) -> bool:
        """检查合约是否临近到期（<5天）或进入交割月"""
        try:
            base = symbol.split(".")[0]
            digits = "".join(ch for ch in base if ch.isdigit())
            if len(digits) < 4:
                return False
            
            yy = 2000 + int(digits[0:2])
            mm = int(digits[2:4])
            last_day = calendar.monthrange(yy, mm)[1]
            
            if isinstance(trading_day, str):
                td = datetime.strptime(trading_day.split(" ")[0], "%Y-%m-%d")
            elif isinstance(trading_day, datetime):
                td = trading_day
            else:
                td = datetime.now()
            
            # 检查是否进入交割月（到期月份与当前月份相同）
            cur_ym = td.year * 100 + td.month
            exp_ym = yy * 100 + mm
            if cur_ym == exp_ym:
                return True  # 已进入交割月，需要平仓
            
            # 检查是否临近到期（<5天）
            ed = datetime(yy, mm, last_day)
            return (ed - td).days < 5
        except Exception:
            return False

    def _get_pre_close(self, bar: Any, symbol: str) -> float:
        """获取前收盘价（优先使用 bar.pre_close，否则使用 prev_close）"""
        try:
            pc = getattr(bar, "pre_close", None)
            if pc is not None and float(pc) > 0:
                return float(pc)
        except Exception:
            pass
        return self.prev_close.get(symbol, 0.0)

    def _calc_median(self, values: List[float]) -> float:
        """计算中位数"""
        if not values:
            return 0.0
        vals = sorted(values)
        n = len(vals)
        mid = n // 2
        return vals[mid] if n % 2 == 1 else (vals[mid - 1] + vals[mid]) / 2.0

    def _select_with_priority(self, candidates: List[Tuple[str, Any]], held_roots: set, 
                               target_count: int, reverse: bool = False) -> List[Tuple[str, Any]]:
        """优先保留已持仓的选择逻辑"""
        result = []
        selected = set()
        
        # 先选已持仓的
        for k, v in candidates:
            if k in held_roots:
                result.append((k, v))
                selected.add(k)
        
        # 补充新标的
        for k, v in candidates:
            if len(result) >= target_count:
                break
            if k not in selected:
                result.append((k, v))
                selected.add(k)
                
        return result[:target_count]

    def adjust_positions(self, long_syms: List[str], short_syms: List[str], 
                         list_bar: List[Any], scores: Dict[str, float]) -> Dict[str, int]:
        """生成目标仓位（单位：手；风险等权）"""
        td = self._get_trading_day(list_bar)
        scores_map = self._last_scores
        
        # 计算排名
        long_rank = sorted(scores_map.items(), key=lambda kv: kv[1], reverse=True)
        short_rank = sorted(scores_map.items(), key=lambda kv: kv[1])
        long_top5 = {r for r, _ in long_rank[:5]}
        short_top5 = {r for r, _ in short_rank[:5]}
        
        # 已持仓的品种根
        held_long_roots = {self._symbol_root(sym) for sym, pos in self.dict_target_position.items() if pos > 0}
        held_short_roots = {self._symbol_root(sym) for sym, pos in self.dict_target_position.items() if pos < 0}
        
        # 选择最终的多空品种根
        selected_long_roots = self._select_roots(held_long_roots, [r for r, _ in long_rank], 
                                                   long_top5, scores_map, self.long_count, True)
        selected_short_roots = self._select_roots(held_short_roots, [r for r, _ in short_rank], 
                                                    short_top5, scores_map, self.short_count, False)
        
        # 获取可用资金
        available = self._get_available_capital()
        budget_total = self.max_deal_pct * (self.fixed_budget_base if self.fixed_budget_base > 0 else available)
        capital_total = budget_total if available <= 0 else min(budget_total, available)
        
        total_signals = len(long_syms) + len(short_syms)
        if total_signals <= 0:
            return {}
        
        capital_per_signal = capital_total / total_signals
        
        # 批量获取价格和乘数
        symbol_info = self._build_symbol_info(long_syms + short_syms, list_bar)
        
        # 计算目标仓位
        new_target: Dict[str, int] = {}
        
        for s in long_syms:
            lots = self._calc_lots(s, capital_per_signal, symbol_info, available, 1)
            if lots > 0:
                new_target[s] = lots
        
        for s in short_syms:
            lots = self._calc_lots(s, capital_per_signal, symbol_info, available, -1)
            if lots > 0:
                new_target[s] = -lots
        
        # 止损检查（如启用）
        if self.enable_stop_loss:
            try:
                stop_targets = self._check_stop_loss(list_bar)
                for sym, tgt in stop_targets.items():
                    new_target[sym] = int(tgt)
            except Exception:
                pass
        
        return new_target

    def _select_roots(self, held_roots: set, rank_roots: List[str], top5: set,
                      scores_map: Dict[str, float], target_count: int, is_long: bool) -> List[str]:
        """选择品种根（考虑最小持有期）"""
        td = self._get_trading_day([]) or datetime.utcnow()
        
        def should_keep(root: str) -> bool:
            last = self._last_change_day_map.get(root)
            age = 0 if last is None else (td - last).days
            return (root in top5) or (age < self.min_hold_days)
        
        result = [r for r in held_roots if should_keep(r)]
        
        for r in rank_roots:
            if len(result) >= target_count:
                break
            if r not in result:
                result.append(r)
        
        key_func = lambda x: scores_map.get(x, 0.0)
        return sorted(result, key=key_func, reverse=is_long)[:target_count]

    def _get_available_capital(self) -> float:
        """获取可用资金"""
        try:
            acct = self.api.get_account()
            return float(getattr(acct, "available", 0.0))
        except Exception:
            try:
                return float(self.api.get_balance())
            except Exception:
                return 1_000_000.0

    def _build_symbol_info(self, symbols: List[str], list_bar: List[Any]) -> Dict[str, Dict[str, float]]:
        """批量构建合约信息（价格、乘数）"""
        # 构建 bar 查找字典（加速）
        bar_dict = {getattr(b, "symbol"): b for b in list_bar[::-1]}
        
        info = {}
        for s in symbols:
            p = self._get_price_from_dict(s, bar_dict)
            m = self._get_cached_multiplier(s, list_bar)
            if p > 0:
                info[s] = {"price": p, "multiplier": m, "value": p * m}
        
        return info

    def _get_price_from_dict(self, symbol: str, bar_dict: Dict[str, Any]) -> float:
        """从字典中获取价格"""
        b = bar_dict.get(symbol)
        if b:
            return float(getattr(b, "close"))
        root = self._symbol_root(symbol)
        arr = self.price_hist.get(root, [])
        return float(arr[-1]) if arr else 0.0

    def _get_cached_multiplier(self, symbol: str, list_bar: List[Any]) -> float:
        """获取乘数（带缓存）"""
        # 检查缓存
        if symbol in self.multiplier_cache:
            cached = self.multiplier_cache[symbol]
            if cached > 1.0:
                return cached
        
        m_val = 1.0
        
        # 尝试从 API 获取
        try:
            m_val = float(getattr(self.api, "account_manager", object()).get_multiplier(symbol))
        except Exception:
            pass
        
        # 尝试从日乘数字典获取
        if m_val <= 1.0:
            try:
                exid = symbol.split(".")[-1] if "." in symbol else ""
                if exid not in ("SH", "SZ"):
                    td = getattr(list_bar[0], "trading_day", None) if list_bar else None
                    if isinstance(td, str):
                        td = td.split(" ")[0]
                    dmult = getattr(self.api, "dict_daily_multiplier", {})
                    if td and td in dmult and symbol in dmult[td]:
                        m_val = float(dmult[td][symbol])
            except Exception:
                pass
        
        # 尝试从合约信息获取
        if m_val <= 1.0:
            try:
                m2 = self._get_multiplier(symbol)
                if m2 and m2 > 1.0:
                    m_val = m2
            except Exception:
                pass
        
        # 验证并缓存
        if m_val > 1.0:
            self.multiplier_cache[symbol] = m_val
        
        return m_val

    def _calc_lots(self, symbol: str, capital: float, symbol_info: Dict[str, Dict[str, float]], 
                   available: float, direction: int) -> int:
        """计算手数"""
        if symbol not in symbol_info:
            return 0
        
        p = symbol_info[symbol]["price"]
        m = symbol_info[symbol]["multiplier"]
        
        margin_rate = self._get_margin_rate(symbol) if self.sizing_mode.lower() == "margin" else 1.0
        denom = max(p * m * margin_rate, 1e-6)
        
        if denom < 1.0:
            return 0
        
        lots = int(capital / denom)
        
        if lots <= 0:
            need1 = p * m * margin_rate
            if available >= need1:
                lots = 1
        
        return lots

    # ==================================================
    # 平台适配入口
    # ==================================================
    def calculate_target_position(self, list_bar: List[Any]) -> Tuple[Dict[str, int], bool]:
        """在线平台回调：返回目标仓位与是否变化标志"""
        try:
            return self.on_cycle(list_bar)
        except Exception as e:
            logging.error("[test_sample_strategy_lite][calc] error: %s", str(e))
            return self.dict_target_position, False

    # ==================================================
    # 工具函数（优化版）
    # ==================================================
    def _push_hist(self, symbol: str, close: float, oi: float) -> None:
        """推送历史数据（限制长度）"""
        root = self._symbol_root(symbol)
        
        if root not in self.price_hist:
            self.price_hist[root] = []
            self.oi_hist[root] = []
        
        self.price_hist[root].append(close)
        self.oi_hist[root].append(oi)
        
        # 限制长度
        if len(self.price_hist[root]) > self.factor_window:
            self.price_hist[root].pop(0)
            self.oi_hist[root].pop(0)

    def _get_trading_day(self, list_bar: List[Any]) -> datetime:
        """获取交易日"""
        # 按优先级尝试不同来源
        if list_bar:
            b = list_bar[-1]
            
            # 1. trading_day 字段
            tday = getattr(b, "trading_day", None)
            if isinstance(tday, datetime):
                return datetime(tday.year, tday.month, tday.day)
            if isinstance(tday, str) and tday:
                s = tday.split(" ")[0]
                if len(s) == 10 and "-" in s:
                    y, m, d = map(int, s.split("-"))
                    return datetime(y, m, d)
                if len(s) == 8 and s.isdigit():
                    return datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]))
            
            # 2. date_time 字段
            dt_str = str(getattr(b, "date_time", "")).split(" ")[0]
            if dt_str:
                y, m, d = map(int, dt_str.split("-"))
                return datetime(y, m, d)
            
            # 3. datetime 字段
            dt = getattr(b, "datetime", None)
            if isinstance(dt, datetime):
                return datetime(dt.year, dt.month, dt.day)
        
        # 4. API 当前交易日
        cur = getattr(self.api, "cur_trading_day", None)
        if isinstance(cur, str) and cur:
            s = cur.split(" ")[0]
            if len(s) == 10 and "-" in s:
                y, m, d = map(int, s.split("-"))
                return datetime(y, m, d)
            if len(s) == 8 and s.isdigit():
                return datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]))
        
        return datetime.utcnow()

    def _need_rebalance(self, trading_day: datetime, list_bar: List[Any]) -> bool:
        """检查是否需要调仓（与原始代码逻辑一致）"""
        unit = self.rebalance_unit.lower()
        
        # 检查 last_rebalance_day 是否异常（未来日期）
        if self.last_rebalance_day and self.last_rebalance_day > trading_day:
            self.last_rebalance_day = None
        
        # bar 模式
        if unit == "bar":
            dt = self._get_bar_dt(list_bar)
            if dt and not self._is_within_trading_window(dt):
                return False
            self._bars_since_last_rebalance += 1
            return self._bars_since_last_rebalance >= self.rebalance_interval
        
        # 使用交易日序列进行周/月/日门控
        tds = self._trading_days_list
        cur_str = trading_day.strftime("%Y-%m-%d")
        
        # 标准化交易日序列
        tds_normalized: List[str] = []
        for d in tds:
            s = str(d).split(" ")[0]
            if len(s) == 10 and "-" in s:
                tds_normalized.append(s)
            elif len(s) == 8 and s.isdigit():
                tds_normalized.append(f"{s[0:4]}-{s[4:6]}-{s[6:8]}")
        
        try:
            idx = tds_normalized.index(cur_str)
        except ValueError:
            idx = -1
        
        ri_cfg = self.rebalance_interval
        ri_eff = ri_cfg
        if isinstance(tds_normalized, list) and len(tds_normalized) > 0 and len(tds_normalized) < ri_cfg:
            ri_eff = 1
        
        # 首日判断（idx == 0）
        if idx == 0:
            dt = self._get_bar_dt(list_bar)
            has_pos = any(int(round(v)) != 0 for v in self.dict_target_position.values())
            allow = (self.last_rebalance_day is None) and (not has_pos)
            if dt and not self._is_within_trading_window(dt):
                return False
            return allow
        
        # idx < 0 时的兜底逻辑
        if idx < 0:
            if self.last_rebalance_day is None:
                return True
            return (trading_day - self.last_rebalance_day).days >= ri_cfg
        
        # 周模式
        if unit == "week":
            if idx > 0:
                prev_dt = datetime.strptime(tds_normalized[idx - 1], "%Y-%m-%d")
                cur_dt = datetime.strptime(tds_normalized[idx], "%Y-%m-%d")
                return (prev_dt.isocalendar()[1] != cur_dt.isocalendar()[1] or 
                        prev_dt.isocalendar()[0] != cur_dt.isocalendar()[0])
            return False
        
        # 月模式
        if unit == "month":
            if idx > 0:
                prev_dt = datetime.strptime(tds_normalized[idx - 1], "%Y-%m-%d")
                cur_dt = datetime.strptime(tds_normalized[idx], "%Y-%m-%d")
                return prev_dt.month != cur_dt.month or prev_dt.year != cur_dt.year
            return False
        
        # 交易日模式（默认）
        ok = (idx % max(1, ri_eff) == 0)
        
        # 时间窗口检查
        dt = self._get_bar_dt(list_bar)
        if dt and not self._is_within_trading_window(dt):
            return False
        
        return ok

    def _get_bar_dt(self, list_bar: List[Any]) -> datetime | None:
        """获取 bar 时间"""
        if not list_bar:
            return None
        b = list_bar[-1]
        raw = getattr(b, "datetime", None) or getattr(b, "date_time", None)
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, str) and " " in raw:
            try:
                return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None
        return None

    def _is_within_trading_window(self, dt: datetime) -> bool:
        """检查是否在交易窗口内"""
        try:
            t = dt.time()
            day_session = self.open_allowed_after <= t <= self.cancel_cutoff_time
            
            try:
                night_enabled = bool(getattr(self.api, "night_trade", False))
            except Exception:
                night_enabled = False
            
            night_session = night_enabled and (t >= time(21, 0) or t <= time(2, 59))
            return day_session or night_session
        except Exception:
            return True

    def _symbol_root(self, symbol: str) -> str:
        """获取品种根（带缓存）"""
        # 检查缓存
        if symbol in self._symbol_root_cache:
            return self._symbol_root_cache[symbol]
        
        try:
            s = symbol.decode() if hasattr(symbol, "decode") else str(symbol)
        except Exception:
            s = str(symbol)
        
        try:
            base, exch = s.split(".", 1)
        except Exception:
            try:
                if hasattr(self.api, "get_symbol_root"):
                    result = self.api.get_symbol_root(s)
                    self._symbol_root_cache[symbol] = result
                    return result
            except Exception:
                pass
            self._symbol_root_cache[symbol] = s
            return s
        
        # 提取字母前缀
        prefix = ""
        for ch in base:
            if ch.isalpha():
                prefix += ch
            else:
                break
        
        if not prefix:
            prefix = base
        
        result = f"{prefix.upper()}.{exch.upper()}"
        
        # 尝试 API 获取
        try:
            if hasattr(self.api, "get_symbol_root"):
                api_root = str(self.api.get_symbol_root(s))
                parts = api_root.split(".")
                if len(parts) == 2 and parts[0].isalpha():
                    result = f"{parts[0].upper()}.{parts[1].upper()}"
        except Exception:
            pass
        
        self._symbol_root_cache[symbol] = result
        return result

    def _get_multiplier(self, symbol: str) -> float:
        """获取合约乘数"""
        try:
            contract = self.api.get_contract_info(symbol)
            return float(getattr(contract, "multiplier", 1.0))
        except Exception:
            return 1.0

    def _get_margin_rate(self, symbol: str) -> float:
        """获取保证金率（带缓存）"""
        if symbol in self._margin_rate_cache:
            return self._margin_rate_cache[symbol]
        
        root = self._symbol_root(symbol)
        
        if root in self.margin_rates:
            result = float(self.margin_rates[root])
            self._margin_rate_cache[symbol] = result
            return result
        
        try:
            mr_platform = float(getattr(self.api, "account_manager", object()).margin_rate)
            if mr_platform is not None:
                self._margin_rate_cache[symbol] = float(mr_platform)
                return float(mr_platform)
        except Exception:
            pass
        
        try:
            contract = self.api.get_contract_info(symbol)
            result = float(getattr(contract, "margin_rate", 0.2))
            self._margin_rate_cache[symbol] = result
            return result
        except Exception:
            self._margin_rate_cache[symbol] = 0.2
            return 0.2

    def _is_main_contract(self, symbol: str) -> bool:
        """判断是否为主力合约"""
        try:
            product_id = self._symbol_root(symbol)
            return symbol == self._main_contracts.get(product_id, (symbol, 0))[0]
        except Exception:
            return True

    def _update_main_contracts(self, trading_day: datetime) -> None:
        """更新主力合约映射（优化版）"""
        try:
            new_mains: Dict[str, Tuple[str, float]] = {}
            
            # 尝试从 API 获取
            if bool(getattr(self.api, "remote_enabled", True)):
                try:
                    df_daily = self.api.get_daily_symbol()
                    if df_daily is not None and hasattr(df_daily, "itertuples"):
                        for row in df_daily.itertuples():
                            try:
                                symbol = getattr(row, "symbol")
                                product_id = self._symbol_root(symbol)
                                oi = float(getattr(row, "open_interest", 0.0))
                                
                                # 检查到期日
                                expire_date = getattr(row, "expire_date", None)
                                days_to_expire = self._calc_days_to_expire(expire_date, symbol, trading_day)
                                
                                # 检查是否进入交割月
                                if days_to_expire is not None and days_to_expire < 5:
                                    continue
                                
                                if product_id not in new_mains or oi > new_mains[product_id][1]:
                                    new_mains[product_id] = (symbol, oi)
                            except Exception:
                                continue
                except Exception:
                    pass
            
            # 本地数据兜底
            if not new_mains and self._roots_subscribed:
                new_mains = self._update_main_from_local(trading_day)
            
            self._main_contracts = new_mains
            
            # 清理无效的入场价格
            valid_roots = set(new_mains.keys())
            for k in list(self.entry_price_map.keys()):
                if k not in valid_roots:
                    del self.entry_price_map[k]
        except Exception as e:
            logging.warning("[test_sample_strategy_lite] update main contracts failed: %s", e)

    def _calc_days_to_expire(self, expire_date: Any, symbol: str, trading_day: datetime) -> int | None:
        """计算到期天数（返回 None 表示已进入交割月或无法解析）"""
        try:
            # 从合约代码解析到期年月
            base = str(symbol).split(".")[0]
            digits = "".join(ch for ch in base if ch.isdigit())
            if len(digits) < 4:
                return None
            
            yy = 2000 + int(digits[0:2])
            mm = int(digits[2:4])
            last_day = calendar.monthrange(yy, mm)[1]
            
            # 检查是否进入交割月（当前月份 == 到期月份）
            cur_ym = trading_day.year * 100 + trading_day.month
            exp_ym = yy * 100 + mm
            if cur_ym == exp_ym:
                return 0  # 已进入交割月，需要立即平仓
            
            # 从 expire_date 计算到期天数
            if expire_date is not None:
                if isinstance(expire_date, datetime):
                    return (expire_date - trading_day).days
                if isinstance(expire_date, str):
                    s = expire_date.strip()
                    if len(s) == 10 and "-" in s:
                        y, m, d = map(int, s.split("-"))
                        ed = datetime(y, m, d)
                        return (ed - trading_day).days
                    elif len(s) == 8 and s.isdigit():
                        ed = datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                        return (ed - trading_day).days
            
            # 从合约代码计算到期天数
            ed = datetime(yy, mm, last_day)
            return (ed - trading_day).days
        except Exception:
            return None

    def _update_main_from_local(self, trading_day: datetime) -> Dict[str, Tuple[str, float]]:
        """从本地数据更新主力合约"""
        new_mains: Dict[str, Tuple[str, float]] = {}
        
        try:
            import pandas as pd
            tday_str = trading_day.strftime("%Y-%m-%d")
            data_type = str(getattr(self.api, "data_type", "1m"))
            scan_type = data_type if data_type.lower() != "5m" else "1m"
            cwd = getattr(self.api, "cwd", os.getcwd())
            
            for r in self._roots_subscribed:
                ex = r.split(".")[-1] if "." in r else ""
                
                for dirp in [
                    os.path.join(cwd, "data", scan_type, ex, r, tday_str),
                    os.path.join(cwd, "data", "data", scan_type, ex, r, tday_str)
                ]:
                    if not os.path.exists(dirp):
                        continue
                    
                    best_sym, best_oi = None, -1.0
                    
                    for name in os.listdir(dirp):
                        fp = os.path.join(dirp, name)
                        if not os.path.isfile(fp):
                            continue
                        
                        try:
                            df = pd.read_csv(fp)
                            if df is None or df.empty or "open_interest" not in df.columns:
                                continue
                            val = float(df["open_interest"].iloc[-1])
                            if val > best_oi:
                                best_oi = val
                                best_sym = name
                        except Exception:
                            continue
                    
                    if best_sym:
                        segs = str(best_sym).split(".")
                        sym_clean = ".".join(segs[:-1]) if len(segs) >= 3 else str(best_sym)
                        
                        # 检查到期日
                        days_to_expire = self._calc_days_to_expire(None, sym_clean, trading_day)
                        
                        if days_to_expire is None or days_to_expire >= 5:
                            if r not in new_mains or best_oi > new_mains[r][1]:
                                new_mains[r] = (sym_clean, best_oi)
        except Exception:
            pass
        
        return new_mains

    def _apply_adj_on_rollover(self, main_bars: List[Any]) -> None:
        """主力切换时复权调整"""
        try:
            # 构建价格字典
            sym_close = {getattr(b, "symbol"): float(getattr(b, "close")) for b in main_bars}
            
            for b in main_bars:
                try:
                    cur_sym = getattr(b, "symbol")
                    root = self._symbol_root(cur_sym)
                    cur_main = self._main_contracts.get(root, (cur_sym, 0))[0]
                    prev_main = self.prev_main_symbol.get(root)
                    
                    if prev_main is None:
                        self.prev_main_symbol[root] = cur_main
                        continue
                    
                    if prev_main != cur_main:
                        arr = self.price_hist.get(root, [])
                        old_px = float(arr[-1]) if arr else 0.0
                        new_px = float(sym_close.get(cur_main, 0.0))
                        
                        if old_px > 0 and new_px > 0:
                            ratio = old_px / new_px
                            self.price_hist[root] = [px * ratio for px in arr]
                            self.root_adj_factor[root] = self.root_adj_factor.get(root, 1.0) * ratio
                            
                            # 调整入场价格
                            ep = self.entry_price_map.get(root, 0.0)
                            if ep > 0:
                                self.entry_price_map[root] = ep * ratio
                        
                        self.prev_main_symbol[root] = cur_main
                except Exception:
                    continue
        except Exception:
            pass

    def _get_current_price(self, symbol: str, list_bar: List[Any]) -> float:
        """获取当前价格"""
        for b in reversed(list_bar):
            if getattr(b, "symbol", "") == symbol:
                return float(getattr(b, "close"))
        
        root = self._symbol_root(symbol)
        arr = self.price_hist.get(root, [])
        return float(arr[-1]) if arr else 0.0

    def _get_entry_price(self, symbol: str) -> float:
        """获取入场价格"""
        try:
            root = self._symbol_root(symbol)
            return float(self.entry_price_map.get(root, 0.0))
        except Exception:
            return 0.0

    def _set_entry_price(self, symbol: str, price: float) -> None:
        """设置入场价格"""
        try:
            root = self._symbol_root(symbol)
            self.entry_price_map[root] = float(price)
        except Exception:
            pass

    def _check_stop_loss(self, list_bar: List[Any]) -> Dict[str, int]:
        """检查止损"""
        res: Dict[str, int] = {}
        try:
            for symbol, pos in self.dict_target_position.items():
                if pos == 0:
                    continue
                ep = self._get_entry_price(symbol)
                cp = self._get_current_price(symbol, list_bar)
                if ep <= 0 or cp <= 0:
                    continue
                pct = cp / ep - 1.0
                if pos > 0 and pct < -self.stop_loss_pct:
                    res[symbol] = 0
                elif pos < 0 and (1.0 - cp / ep) < -self.stop_loss_pct:
                    res[symbol] = 0
        except Exception:
            pass
        return res

    def _get_market_volume(self, symbol: str, list_bar: List[Any]) -> float:
        """获取市场成交量"""
        try:
            for b in reversed(list_bar):
                if getattr(b, "symbol", "") == symbol:
                    try:
                        return float(getattr(b, "volume"))
                    except Exception:
                        try:
                            return float(getattr(b, "vol"))
                        except Exception:
                            return 0.0
        except Exception:
            pass
        return 0.0

    def _dict_almost_equal(self, d1: Dict, d2: Dict, tol: float = 1e-6) -> bool:
        """字典近似相等"""
        try:
            if set(d1.keys()) != set(d2.keys()):
                return False
            return all(abs(float(d1[k]) - float(d2.get(k, 0))) < tol for k in d1.keys())
        except Exception:
            return d1 == d2

    def _dataset_cache_ready_for_period(self) -> bool:
        """检查数据集缓存是否就绪"""
        try:
            base = os.getcwd()
            data_type = str(getattr(self.api, "data_type", "1m"))
            root_dir = os.path.join(base, "dataset", "data", data_type)
            
            if not self._roots_subscribed:
                return False
            
            tds = list(getattr(self.api, "trading_date_section", []))
            if not tds:
                return False
            
            last_day = str(tds[-1]).split(" ")[0]
            expire_days = self.cache_expire_days
            expire_threshold = datetime.now() - timedelta(days=expire_days) if expire_days > 0 else None
            
            for d in tds:
                day = str(d).split(" ")[0]
                for r in self._roots_subscribed:
                    ex = r.split(".")[-1] if "." in r else ""
                    dirp = os.path.join(root_dir, ex, r, day)
                    
                    if not os.path.isdir(dirp):
                        return False
                    
                    try:
                        if not any(os.path.isfile(os.path.join(dirp, n)) for n in os.listdir(dirp)):
                            return False
                    except Exception:
                        return False
                    
                    if expire_threshold and day == last_day:
                        try:
                            mtimes = [datetime.fromtimestamp(os.path.getmtime(os.path.join(dirp, n)))
                                     for n in os.listdir(dirp) if os.path.isfile(os.path.join(dirp, n))]
                            if mtimes and max(mtimes) < expire_threshold:
                                return False
                        except Exception:
                            pass
            
            return True
        except Exception:
            return False

    def _data_cache_ready(self) -> bool:
        """检查数据缓存是否就绪"""
        try:
            base = os.getcwd()
            data_type = str(getattr(self.api, "data_type", "1m"))
            root_dir = os.path.join(base, "data", data_type)
            cfg_path = os.path.join(base, "test_config.json")
            
            subs: List[str] = []
            ex_map: Dict[str, str] = {}
            start_day = None
            end_day = None
            
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    js = json.load(f)
                subs = [str(x) for x in js.get("subscribe symbol", [])]
                ex_map = {str(k): str(v) for k, v in js.get("exchanges", {}).items()}
                bp = js.get("backtest_period", "")
                if isinstance(bp, str) and "~" in bp:
                    start_day = bp.split("~")[0].strip()
                    end_day = bp.split("~")[1].strip()
            except Exception:
                return False
            
            if not subs or not ex_map or not start_day:
                return False
            
            roots = [f"{s}.{ex_map.get(s, '')}".strip(".") for s in subs if s in ex_map]
            if not roots:
                return False
            
            for r in roots:
                ex = r.split(".")[-1] if "." in r else ""
                filep = os.path.join(root_dir, ex, r, start_day)
                if not os.path.exists(filep) or not os.path.isdir(filep):
                    return False
                try:
                    if not any(os.path.isfile(os.path.join(filep, name)) for name in os.listdir(filep)):
                        return False
                except Exception:
                    return False
                if end_day:
                    filep2 = os.path.join(root_dir, ex, r, end_day)
                    if not os.path.exists(filep2) or not os.path.isdir(filep2):
                        return False
                    try:
                        if not any(os.path.isfile(os.path.join(filep2, name)) for name in os.listdir(filep2)):
                            return False
                    except Exception:
                        return False
            return True
        except Exception:
            return False

    def _dataset_days(self, ds_base: str, data_type: str, roots: List[str], begin_day: str, end_day: str) -> List[str]:
        """获取数据集天数列表"""
        try:
            days = set()
            scan_type = data_type if str(data_type).lower() != "5m" else "1m"
            for r in roots:
                ex = r.split(".")[-1] if "." in r else ""
                base1 = os.path.join(ds_base, "data", scan_type, ex, r)
                base2 = os.path.join(ds_base, "data", "data", scan_type, ex, r)
                for base in (base1, base2):
                    if not os.path.exists(base) or not os.path.isdir(base):
                        continue
                    for name in os.listdir(base):
                        path = os.path.join(base, name)
                        if os.path.isdir(path):
                            day = str(name).split(" ")[0]
                            if begin_day <= day <= end_day:
                                days.add(day)
            return sorted(list(days))
        except Exception:
            return []

    def _get_contract_month(self, symbol: str) -> int:
        """获取合约月份"""
        try:
            base = symbol.split('.')[0]
            digits = "".join(ch for ch in base if ch.isdigit())
            if len(digits) >= 3:
                yy = int(digits[0:2])
                mm = int(digits[2:4]) if len(digits) >= 4 else int(digits[2:3])
                year = 2000 + yy
                return year * 100 + mm
        except Exception:
            pass
        return 0

    def _winsorize(self, d: Dict[str, float], limits: Tuple[float, float] = (0.05, 0.05)) -> Dict[str, float]:
        """缩尾处理"""
        if not d:
            return {}
        vals = sorted(d.values())
        n = len(vals)
        lower_idx = int(n * limits[0])
        upper_idx = int(n * (1 - limits[1]))
        lower_val = vals[max(0, lower_idx)]
        upper_val = vals[min(n - 1, upper_idx)]
        return {k: min(max(v, lower_val), upper_val) for k, v in d.items()}