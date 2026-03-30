from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Dict, List, Tuple, Any
import logging
import os
import json


class test_sample_strategy:
    """
    多因子截面调仓策略（与离线适配器逻辑保持一致）
    - 因子：相对强弱（超额收益，主力近月相对截面中位数）
    - 回调接口：init(api), calculate_target_position(list_bar)
    - 内部流程：on_init → on_cycle → generate_signals → adjust_positions
    - 关键一致性点：
      1) 主力选择：剔除距离到期<5天的合约，持仓量最大者为主力
      2) 调仓门控：首日不调、时间窗口、交易日/周/月/bar 间隔
      3) 仓位换算：风险等权，按 available × max_deal_pct / (price × multiplier × margin_rate)
      4) 比较变化：以整数手数对比，避免浮点误差
    """

    def __init__(self) -> None:
        # ===== 可配置参数（默认值，可由 JSON 覆盖） =====
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

        # 因子权重（当前策略仅用超额收益打分，权重占位以兼容配置）
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

    # ==================================================
    # 平台初始化
    # ==================================================
    def init(self, api, **kwargs) -> int:
        """
        策略初始化：加载 JSON 参数、订阅根合约、配置数据集缓存策略
        """
        self.api = api
        # 1) 从 kwargs 注入
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        # 2) 从策略 JSON 注入（strategy/config/<user>/test_sample_strategy.json）
        try:
            user = getattr(api, "user_name", "test")
            cwd = os.getcwd()
            cfg_path = os.path.join(cwd, "strategy", "config", user, "test_sample_strategy.json")
            if os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                params = cfg.get("params", {})
                def _set(name, conv=None):
                    val = params.get(name, cfg.get(name))
                    if val is None:
                        return
                    setattr(self, name, conv(val) if conv else val)
                _set("rebalance_interval", int)
                _set("rebalance_unit", str)
                _set("factor_window", int)
                _set("long_count", int)
                _set("short_count", int)
                _set("max_deal_pct", float)
                _set("momentum_weight", float)
                _set("term_structure_weight", float)
                _set("oi_weight", float)
                _set("enable_trend_filter", bool)
                _set("trend_filter_window", int)
                _set("signal_threshold", float)
                costs = params.get("costs", cfg.get("costs", {})) or {}
                try:
                    cp = costs.get("commission_per_lot", None)
                    sp = costs.get("slippage_per_lot", None)
                    ip = costs.get("impact_cost_pct", None)
                    if cp is not None:
                        self.commission_per_lot = float(cp)
                    if sp is not None:
                        self.slippage_per_lot = float(sp)
                    if ip is not None:
                        self.impact_cost_pct = float(ip)
                except Exception:
                    pass
                try:
                    mr = cfg.get("margin_rates", {})
                    if isinstance(mr, dict):
                        self.margin_rates = {str(k): float(v) for k, v in mr.items()}
                except Exception:
                    self.margin_rates = {}
                _set("enable_strategy_cost", bool)
                _set("enable_stop_loss", bool)
                _set("stop_loss_pct", float)
                _set("retain_pool_mult", float)
                _set("enable_market_volume_check", bool)
                _set("market_volume_cap_pct", float)
                _set("min_hold_days", int)
                _set("sizing_mode", str)
                # 时间字符串转 time
                def _to_time(s: str) -> time:
                    hh, mm = s.split(":")[0:2]
                    return time(int(hh), int(mm))
                oa = params.get("open_allowed_after", cfg.get("open_allowed_after"))
                ct = params.get("cancel_cutoff_time", cfg.get("cancel_cutoff_time"))
                if isinstance(oa, str):
                    self.open_allowed_after = _to_time(oa)
                if isinstance(ct, str):
                    self.cancel_cutoff_time = _to_time(ct)
        except Exception as e:
            logging.warning("[test_sample_strategy][init] load json params fail: %s", str(e))
        try:
            base_bal = 0.0
            try:
                base_bal = float(getattr(getattr(self.api, "account_manager", object()), "init_balance", 0.0))
            except Exception:
                base_bal = 0.0
            if base_bal <= 0:
                try:
                    base_bal = float(getattr(self.api, "get_balance")())
                except Exception:
                    base_bal = 0.0
            self.fixed_budget_base = float(base_bal if base_bal and base_bal > 0 else 0.0)
            logging.info("[test_sample_strategy][init] fixed_budget_base=%.2f sizing_mode=%s", float(self.fixed_budget_base), str(getattr(self, "sizing_mode", "nominal")))
        except Exception:
            pass

        # 3) 统一订阅为 test_config.json 中的全部品种根，并根据数据集就绪情况决定是否下载缺失数据
        try:
            base = os.getcwd()
            cfg_main = os.path.join(base, "test_config.json")
            if os.path.exists(cfg_main):
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
                roots = [f"{s}.{ex_map.get(s, '')}".strip(".") for s in subs if s in ex_map]
                begin_day = None
                end_day = None
                try:
                    bp = js_main.get("backtest_period", "")
                    if isinstance(bp, str) and "~" in bp:
                        begin_day = bp.split("~")[0].strip()
                        end_day = bp.split("~")[1].strip()
                except Exception:
                    begin_day = None
                    end_day = None
                if roots:
                    try:
                        self.api.subscribe_symbol_root(roots)
                    except Exception:
                        pass
                    try:
                        setattr(self.api, "list_sub_symbol_root", roots)
                    except Exception:
                        pass
                ds_base = os.path.join(base, "dataset")
                try:
                    remote_ok = bool(getattr(self.api, "remote_enabled", True))
                except Exception:
                    remote_ok = True
                if not remote_ok:
                    ready = False
                    try:
                        ready = self._dataset_cache_ready_for_period()
                    except Exception:
                        ready = False
                    if ready:
                        try:
                            setattr(self.api, "cwd", ds_base)
                            raw = getattr(self.api, "_api", None)
                            if raw is not None:
                                setattr(raw, "cwd", ds_base)
                        except Exception:
                            pass
                        try:
                            setattr(self.api, "dataset_allow_download", False)
                            if hasattr(getattr(self.api, "_api", None), "dataset_allow_download"):
                                setattr(self.api._api, "dataset_allow_download", False)
                        except Exception:
                            pass
                        try:
                            if begin_day and end_day and roots:
                                tds = self._dataset_days(ds_base, str(getattr(self.api, "data_type", "1m")), roots, begin_day, end_day)
                                if isinstance(tds, list) and len(tds) > 0:
                                    setattr(self.api, "trading_date_section", tds)
                                    raw = getattr(self.api, "_api", None)
                                    if raw is not None:
                                        setattr(raw, "trading_date_section", tds)
                                    try:
                                        logging.info("[test_sample_strategy][init] trading_days=%d [%s ... %s]", len(tds), tds[0] if tds else "", tds[-1] if tds else "")
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                    else:
                        try:
                            setattr(self.api, "cwd", ds_base)
                            raw = getattr(self.api, "_api", None)
                            if raw is not None:
                                setattr(raw, "cwd", ds_base)
                        except Exception:
                            pass
                        try:
                            setattr(self.api, "dataset_allow_download", True)
                            if hasattr(getattr(self.api, "_api", None), "dataset_allow_download"):
                                setattr(self.api._api, "dataset_allow_download", True)
                        except Exception:
                            pass
                        try:
                            cur_tds = list(getattr(self.api, "trading_date_section", []))
                            if isinstance(cur_tds, list) and len(cur_tds) > 0:
                                try:
                                    logging.info("[test_sample_strategy][init] keep remote trading_days=%d [%s ... %s]", len(cur_tds), cur_tds[0] if cur_tds else "", cur_tds[-1] if cur_tds else "")
                                except Exception:
                                    pass
                            else:
                                if begin_day and end_day:
                                    # 仅在远端交易日不可用时，按回测区间生成基础日序列
                                    try:
                                        from datetime import datetime, timedelta
                                        b = datetime.strptime(begin_day, "%Y-%m-%d")
                                        e = datetime.strptime(end_day, "%Y-%m-%d")
                                        days = []
                                        cur = b
                                        while cur <= e:
                                            days.append(cur.strftime("%Y-%m-%d"))
                                            cur += timedelta(days=1)
                                        setattr(self.api, "trading_date_section", days)
                                        raw = getattr(self.api, "_api", None)
                                        if raw is not None:
                                            setattr(raw, "trading_date_section", days)
                                        logging.info("[test_sample_strategy][init] fallback trading_days=%d [%s ... %s]", len(days), days[0] if days else "", days[-1] if days else "")
                                    except Exception:
                                        pass
                        except Exception:
                            pass
        except Exception:
            pass

        try:
            self.factor_window = max(int(self.momentum_window), int(self.oi_change_window))
        except Exception:
            pass
        logging.info(
            "[test_sample_strategy][init] rebalance=%d window=%d long=%d short=%d weights=(m=%.2f,ts=%.2f,oi=%.2f)",
            self.rebalance_interval, self.factor_window, self.long_count, self.short_count,
            self.momentum_weight, self.term_structure_weight, self.oi_weight
        )
        # 离线适配器覆盖项（在线保持默认关闭，确保一致性验证时由外层控制）
        try:
            self.enable_trend_filter = False
        except Exception:
            pass
        try:
            self.signal_threshold = 0.0
        except Exception:
            pass
        try:
            self.enable_strategy_cost = False
        except Exception:
            pass
        try:
            self.enable_stop_loss = False
        except Exception:
            pass
        try:
            self.enable_market_volume_check = False
        except Exception:
            pass
        try:
            logging.info("[test_sample_strategy][init] overrides: trend_filter=%s signal_threshold=%.4f", str(self.enable_trend_filter), float(self.signal_threshold))
        except Exception:
            pass
        self.on_init()
        return 0

    # ==================================================
    # 生命周期方法（内部使用）
    # ==================================================
    def on_init(self) -> None:
        self.dict_target_position.clear()
        self.price_hist.clear()
        self.oi_hist.clear()
        self.last_rebalance_day = None
        try:
            remote_ok = bool(getattr(self.api, "remote_enabled", True))
            use_ds = False
            try:
                use_ds = (not remote_ok) and self._dataset_cache_ready_for_period()
            except Exception:
                use_ds = False
            if use_ds:
                ds_base = os.path.join(os.getcwd(), "dataset")
                if os.path.exists(ds_base):
                    setattr(self.api, "cwd", ds_base)
                    try:
                        raw = getattr(self.api, "_api", None)
                        if raw is not None:
                            setattr(raw, "cwd", ds_base)
                    except Exception:
                        pass
                    logging.info(f"[test_sample_strategy][on_init] using dataset cwd={ds_base}")
        except Exception:
            pass
        try:
            self.main_contracts: Dict[str, Tuple[str, float]] = {}
            df_daily = None
            try:
                if bool(getattr(self.api, "remote_enabled", True)):
                    df_daily = getattr(self.api, "get_daily_symbol")()
            except Exception:
                df_daily = None
            if df_daily is not None:
                for row in df_daily.itertuples():
                    symbol = getattr(row, "symbol")
                    product_id = self._symbol_root(symbol)
                    oi = float(getattr(row, "open_interest", 0.0))
                    if product_id not in self.main_contracts or oi > self.main_contracts[product_id][1]:
                        self.main_contracts[product_id] = (symbol, oi)
            logging.info(f"[test_sample_strategy][on_init] main map size={len(getattr(self, 'main_contracts', {}))}")
        except Exception:
            logging.info("[test_sample_strategy][on_init] main map unavailable")
        try:
            cache_ready = self._data_cache_ready()
            logging.info(f"[test_sample_strategy][on_init] local data cache ready={cache_ready}")
        except Exception:
            pass
        logging.info("[test_sample_strategy][on_init] ready")
        return

    def on_bar(self, bar: Any) -> None:
        """
        单根 bar 处理（仅维护主力的价格与持仓量历史）
        """
        try:
            symbol = getattr(bar, "symbol")
            if not self._is_main_contract(symbol):
                return
            close = float(getattr(bar, "close"))
            oi = float(getattr(bar, "open_interest", 0.0))
            self._push_hist(symbol, close, oi)
        except Exception:
            pass
        return

    def on_cycle(self, list_bar: List[Any]) -> Tuple[Dict[str, int], bool]:
        """
        截面周期调仓入口：门控 → 信号 → 目标仓位
        """
        import logging
        try:
            logging.info("[DIAG_ON_CYCLE] ENTRY len_list_bar=%d", 
                     len(list_bar) if list_bar else 0)
            logging.info("[DIAG_ON_CYCLE] list_bar=%s", str(list_bar))
        except Exception as e:
            logging.error("[DIAG_ON_CYCLE] ENTRY logging failed: %s", str(e))

        if not list_bar:
            logging.info("[test_sample_strategy][cycle] empty bars, skip")
            return self.dict_target_position, False
        trading_day = self._get_trading_day(list_bar)
        try:
            if getattr(self, "_last_main_update_day", None) != trading_day:
                self._update_main_contracts(trading_day)
                self._last_main_update_day = trading_day
                logging.info("[test_sample_strategy][cycle] main map refreshed for %s size=%d", trading_day.strftime("%Y-%m-%d"), int(len(getattr(self, "main_contracts", {}))))
            else:
                logging.info("[test_sample_strategy][cycle] main map not updated for %s", trading_day.strftime("%Y-%m-%d"))
        except Exception:
            try:
                self._update_main_contracts(trading_day)
                logging.info("[test_sample_strategy][cycle] main map refreshed (fallback) for %s size=%d", trading_day.strftime("%Y-%m-%d"), int(len(getattr(self, "main_contracts", {}))))
            except Exception:
                logging.info("[test_sample_strategy][cycle] main map refresh failed for %s", trading_day.strftime("%Y-%m-%d"))
                pass
        main_bars = [b for b in list_bar if self._is_main_contract(getattr(b, "symbol", ""))]
        if not main_bars:
            logging.info("[test_sample_strategy][cycle] no main bars, skip")
            return self.dict_target_position, False
        try:
            mb_syms = [str(getattr(b, "symbol")) for b in main_bars]
            logging.info("[test_sample_strategy][cycle] day=%s bars=%d main_bars=%d mains=%s", trading_day.strftime("%Y-%m-%d"), len(list_bar), len(main_bars), ",".join(mb_syms))
        except Exception:
            pass

        try:
            logging.info("[test_sample_strategy][cycle] apply adj on rollover")
            self._apply_adj_on_rollover(main_bars)
        except Exception:
            pass

        if not self._need_rebalance(trading_day, main_bars):
            try:
                for bar in main_bars:
                    dt_raw = getattr(bar, "datetime", None) or getattr(bar, "date_time", None)
                    dt_val = None
                    if isinstance(dt_raw, datetime):
                        dt_val = dt_raw
                    elif isinstance(dt_raw, str) and " " in dt_raw:
                        try:
                            dt_val = datetime.strptime(dt_raw, "%Y-%m-%d %H:%M:%S")
                        except Exception:
                            dt_val = None
                    if dt_val is not None and self._is_within_trading_window(dt_val):
                        symbol = getattr(bar, "symbol")
                        close = float(getattr(bar, "close"))
                        oi = float(getattr(bar, "open_interest", 0.0))
                        self._push_hist(symbol, close, oi)
                try:
                    for b in list_bar:
                        dt_raw2 = getattr(b, "datetime", None) or getattr(b, "date_time", None)
                        dt_val2 = None
                        if isinstance(dt_raw2, datetime):
                            dt_val2 = dt_raw2
                        elif isinstance(dt_raw2, str) and " " in dt_raw2:
                            try:
                                dt_val2 = datetime.strptime(dt_raw2, "%Y-%m-%d %H:%M:%S")
                            except Exception:
                                dt_val2 = None
                        if dt_val2 is not None and self._is_within_trading_window(dt_val2):
                            s = str(getattr(b, "symbol"))
                            c = float(getattr(b, "close"))
                            if c > 0:
                                self.prev_close[s] = c
                except Exception:
                    pass
                self._prev_main_bars = main_bars
                self._prev_all_bars = list_bar
            except Exception:
                pass
            return self.dict_target_position, False
        prev_main = getattr(self, "_prev_main_bars", None)
        prev_all = getattr(self, "_prev_all_bars", None)
        long_syms, short_syms, scores = self.generate_signals(main_bars, list_bar)
        try:
            logging.info("[test_sample_strategy][signals] long_n=%d short_n=%d long=%s short=%s", len(long_syms), len(short_syms), ",".join(long_syms), ",".join(short_syms))
        except Exception:
            pass
        new_target = self.adjust_positions(long_syms, short_syms, main_bars, scores)
        # 与离线版本一致：以整数手数对比是否变化
        try:
            last_int = {str(k): int(round(v)) for k, v in self.dict_target_position.items()}
            cur_int = {str(k): int(round(v)) for k, v in new_target.items()}
            changed = (cur_int != last_int)
            logging.info("[test_sample_strategy][signals] changed=%s last=%s cur=%s", str(changed), str(last_int), str(cur_int))
        except Exception:
            changed = not self._dict_almost_equal(new_target, self.dict_target_position)
            logging.info("[test_sample_strategy][signals][_almost_equal] changed=%s last=%s cur=%s", str(changed), str(last_int), str(cur_int))
        if changed:
            try:
                prev_side_map: Dict[str, int] = {}
                for sym, pos in self.dict_target_position.items():
                    r = self._symbol_root(sym)
                    sgn = 1 if float(pos) > 0 else (-1 if float(pos) < 0 else 0)
                    prev_side_map[r] = sgn
                new_side_map: Dict[str, int] = {}
                for sym, pos in new_target.items():
                    r = self._symbol_root(sym)
                    sgn = 1 if float(pos) > 0 else (-1 if float(pos) < 0 else 0)
                    new_side_map[r] = sgn
                for r in set(list(prev_side_map.keys()) + list(new_side_map.keys())):
                    if prev_side_map.get(r, 0) != new_side_map.get(r, 0):
                        self._last_change_day_map[r] = trading_day
                try:
                    for r in set(list(prev_side_map.keys()) + list(new_side_map.keys())):
                        prev_s = int(prev_side_map.get(r, 0))
                        new_s = int(new_side_map.get(r, 0))
                        if new_s == 0:
                            if r in self.entry_price_map:
                                del self.entry_price_map[r]
                                logging.info("[test_sample_strategy][entry_price_clear] root=%s prev_side=%d new_side=%d", r, prev_s, new_s)
                            continue
                        need_set = (prev_s != new_s) or (float(self.entry_price_map.get(r, 0.0)) <= 0.0)
                        if need_set and new_s != 0:
                            sym_pick = None
                            for s in new_target.keys():
                                if self._symbol_root(s) == r:
                                    sym_pick = s
                                    break
                            if sym_pick:
                                cp = self._get_current_price(sym_pick, list_bar)
                                if cp and cp > 0:
                                    self.entry_price_map[r] = float(cp)
                                    logging.info("[test_sample_strategy][entry_price_set] root=%s symbol=%s price=%.6f prev_side=%d new_side=%d", r, sym_pick, cp, prev_s, new_s)
                except Exception:
                    pass
            except Exception:
                pass
            self.dict_target_position = new_target
        # 移除当日同日门控依赖于 last_rebalance_day 的设置，改由基础层的 _acted_today 控制
        try:
            if str(getattr(self, "rebalance_unit", "trading_day")).lower() == "bar":
                self._bars_since_last_rebalance = 0
        except Exception:
            pass
        try:
            for bar in main_bars:
                dt_raw = getattr(bar, "datetime", None) or getattr(bar, "date_time", None)
                dt_val = None
                if isinstance(dt_raw, datetime):
                    dt_val = dt_raw
                elif isinstance(dt_raw, str) and " " in dt_raw:
                    try:
                        dt_val = datetime.strptime(dt_raw, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        dt_val = None
                if dt_val is not None and self._is_within_trading_window(dt_val):
                    symbol = getattr(bar, "symbol")
                    close = float(getattr(bar, "close"))
                    oi = float(getattr(bar, "open_interest", 0.0))
                    self._push_hist(symbol, close, oi)
            try:
                for b in list_bar:
                    dt_raw2 = getattr(b, "datetime", None) or getattr(b, "date_time", None)
                    dt_val2 = None
                    if isinstance(dt_raw2, datetime):
                        dt_val2 = dt_raw2
                    elif isinstance(dt_raw2, str) and " " in dt_raw2:
                        try:
                            dt_val2 = datetime.strptime(dt_raw2, "%Y-%m-%d %H:%M:%S")
                        except Exception:
                            dt_val2 = None
                    if dt_val2 is not None and self._is_within_trading_window(dt_val2):
                        s = str(getattr(b, "symbol"))
                        c = float(getattr(b, "close"))
                        if c > 0:
                            self.prev_close[s] = c
            except Exception:
                pass
            self._prev_main_bars = main_bars
            self._prev_all_bars = list_bar
        except Exception:
            pass
        try:
            import logging
            logging.info("[test_sample_strategy][cycle] day=%s changed=%s targets=%s", trading_day.strftime("%Y-%m-%d"), str(bool(changed)), str(self.dict_target_position))
            if not changed:
                logging.info("[test_sample_strategy][cycle] unchanged: last=%s new=%s", str(self.dict_target_position), str(new_target))
        except Exception:
            pass
        return self.dict_target_position, changed

    # ==================================================
    # 信号与仓位
    # ==================================================
    def _select_pool(self, candidates, held_roots, target_count, score, thr, is_long=True):
        """统一的池子选择逻辑"""
        scale = 10000.0  # 如需兼容旧逻辑保留，否则建议删除
        thr_scaled = thr * scale
        mult = float(getattr(self, "retain_pool_mult", 2.0))
        pool_size = max(target_count, int(round(target_count * mult)))
        
        # 筛选
        if is_long:
            filtered = [(k, v) for k, v in candidates if score.get(k, 0.0) * scale > thr_scaled]
            filtered.sort(key=lambda x: score.get(x[0], 0.0), reverse=True)
        else:
            filtered = [(k, v) for k, v in candidates if score.get(k, 0.0) * scale < -thr_scaled]
            filtered.sort(key=lambda x: score.get(x[0], 0.0), reverse=False)  # 最负在前
        
        filtered = filtered[:pool_size]
        
        # 优先保留已持仓
        result = []
        selected_keys = set()
        
        for k, v in filtered:
            if k in held_roots:
                result.append((k, v))
                selected_keys.add(k)
        
        # 补充新标的
        for k, v in filtered:
            if len(result) >= target_count:
                break
            if k not in selected_keys:
                result.append((k, v))
                selected_keys.add(k)
                
        return result[:target_count]

 
    def generate_signals(self, main_bars: List[Any], all_bars: List[Any]) -> Tuple[List[str], List[str], Dict[str, float]]:
        """
        生成相对强弱信号：对每个根取主力近月，计算当日收益相对截面中位数的超额收益
        """
        try:
            logging.info("[test_sample_strategy][signals][pre] main_bars=%d all_bars=%d", int(len(main_bars) if isinstance(main_bars, list) else 0), int(len(all_bars) if isinstance(all_bars, list) else 0))
        except Exception:
            pass
        rec: Dict[str, Dict[str, Any]] = {}
        returns: List[float] = []
        try:
            grouped: Dict[str, List[Any]] = {}
            for b in all_bars:
                try:
                    s = str(getattr(b, "symbol"))
                    r = self._symbol_root(s)
                    if r not in grouped:
                        grouped[r] = []
                    grouped[r].append(b)
                except Exception:
                    continue
            for r, bars in grouped.items():
                mb = None
                try:
                    main_sym = None
                    try:
                        main_sym = str(getattr(self, "main_contracts", {}).get(r, (None, 0))[0])
                    except Exception:
                        main_sym = None
                    if main_sym:
                        for bar in bars:
                            try:
                                if str(getattr(bar, "symbol")) == main_sym:
                                    mb = bar
                                    break
                            except Exception:
                                continue
                        try:
                            logging.info("[test_sample_strategy][signals][main_pick] root=%s main_sym=%s found=%s bars=%d", r, str(main_sym), str(bool(mb is not None)), int(len(bars)))
                        except Exception:
                            pass
                    if mb is None:
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
                                tday = None
                            try:
                                base = s.split(".")[0]
                                digits = "".join(ch for ch in base if ch.isdigit())
                                if len(digits) >= 4 and isinstance(tday, (str, datetime)):
                                    yy = 2000 + int(digits[0:2])
                                    mm = int(digits[2:4])
                                    cur_ym_sel = None
                                    try:
                                        if isinstance(tday, str):
                                            tdt = datetime.strptime(tday.split(" ")[0], "%Y-%m-%d")
                                        else:
                                            tdt = tday if isinstance(tday, datetime) else None
                                        if tdt is not None:
                                            cur_ym_sel = tdt.year * 100 + tdt.month
                                    except Exception:
                                        cur_ym_sel = None
                                    exp_ym_sel = yy * 100 + mm
                                    if cur_ym_sel is not None and exp_ym_sel == cur_ym_sel:
                                        continue
                                    import calendar
                                    last_day = calendar.monthrange(yy, mm)[1]
                                    if isinstance(tday, str):
                                        td = datetime.strptime(tday.split(" ")[0], "%Y-%m-%d")
                                    else:
                                        td = tday if isinstance(tday, datetime) else None
                                    if td is not None:
                                        ed = datetime(yy, mm, last_day)
                                        days_to_expire = (ed - td).days
                                        if days_to_expire < 5:
                                            continue
                            except Exception:
                                pass
                            if oi > best_oi:
                                best_oi = oi
                                best = bar
                        mb = best if best is not None else (bars[-1] if bars else None)
                        try:
                            logging.info("[test_sample_strategy][signals][fallback_pick] root=%s sym=%s oi=%.2f bars=%d", r, (str(getattr(mb, "symbol")) if mb else "None"), float(best_oi), int(len(bars)))
                        except Exception:
                            pass
                except Exception:
                    mb = bars[-1] if bars else None
                if not mb:
                    try:
                        logging.warning("[test_sample_strategy][signals][no_main_bar] root=%s bars=%d", r, int(len(bars)))
                    except Exception:
                        pass
                    continue
                try:
                    s = str(getattr(mb, "symbol"))
                    c = float(getattr(mb, "close"))
                    pc = None
                    try:
                        pc = float(getattr(mb, "pre_close"))
                    except Exception:
                        pc = None
                    if (pc is None) or (pc <= 0):
                        pc = float(self.prev_close.get(s, 0.0))
                    rtn = 0.0
                    if pc and pc > 0 and c > 0:
                        rtn = c / pc - 1.0
                    returns.append(rtn)
                    rec[r] = {"symbol": s, "return": rtn, "close": c}
                    try:
                        logging.info("[test_sample_strategy][signals][root] %s symbol=%s close=%.6f pre_close=%.6f prev_close=%.6f return=%.6f", r, s, c, float(pc if pc is not None else 0.0), float(self.prev_close.get(s, 0.0)), rtn)
                    except Exception:
                        pass
                except Exception:
                    continue
        except Exception:
            pass
        mkt = 0.0
        try:
            if returns:
                vals = sorted(returns)
                mid = len(vals) // 2
                mkt = (vals[mid] if len(vals) % 2 == 1 else (vals[mid - 1] + vals[mid]) / 2.0)
        except Exception:
            mkt = 0.0
        score: Dict[str, float] = {}
        for r, info in rec.items():
            score[r] = float(info.get("return", 0.0)) - mkt
        thr = float(getattr(self, "signal_threshold", 0.0))
        try:
            logging.info("[test_sample_strategy][signals] mkt_median=%.6f thr=%.6f roots=%d", float(mkt), float(thr), int(len(rec)))
            for t, infffo in rec.items():
                logging.info("[ccccc] score[%s]= %.6f", str(infffo), score[t])        
        except Exception:
            pass
        scale = 10000.0
        thr_scaled = float(thr) * float(scale)
        try:
            self._last_scores = dict(score)
            self._main_symbol_map = {str(k): str(v.get("symbol")) for k, v in rec.items()}
        except Exception:
            self._last_scores = dict(score)
        items = [(k, v) for k, v in rec.items()]
        long_pool_size = max(int(self.long_count), int(round(self.long_count * float(getattr(self, "retain_pool_mult", 2.0)))))
        short_pool_size = max(int(self.short_count), int(round(self.short_count * float(getattr(self, "retain_pool_mult", 2.0)))))
        long_candidates = sorted([it for it in items if (score.get(it[0], 0.0) * scale > thr_scaled)], key=lambda x: score.get(x[0], 0.0), reverse=True)[: long_pool_size]
        short_candidates = sorted([it for it in items if (score.get(it[0], 0.0) * scale < -thr_scaled)], key=lambda x: score.get(x[0], 0.0))[: short_pool_size]
        held_long_roots = set([self._symbol_root(sym) for sym, pos in self.dict_target_position.items() if float(pos) > 0])
        held_short_roots = set([self._symbol_root(sym) for sym, pos in self.dict_target_position.items() if float(pos) < 0])
        longs: List[Tuple[str, Dict[str, Any]]] = []
        shorts: List[Tuple[str, Dict[str, Any]]] = []
        for it in long_candidates:
            if it[0] in held_long_roots:
                longs.append(it)
        for it in long_candidates:
            if len(longs) >= int(self.long_count):
                break
            if it not in longs:
                longs.append(it)
        longs = longs[: int(self.long_count)]
        for it in short_candidates:
            if it[0] in held_short_roots:
                shorts.append(it)
        for it in short_candidates:
            if len(shorts) >= int(self.short_count):
                break
            if it not in shorts:
                shorts.append(it)
        shorts = shorts[: int(self.short_count)]
        long_syms: List[str] = [str(info["symbol"]) for _, info in longs]
        short_syms: List[str] = [str(info["symbol"]) for _, info in shorts]
        final_scores: Dict[str, float] = {}
        for s in long_syms + short_syms:
            final_scores[s] = score.get(self._symbol_root(s), 0.0)
        logging.debug("[test_sample_strategy][signals] long=%s short=%s", str(long_syms), str(short_syms))
        return long_syms, short_syms, final_scores
 
    def adjust_positions(self, long_syms: List[str], short_syms: List[str], list_bar: List[Any], scores: Dict[str, float]) -> Dict[str, int]:
        """
        生成目标仓位（单位：手；风险等权，与离线规则一致）
        """
        try:
            td = self._get_trading_day(list_bar)
        except Exception:
            td = datetime.utcnow()
        try:
            scores_map = dict(getattr(self, "_last_scores", {}))
        except Exception:
            scores_map = {}
        try:
            long_rank = sorted(scores_map.items(), key=lambda kv: float(kv[1]), reverse=True)
            short_rank = sorted(scores_map.items(), key=lambda kv: float(kv[1]))
            long_top5 = set([str(r) for r, _ in long_rank[:5]])
            short_top5 = set([str(r) for r, _ in short_rank[:5]])
        except Exception:
            long_top5 = set()
            short_top5 = set()
        try:
            held_long_roots = set([self._symbol_root(sym) for sym, pos in self.dict_target_position.items() if float(pos) > 0])
            held_short_roots = set([self._symbol_root(sym) for sym, pos in self.dict_target_position.items() if float(pos) < 0])
        except Exception:
            held_long_roots = set()
            held_short_roots = set()
        try:
            min_days = int(getattr(self, "min_hold_days", 5))
        except Exception:
            min_days = 5
        def _should_keep(root: str, is_long: bool) -> bool:
            try:
                last = getattr(self, "_last_change_day_map", {}).get(root, None)
                age = 0 if last is None else (td - last).days
                in_top5 = (root in (long_top5 if is_long else short_top5))
                return in_top5 or (age < min_days)
            except Exception:
                return True
        try:
            main_map = dict(getattr(self, "_main_symbol_map", {}))
        except Exception:
            main_map = {}
        try:
            long_rank_roots = [str(r) for r, _ in long_rank]
            short_rank_roots = [str(r) for r, _ in short_rank]
        except Exception:
            long_rank_roots = []
            short_rank_roots = []
        selected_long_roots: List[str] = []
        for r in held_long_roots:
            if _should_keep(r, True):
                selected_long_roots.append(r)
        for r in long_rank_roots:
            if len(selected_long_roots) >= int(self.long_count):
                break
            if r in selected_long_roots:
                continue
            selected_long_roots.append(r)
        selected_long_roots = sorted(selected_long_roots, key=lambda x: float(scores_map.get(x, 0.0)), reverse=True)[: int(self.long_count)]
        selected_short_roots: List[str] = []
        for r in held_short_roots:
            if _should_keep(r, False):
                selected_short_roots.append(r)
        for r in short_rank_roots:
            if len(selected_short_roots) >= int(self.short_count):
                break
            if r in selected_short_roots:
                continue
            selected_short_roots.append(r)
        selected_short_roots = sorted(selected_short_roots, key=lambda x: float(scores_map.get(x, 0.0)))[: int(self.short_count)]
        try:
            logging.info("[test_sample_strategy][adjust][roots] keep_long=%s keep_short=%s sel_long=%s sel_short=%s", str(sorted(list(held_long_roots))), str(sorted(list(held_short_roots))), str(selected_long_roots), str(selected_short_roots))
        except Exception:
            pass
        new_target: Dict[str, int] = {}
        available = 0.0
        try:
            acct = getattr(self.api, "get_account")()
            available = float(getattr(acct, "available", 0.0))
        except Exception:
            try:
                available = float(getattr(self.api, "get_balance")())
            except Exception:
                available = 1_000_000.0
        budget_total = max(0.0, float(getattr(self, "max_deal_pct", 0.2)) * (self.fixed_budget_base if float(getattr(self, "fixed_budget_base", 0.0)) > 0 else available))
        capital_total = budget_total if available <= 0.0 else min(budget_total, available)
        long_pool = long_syms or []
        short_pool = short_syms or []
        total_signals = len(long_pool) + len(short_pool)
        if total_signals <= 0:
            try:
                logging.info("[test_sample_strategy][adjust] no signals, skip")
            except Exception:
                pass
            return {}
        capital_per_signal = capital_total / total_signals
        try:
            logging.info("[test_sample_strategy][adjust] avail=%.2f base=%.2f mdp=%.2f sizing=%s total_signals=%d cap_total=%.2f cap_per=%.2f", float(available), float(getattr(self, "fixed_budget_base", 0.0)), float(getattr(self, "max_deal_pct", 0.2)), str(getattr(self, "sizing_mode", "nominal")), int(total_signals), float(capital_total), float(capital_per_signal))
        except Exception:
            pass
        try:
            logging.debug("[test_sample_strategy][adjust] capital_per_signal_calc cap_total=%.2f total_signals=%d cap_per=%.2f", float(capital_total), int(total_signals), float(capital_per_signal))
        except Exception:
            pass
        def last_px(sym: str) -> float:
            try:
                for b in list_bar[::-1]:
                    if getattr(b, "symbol", "") == sym:
                        return float(getattr(b, "close"))
            except Exception:
                pass
            root = self._symbol_root(sym)
            arr = self.price_hist.get(root, [])
            return float(arr[-1]) if arr else 0.0
        def eff_mult(sym: str) -> float:
            try:
                if sym in self.multiplier_cache and float(self.multiplier_cache[sym]) > 1.0:
                    return float(self.multiplier_cache[sym])
            except Exception:
                pass
            m_val = 1.0
            try:
                m_val = float(getattr(getattr(self.api, "account_manager", object()), "get_multiplier")(sym))
            except Exception:
                m_val = 1.0
            try:
                exid = sym.split(".")[-1] if "." in sym else ""
                if m_val <= 1.0 and exid not in ("SH", "SZ"):
                    td = None
                    try:
                        td = getattr(list_bar[0], "trading_day")
                    except Exception:
                        td = getattr(list_bar[0], "date_time", "").split(" ")[0]
                    dmult = getattr(self.api, "dict_daily_multiplier", {})
                    if td in dmult and sym in dmult.get(td, {}):
                        m_val = float(dmult[td][sym])
                if m_val <= 1.0 and exid not in ("SH", "SZ"):
                    m2 = self._get_multiplier(sym)
                    if m2 and m2 > 1.0:
                        m_val = float(m2)
            except Exception:
                pass
            try:
                m_check = float(m_val)
                if (m_check != m_check) or (m_check <= 0.0) or (abs(m_check) < 1e-6) or (abs(m_check) > 1e9):
                    m_check = 1.0
                m_val = m_check
            except Exception:
                m_val = 1.0
            try:
                if m_val > 1.0:
                    self.multiplier_cache[sym] = float(m_val)
            except Exception:
                pass
            return float(m_val)
        symbol_info: Dict[str, Dict[str, float]] = {}
        for s in long_pool + short_pool:
            p = last_px(s)
            m = eff_mult(s)
            if p <= 0:
                # 跳过无效价格的标的，避免不合理的仓位计算
                continue
            symbol_info[s] = {"price": p, "multiplier": m, "value": p * m}
        try:
            logging.info("[test_sample_strategy][adjust] symbol_info_count=%d long=%d short=%d", int(len(symbol_info)), int(len(long_pool)), int(len(short_pool)))
        except Exception:
            pass
        for s in long_pool:
            if len(long_pool) <= 0:
                continue
            target_value = capital_per_signal
            margin_rate = self._get_margin_rate(s) if str(getattr(self, "sizing_mode", "nominal")).lower() == "margin" else 1.0
            if s not in symbol_info:
                continue
            p = float(symbol_info[s]["price"])
            m = float(symbol_info[s]["multiplier"])
            denom = max(p * m * margin_rate, 1e-6)
            if denom < 1.0:
                try:
                    logging.warning("[test_sample_strategy][adjust][skip_low_denom] sym=%s tv=%.2f p=%.6f m=%.4f mr=%.4f denom=%.6f", s, target_value, p, m, margin_rate, denom)
                except Exception:
                    pass
                continue
            lots = int(target_value / denom)
            if lots <= 0:
                avail2 = 0.0
                try:
                    avail2 = float(getattr(self.api, "get_available")())
                except Exception:
                    try:
                        avail2 = float(getattr(self.api, "get_balance")())
                    except Exception:
                        avail2 = float(available)
                need1 = float(p) * float(m) * float(margin_rate)
                if avail2 >= need1:
                    lots = 1
                    try:
                        logging.info("[test_sample_strategy][adjust][long_force_one] sym=%s tv=%.2f p=%.6f m=%.4f mr=%.4f need=%.2f avail=%.2f", s, target_value, p, m, margin_rate, need1, avail2)
                    except Exception:
                        pass
                else:
                    try:
                        logging.info("[test_sample_strategy][adjust][long_zero] sym=%s tv=%.2f p=%.6f m=%.4f mr=%.4f denom=%.6f avail=%.2f", s, target_value, p, m, margin_rate, denom, avail2)
                    except Exception:
                        pass
                    continue
            try:
                try:
                    need_est = float(p) * float(m) * float(lots) * float(margin_rate)
                    avail = float(getattr(self.api, "get_balance")())
                    logging.debug("[test_sample_strategy][adjust][long_need] sym=%s need=%.2f avail=%.2f", s, need_est, avail)
                except Exception:
                    pass
                logging.info("[test_sample_strategy][adjust][long] sym=%s tv=%.2f p=%.6f m=%.4f mr=%.4f lots=%d", s, target_value, p, m, margin_rate, lots)
            except Exception:
                pass
            new_target[s] = lots
        for s in short_pool:
            if len(short_pool) <= 0:
                continue
            target_value = capital_per_signal
            margin_rate = self._get_margin_rate(s) if str(getattr(self, "sizing_mode", "nominal")).lower() == "margin" else 1.0
            if s not in symbol_info:
                continue
            p = float(symbol_info[s]["price"])
            m = float(symbol_info[s]["multiplier"])
            denom = max(p * m * margin_rate, 1e-6)
            if denom < 1.0:
                try:
                    logging.warning("[test_sample_strategy][adjust][skip_low_denom] sym=%s tv=%.2f p=%.6f m=%.4f mr=%.4f denom=%.6f", s, target_value, p, m, margin_rate, denom)
                except Exception:
                    pass
                continue
            lots = int(target_value / denom)
            if lots <= 0:
                avail2 = 0.0
                try:
                    avail2 = float(getattr(self.api, "get_available")())
                except Exception:
                    try:
                        avail2 = float(getattr(self.api, "get_balance")())
                    except Exception:
                        avail2 = float(available)
                need1 = float(p) * float(m) * float(margin_rate)
                if avail2 >= need1:
                    lots = 1
                    try:
                        logging.info("[test_sample_strategy][adjust][short_force_one] sym=%s tv=%.2f p=%.6f m=%.4f mr=%.4f need=%.2f avail=%.2f", s, target_value, p, m, margin_rate, need1, avail2)
                    except Exception:
                        pass
                else:
                    try:
                        logging.info("[test_sample_strategy][adjust][short_zero] sym=%s tv=%.2f p=%.6f m=%.4f mr=%.4f denom=%.6f avail=%.2f", s, target_value, p, m, margin_rate, denom, avail2)
                    except Exception:
                        pass
                    continue
            try:
                try:
                    need_est = float(p) * float(m) * float(abs(lots)) * float(margin_rate)
                    avail = float(getattr(self.api, "get_balance")())
                    logging.debug("[test_sample_strategy][adjust][short_need] sym=%s need=%.2f avail=%.2f", s, need_est, avail)
                except Exception:
                    pass
                logging.info("[test_sample_strategy][adjust][short] sym=%s tv=%.2f p=%.6f m=%.4f mr=%.4f lots=%d", s, target_value, p, m, margin_rate, lots)
            except Exception:
                pass
            new_target[s] = -lots
        # 止损（如启用）：覆盖为 0
        if bool(getattr(self, "enable_stop_loss", False)):
            try:
                stop_targets = self._check_stop_loss(list_bar)
                for sym, tgt in stop_targets.items():
                    new_target[sym] = int(tgt)
            except Exception:
                pass
        return new_target

    # ==================================================
    # 平台适配入口
    # ==================================================
    def calculate_target_position(self, list_bar: List[Any]) -> Tuple[Dict[str, int], bool]:
        """
        在线平台回调：返回目标仓位与是否变化标志
        """
        try:
            return self.on_cycle(list_bar)
        except Exception as e:
            logging.error("[test_sample_strategy][calc] error: %s", str(e))
            return self.dict_target_position, False

    # ==================================================
    # 工具函数
    # ==================================================
    def _push_hist(self, symbol: str, close: float, oi: float) -> None:
        root = self._symbol_root(symbol)
        if root not in self.price_hist:
            self.price_hist[root] = []
        if root not in self.oi_hist:
            self.oi_hist[root] = []
        self.price_hist[root].append(close)
        self.oi_hist[root].append(oi)
        if len(self.price_hist[root]) > self.factor_window:
            self.price_hist[root].pop(0)
        if len(self.oi_hist[root]) > self.factor_window:
            self.oi_hist[root].pop(0)
        return

    def _get_trading_day(self, list_bar: List[Any]) -> datetime:
        try:
            tday = getattr(list_bar[-1], "trading_day")
            if isinstance(tday, datetime):
                res = datetime(tday.year, tday.month, tday.day)
                try:
                    logging.info("[test_sample_strategy][tday_src] src=trading_day_datetime val=%s", res.strftime("%Y-%m-%d"))
                except Exception:
                    pass
                return res
            try:
                if hasattr(tday, "to_pydatetime"):
                    dt = tday.to_pydatetime()
                    res = datetime(dt.year, dt.month, dt.day)
                    try:
                        logging.info("[test_sample_strategy][tday_src] src=trading_day_timestamp val=%s", res.strftime("%Y-%m-%d"))
                    except Exception:
                        pass
                    return res
            except Exception:
                pass
            if isinstance(tday, str) and tday:
                s = tday.split(" ")[0]
                if len(s) == 10 and "-" in s:
                    y, m, d = map(int, s.split("-"))
                    res = datetime(y, m, d)
                    try:
                        logging.info("[test_sample_strategy][tday_src] src=trading_day_str val=%s", res.strftime("%Y-%m-%d"))
                    except Exception:
                        pass
                    return res
                if len(s) == 8 and s.isdigit():
                    res = datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                    try:
                        logging.info("[test_sample_strategy][tday_src] src=trading_day_compact val=%s", res.strftime("%Y-%m-%d"))
                    except Exception:
                        pass
                    return res
        except Exception:
            pass
        try:
            dt_str = str(getattr(list_bar[-1], "date_time", "")).split(" ")[0]
            if dt_str:
                y, m, d = map(int, dt_str.split("-"))
                res = datetime(y, m, d)
                try:
                    logging.info("[test_sample_strategy][tday_src] src=date_time val=%s", res.strftime("%Y-%m-%d"))
                except Exception:
                    pass
                return res
        except Exception:
            pass
        try:
            dt = getattr(list_bar[-1], "datetime")
            if isinstance(dt, datetime):
                res = datetime(dt.year, dt.month, dt.day)
                try:
                    logging.info("[test_sample_strategy][tday_src] src=datetime_field val=%s", res.strftime("%Y-%m-%d"))
                except Exception:
                    pass
                return res
        except Exception:
            pass
        try:
            cur = getattr(self.api, "cur_trading_day", None)
            if isinstance(cur, str) and cur:
                s = cur.split(" ")[0]
                if len(s) == 10 and "-" in s:
                    y, m, d = map(int, s.split("-"))
                    res = datetime(y, m, d)
                    try:
                        logging.info("[test_sample_strategy][tday_src] src=api_cur_day val=%s", res.strftime("%Y-%m-%d"))
                    except Exception:
                        pass
                    return res
                if len(s) == 8 and s.isdigit():
                    res = datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                    try:
                        logging.info("[test_sample_strategy][tday_src] src=api_cur_compact val=%s", res.strftime("%Y-%m-%d"))
                    except Exception:
                        pass
                    return res
        except Exception:
            pass
        try:
            logging.warning("[test_sample_strategy][tday_src] src=fallback_utc val=%s", datetime.utcnow().strftime("%Y-%m-%d"))
        except Exception:
            pass
        return datetime.utcnow()

    def _need_rebalance(self, trading_day: datetime, list_bar: List[Any]) -> bool:
        # 强制诊断日志（不受try-except影响）
        tds = list(getattr(self.api, "trading_date_section", []))
        cur_str = trading_day.strftime("%Y-%m-%d")
        idx = tds.index(cur_str) if (tds and cur_str in tds) else -1
        
        logging.info("[DIAG] tds_len=%d tds_sample=%s cur=%s idx=%d", 
                    len(tds), str(tds[:3]) if tds else "EMPTY", cur_str, idx)
    
        # 原有代码...
        try:
            unit = str(getattr(self, "rebalance_unit", "trading_day")).lower()
        except Exception:
            unit = "trading_day"
        try:
            if isinstance(getattr(self, "last_rebalance_day", None), datetime) and self.last_rebalance_day and (self.last_rebalance_day > trading_day):
                try:
                    logging.warning("[test_sample_strategy][rebalance_gate] last_rebalance_day_future last=%s cur=%s", self.last_rebalance_day.strftime("%Y-%m-%d"), trading_day.strftime("%Y-%m-%d"))
                except Exception:
                    pass
                self.last_rebalance_day = None
        except Exception:
            pass
        # 同日计算由底层执行门控控制，策略层不再因 last_rebalance_day 阻断
        try:
            if unit == "bar":
                dt = self._get_bar_dt(list_bar)
                if dt and not self._is_within_trading_window(dt):
                    logging.info("[test_sample_strategy][rebalance_gate] skip time window dt=%s allow_after=%s cutoff=%s", dt.time().isoformat(), str(self.open_allowed_after), str(self.cancel_cutoff_time))
                    return False
        except Exception:
            pass
        try:
            if self.last_rebalance_day is None or (trading_day - self.last_rebalance_day).days >= 1:
                self._update_main_contracts(trading_day)
        except Exception:
            pass
        if unit == "bar":
            try:
                self._bars_since_last_rebalance = int(getattr(self, "_bars_since_last_rebalance", 0)) + 1
            except Exception:
                self._bars_since_last_rebalance = 1
            logging.info("[test_sample_strategy][rebalance_gate] unit=bar bars_since=%d interval=%d pass=%s", self._bars_since_last_rebalance, int(getattr(self, "rebalance_interval", 5)), str(bool(self._bars_since_last_rebalance >= int(getattr(self, "rebalance_interval", 5)))))
            return self._bars_since_last_rebalance >= int(getattr(self, "rebalance_interval", 5))
        # 使用交易日序列进行周/月/日门控
        try:
            tds_raw = list(getattr(self.api, "trading_date_section", []))
            tds: List[str] = []
            for d in tds_raw:
                s = str(d).split(" ")[0]
                if len(s) == 10 and "-" in s:
                    tds.append(s)
                elif len(s) == 8 and s.isdigit():
                    tds.append(f"{s[0:4]}-{s[4:6]}-{s[6:8]}")
            cur_str = trading_day.strftime("%Y-%m-%d")
            idx = tds.index(cur_str) if (tds and cur_str in tds) else -1
            ri_cfg = int(getattr(self, "rebalance_interval", 5))
            ri_eff = ri_cfg
            try:
                if isinstance(tds, list) and len(tds) > 0 and len(tds) < ri_cfg:
                    ri_eff = 1
            except Exception:
                ri_eff = ri_cfg
            logging.info("[test_sample_strategy][rebalance_gate] tds_len=%d cur=%s idx=%d unit=%s ri=%d ri_eff=%d", int(len(tds) if isinstance(tds, list) else 0), cur_str, int(idx), str(getattr(self, "rebalance_unit", "trading_day")), ri_cfg, ri_eff)
        except Exception:
            idx = -1
        ri = int(getattr(self, "rebalance_interval", 5))
        if idx < 0:
            try:
                try:
                    head = tds[0] if isinstance(tds, list) and len(tds) > 0 else ""
                except Exception:
                    head = ""
                try:
                    tail = tds[-1] if isinstance(tds, list) and len(tds) > 0 else ""
                except Exception:
                    tail = ""
                try:
                    logging.info("[test_sample_strategy][rebalance_gate] idx<0 cur=%s tds_head=%s tds_tail=%s len=%d", cur_str, head, tail, int(len(tds) if isinstance(tds, list) else 0))
                except Exception:
                    pass
                if self.last_rebalance_day is None:
                    logging.info("[test_sample_strategy][rebalance_gate] idx<0 and no last_rebalance_day, allow")
                    return True
                ok = (trading_day - self.last_rebalance_day).days >= ri
                logging.info("[test_sample_strategy][rebalance_gate] idx<0 day_diff=%d ri=%d pass=%s", (trading_day - self.last_rebalance_day).days if isinstance(self.last_rebalance_day, datetime) else 0, ri, str(bool(ok)))
                return ok
            except Exception:
                return False
        if idx == 0:
            try:
                dt = self._get_bar_dt(list_bar)
            except Exception:
                dt = None
            try:
                has_pos = any(int(round(v)) != 0 for v in self.dict_target_position.values())
            except Exception:
                has_pos = bool(self.dict_target_position)
            allow = (self.last_rebalance_day is None) and (not has_pos)
            try:
                if dt and not self._is_within_trading_window(dt):
                    logging.info("[test_sample_strategy][rebalance_gate] unit=%s idx=%d pass=%s", unit, idx, "False(first_day_time_window)")
                    return False
            except Exception:
                pass
            try:
                logging.info("[test_sample_strategy][rebalance_gate] unit=%s idx=%d pass=%s", unit, idx, str(bool(allow)))
            except Exception:
                pass
            return bool(allow)
        if unit == "week":
            try:
                if idx > 0:
                    prev_dt = datetime.strptime(tds[idx - 1], "%Y-%m-%d")
                    cur_dt = datetime.strptime(tds[idx], "%Y-%m-%d")
                    ok = (prev_dt.isocalendar()[1] != cur_dt.isocalendar()[1]) or (prev_dt.isocalendar()[0] != cur_dt.isocalendar()[0])
                    logging.info("[test_sample_strategy][rebalance_gate] unit=week idx=%d pass=%s", idx, str(bool(ok)))
                    return ok
            except Exception:
                return False
            return False
        if unit == "month":
            try:
                if idx > 0:
                    prev_dt = datetime.strptime(tds[idx - 1], "%Y-%m-%d")
                    cur_dt = datetime.strptime(tds[idx], "%Y-%m-%d")
                    ok = (prev_dt.month != cur_dt.month) or (prev_dt.year != cur_dt.year)
                    logging.info("[test_sample_strategy][rebalance_gate] unit=month idx=%d pass=%s", idx, str(bool(ok)))
                    return ok
            except Exception:
                return False
            return False
        try:
            ri_cfg2 = int(getattr(self, "rebalance_interval", 5))
        except Exception:
            ri_cfg2 = ri
        ri_eff2 = ri_cfg2
        try:
            if isinstance(tds, list) and len(tds) > 0 and len(tds) < ri_cfg2:
                ri_eff2 = 1
        except Exception:
            ri_eff2 = ri_cfg2
        ok = (idx % max(1, ri_eff2) == 0)
        try:
            logging.info("[test_sample_strategy][rebalance_gate] unit=trading_day idx=%d ri=%d ri_eff=%d pass=%s", idx, ri_cfg2, ri_eff2, str(bool(ok)))
        except Exception:
            pass
        try:
            dt = self._get_bar_dt(list_bar)
            if dt and not self._is_within_trading_window(dt):
                logging.info("[test_sample_strategy][rebalance_gate] time window blocked dt=%s allow_after=%s cutoff=%s", dt.time().isoformat(), str(self.open_allowed_after), str(self.cancel_cutoff_time))
                return False
        except Exception:
            pass
        return ok

    def _dataset_days(self, ds_base: str, data_type: str, roots: List[str], begin_day: str, end_day: str) -> List[str]:
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
                            if day >= begin_day and day <= end_day:
                                days.add(day)
            return sorted(list(days))
        except Exception:
            return []

    def _get_contract_month(self, symbol: str) -> int:
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
        if not d:
            return {}
        vals = sorted(d.values())
        n = len(vals)
        lower_idx = int(n * limits[0])
        upper_idx = int(n * (1 - limits[1]))
        lower_val = vals[max(0, lower_idx)]
        upper_val = vals[min(n - 1, upper_idx)]
        return {k: min(max(v, lower_val), upper_val) for k, v in d.items()}

    def _dict_almost_equal(self, d1: Dict[str, int] | Dict[str, float], d2: Dict[str, int] | Dict[str, float], tol: float = 1e-6) -> bool:
        try:
            if set(d1.keys()) != set(d2.keys()):
                return False
            for k in d1.keys():
                v1 = float(d1[k])
                v2 = float(d2.get(k, 0))
                if abs(v1 - v2) >= tol:
                    return False
            return True
        except Exception:
            return d1 == d2

    def _get_bar_dt(self, list_bar: List[Any]) -> datetime | None:
        try:
            b = list_bar[-1]
            raw = getattr(b, "datetime", None) or getattr(b, "date_time", None)
            if isinstance(raw, datetime):
                return raw
            if isinstance(raw, str) and ' ' in raw:
                return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
        return None

    def _is_within_trading_window(self, dt: datetime) -> bool:
        try:
            t = dt.time()
            day_session = (t >= self.open_allowed_after) and (t <= self.cancel_cutoff_time)
            night_enabled = False
            try:
                night_enabled = bool(getattr(self.api, "night_trade", False))
            except Exception:
                night_enabled = False
            night_session = night_enabled and ((t >= time(21, 0)) or (t <= time(2, 59)))
            return day_session or night_session
        except Exception:
            return True

    def _symbol_root(self, symbol: str) -> str:
        try:
            s = symbol.decode() if hasattr(symbol, "decode") else str(symbol)
        except Exception:
            s = str(symbol)
        try:
            base, exch = s.split('.', 1)
        except Exception:
            try:
                if hasattr(self.api, "get_symbol_root"):
                    return self.api.get_symbol_root(s)
            except Exception:
                pass
            return s
        try:
            prefix = ""
            for ch in base:
                if ch.isalpha():
                    prefix += ch
                else:
                    break
            if not prefix:
                i = 0
                while i < len(base) and base[i].isalpha():
                    i += 1
                prefix = base[:i] if i > 0 else base
        except Exception:
            prefix = base
        manual_root = (prefix.upper() + "." + exch.upper())
        try:
            if hasattr(self.api, "get_symbol_root"):
                api_root = str(self.api.get_symbol_root(s))
                parts = api_root.split(".")
                if len(parts) == 2 and parts[0].isalpha():
                    return parts[0].upper() + "." + parts[1].upper()
        except Exception:
            pass
        return manual_root

    def _get_multiplier(self, symbol: str) -> float:
        try:
            contract = getattr(self.api, "get_contract_info")(symbol)
            return float(getattr(contract, "multiplier", 1.0))
        except Exception:
            return 1.0

    def _get_margin_rate(self, symbol: str) -> float:
        """
        保证金率优先顺序：策略配置 → 平台账户管理器 → 合约信息 → 默认0.2
        """
        try:
            root = self._symbol_root(symbol)
            if root in self.margin_rates:
                return float(self.margin_rates[root])
            try:
                mr_platform = float(getattr(getattr(self.api, "account_manager", object()), "margin_rate", 0.2))
            except Exception:
                mr_platform = 0.2
            if mr_platform is not None:
                return float(mr_platform)
            try:
                contract = getattr(self.api, "get_contract_info")(symbol)
                return float(getattr(contract, "margin_rate", 0.2))
            except Exception:
                return 0.2
        except Exception:
            return 0.2

    def _is_main_contract(self, symbol: str) -> bool:
        try:
            product_id = self._symbol_root(symbol)
            if not hasattr(self, "main_contracts"):
                return True
            return symbol == self.main_contracts.get(product_id, (symbol, 0))[0]
        except Exception:
            return True

    def _update_main_contracts(self, trading_day: datetime) -> None:
        """
        更新主力映射：剔除到期<5天，持仓量最大者为主力；本地数据兜底
        """
        try:
            import logging
            import calendar
            df_daily = None
            try:
                if bool(getattr(self.api, "remote_enabled", True)):
                    df_daily = getattr(self.api, "get_daily_symbol")()
            except Exception:
                df_daily = None
            logging.info("[test_sample_strategy][main_update] tday=%s remote_enabled=%s df_daily=%s", trading_day.strftime("%Y-%m-%d"), str(bool(getattr(self.api, "remote_enabled", True))), ("Y" if (df_daily is not None and hasattr(df_daily, "itertuples")) else "N"))

            new_mains: Dict[str, Tuple[str, float]] = {}
            exp_excluded: Dict[str, int] = {}
            candidate_seen: Dict[str, int] = {}
            if df_daily is not None and hasattr(df_daily, "itertuples"):
                for row in df_daily.itertuples():
                    try:
                        symbol = getattr(row, "symbol")
                        product_id = self._symbol_root(symbol)
                        oi = float(getattr(row, "open_interest", 0.0))
                        expire_date = getattr(row, "expire_date", None)
                        days_to_expire = None
                        exp_ym = None
                        try:
                            if expire_date is None:
                                days_to_expire = None
                            elif isinstance(expire_date, datetime):
                                days_to_expire = (expire_date - trading_day).days
                                exp_ym = expire_date.year * 100 + expire_date.month
                            elif isinstance(expire_date, str):
                                s = expire_date.strip()
                                if len(s) == 10 and "-" in s:
                                    y, m, d = map(int, s.split("-"))
                                    ed = datetime(y, m, d)
                                    days_to_expire = (ed - trading_day).days
                                    exp_ym = y * 100 + m
                                elif len(s) == 8 and s.isdigit():
                                    ed = datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                                    days_to_expire = (ed - trading_day).days
                                    exp_ym = int(s[0:4]) * 100 + int(s[4:6])
                            else:
                                days_to_expire = None
                        except Exception:
                            days_to_expire = None
                        if days_to_expire is None:
                            try:
                                base = str(symbol).split(".")[0]
                                digits = "".join(ch for ch in base if ch.isdigit())
                                if len(digits) >= 4:
                                    yy = 2000 + int(digits[0:2])
                                    mm = int(digits[2:4])
                                    last_day = calendar.monthrange(yy, mm)[1]
                                    ed2 = datetime(yy, mm, last_day)
                                    days_to_expire = (ed2 - trading_day).days
                                    exp_ym = yy * 100 + mm
                                    
                            except Exception:
                                days_to_expire = None
                        try:
                            candidate_seen[product_id] = int(candidate_seen.get(product_id, 0)) + 1
                        except Exception:
                            pass
                        try:
                            cur_ym = trading_day.year * 100 + trading_day.month
                        except Exception:
                            cur_ym = None
                        if exp_ym is not None and cur_ym is not None and exp_ym == cur_ym:
                            try:
                                exp_excluded[product_id] = int(exp_excluded.get(product_id, 0)) + 1
                            except Exception:
                                pass
                            continue
                        if days_to_expire is None:
                            try:
                                exp_excluded[product_id] = int(exp_excluded.get(product_id, 0)) + 1
                                # logging.info("[test_sample_strategy][main_update][remote_drop_no_expiry] root=%s sym=%s", product_id, str(symbol))
                            except Exception:
                                pass
                            continue
                        if days_to_expire < 5:
                            try:
                                exp_excluded[product_id] = int(exp_excluded.get(product_id, 0)) + 1
                                # logging.info("[test_sample_strategy][main_update][remote_drop_lt5] root=%s sym=%s dte=%d", product_id, str(symbol), int(days_to_expire))
                            except Exception:
                                pass
                            continue
                        if days_to_expire >= 5 and (product_id not in new_mains or oi > new_mains[product_id][1]):
                            new_mains[product_id] = (symbol, oi)
                    except Exception:
                        continue
                try:
                    tot_candidates = sum(int(v) for v in candidate_seen.values()) if candidate_seen else 0
                    tot_excl = sum(int(v) for v in exp_excluded.values()) if exp_excluded else 0
                    logging.info("[test_sample_strategy][main_update] branch=remote tday=%s mains_size=%d candidates=%d excluded_by_expiry=%d", trading_day.strftime("%Y-%m-%d"), int(len(new_mains)), int(tot_candidates), int(tot_excl))
                    logging.info("[test_sample_strategy][main_update] mains=%s", str({k: new_mains[k][0] for k in new_mains.keys()}))
                except Exception:
                    pass
            if not new_mains:
                try:
                    import pandas as pd
                    import calendar
                    tday_str = trading_day.strftime("%Y-%m-%d")
                    roots: List[str] = []
                    try:
                        roots = [str(x) for x in getattr(self.api, "list_sub_symbol_root", [])]
                    except Exception:
                        roots = []
                    for r in roots:
                        ex = r.split(".")[-1] if "." in r else ""
                        data_type = str(getattr(self.api, "data_type", "1m"))
                        scan_type = data_type if data_type.lower() != "5m" else "1m"
                        dirp = os.path.join(getattr(self.api, "cwd", os.getcwd()), "data", scan_type, ex, r, tday_str)
                        dirp2 = os.path.join(getattr(self.api, "cwd", os.getcwd()), "data", "data", scan_type, ex, r, tday_str)
                        best_sym = None
                        best_oi = -1.0
                        try:
                            if os.path.exists(dirp) and os.path.isdir(dirp):
                                for name in os.listdir(dirp):
                                    fp = os.path.join(dirp, name)
                                    if not os.path.isfile(fp):
                                        continue
                                    try:
                                        df = pd.read_csv(fp)
                                    except Exception:
                                        try:
                                            df = pd.read_table(fp)
                                        except Exception:
                                            df = pd.DataFrame()
                                    if df is None or df.shape[0] == 0:
                                        continue
                                    oi_col = "open_interest" if "open_interest" in df.columns else None
                                    if oi_col is None:
                                        continue
                                    val = float(df[oi_col].iloc[-1])
                                    if val > best_oi:
                                        best_oi = val
                                        best_sym = name
                            if best_sym is None and os.path.exists(dirp2) and os.path.isdir(dirp2):
                                for name in os.listdir(dirp2):
                                    fp = os.path.join(dirp2, name)
                                    if not os.path.isfile(fp):
                                        continue
                                    try:
                                        df = pd.read_csv(fp)
                                    except Exception:
                                        try:
                                            df = pd.read_table(fp)
                                        except Exception:
                                            df = pd.DataFrame()
                                    if df is None or df.shape[0] == 0:
                                        continue
                                    oi_col = "open_interest" if "open_interest" in df.columns else None
                                    if oi_col is None:
                                        continue
                                    val = float(df[oi_col].iloc[-1])
                                    if val > best_oi:
                                        best_oi = val
                                        best_sym = name
                        except Exception:
                            pass
                        if best_sym:
                            segs = str(best_sym).split(".")
                            sym_clean = ".".join(segs[:-1]) if len(segs) >= 3 else str(best_sym)
                            base = sym_clean.split(".")[0]
                            digits = "".join(ch for ch in base if ch.isdigit())
                            yy = 2000 + int(digits[0:2]) if len(digits) >= 2 else trading_day.year
                            mm = int(digits[2:4]) if len(digits) >= 4 else trading_day.month
                            last_day = calendar.monthrange(yy, mm)[1]
                            ed = datetime(yy, mm, last_day)
                            days_to_expire = (ed - trading_day).days
                            cur_ym2 = trading_day.year * 100 + trading_day.month
                            exp_ym2 = yy * 100 + mm
                            logging.info("[test_sample_strategy][main_update][local_pick] root=%s best_sym=%s best_oi=%.2f dte=%s", r, str(sym_clean), float(best_oi), (str(days_to_expire) if days_to_expire is not None else "None"))
                            if (exp_ym2 != cur_ym2) and (days_to_expire is None or days_to_expire >= 5):
                                if r not in new_mains or best_oi > new_mains[r][1]:
                                    new_mains[r] = (sym_clean, best_oi)
                            else:
                                try:
                                    logging.info("[test_sample_strategy][main_update][local_drop] root=%s sym=%s dte=%d (<5)", r, str(sym_clean), int(days_to_expire))
                                except Exception:
                                    pass
                except Exception:
                    pass
                try:
                    logging.info("[test_sample_strategy][main_update] branch=local tday=%s mains_size=%d mains=%s", trading_day.strftime("%Y-%m-%d"), int(len(new_mains)), str({k: new_mains[k][0] for k in new_mains.keys()}))
                except Exception:
                    pass
            self.main_contracts = new_mains
            try:
                valid_roots = set(self.main_contracts.keys())
                for k in list(getattr(self, "entry_price_map", {}).keys()):
                    if k not in valid_roots:
                        del self.entry_price_map[k]
            except Exception:
                pass
            logging.debug(f"[test_sample_strategy] updated main contracts: {self.main_contracts}")
        except Exception as e:
            logging.warning(f"[test_sample_strategy] update main contracts failed: {e}")

    def _apply_adj_on_rollover(self, main_bars: List[Any]) -> None:
        """
        主力切换发生时对历史价格进行复权调整（保持离线一致）
        """
        try:
            sym_close: Dict[str, float] = {}
            for b in main_bars:
                try:
                    s = getattr(b, "symbol")
                    c = float(getattr(b, "close"))
                    sym_close[s] = c
                except Exception:
                    continue
            for b in main_bars:
                try:
                    cur_sym = getattr(b, "symbol")
                    root = self._symbol_root(cur_sym)
                    cur_main = self.main_contracts.get(root, (cur_sym, 0))[0]
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
                            self.root_adj_factor[root] = float(getattr(self, "root_adj_factor", {}).get(root, 1.0)) * ratio
                            try:
                                ep = float(self.entry_price_map.get(root, 0.0))
                                if ep > 0:
                                    self.entry_price_map[root] = ep * ratio
                                    logging.info("[test_sample_strategy][rollover_adj_entry] root=%s ratio=%.6f old_ep=%.6f new_ep=%.6f", root, float(ratio), float(ep), float(self.entry_price_map.get(root, 0.0)))
                            except Exception:
                                pass
                        self.prev_main_symbol[root] = cur_main
                except Exception:
                    continue
        except Exception:
            pass

    def _get_current_price(self, symbol: str, list_bar: List[Any]) -> float:
        try:
            for b in list_bar[::-1]:
                if getattr(b, "symbol", "") == symbol:
                    return float(getattr(b, "close"))
            root = self._symbol_root(symbol)
            arr = self.price_hist.get(root, [])
            if arr:
                return float(arr[-1])
        except Exception:
            pass
        return 0.0

    def _get_entry_price(self, symbol: str) -> float:
        try:
            root = self._symbol_root(symbol)
            return float(self.entry_price_map.get(root, 0.0))
        except Exception:
            return 0.0
    def _set_entry_price(self, symbol: str, price: float) -> None:
        try:
            root = self._symbol_root(symbol)
            self.entry_price_map[root] = float(price)
        except Exception:
            pass

    def _check_stop_loss(self, list_bar: List[Any]) -> Dict[str, int]:
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
                try:
                    logging.debug("[test_sample_strategy][stop_loss] symbol=%s pos=%d ep=%.6f cp=%.6f pct=%.6f sl_pct=%.6f", str(symbol), int(pos), float(ep), float(cp), float(pct), float(self.stop_loss_pct))
                except Exception:
                    pass
                if pos > 0 and pct < -self.stop_loss_pct:
                    try:
                        logging.info("[test_sample_strategy][stop_loss_trigger] symbol=%s side=long ep=%.6f cp=%.6f pct=%.6f", str(symbol), float(ep), float(cp), float(pct))
                    except Exception:
                        pass
                    res[symbol] = 0
                elif pos < 0 and (1.0 - cp / ep) < -self.stop_loss_pct:
                    try:
                        logging.info("[test_sample_strategy][stop_loss_trigger] symbol=%s side=short ep=%.6f cp=%.6f pct=%.6f", str(symbol), float(ep), float(cp), float(1.0 - cp / ep))
                    except Exception:
                        pass
                    res[symbol] = 0
        except Exception:
            return {}
        return res

    def _get_market_volume(self, symbol: str, list_bar: List[Any]) -> float:
        try:
            for b in list_bar[::-1]:
                if getattr(b, "symbol", "") == symbol:
                    v = None
                    try:
                        v = float(getattr(b, "volume"))
                    except Exception:
                        try:
                            v = float(getattr(b, "vol"))
                        except Exception:
                            v = None
                    if v is not None and v > 0:
                        return v
                    break
        except Exception:
            pass
        return 0.0

    def _data_cache_ready(self) -> bool:
        try:
            is_option = False
            try:
                is_option = bool(getattr(self.api, "get_is_option")())
            except Exception:
                is_option = False
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
                pass
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
                    has_file = any(os.path.isfile(os.path.join(filep, name)) for name in os.listdir(filep))
                except Exception:
                    has_file = False
                if not has_file:
                    return False
                if end_day:
                    filep2 = os.path.join(root_dir, ex, r, end_day)
                    if not os.path.exists(filep2) or not os.path.isdir(filep2):
                        return False
                    try:
                        has_file2 = any(os.path.isfile(os.path.join(filep2, name)) for name in os.listdir(filep2))
                    except Exception:
                        has_file2 = False
                    if not has_file2:
                        return False
            return True
        except Exception:
            return False

    def _dataset_cache_ready_for_period(self) -> bool:
        try:
            is_option = False
            try:
                is_option = bool(getattr(self.api, "get_is_option")())
            except Exception:
                is_option = False
            base = os.getcwd()
            data_type = str(getattr(self.api, "data_type", "1m"))
            root_dir = os.path.join(base, "dataset", "data", data_type)
            cfg_path = os.path.join(base, "test_config.json")
            subs: List[str] = []
            ex_map: Dict[str, str] = {}
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    js = json.load(f)
                subs = [str(x) for x in js.get("subscribe symbol", [])]
                ex_map = {str(k): str(v) for k, v in js.get("exchanges", {}).items()}
            except Exception:
                return False
            roots = [f"{s}.{ex_map.get(s, '')}".strip(".") for s in subs if s in ex_map]
            tds = list(getattr(self.api, "trading_date_section", []))
            if not roots or not tds:
                return False
            last_day = str(tds[-1]).split(" ")[0]
            expire_days = int(getattr(self, "cache_expire_days", 0))
            expire_threshold = None
            if expire_days and expire_days > 0:
                try:
                    expire_threshold = datetime.now() - timedelta(days=expire_days)
                except Exception:
                    expire_threshold = None
            for d in tds:
                day = str(d).split(" ")[0]
                for r in roots:
                    ex = r.split(".")[-1] if "." in r else ""
                    dirp = os.path.join(root_dir, ex, r, day)
                    if (not os.path.exists(dirp)) or (not os.path.isdir(dirp)):
                        return False
                    try:
                        has_file = any(os.path.isfile(os.path.join(dirp, name)) for name in os.listdir(dirp))
                    except Exception:
                        has_file = False
                    if not has_file:
                        return False
                    if expire_threshold is not None and (day == last_day):
                        try:
                            mtimes = []
                            for name in os.listdir(dirp):
                                fp = os.path.join(dirp, name)
                                if os.path.isfile(fp):
                                    mtimes.append(datetime.fromtimestamp(os.path.getmtime(fp)))
                            if mtimes and max(mtimes) < expire_threshold:
                                return False
                        except Exception:
                            pass
            return True
        except Exception:
            return False
