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
    def __init__(self):
        self.api = None
        self.cur_trading_day = None
        self.strat = test_sample_strategy()
        self._last_rebalance_day = None
        self._pending_rebalance_day = None
        self._pending_pre_fill_total = 0.0
        self.BUY = 1
        self.SELL = -1

    def init(self, api):
        self.api = api
        self.strat.init(api)
        try:
            self.strat.enable_trend_filter = False
            self.strat.signal_threshold = 0.0
        except Exception:
            pass
        return

    def on_bod(self, trading_day):
        self.cur_trading_day = trading_day
        logging.info("[offline][on_bod] %s", trading_day)
        return

    def on_eod(self, trading_day):
        logging.info("[offline][on_eod] %s", trading_day)
        return

    def handle_bar(self, bar):
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
                        for sym, pos in pm.dict_positions.items():
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

    def handle_section_bar(self, list_bar):
        try:
            logging.info("[offline][section] BAR TRIGGERED, size=%d", len(list_bar))
        except Exception:
            pass
        td = self.cur_trading_day
        gate_ok = False
        try:
            if isinstance(td, datetime.date):
                td_str = f"{td.year:04d}-{td.month:02d}-{td.day:02d}"
            elif isinstance(td, datetime.datetime):
                td_str = f"{td.year:04d}-{td.month:02d}-{td.day:02d}"
            elif isinstance(td, str):
                if len(td) == 8 and td.isdigit():
                    td_str = f"{td[0:4]}-{td[4:6]}-{td[6:8]}"
                else:
                    td_str = td
            else:
                td_str = ""
        except Exception:
            td_str = ""
        if not td_str:
            try:
                b = list_bar[0]
                tday = getattr(b, "trading_day", None)
                if isinstance(tday, str):
                    if len(tday) == 8 and tday.isdigit():
                        td_str = f"{tday[0:4]}-{tday[4:6]}-{tday[6:8]}"
                    else:
                        td_str = tday
                else:
                    dt = getattr(b, "datetime", None)
                    if dt:
                        td_str = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
            except Exception:
                td_str = ""
        tds = getattr(self.api, "trading_date_section", [])
        try:
            idx = tds.index(td_str) if isinstance(tds, list) and td_str in tds else -1
        except Exception:
            idx = -1
        try:
            ri = int(getattr(self.strat, "rebalance_interval", 5))
        except Exception:
            ri = 5
        try:
            unit = str(getattr(self.strat, "rebalance_unit", "trading_day")).lower()
        except Exception:
            unit = "trading_day"
        if idx < 0:
            return
        if ri <= 0:
            ri = 1
        # 强制首日不调仓，避免样本起点锚定偏差
        if idx == 0:
            logging.info("[offline][rebalance_gate] td=%s idx=%d ri=%d unit=%s pass=%s", td_str, idx, ri, unit, "False(first_day)")
            return
        # 自然周期门控：week/month；否则按交易日周期
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
        dict_target_position, is_pos_changed = self.strat.calculate_target_position(list_bar)
        try:
            logging.info("[offline][section] signal changed=%s targets=%s", str(is_pos_changed), str(dict_target_position))
        except Exception:
            pass
        if not is_pos_changed and dict_target_position:
            try:
                logging.warning("[offline][section] strategy says no change but targets not empty; continue diff check")
            except Exception:
                pass

        balance = self.api.get_balance()
        try:
            dict_cur_pos_raw = self.api.get_current_position()
            dict_cur_pos = {(k.decode('utf-8') if isinstance(k, bytes) else str(k)): v for k, v in dict_cur_pos_raw.items()}
        except Exception:
            dict_cur_pos = self.api.get_current_position()
        logging.debug("[offline][section] cur_pos=%s balance=%.2f", str(dict_cur_pos), balance)

        # open/adjust positions with explicit close-then-open on flip（顺序：先处理不在目标中的旧仓，再处理有目标的仓位）
        try:
            pm = self.api.get_pm()
            pre_total = 0.0
            for sym, pos in pm.dict_positions.items():
                pre_total += float(getattr(pos, "long_total_filled", 0.0)) + float(getattr(pos, "short_total_filled", 0.0))
            self._pending_rebalance_day = td_str
            self._pending_pre_fill_total = pre_total
        except Exception:
            self._pending_rebalance_day = td_str
            self._pending_pre_fill_total = 0.0
        traded = False
        for symbol, net_pos in dict_cur_pos.items():
            if symbol not in dict_target_position.keys():
                if abs(net_pos) != 0:
                    try:
                        direction = self.SELL if net_pos > 0 else self.BUY
                        oid = self.api.send_order(symbol, abs(int(round(net_pos))), direction)
                        if oid == -1:
                            logging.error("[offline][close][perm_fail] symbol=%s sz=%d dir=%d", symbol, abs(int(round(net_pos))), 0 if direction==self.BUY else 1)
                            continue
                        else:
                            traded = True
                    except Exception as e:
                        logging.error("[offline][close][fail] symbol=%s err=%s", symbol, str(e))
                        continue
                    logging.debug("[offline][close] symbol=%s sz=%d side=%d", symbol, abs(int(round(net_pos))), 1 if direction==self.SELL else 0)

        # 处理目标集合
        for symbol, target_pos in dict_target_position.items():
            tgt_i = int(round(target_pos))

            if symbol not in dict_cur_pos.keys():
                if abs(tgt_i) != 0:
                    try:
                        direction = self.BUY if tgt_i > 0 else self.SELL
                        oid = self.api.send_order(symbol, abs(tgt_i), direction)
                        if oid == -1:
                            logging.error("[offline][send_order][perm_fail] symbol=%s sz=%d dir=%d", symbol, abs(tgt_i), 0 if tgt_i>=0 else 1)
                            continue
                        else:
                            traded = True
                    except Exception as e:
                        logging.error("[offline][send_order][fail] symbol=%s err=%s", symbol, str(e))
                        continue
                    logging.debug("[offline][send_order] symbol=%s sz=%d side=%d", symbol, abs(tgt_i), 0 if direction==self.BUY else 1)
            else:
                cur_net = int(round(dict_cur_pos[symbol]))
                if cur_net == tgt_i:
                    continue
                if cur_net * tgt_i < 0:
                    try:
                        close_dir = self.SELL if cur_net > 0 else self.BUY
                        oid1 = self.api.send_order(symbol, abs(cur_net), close_dir)
                        if oid1 == -1:
                            logging.error("[offline][close_then_open][close_fail] symbol=%s sz=%d dir=%d", symbol, abs(cur_net), 1 if cur_net>0 else 0)
                            continue
                        else:
                            traded = True
                    except Exception as e:
                        logging.error("[offline][close_then_open][close_err] symbol=%s err=%s", symbol, str(e))
                        continue
                    try:
                        open_dir = self.BUY if tgt_i > 0 else self.SELL
                        oid2 = self.api.send_order(symbol, abs(tgt_i), open_dir)
                        if oid2 == -1:
                            logging.error("[offline][close_then_open][open_fail] symbol=%s sz=%d dir=%d", symbol, abs(tgt_i), 0 if tgt_i>=0 else 1)
                            continue
                        else:
                            traded = True
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
                                logging.error("[offline][send_order][perm_fail] symbol=%s sz=%d dir=%d", symbol, abs(diff_pos), 0 if diff_pos>=0 else 1)
                                continue
                            else:
                                traded = True
                        except Exception as e:
                            logging.error("[offline][send_order][fail] symbol=%s err=%s", symbol, str(e))
                            continue
                    logging.debug("[offline][send_order] symbol=%s sz=%d side=%d", symbol, abs(diff_pos), 0 if direction==self.BUY else 1)

        # _last_rebalance_day 将在 handle_bar 中检测到成交后更新
        return

def _roots_from_config(js):
    roots = []
    subs = [str(x) for x in js.get("subscribe symbol", [])]
    ex_map = {str(k): str(v) for k, v in js.get("exchanges", {}).items()}
    for s in subs:
        if s in ex_map:
            roots.append(f"{s}.{ex_map[s]}")
    return roots


def _dataset_days(cwd_ds, data_type, roots, begin_day, end_day):
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


def _setup_logger():
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
    _setup_logger()
    cfg_path = os.path.join(os.getcwd(), "test_config.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        js = json.load(f)

    api = client_api.client_api()
    api.init(js.get("server_addr", "tcp://127.0.0.1:50010"))
    api.set_live_mode(False)
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

    roots = _roots_from_config(js)
    api.subscribe_symbol_root(roots)

    # 解析回测日期区间
    bp = str(js.get("backtest_period", "2020-01-01~2025-08-31"))
    begin_day, end_day = bp.split("~")
    begin_day = begin_day.strip()
    end_day = end_day.strip()

    # 从 dataset 汇总可用交易日
    api.trading_date_section = _dataset_days(api.cwd, api.data_type, roots, begin_day, end_day)
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
    api.register_section_bar(adapter.handle_section_bar)
    api.register_bar(adapter.handle_bar)

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
