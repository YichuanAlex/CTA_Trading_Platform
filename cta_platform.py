import client_api
import strategy_base
import threading
import os
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import json
import pdb
import time
import sys
import logging
import os
UPLOAD_STRAT_CONF_DIR = os.path.join("strategy", "config")

api = client_api.client_api()

g_result = pd.DataFrame()

global g_result_path
g_result_path = ""
global g_missing_data
g_missing_data = {}

def on_result(result):
    global g_result
    g_result = result
    return

class _Tee(object):
    def __init__(self, *streams):
        self.streams = streams
    def write(self, data):
        for s in self.streams:
            try:
                s.write(data)
            except Exception:
                pass
    def flush(self):
        for s in self.streams:
            try:
                s.flush()
            except Exception:
                pass
class _LogWriter(object):
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
    def write(self, data):
        try:
            s = str(data)
        except Exception:
            s = ""
        if not s:
            return
        for line in s.splitlines():
            t = line.strip()
            if t:
                self.logger.log(self.level, t)
    def flush(self):
        return

def _setup_run_logger(strategy_name, begin_date, end_date, cfg=None):
    try:
        base = os.getcwd()
        log_dir = os.path.join(base, "log")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        fn = f"{strategy_name.replace('.py','')}_{begin_date}_{end_date}.log"
        log_path = os.path.join(log_dir, fn)
        from logging.handlers import TimedRotatingFileHandler
        fmt = logging.Formatter('[%(levelname)s] %(message)s')
        root = logging.getLogger()
        root.handlers = []
        root_level = logging.INFO
        if isinstance(cfg, dict):
            try:
                root_level = getattr(logging, str(cfg.get("log_level", "INFO")).upper(), logging.INFO)
            except Exception:
                root_level = logging.INFO
        root.setLevel(root_level)
        ch = logging.StreamHandler()
        ch.setLevel(root_level)
        ch.setFormatter(fmt)
        fh = TimedRotatingFileHandler(log_path, when="midnight", interval=1, backupCount=0, encoding="utf-8", delay=True)
        fh.setLevel(root_level)
        fh.setFormatter(fmt)
        root.addHandler(ch)
        root.addHandler(fh)
        sys.stdout = _LogWriter(root, logging.INFO)
        sys.stderr = _LogWriter(root, logging.ERROR)
        logging.getLogger('matplotlib').setLevel(logging.WARNING)
    except Exception:
        pass

def main(list_config_fn):
    print("[debug] main() started")
    logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')
    global g_result_path
    global g_missing_data
    dict_strat_balance_ratio = {}
    trading_session_by_strat = {}
    dict_user_name = {}
    print(list_config_fn)

    for config_fn in list_config_fn:
        with open(config_fn, 'r') as config_f:
            config_js = json.load(config_f)

        #初始化
        api.init(config_js["server_addr"])

        dict_symbol_root = {}
        begin_date = "9999-99-99"
        end_date = "0000-00-00"
        # 优先使用 test_config.json 的订阅列表（86个期货）
        try:
            subs = [str(x) for x in config_js.get("subscribe symbol", [])]
            ex_map = {str(k): str(v) for k, v in config_js.get("exchanges", {}).items()}
            for s in subs:
                if s in ex_map:
                    dict_symbol_root[f"{s}.{ex_map[s]}"] = 1
        except Exception:
            pass
        # 配置文件目录选择：优先使用上传目录，其次使用本地默认目录（相对路径）
        base_conf_dir = UPLOAD_STRAT_CONF_DIR if os.path.exists(UPLOAD_STRAT_CONF_DIR) else os.path.join("strategy", "config")
        strat_conf_path = os.path.join(base_conf_dir, config_js["user_name"])
        dict_user_name[config_js["user_name"]] = config_js["user_name"]
        cur_strategy_name = config_js["strategy_name"]

        for root, dirs, files in os.walk(strat_conf_path):
            for f in files:
                if ".json" not in f:
                    continue
                fn = os.path.join(root, f)
                with open(fn, 'r') as strat_f:
                    strat_cfg = json.load(strat_f)
                strat_name = f.replace('.json', '.py')
                if strat_name != os.path.basename(cur_strategy_name):
                    continue
                else:
                    g_result_path = os.path.basename(cur_strategy_name)
                for symbol_root in strat_cfg["subscribe_symbol"]:   
                    dict_symbol_root[symbol_root] = 1
                if begin_date > strat_cfg["start_date"]:
                    begin_date = strat_cfg["start_date"]
                if end_date < strat_cfg["end_date"]:
                    end_date = strat_cfg["end_date"]
                dict_strat_balance_ratio[strat_name] = strat_cfg["acct_balance_ratio"]
                if strat_name not in trading_session_by_strat.keys():
                    trading_session_by_strat[strat_name] = {}
                trading_session_by_strat[strat_name]["begin_date"] = strat_cfg["start_date"]
                trading_session_by_strat[strat_name]["end_date"] = strat_cfg["end_date"]
                trading_session_by_strat[strat_name]["subscribe_symbol"] = strat_cfg["subscribe_symbol"]
                print("[cta_platform] load config file [%s]" % (fn))

        # 按品种订阅合约（来自 test_config.json 的完整列表）
        api.subscribe_symbol_root(list(dict_symbol_root.keys()))
        print("[cta_platform] subscribe roots: %s" % str(list(dict_symbol_root.keys())))

        # 使用 test_config.json 的回测区间
        bp = str(config_js.get("backtest_period", f"{begin_date}~{end_date}"))
        try:
            if "~" in bp:
                begin_date = bp.split("~")[0].strip()
                end_date = bp.split("~")[1].strip()
        except Exception:
            pass
        if config_js["is_live_mode"]:
            end_date = str(datetime.datetime.now()).split(' ')[0]
            
        api.set_date_section(begin_date, end_date)
        logging.info("[cta_platform] set_date_section: begin=%s end=%s", begin_date, end_date)
        try:
            for sname in list(trading_session_by_strat.keys()):
                trading_session_by_strat[sname]["begin_date"] = begin_date
                trading_session_by_strat[sname]["end_date"] = end_date
            logging.info("[cta_platform] override strategy session: begin=%s end=%s", begin_date, end_date)
        except Exception:
            pass
        _setup_run_logger(cur_strategy_name, begin_date, end_date, config_js)
        g_result_path = f"{cur_strategy_name.replace('.py','')}_{begin_date}_{end_date}"
        download_mode = str(config_js.get("download_mode", "full")).lower()
        skip_missing_data = bool(config_js.get("skip_missing_data", True))
        try:
            logging.getLogger("match_engine").setLevel(getattr(logging, str(config_js.get("log_level_match_engine", "WARNING")).upper(), logging.WARNING))
            logging.getLogger("account_manager").setLevel(getattr(logging, str(config_js.get("log_level_account_manager", "WARNING")).upper(), logging.WARNING))
        except Exception:
            pass

        dict_daily_symbols = {}
        if config_js["is_main_symbol_only"]:
            for symbol_root in dict_symbol_root.keys():
                exchange_id = symbol_root.split('.')[-1]
                df_main_symbol = api.get_main_symbol_info(symbol_root)
                for tup in df_main_symbol.itertuples():
                    tday_raw = str(getattr(tup, "TradingDay"))
                    # 规范化 TradingDay 为 YYYY-MM-DD
                    if '-' in tday_raw or '_' in tday_raw:
                        tday = tday_raw.replace("_", "-")
                    elif len(tday_raw) == 8 and tday_raw.isdigit():
                        tday = tday_raw[0:4] + '-' + tday_raw[4:6] + '-' + tday_raw[6:8]
                    else:
                        try:
                            import pandas as pd
                            tday = pd.to_datetime(tday_raw).strftime("%Y-%m-%d")
                        except Exception:
                            continue
                    if tday in api.trading_date_section:
                        if tday not in dict_daily_symbols.keys():
                            dict_daily_symbols[tday] = {}
                        main_symbol = getattr(tup, "MainInst") +"." + exchange_id
                        second_symbol = getattr(tup, "SecInst") + "." + exchange_id
                        third_symbol = getattr(tup, "SecInstFwd") + "." + exchange_id
                        dict_daily_symbols[tday][main_symbol] = main_symbol
                        dict_daily_symbols[tday][second_symbol] = second_symbol
                        dict_daily_symbols[tday][third_symbol] = third_symbol

        api.set_is_option(config_js["is_option"])
        # 登录
        ret = api.login(config_js["user_name"], config_js["user_passwd"])
        if ret:
            print("login success")
        else:
            print("login failed, continue in offline mode")

        if config_js["is_main_symbol_only"]:
            api.dict_daily_symbol = dict_daily_symbols

        strat = strategy_base.strategy_base()
        api.register_bod(strat.on_bod)
        api.register_eod(strat.on_eod)
        #注册截面bar数据回调函数
        api.register_section_bar(strat.handle_section_bar)
        #注册bar数据回调函数
        api.register_bar(strat.handle_bar)
        # 注册回测结果回调函数
        api.register_result_cb(on_result)
        api.set_live_mode(config_js["is_live_mode"])
        #设置回测数据粒度
        # 1m：分钟；1d：日
        api.set_data_type(config_js["data_type"])
        #设置初始资金
        api.set_balance(config_js["init_money"])
        #设置保证金比例
        api.set_margin_rate(config_js["margin_rate"])
        #设置滑点类型
        api.set_slippage_type(config_js["slippage_type"])
        #设置滑点值
        api.set_slippage_value(config_js["slippage_value"])
        #设置买入手续费类型
        api.set_buy_fee_type(config_js["buy_fee_type"])
        #设置买入手续费
        api.set_buy_fee_value(config_js["buy_fee_value"])
        #设置卖出手续费类型
        api.set_sell_fee_type(config_js["sell_fee_type"])
        #设置卖出手续费
        api.set_sell_fee_value(config_js["sell_fee_value"])
        # 设置成交方法：'close' 以 bar 收盘价；'vwap' 以 bar 内 成交额/成交量；'open' 以 bar 开盘价
        api.set_deal_type(config_js["deal_type"])
        #设置最大成交比例
        api.set_max_deal_pct(config_js["max_deal_pct"])
        #设置是否考虑市场成交量
        api.set_check_market_volume(config_js["is_check_market_volume"])
        #设置是否参与夜盘
        api.set_night_trade(config_js["night_trade"])
        #策略初始化
        print("[debug] strat.init() called")
        strat.init(api, dict_user_name, dict_strat_balance_ratio, trading_session_by_strat)
        print("[debug] strat.init() returned")
        try:
            api.match_engine.set_log_config(config_js.get("log_level_match_engine", None), config_js.get("log_sample_n", 100))
            api.account_manager.set_log_config(config_js.get("log_level_account_manager", None), config_js.get("log_sample_n", 100))
        except Exception:
            pass
        if download_mode == "incremental":
            try:
                if hasattr(api, "remote_enabled") and (not api.remote_enabled):
                    logging.info("[download][skip] remote disabled")
                else:
                    g_missing_data = {}
                    try:
                        days = list(api.trading_date_section or [])
                        for root_symbol in dict_symbol_root.keys():
                            info = api.compute_missing_days_by_root(root_symbol)
                            head_ok = bool(info.get("head_ok"))
                            tail_ok = bool(info.get("tail_ok"))
                            missing_days = list(info.get("missing_days") or [])
                            if head_ok and tail_ok:
                                logging.info("[download][skip] %s covered, head&tail exist", root_symbol)
                                continue
                            if len(missing_days) == 0:
                                logging.info("[download][skip] %s no missing days", root_symbol)
                                continue
                            logging.info("[download][incremental] %s missing=%s", root_symbol, str(missing_days))
                            from concurrent.futures import ThreadPoolExecutor, as_completed
                            tasks = []
                            with ThreadPoolExecutor(max_workers=min(8, max(1, len(missing_days)))) as ex:
                                for d in missing_days:
                                    syms = list(api.get_daily_symbols(d) or [])
                                    for s in syms:
                                        if api.get_symbol_root(s) != root_symbol:
                                            continue
                                        exchange_id = s.split(".")[-1] if "." in s else ""
                                        def _job(day=d, symbol=s, exid=exchange_id):
                                            try:
                                                if api.get_is_option():
                                                    df = api.get_option_bar(day, api.data_type, exid, symbol)
                                                else:
                                                    if exid in ("SH", "SZ"):
                                                        df = api.get_stock_index_minute_bar(day, symbol)
                                                    else:
                                                        df = api.get_future_bar(day, api.data_type, exid, symbol)
                                                if not isinstance(df, pd.DataFrame) or df.shape[0] == 0:
                                                    logging.warning("[download][missing] %s %s %s", root_symbol, day, symbol)
                                                    g_missing_data.setdefault(root_symbol, {}).setdefault(day, []).append(symbol)
                                                    return False
                                                return True
                                            except Exception:
                                                logging.warning("[download][error] %s %s %s", root_symbol, day, symbol)
                                                g_missing_data.setdefault(root_symbol, {}).setdefault(day, []).append(symbol)
                                                return False
                                        tasks.append(ex.submit(_job))
                                for t in as_completed(tasks):
                                    _ = t.result()
                            if not skip_missing_data:
                                has_missing = root_symbol in g_missing_data and any(len(v)>0 for v in g_missing_data[root_symbol].values())
                                if has_missing:
                                    logging.warning("[download][abort] missing segments for %s", root_symbol)
                    except Exception:
                        pass
            except Exception:
                pass
        t1 = time.time()
        # 开始回测
        print("[debug] api.start() called")
        api.start()
        print("[debug] api.start() returned")
        if api.get_live_mode():
            while True:
                if api.is_done():
                    break
                else:
                    time.sleep(1)
            
            # get live market data
            import live_md.live_md as live_md_api
            trading_day = str(datetime.datetime.now()).split(' ')[0]
            # 与 client_api.get_daily_symbols 的键一致，传入 'YYYY-MM-DD' 格式
            list_symbols = api.get_daily_symbols(trading_day)
            df_trading_day = api.get_trading_day()
            dict_pre_trading_day = {}
            if df_trading_day is not None:
                pre_tday = ""
                for tup in df_trading_day.itertuples():
                    tday = getattr(tup, "trade_day")
                    dict_pre_trading_day[tday] = pre_tday
                    pre_tday = tday
            print(list_symbols)
            pre_trading_day = dict_pre_trading_day[trading_day]
            live_md_api.get_live_md(list_symbols, pre_trading_day.replace('-', ""), trading_day.replace('-', ""), strat.handle_section_bar) 
    
        #等待回测结束
        print("[debug] api.join() called")
        api.join()
        print("[debug] api.join() returned")
        t2 = time.time()
        t = t2 - t1
        print("耗时: {}m{}s".format(int(t / 60), int(t) % 60))
        return

def execute(list_config_fn):
        th = threading.Thread(target=main, args=(list_config_fn,))
        th.start()
        while True:
            if api.is_done() and not api.get_live_mode():
                time.sleep(5)
                df_net = g_result[0].copy()
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
                if "net_value" in df_net.columns:
                    s = pd.to_numeric(df_net["net_value"], errors="coerce").astype(float)
                    s = s.replace([float("inf"), float("-inf")], pd.NA).ffill().bfill()
                    if s.shape[0] > 0:
                        if not (bool(s.nunique() > 1) or bool(s.iloc[-1] != s.iloc[0])):
                            logging.warning("净值未发生变化，可能未产生交易：%.6f", float(s.iloc[-1]))
                try:
                    (df_net["net_value"] if "net_value" in df_net.columns else df_net).plot(ax=ax)
                except Exception:
                    pass
                ax.set_title("Net Value")
                ax.grid(True)
                fig.tight_layout()
                cwd = os.getcwd()
                result_path = cwd + "/result/" + g_result_path + "/"
                if not os.path.exists(result_path):
                    os.makedirs(result_path)
                try:
                    df_net_out = df_net.reset_index()
                    df_net_out.to_csv(result_path + "/net_value.csv", header=True, index=False)
                except Exception:
                    g_result[0].to_csv(result_path + "/net_value.csv", header=True)
                g_result[1].to_csv(result_path + "/trade_order.csv", header=True, index=False)
                str_evaluating_indicator = (
                    f"年化收益率：{g_result[2]}\n"
                    f"最大回撤：{g_result[3]}\n"
                    f"年化波动率：{g_result[4]}\n"
                    f"夏普率：{g_result[5]}\n"
                    f"日胜率：{g_result[6]}\n"
                    f"月胜率：{g_result[7]}\n"
                    f"日盈亏比：{g_result[8]}\n"
                    f"月盈亏比：{g_result[9]}\n"
                )
                logging.info(str_evaluating_indicator)
                f = open(result_path + "/evaluating_indicator.txt", "w")
                f.write(str_evaluating_indicator)
                f.close()
                try:
                    if isinstance(g_missing_data, dict) and len(g_missing_data) > 0:
                        with open(result_path + "/evaluating_indicator.txt", "a", encoding="utf-8") as f2:
                            f2.write("\n缺失数据：\n")
                            for r, days in g_missing_data.items():
                                for d, syms in days.items():
                                    f2.write(f"{r} {d} {','.join(syms)}\n")
                except Exception:
                    pass
                #plt.show()
                fig_name = result_path + "/pnl.png"
                plt.savefig(fig_name)
                try:
                    plt.close(fig)
                except Exception:
                    pass
                api.stop()
                api.join()
                logging.shutdown()
                sys.exit(0)
            else:
                time.sleep(1)
                if api.get_live_mode():
                    cur_tm = str(datetime.datetime.now()).split(' ')[-1].split('.')[0]
                    if cur_tm >= "17:00:00":
                        logging.shutdown()
                        sys.exit(0)
        return

if __name__ == "__main__":
        if len(sys.argv) < 2:
            print("usage: python cta_platform.py <config1.json> [config2.json ...]")
            exit(0)
        list_config_fn = sys.argv[1:]
        th = threading.Thread(target=main, args=(list_config_fn,))
        th.start()
        
        while True:
            if api.is_done() and not api.get_live_mode():
                time.sleep(5)
                df_net = g_result[0].copy()
                try:
                    if "trading_day" in df_net.columns:
                        df_net["trading_day"] = pd.to_datetime(df_net["trading_day"])
                        df_net = df_net.set_index("trading_day").sort_index()
                except Exception:
                    pass
                fig = plt.figure(figsize=(12, 4))
                ax = fig.add_subplot(1,1,1)
                if "net_value" in df_net.columns:
                    s = pd.to_numeric(df_net["net_value"], errors="coerce").astype(float)
                    s = s.replace([float("inf"), float("-inf")], pd.NA).ffill().bfill()
                    if s.shape[0] > 0:
                        if not (bool(s.nunique() > 1) or bool(s.iloc[-1] != s.iloc[0])):
                            logging.warning("净值未发生变化，可能未产生交易：%.6f", float(s.iloc[-1]))
                try:
                    (df_net["net_value"] if "net_value" in df_net.columns else df_net).plot(ax=ax)
                except Exception:
                    pass
                ax.set_title("Net Value")
                ax.grid(True)
                fig.tight_layout()
                cwd = os.getcwd()
                result_path = cwd + "/result/" + g_result_path + "/"
                if not os.path.exists(result_path):
                    os.makedirs(result_path)
                try:
                    df_net_out = df_net.reset_index()
                    df_net_out.to_csv(result_path + "/net_value.csv", header=True, index=False)
                except Exception:
                    g_result[0].to_csv(result_path + "/net_value.csv", header=True)
                g_result[1].to_csv(result_path + "/trade_order.csv", header=True, index=False)
                str_evaluating_indicator = (
                    f"年化收益率：{g_result[2]}\n"
                    f"最大回撤：{g_result[3]}\n"
                    f"年化波动率：{g_result[4]}\n"
                    f"夏普率：{g_result[5]}\n"
                    f"日胜率：{g_result[6]}\n"
                    f"月胜率：{g_result[7]}\n"
                    f"日盈亏比：{g_result[8]}\n"
                    f"月盈亏比：{g_result[9]}\n"
                )
                logging.info(str_evaluating_indicator)
                f = open(result_path + "/evaluating_indicator.txt", "w")
                f.write(str_evaluating_indicator)
                f.close()
                fig_name = result_path + "/pnl.png"
                plt.savefig(fig_name)
                try:
                    plt.close(fig)
                except Exception:
                    pass
                api.stop()
                api.join()
                logging.shutdown()
                sys.exit(0)
            else:
                time.sleep(1)
                if api.get_live_mode():
                    cur_tm = str(datetime.datetime.now()).split(' ')[-1].split('.')[0]
                    if cur_tm >= "17:00:00":
                        logging.shutdown()
                        sys.exit(0)
