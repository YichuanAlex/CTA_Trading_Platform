import os
import sys
import json
import datetime

try:
    import client_api
except ModuleNotFoundError as e:
    print(str(e))
    sys.exit(1)


def _noop(*args, **kwargs):
    return


def _build_roots(cfg, base):
    subs = cfg.get("subscribe symbol", []) or []
    ex_map = cfg.get("exchanges", {}) or {}
    data_type = str(cfg.get("data_type", "1m"))
    scan_data_type = data_type if data_type.lower() != "5m" else "1m"
    roots = []
    for s in subs:
        ex = ex_map.get(s)
        if ex:
            roots.append(f"{s}.{ex}")
    data_root = os.path.join(base, "data", scan_data_type)
    if (not os.path.isdir(data_root)) and os.path.isdir(os.path.join(base, "data", "data", scan_data_type)):
        data_root = os.path.join(base, "data", "data", scan_data_type)
    if os.path.exists(data_root) and os.path.isdir(data_root):
        for ex in os.listdir(data_root):
            ex_dir = os.path.join(data_root, ex)
            if not os.path.isdir(ex_dir):
                continue
            for r in os.listdir(ex_dir):
                r_dir = os.path.join(ex_dir, r)
                if not os.path.isdir(r_dir):
                    continue
                base_sym = r.split(".")[0]
                if subs:
                    if base_sym in subs and r not in roots:
                        roots.append(r)
                else:
                    if r not in roots:
                        roots.append(r)
    return roots

def _roots_from_server(api):
    roots = []
    try:
        df = api.get_daily_symbol()
        if df is not None and hasattr(df, "itertuples"):
            cols = list(df.columns)
            def pick_col(candidates):
                for c in candidates:
                    if c in cols:
                        return c
                    for col in cols:
                        if col.lower() == c.lower():
                            return col
                return None
            col_pid = pick_col(["product_id","product","ProductId"]) or "product_id"
            for row in df.itertuples():
                try:
                    pid = str(getattr(row, col_pid))
                    if pid and pid not in roots:
                        roots.append(pid)
                except Exception:
                    continue
    except Exception:
        pass
    return roots

def _print_roots(label, roots):
    try:
        print(f"[dataset] {label} count={len(roots)} roots={roots}")
    except Exception:
        pass


def main():
    start = "2020-01-01"
    end = "2025-08-31"
    if len(sys.argv) >= 3:
        start = sys.argv[1]
        end = sys.argv[2]
    cfg_path = os.path.join(os.getcwd(), "test_config.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    api = client_api.client_api()
    api.init(cfg.get("server_addr", "tcp://127.0.0.1:50010"))
    base = os.path.join(os.getcwd(), "dataset")
    if not os.path.exists(base):
        os.makedirs(base)
    api.cwd = base
    try:
        setattr(api, "dataset_allow_download", True)
    except Exception:
        pass
    # 登录并设置日期区间
    api.set_date_section(start, end)
    ret = api.login(str(cfg.get("user_name", "")), str(cfg.get("user_passwd", "")))
    if not ret:
        print("login failed")
        sys.exit(1)
    # 从配置与服务器枚举汇总根列表
    roots_cfg = _build_roots(cfg, base)
    roots_srv = _roots_from_server(api)
    roots = list({r for r in roots_cfg + roots_srv})
    _print_roots("enumerated roots", roots)
    api.subscribe_symbol_root(roots)
    api.set_is_option(bool(cfg.get("is_option", False)))
    api.set_data_type(str(cfg.get("data_type", "1m")))
    api.set_balance(float(cfg.get("init_money", 1000000)))
    api.set_margin_rate(float(cfg.get("margin_rate", 0.2)))
    api.set_slippage_type(str(cfg.get("slippage_type", "pct")))
    api.set_slippage_value(float(cfg.get("slippage_value", 0.0001)))
    api.set_buy_fee_type(str(cfg.get("buy_fee_type", "pct")))
    api.set_buy_fee_value(float(cfg.get("buy_fee_value", 0.00007)))
    api.set_sell_fee_type(str(cfg.get("sell_fee_type", "pct")))
    api.set_sell_fee_value(float(cfg.get("sell_fee_value", 0.00007)))
    api.set_deal_type(str(cfg.get("deal_type", "close")))
    api.set_max_deal_pct(float(cfg.get("max_deal_pct", 0.2)))
    api.set_check_market_volume(bool(cfg.get("is_check_market_volume", True)))
    api.set_night_trade(bool(cfg.get("night_trade", True)))
    api.set_live_mode(False)
    def _is_day_cached(day):
        data_type = str(getattr(api, "data_type", "1m"))
        scan_data_type = data_type if data_type.lower() != "5m" else "1m"
        for r in roots:
            ex = r.split('.')[-1] if '.' in r else ""
            dirp = os.path.join(base, "data", scan_data_type, ex, r, day)
            if not os.path.isdir(dirp):
                alt = os.path.join(base, "data", "data", scan_data_type, ex, r, day)
                dirp = alt
            if (not os.path.exists(dirp)) or (not os.path.isdir(dirp)):
                return False
            has_file = False
            try:
                for name in os.listdir(dirp):
                    fp = os.path.join(dirp, name)
                    if os.path.isfile(fp):
                        has_file = True
                        break
            except Exception:
                has_file = False
            if not has_file:
                return False
        return True
    if hasattr(api, "trading_date_section") and isinstance(api.trading_date_section, list):
        api.trading_date_section = [d for d in api.trading_date_section if not _is_day_cached(str(d))]
    # 显式逐日下载（期货/指数等），统一写入 dataset/data/...
    days = list(getattr(api, "trading_date_section", []))
    for d in days:
        try:
            api._load_data_(d)
        except Exception:
            pass
    print(os.path.join(base, "data"))
    # 期权数据补充下载：切换为期权模式并枚举期权信息
    # 期权数据补充下载：使用独立实例避免 ZMQ REQ 状态冲突
    try:
        api_opt = client_api.client_api()
        api_opt.init(cfg.get("server_addr", "tcp://127.0.0.1:50010"))
        api_opt.cwd = base
        try:
            setattr(api_opt, "dataset_allow_download", True)
        except Exception:
            pass
        api_opt.set_is_option(True)
        api_opt.set_data_type(str(cfg.get("data_type", "1m")))
        api_opt.set_date_section(start, end)
        ret2 = api_opt.login(str(cfg.get("user_name", "")), str(cfg.get("user_passwd", "")))
        if not ret2:
            print("[dataset] option login failed")
        else:
            api_opt.subscribe_symbol_root(roots)
            # 登录时已调用 get_option_info；无需重复调用，以避免 REQ 状态异常
            df_opt = getattr(api_opt, "df_option_info", None)
            cnt_opt = int(getattr(df_opt, "shape", [0,0])[0]) if df_opt is not None else 0
            print(f"[dataset] option rows={cnt_opt}")
            days2 = list(getattr(api_opt, "trading_date_section", []))
            for d in days2:
                try:
                    api_opt._load_option_data_(d)
                except Exception:
                    pass
    except Exception as e:
        print(f"[dataset] option download failed: {e}")


if __name__ == "__main__":
    main()
