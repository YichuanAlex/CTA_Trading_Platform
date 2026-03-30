import os
import json
import datetime
import math
import random
import pandas as pd

def _day_list(begin_day, end_day):
    d0 = datetime.datetime.strptime(begin_day, "%Y-%m-%d")
    d1 = datetime.datetime.strptime(end_day, "%Y-%m-%d")
    days = []
    cur = d0
    while cur <= d1:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += datetime.timedelta(days=1)
    return days

def _gen_times():
    """
    生成更贴近真实的交易时间窗口（仅日盘）
    - 09:00~10:14
    - 10:30~11:30
    - 13:30~14:57
    """
    def _range(start, end):
        base = datetime.datetime.strptime(start, "%H:%M:%S")
        end_dt = datetime.datetime.strptime(end, "%H:%M:%S")
        out = []
        cur = base
        while cur <= end_dt:
            out.append(cur.strftime("%H:%M:%S"))
            cur += datetime.timedelta(minutes=1)
        return out
    return (
        _range("09:00:00", "10:14:00")
        + _range("10:30:00", "11:30:00")
        + _range("13:30:00", "14:57:00")
    )

def _root_prefix(root):
    base = root.split(".")[0]
    prefix = ""
    for ch in base:
        if ch.isdigit():
            break
        prefix += ch
    return prefix if prefix else base

def _nearest_month_code(day_str):
    """
    根据交易日生成最近月合约代码（YYMM）
    """
    try:
        d = datetime.datetime.strptime(day_str, "%Y-%m-%d")
        yy = d.year % 100
        mm = d.month
        return f"{yy:02d}{mm:02d}"
    except Exception:
        return "2509"

def _symbol_name(root, exch, trading_day):
    return _root_prefix(root) + _nearest_month_code(trading_day) + "." + exch

def _base_price_for_root(root):
    s = sum(ord(c) for c in root)
    return 1000.0 + (s % 1000)

def _ensure_dirs(base_root, exch, root, day):
    p1 = os.path.join(base_root, exch, root, day)
    if not os.path.exists(p1):
        os.makedirs(p1)
    return p1

def _write_csv(fp, rows):
    df = pd.DataFrame(rows, columns=[
        "symbol","date_time","trading_day","open","high","low","close",
        "turnover","volume","open_interest","pre_close","bid_price1",
        "bid_size1","ask_price1","ask_size1"
    ])
    df.to_csv(fp, index=False)

def generate_mock_1m():
    cwd = os.getcwd()
    cfg_path = os.path.join(cwd, "test_config.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        js = json.load(f)
    subs = [str(x) for x in js.get("subscribe symbol", [])]
    ex_map = {str(k): str(v) for k, v in js.get("exchanges", {}).items()}
    bp = str(js.get("backtest_period", "2024-08-31~2025-08-31"))
    begin_day, end_day = [x.strip() for x in bp.split("~")]
    days = _day_list(begin_day, end_day)
    times = _gen_times()
    bases = [
        os.path.join(cwd, "data", "1m"),
        os.path.join(cwd, "dataset", "data", "1m")
    ]
    prev_close_map = {}
    random.seed(12345)
    # 为每个root生成基础参数（波动与成交规模）
    root_params = {}
    for root_key in subs:
        if root_key not in ex_map:
            continue
        exch = ex_map[root_key]
        root = f"{root_key}.{exch}"
        if root not in root_params:
            base_px = _base_price_for_root(root)
            # 根品种级别的日波动与成交规模
            vol_scale = 0.004 + (sum(ord(c) for c in root_key) % 7) * 0.0004
            vol_base = (sum(ord(c) for c in root_key) % 300) + 300
            oi_base = 10000 + (sum(ord(c) for c in root_key) % 5000)
            root_params[root] = {
                "base_px": base_px,
                "vol_scale": vol_scale,
                "vol_base": vol_base,
                "oi_base": oi_base
            }
        base_px = root_params[root]["base_px"]
        for day_idx, day in enumerate(days):
            symbol = _symbol_name(root, exch, day)
            rows = []
            tday_str = day
            oi0 = root_params[root]["oi_base"] + day_idx * 50
            prev_close = prev_close_map.get(root, base_px)
            # 日内均值回复+轻微漂移
            mu = prev_close * (1.0 + random.uniform(-0.002, 0.002))
            kappa = 0.12
            sigma = root_params[root]["vol_scale"]
            for i, tm in enumerate(times):
                dt_str = f"{day} {tm}"
                # OU过程
                eps = random.gauss(0.0, 1.0)
                close = prev_close + kappa * (mu - prev_close) + sigma * eps * prev_close
                close = max(0.01, close)
                # K线价格
                spread = max(0.01, close * 0.0005)
                open_ = close + random.uniform(-spread, spread)
                high = max(open_, close) + abs(random.uniform(0, spread))
                low = min(open_, close) - abs(random.uniform(0, spread))
                # 成交与持仓
                vbase = root_params[root]["vol_base"]
                volume = float(max(1, int(vbase * (1.0 + random.uniform(-0.35, 0.35)))))
                turnover = float(close * volume)
                oi = float(oi0 + max(0, int(i * (1.0 + random.uniform(-0.2, 0.2)))))
                # 深度
                bid_p = close - spread / 2.0
                ask_p = close + spread / 2.0
                bid_s = int(max(1, volume * 0.05))
                ask_s = int(max(1, volume * 0.05))
                rows.append([
                    symbol, dt_str, tday_str, open_, high, low, close,
                    turnover, volume, oi, prev_close, bid_p, bid_s, ask_p, ask_s
                ])
                prev_close = close
            prev_close_map[root] = prev_close
            for b in bases:
                dirp = _ensure_dirs(b, exch, root, day)
                fp = os.path.join(dirp, symbol)
                _write_csv(fp, rows)

if __name__ == "__main__":
    generate_mock_1m()
