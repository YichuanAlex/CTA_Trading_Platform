import math
import pdb

# 计算年化收益率
#（nav[-1]/nav[0]）^（252/回测区间交易日天数）－1
def calculate_annualized_return(nav, trading_day_count):
    try:
        if trading_day_count is None or trading_day_count <= 0:
            return 0.0
        base = float(nav[0])
        end = float(nav[-1])
        if base <= 0.0 or end <= 0.0:
            return 0.0
        ratio = end / base
        return ratio ** (252 / float(trading_day_count)) - 1.0
    except Exception:
        return 0.0

# 计算最大回撤
#在每一个点算该点投入后的回撤，然后取最值
def calculate_max_drawdown(list_net_value):
    try:
        if not list_net_value or len(list_net_value) == 0:
            return 0.0
        peak = float(list_net_value[0])
        max_dd = 0.0
        for v in list_net_value:
            try:
                x = float(v)
            except Exception:
                continue
            if x > peak:
                peak = x
            if peak > 0.0:
                dd = (peak - x) / peak
            else:
                dd = 0.0
            if dd > max_dd:
                max_dd = dd
        return float(max_dd)
    except Exception:
        return 0.0

# 计算年化波动率
#先用净值计算日收益率，求std，后乘sqrt(252)
def calculate_annualized_volatility(df):
    try:
        s = df.copy()
        try:
            import pandas as pd
            s = pd.to_numeric(s, errors="coerce")
            s = s.dropna()
        except Exception:
            pass
        vol = s.std() * math.sqrt(252)
        try:
            if vol != vol or vol == float("inf") or vol == float("-inf"):
                return 0.0
        except Exception:
            pass
        return float(vol)
    except Exception:
        return 0.0

# 计算夏普率
#年化收益率/年化波动率
def calculate_sharp_rate(annualized_return, annualized_volatility):
    try:
        if annualized_volatility is None:
            return 0.0
        vol = float(annualized_volatility)
        if vol == 0.0:
            return 0.0
        return float(annualized_return) / vol
    except Exception:
        return 0.0

# 计算胜率
#收益率为正的天数占比
def calculate_win_rate(list_diff):
    try:
        if len(list_diff) == 0:
            return 0.0
        s = []
        for x in list_diff:
            try:
                fx = float(x)
                if fx == float("inf") or fx == float("-inf") or fx != fx:
                    continue
                s.append(fx)
            except Exception:
                continue
        if len(s) == 0:
            return 0.0
        win_day_count = 0
        for i in range(len(s)):
            if s[i] > 0.0:
                win_day_count += 1
        return win_day_count / len(s)
    except Exception:
        return 0.0

# 计算盈亏比
#收益率为正的平均收益率：收益率为负的平均收益率
def calculate_win_loss_rate(list_diff):
    try:
        s = []
        for x in list_diff:
            try:
                fx = float(x)
                if fx == float("inf") or fx == float("-inf") or fx != fx:
                    continue
                s.append(fx)
            except Exception:
                continue
        if len(s) == 0:
            return 0.0
        wins = [x for x in s if x > 0.0]
        losses = [x for x in s if x < 0.0]
        if len(wins) == 0 or len(losses) == 0:
            return 0.0
        win_avg_return = sum(wins) / float(len(wins))
        loss_avg_return = abs(sum(losses) / float(len(losses)))
        if loss_avg_return <= 0.0:
            return 0.0
        return float(win_avg_return / loss_avg_return)
    except Exception:
        return 0.0
