import multiprocessing
import cta_platform
import sys
import json
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os

def exec():
    """
    并行执行多个配置，聚合净值并输出图表
    - 启动独立进程执行回测
    - 聚合各策略净值为单一时间序列（对齐交易日）
    - 保存聚合净值曲线到 result 目录
    """
    list_config_fn = sys.argv[1:]
    list_process = []
    for config_fn in list_config_fn:
        process = multiprocessing.Process(target=cta_platform.execute, args=([config_fn],))
        process.start()
        list_process.append(process)
    for process in list_process:
        process.join()
    df_net_value = pd.DataFrame()
    for config_fn in list_config_fn:
        conf_f = open(config_fn)
        conf_js = json.load(conf_f)
        conf_f.close()
        user_name = conf_js["user_name"]
        strategy_name = conf_js["strategy_name"]
        result_path = "./result/" + user_name + "/" + strategy_name + "/"
        net_value_fn = result_path + "net_value.csv"
        df = pd.read_csv(net_value_fn)
        if df_net_value.shape[0] == 0:
            df_net_value = df.copy()
        else:
            try:
                if "trading_day" in df.columns:
                    df["trading_day"] = pd.to_datetime(df["trading_day"])
                    df_net_value["trading_day"] = pd.to_datetime(df_net_value["trading_day"])
                    df_net_value = df_net_value.merge(df[["trading_day","net_value"]], on="trading_day", how="outer", suffixes=("_old","_new")).sort_values("trading_day")
                    old = df_net_value["net_value_old"].fillna(1.0)
                    new = df_net_value["net_value_new"].fillna(1.0)
                    df_net_value["net_value"] = old + new - 1.0
                    df_net_value = df_net_value[["trading_day","net_value"]]
                else:
                    df_net_value["net_value"] = df_net_value["net_value"] + df["net_value"] - 1.0
            except Exception:
                df_net_value["net_value"] = df_net_value.get("net_value", df_net_value.iloc[:, -1]) + df.iloc[:, -1] - 1.0
    try:
        if "trading_day" in df_net_value.columns:
            df_net_value["trading_day"] = pd.to_datetime(df_net_value["trading_day"])
            df_net_value = df_net_value.set_index("trading_day").sort_index()
    except Exception:
        pass
    fig = plt.figure(figsize=(12, 4))
    ax = fig.add_subplot(1,1,1)
    try:
        (df_net_value["net_value"] if "net_value" in df_net_value.columns else df_net_value).plot(ax=ax)
    except Exception:
        pass
    ax.set_title("Net Value (Aggregated)")
    ax.grid(True)
    fig.tight_layout()
    fig_dir = os.path.join(os.getcwd(), "result", user_name)
    try:
        if not os.path.exists(fig_dir):
            os.makedirs(fig_dir)
    except Exception:
        pass
    fig_name = os.path.join(fig_dir, str(datetime.datetime.now()).split('.')[0].replace(':', "").replace(' ', "_") + ".png")
    plt.savefig(fig_name)
    try:
        plt.close(fig)
    except Exception:
        pass
    return

if __name__ == "__main__":
    exec()
