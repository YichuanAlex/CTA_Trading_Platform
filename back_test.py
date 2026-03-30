import client_api
import strategy_base
import threading
import os
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import json
import pdb
# 具体策略类请按需导入
api = client_api.client_api()
g_result = pd.DataFrame()
def on_result(result):
    global g_result
    g_result = result
    return
def main():
    config_f = open("config.json")
    config_js = json.load(config_f)
    config_f.close()
    #初始化
    api.init(config_js["server_addr"])
    # 按品种订阅合约
    api.subscribe_symbol_root(config_js["subscribe_symbol"])
    # 设置回测开始日期和结束日期
    api.set_date_section(config_js["start_date"], config_js["end_date"])
    #登录
    ret = api.login(config_js["user_name"], config_js["user_passwd"])
    if ret:
        print("login success")
    else:
        print("login failed")
        return
    strat = strategy_base.strategy_base()
    api.register_bod(strat.on_bod)
    api.register_eod(strat.on_eod)
    #注册截面bar数据回调函数
    api.register_option_section_bar(strat.handle_section_bar)
    # 注册bar数据回调函数
    api.register_option_bar(strat.handle_bar)
    #注册回测结果回调函数
    api.register_result_cb(on_result)
    #设置回测数据粒度
    # 1m：分钟；1d：日
    api.set_data_type(config_js["data_type"])
    #设置初始资金
    api.set_balance(config_js["init_money"])
    #设置无风险收益率
    api.set_riskless_rate(config_js["riskless_rate"])
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
    #设置成交方法‘close'：以bar收盘价成交，"vwap'：以bar内"成交额/成交量”成交
    api.set_deal_type(config_js["deal_type"])
    #设置最大成交比例
    api.set_max_deal_pct(config_js["max_deal_pct"])
    #设置是否考虑市场成交量
    api.set_check_market_volume(config_js["is_check_market_volume"])
    #设置是否参与夜盘
    api.set_night_trade(config_js["night_trade"])
    #策略初始化
    strat.init(api, {}, {}, {})
    # 开始回测
    api.start()
    # 等待回测结束
    api.join()
    return

if __name__ == "__main__":
    th = threading.Thread(target=main)
    th.start()
    while True:
        cmd = input("input your cmd:")
        if cmd == "quit" or cmd == "exit":
            api.stop()
            api.join()
            os._exit(0)
        elif cmd == "result":
            g_result[0].plot()
            cwd = os.getcwd()
            result_path = cwd + "/result/"
            if not os.path.exists(result_path):
                os.makedirs(result_path)
            g_result[0].to_csv(result_path + "/net_value.csv", header=True)
            g_result[1].to_csv(result_path + "/trade_order.csv", header=True, index=False)
            str_evaluating_indicator="年化收益率：%f\n最大回撤：%f\n年化波动率：%f\n夏普率：%f\n日胜率：%f\n月胜率：%f\n日盈亏比：%f\n月盈亏比：%f\n"\
                 % (g_result[2], g_result[3], g_result[4], g_result[5], g_result[6], g_result[7], g_result[8], g_result[9])
            print(str_evaluating_indicator)
            f = open(result_path + "/evaluating_indicator.txt", "w")
            f.write(str_evaluating_indicator)
            f.close()
            plt.show()
        else:
            print("unknow cmd")
