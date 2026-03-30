import client_api
import threading
import os
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import pdb
import json
import sys

api = client_api.client_api()

g_result = pd.DataFrame()

def on_bod(trading_day):
    return

def on_eod(trading_day):
    return

def on_option_bar(bar):
    return

def handle_section_bar(list_bar):
    return
    
def on_result(result):
    return

def download_data(begin_date, end_date):
    print("下载数据，开始日期：[%s] 结束日期：[%s]" % (begin_date, end_date))
    config_f = open("config.json")
    config_js = json.load(config_f)
    config_f.close()

    #初始化
    api.init(config_js["server_addr"])

    #按品种订阅合约
    api.subscribe_symbol_root(config_js["subscribe_symbol"])
    '''
    # 设置是否期权模式（影响数据路径与回放逻辑）
    api.set_is_option(config_js.get("is_option", False))
    '''
    #设置回测开始日期和结束日期
    api.set_date_section(begin_date, end_date)

    #登录
    ret = api.login(config_js["user_name"], config_js["user_passwd"])
    if ret:
        print("login success")
    else:
        print("login failed")
        return

    api.register_bod(on_bod)
    api.register_eod(on_eod)

    # 注册截面bar数据回调函数
    api.register_option_section_bar(handle_section_bar)

    # 注册bar数据回调函数
    api.register_option_bar(on_option_bar)

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

    # 设置成交方法：'close' 以 bar 收盘价；'vwap' 以 bar 内 成交额/成交量；'open' 以 bar 开盘价
    api.set_deal_type(config_js["deal_type"])

    #设置最大成交比例
    api.set_max_deal_pct(config_js["max_deal_pct"])

    #设置是否考虑市场成交量
    api.set_check_market_volume(config_js["is_check_market_volume"])

    #设置是否参与夜盘
    api.set_night_trade(config_js["night_trade"])

    # 开始回测
    api.start()

    # 等待回测结束
    api.join()
    
    return

if __name__ == "__main__":
        download_data(sys.argv[1], sys.argv[2])
