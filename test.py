import client_api
import threading
import os
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import pdb

api = client_api.client_api()
g_result = pd.DataFrame()

def on_bod(trading_day):
    print("\nbegin of day:%s\n" % trading_day)
    return

def on_eod(trading_day):
    print("\nend of day:%s\n" % trading_day)
    return

def on_bar(bar):
    try:
        print(bar.symbol, ",", bar.trading_day, ",", bar.date_time, ",")
        dict_pos = api.get_current_position()
        symbol = bar.symbol
        net_pos = 0.0
        
        if symbol in dict_pos.keys():
            net_pos = dict_pos[symbol]
            
        if abs(net_pos - 0.0) < 0.0000001 and bar.date_time.split(' ')[-1] == "09:01:00":
            api.send_order(symbol, 1, -1)
        elif bar.date_time.split(' ')[-1] >= "14:30:00":
            if net_pos > 0:
                api.send_order(symbol, net_pos, -1)
            elif net_pos < 0:
                api.send_order(symbol, -net_pos, 1)
    except Exception as e:
        print(str(e))
        pdb.set_trace()
    return

def handle_section_bar(list_bar):
    try:
        for bar in list_bar:
            print(bar.option_code, ",", bar.trade_day, ",", bar.rt_time, ",", bar.underlying_code)
    except Exception:
        pass
    return

def on_result(result):
    print(result[0])
    print(result[1])
    global g_result
    g_result = result
    # 获取当前账户总资产
    print(api.get_account_asset())
    return

def test():
    # 初始化
    api.init("tcp://127.0.0.1:50010")
    
    # 按品种订阅合约
    api.subscribe_symbol_root(["A.DCE", "IC.CFFEX", "CU.SHFE"])
    
    # 设置回测开始日期和结束日期
    api.set_date_section("2022-01-05", "2022-01-11")
    
    # 登录
    ret = api.login("test", "test123")
    if ret:
        print("login success")
    else:
        print("login failed")
        return
    
    # 注册回调函数
    api.register_bod(on_bod)
    api.register_eod(on_eod)
    api.register_bar(on_bar)
    api.register_result_cb(on_result)
    api.register_section_bar(handle_section_bar)
    
    # 设置回测数据粒度 (1m:分钟; 1d:日)
    api.set_data_type("1m")
    
    # 设置初始资金
    api.set_balance(1e6)
    
    # 设置保证金比例
    api.set_margin_rate(0.2)
    
    # 设置滑点
    api.set_slippage_type('pct')
    api.set_slippage_value(0.0005)
    
    # 设置手续费
    api.set_buy_fee_type('pct')
    api.set_buy_fee_value(0.00025)
    api.set_sell_fee_type('pct')
    api.set_sell_fee_value(0.00025)
    
    # 设置成交方式 ('close':收盘价; 'vwap':成交额/成交量)
    api.set_deal_type('close')
    
    # 设置最大成交比例
    api.set_max_deal_pct(0.1)
    
    # 设置是否参与夜盘
    api.set_night_trade(False)
    
    # 获取交易日列表
    df = api.get_trading_day()
    print(df)
    
    # 获取期货K线数据
    df = api.get_future_bar("20240531", "1m", "SHFE", "CU2409.SHFE")
    print(df)
    
    df = api.get_future_bar("20240531", "1d", "SHFE", "CU2409.SHFE")
    print(df)
    
    # 获取主力/连续合约数据
    df = api.get_future_origin_daily_bar("CU.SHFE", "20210101", "20240601", "main")
    print(df)
    
    df = api.get_future_spec_daily_bar("CU.SHFE", "20210101", "20240601", "v3")
    print(df)
    
    # 分钟级数据（注释）
    # df = api.get_future_origin_minute_bar("CU.SHFE", "20210101 09:00:00", "20240601 15:00:00", "main")
    # print(df)
    # df = api.get_future_adj_factor_minute_bar("CU.SHFE", "20210101 09:00:00", "20240601 15:00:00")
    # print(df)
    
    # 获取合约信息
    df = api.get_main_symbol_info("CU.SHFE")
    print(df)
    
    df = api.get_main_symbol_adj_factor_info("CU.SHFE")
    print(df)
    
    df = api.get_daily_symbol()
    print(df)
    
    pdb.set_trace()
    
    # 开始回测
    api.start()
    
    # 等待回测结束
    api.join()
    return

if __name__ == "__main__":
    th = threading.Thread(target=test)
    th.start()
    
    while True:
        cmd = input("input your cmd:")
        
        if cmd == "quit" or cmd == "exit":
            api.stop()
            api.join()
            os._exit(0)
            
        elif cmd == "result":
            # 绘制净值曲线
            g_result[0].plot()
            
            # 创建结果目录
            cwd = os.getcwd()
            result_path = cwd + "/result/"
            if not os.path.exists(result_path):
                os.makedirs(result_path)
            
            # 保存结果文件
            g_result[0].to_csv(result_path + "/net_value.csv", header=True)
            g_result[1].to_csv(result_path + "/trade_order.csv", header=True, index=False)
            
            # 打印并保存评估指标
            str_evaluating_indicator = "年化收益率：%f\n最大回撤：%f\n年化波动率：%f\n夏普率：%f\n日胜率：%f\n月胜率：%f\n日盈亏比：%f\n月盈亏比：%f\n" \
                % (g_result[2], g_result[3], g_result[4], g_result[5], g_result[6], g_result[7], g_result[8], g_result[9])
            print(str_evaluating_indicator)
            
            f = open(result_path + "/evaluating_indicator.txt", "w")
            f.write(str_evaluating_indicator)
            f.close()
            
            plt.show()
            
        else:
            print("unknown cmd")
