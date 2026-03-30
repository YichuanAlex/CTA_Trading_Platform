import json
import sys
import os
import datetime

def download_data_parallelism():
    # 读取配置文件（包含 start_date 与 end_date）
    config_f = open("config.json")
    config_js = json.load(config_f)
    config_f.close()
    begin_date = config_js["start_date"]
    end_date = config_js["end_date"]
    begin_dt = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_dt - begin_dt).days
    parall_num = 5
    avg_days = int(total_days / parall_num) if parall_num > 0 else total_days
    
    for i in range(parall_num):
        begin_date = str(begin_dt).split(' ')[0]
        stop_dt = begin_dt + datetime.timedelta(days=avg_days)
        end_date = str(stop_dt).split(' ')[0]
        cmd = "start python download_data.py " + begin_date + " " + end_date
        os.system(cmd)
        begin_dt = stop_dt
    
    if begin_dt < end_dt:
        begin_date = str(begin_dt).split(' ')[0]
        end_date = str(end_dt).split(' ')[0]
        cmd = "start python download_data.py " + begin_date + " " + end_date
        os.system(cmd)
    
    return

if __name__ == "__main__":
    download_data_parallelism()
