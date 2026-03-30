import zmq
import uuid
import protocol
import struct
import pickle
import pandas as pd
import logging
import datetime
import threading
import match_engine
import account_manager
import date_time_util
import market_data_type
import numpy as np
import os
import pdb
import traceback
class StopReplayError(Exception):
    pass
try:
    import md_minute_bar_api as md_live_api
except Exception:
    md_live_api = None

class client_api(object):
    def __init__(self) -> None:
        self._zmq_ctx_ = 0
        self._zmq_sock_ = 0
        self.server_addr = "tcp://127.0.0.1:50010"
        self.cur_uuid = str(uuid.uuid4())
        self.user_name = ""
        self.user_pwd = ""
        self.begin_date = 0
        self.end_date = 0
        self.df_trading_day = pd.DataFrame()
        self.df_daily_symbol = pd.DataFrame()
        self.df_option_info = pd.DataFrame()
        self.trading_date_section = []
        self.list_sub_symbol_root =[]
        self.list_sub_symbol = []
        self.data_type = "1m"
        self.night_trade = False
        self.on_bod = 0
        self.on_eod = 0
        self.on_bar = None
        self.on_section_bar = None
        self.on_result = None
        self.th = 0
        self.running = True
        self.cur_date = 0
        self.match_engine = match_engine.match_engine()
        self.match_engine.register_on_ack_cb(self.on_ack)
        self.match_engine.register_on_conf_cb(self.on_conf)
        self.match_engine.register_on_fill_cb(self.on_fill)
        self.match_engine.register_on_cxl_cb(self.on_cxl)
        self.match_engine.register_on_cxl_rej_cb(self.on_cxl_rej)
        self.match_engine.register_on_rej_cb(self.on_rej)
        self.account_manager = account_manager.account_manager()
        self.date_time_util = date_time_util.date_time_util()
        self.dict_daily_symbol = {}
        self.dict_daily_multiplier = {}
        self.dict_expired_date = {}
        self.dict_call_or_put = {}
        self.dict_daily_strike_price = {}
        self.dict_underlying_code = {}
        self.dict_product_id = {}
        self.dict_pre_close = {}
        self.dict_pre_imp_vol = {}
        self.cwd = ""
        self.md_live_api = None
        self.is_live_mode = False
        self.is_option = False
        self.data_path = "future"
        self._is_done_ = False
        self.dataset_allow_download = False
        self.remote_enabled = True
        pass
        self._last_nav_ = None
        self._stable_nav_count_ = 0
        self._last_section_time_ = ""
        self._section_count_ = 0

    # 初始化
    # server_addr 回测平台服务端地址
    def init(self, server_addr="tcp://127.0.0.1:50010"):
        self.server_addr = server_addr
        self._zmq_ctx_ = zmq.Context()
        self._zmq_sock_ = self._zmq_ctx_.socket(zmq.REQ)
        self._zmq_sock_.connect(self.server_addr)
        try:
            self._zmq_sock_.setsockopt(zmq.RCVTIMEO, 10000)
            self._zmq_sock_.setsockopt(zmq.LINGER, 0)
        except Exception:
            pass
        self.cwd = os.getcwd()
        today = datetime.datetime.now().isoformat().split('T')[0]
        log_path = self.cwd + "/log/"
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        try:
            log_name = f"{today}_back_test_client_{self.cur_uuid[:8]}.log"
            file_path = os.path.join(log_path, log_name)
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.INFO)
            exists = False
            for h in list(getattr(root_logger, "handlers", [])):
                try:
                    if hasattr(h, "baseFilename") and os.path.basename(h.baseFilename) == log_name:
                        exists = True
                        break
                except Exception:
                    continue
            if not exists:
                fh = logging.FileHandler(file_path, mode="a", encoding="utf-8")
                fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
                fh.setFormatter(fmt)
                fh.setLevel(logging.INFO)
                root_logger.addHandler(fh)
        except Exception:
            try:
                logging.basicConfig(filename=os.path.join(log_path, f"{today}_back_test_client_fallback.log"), level=logging.INFO, filemode="a")
            except Exception:
                pass
        self.account_manager.init()
        try:
            if self.is_live_mode and md_live_api is not None:
                self.md_live_api = md_live_api.MdMinuteBarApi()
            else:
                self.md_live_api = None
        except Exception:
            self.md_live_api = None
        return

    def _reset_socket(self):
        try:
            if self._zmq_sock_:
                try:
                    self._zmq_sock_.close(linger=0)
                except Exception:
                    try:
                        self._zmq_sock_.close()
                    except Exception:
                        pass
            if not self._zmq_ctx_:
                self._zmq_ctx_ = zmq.Context()
            self._zmq_sock_ = self._zmq_ctx_.socket(zmq.REQ)
            self._zmq_sock_.connect(self.server_addr)
            try:
                self._zmq_sock_.setsockopt(zmq.RCVTIMEO, 10000)
                self._zmq_sock_.setsockopt(zmq.LINGER, 0)
            except Exception:
                pass
        except Exception:
            pass
        return

    def _safe_req(self, parts, tag="req"):
        if not self.remote_enabled:
            return []
        tries = 2
        for _ in range(tries):
            try:
                if not self._zmq_sock_:
                    self._reset_socket()
                self._zmq_sock_.send_multipart(parts)
                rsp = self._zmq_sock_.recv_multipart()
                return rsp
            except zmq.error.Again:
                try:
                    self._reset_socket()
                except Exception:
                    pass
                continue
            except zmq.ZMQError:
                try:
                    self._reset_socket()
                except Exception:
                    pass
                continue
            except Exception:
                try:
                    self._reset_socket()
                except Exception:
                    pass
                continue
        self.remote_enabled = False
        return []

    # 登录
    # user_name 用户名
    # user_pwd 用户密码
    #返回 True：登录成功；False：登录失败
    def login(self, user_name, user_pwd):
        self.user_name = user_name
        self.user_pwd = user_pwd
        req_msg = protocol.msg_header()
        req_msg.session_id = self.cur_uuid.encode('utf-8')
        req_msg.user_name = self.user_name.encode('utf-8')
        req_msg.user_pwd = self.user_pwd.encode('utf-8')
        req_msg.msg_type = 1
        req_msg = struct.pack("<i36s16s16s", req_msg.msg_type, req_msg.session_id, req_msg.user_name, req_msg.user_pwd)
        rsp_msg = self._safe_req([req_msg], tag="login")
        try:
            if len(rsp_msg) >= 1:
                logging.info("[login] [%s] user:%s rsp_len=%d", datetime.datetime.now().isoformat(), user_name, len(rsp_msg))
            else:
                logging.info("[login] [%s] user:%s no response", datetime.datetime.now().isoformat(), user_name)
        except Exception:
            pass
        if len(rsp_msg) >= 1:
            self.remote_enabled = True
            try:
                dt = pickle.loads(rsp_msg[-1])
            except Exception:
                try:
                    dt = rsp_msg[-1]
                except Exception:
                    dt = None
            try:
                s = dt.decode() if isinstance(dt, (bytes, bytearray)) else str(dt)
            except Exception:
                s = str(dt)
            if s == "None":
                return False
            else:
                if self.is_option:
                    self.get_option_info()
                else:
                    self.get_daily_symbol()
                try:
                    self.account_manager.set_daily_multiplier(self.dict_daily_multiplier)
                    self.account_manager.set_product_id(self.dict_product_id)
                except Exception:
                    pass
                return True
        self.remote_enabled = False
        return False

    #注册每个交易日开始回调函数
    def register_bod(self, func):
        self.on_bod = func
        return

    #注册每个交易日结束回调函数
    def register_eod(self, func):
        self.on_eod = func
        return

    #注册接收截面bar数据回调函数
    def register_section_bar(self, func):
        self.on_section_bar = func
        return

    #注册接收bar数据回调函数
    def register_bar(self, func):
        self.on_bar = func
        return

    #注册接收回测结果回调函数
    def register_result_cb(self, func):
        self.on_result = func
        return

    def set_live_mode(self, value):
        self.is_live_mode = value
        return

    def get_live_mode(self):
        return self.is_live_mode

    def set_is_option(self, value):
        self.is_option = value
        self.match_engine.set_is_option(value)
        self.account_manager.set_is_option(value)
        if value:
            self.data_path = "option"
        else:
            self.data_path = "future"
        return

    def get_is_option(self):
        return self.is_option
    def _norm_day(self, s):
        try:
            s = str(s).strip()
            if len(s) == 8 and s.isdigit():
                return s[0:4] + "-" + s[4:6] + "-" + s[6:8]
            return s.replace("_", "-")
        except Exception:
            return str(s)
    def _tm_to_sec(self, t):
        try:
            t = str(t).strip()
            if len(t) == 8 and ":" in t:
                hh, mm, ss = t.split(":")
                return int(hh) * 3600 + int(mm) * 60 + int(ss)
        except Exception:
            pass
        return -1

    #设置回测数据粒度
    # data_type 数据类型：1m：分钟；1d：日
    def set_data_type(self, data_type):
        self.data_type = data_type
        self.match_engine.set_data_type(data_type)
        self.account_manager.set_data_type(data_type)
        return

    #获取可用资金
    def get_available(self):
        return self.account_manager.get_available()

    #设置回测开始日期和结束日期
    # begin_date 开始日期
    # end_date 结束日期
    def set_date_section(self, begin_date, end_date):
        import logging
        logging.info("[set_date_section]")
        self.begin_date = begin_date
        self.end_date = end_date
        self.trading_date_section = []
        if self.df_trading_day is None or self.df_trading_day.shape[0] == 0:
            self.df_trading_day = self.get_trading_day()
        def norm(s):
            try:
                s = str(s).strip()
                if len(s) == 8 and s.isdigit():
                    return s[0:4] + "-" + s[4:6] + "-" + s[6:8]
                return s.replace("_", "-")
            except Exception:
                return str(s)
        b = norm(self.begin_date)
        e = norm(self.end_date)
        try:
            days_remote = []
            if self.df_trading_day is not None and hasattr(self.df_trading_day, "itertuples"):
                for tup in self.df_trading_day.itertuples():
                    tday_raw = getattr(tup, "trade_day")
                    tday = norm(tday_raw)
                    if tday >= b and tday <= e:
                        days_remote.append(tday)
            days_remote = sorted(list(set(days_remote)))
        except Exception:
            days_remote = []
        try:
            scan_type = self.data_type if str(self.data_type).lower() != "5m" else "1m"
            days_ds = set()
            roots = list(self.list_sub_symbol_root or [])
            for r in roots:
                ex = r.split(".")[-1] if "." in r else ""
                base1 = os.path.join(self.cwd, "data", scan_type, ex, r)
                base2 = os.path.join(self.cwd, "data", "data", scan_type, ex, r)
                for base in (base1, base2):
                    if not os.path.exists(base) or not os.path.isdir(base):
                        continue
                    for name in os.listdir(base):
                        path = os.path.join(base, name)
                        if os.path.isdir(path):
                            d = str(name).split(" ")[0]
                            d = norm(d)
                            if d < b or d > e:
                                continue
                            ok = False
                            try:
                                for n in os.listdir(path):
                                    if os.path.isfile(os.path.join(path, n)):
                                        ok = True
                                        break
                            except Exception:
                                ok = False
                            if ok:
                                days_ds.add(d)
            days_ds = sorted(list(days_ds))
        except Exception:
            days_ds = []
        try:
            if days_remote:
                self.trading_date_section = days_remote
            else:
                self.trading_date_section = days_ds
        except Exception:
            self.trading_date_section = []
        try:
            if isinstance(self.trading_date_section, list) and len(self.trading_date_section) > 0:
                logging.info("[set_date_section] begin=%s end=%s count=%d head=%s tail=%s", b, e, len(self.trading_date_section), self.trading_date_section[0], self.trading_date_section[-1])
            else:
                logging.info("[set_date_section] begin=%s end=%s count=%d", b, e, 0)
        except Exception:
            pass
        return

    # 按品种订阅合约
    # symbol_root 品种
    def subscribe_symbol_root(self, list_symbol_root):
        self.list_sub_symbol_root = list_symbol_root
        return

    #订阅合约
    # symbol 合约
    def subscribe_symbol(self, list_symbol):
        self.list_sub_symbol = list_symbol
        return

    #设置初始资金
    #balance初始资金
    def set_balance(self, balance):
        self.balance = balance
        self.account_manager.set_balance(balance)
        return

    def get_balance(self):
        return self.balance

    #设置保证金比例
    # rate 比例
    def set_margin_rate(self, rate):
        self.account_manager.set_margin_rate(rate)
        return

    #设置滑点类型
    # type 滑点类型 'pct'：按成交额比例；'fix'：固定值（元）
    def set_slippage_type(self, type):
        self.account_manager.set_slippage_type(type)
        return

    #设置滑点值
    # value 滑点值
    # 如果滑点类型为 'pct'，value 代表百分比，例如：0.01 即 1%
    # 如果滑点类型为 'fix'，value 代表固定值（元）
    def set_slippage_value(self, value):
        self.account_manager.set_slippage_value(value)
        return

    #设置买入手续费类型
    # type 类型 'pct'：按成交额比例；'fix'：固定值（元）
    def set_buy_fee_type(self, type):
        self.account_manager.set_buy_fee_type(type)
        return

    #设置买入手续费
    # value 手续费
    # 若类型为 'pct'：按成交额比例
    # 若类型为 'fix'：固定值（元）
    def set_buy_fee_value(self, value):
        self.account_manager.set_buy_fee_value(value)
        return

    #设置卖出手续费类型
    # type 类型 'pct'：按成交额比例；'fix'：固定值（元）
    def set_sell_fee_type(self, type):
        self.account_manager.set_sell_fee_type(type)
        return

    #设置卖出手续费
    # value 手续费
    # 若类型为 'pct'：按成交额比例
    # 若类型为 'fix'：固定值（元）
    def set_sell_fee_value(self, value):
        self.account_manager.set_sell_fee_value(value)
        return

    #设置成交方法
    # type 成交方法：
    # 'close'：以 bar 收盘价成交
    # 'vwap'：以 bar 内成交额/成交量成交
    # 'open'：以 bar 开盘价成交
    def set_deal_type(self, type):
        self.account_manager.set_deal_type(type)
        self.match_engine.set_deal_type(type)
        return

    #设置最大成交比例
    # pct 比例：实际成交量不超过 bar 内成交量 * 成交比例，例如 0.1 表示 10%
    def set_max_deal_pct(self, pct):
        self.match_engine.set_max_deal_pct(pct)
        return

    #设置是否考虑市场成交量来进行摄合
    #flag True 考虑市场成交量；False 不考虑市场成交量
    def set_check_market_volume(self, flag):
        self.match_engine.set_check_market_volume(flag)
        return

    #设置是否参与夜盘
    # flag 标识：True 参与夜盘并正常交易；False 不参与（合成行情与 handle_bar 均不考虑夜盘数据）
    def set_night_trade(self, flag):
        self.night_trade = flag
        return

    #获取仓位信息
    # 返回一个position_manager对象
    def get_pm(self):
        return self.match_engine.get_position_manager()

    #获取仓位信息
    # 返回一个dict key:symbol, value:net_position
    def get_current_position(self):
        net_pos = {}
        for symbol, net_p in self.get_pm().dict_net_pos.items():
            if net_p > 0 or net_p < 0:
                net_pos[symbol] = net_p
        return net_pos

    #获取账户总资产
    def get_account_asset(self) :
        return self.account_manager.calculate_pnl(self.get_pm())

    # 报单
    # symbol 合约
    # size 报单数量
    #direction 买卖方向正数：买入；负数：卖出
    #price 价格
    # 返回订单编号：大于 0 表示成功；小于 0 表示失败
    def send_order(self, symbol, size, direction, price=0.0):
        side = 0
        if direction > 0:
            side = 0
        else:
            side = 1
        try:
            logging.info("[client_api][send_order_pre] user=%s symbol=%s size=%f dir=%d", self.user_name, str(symbol), float(size), int(side))
        except Exception:
            pass
        dict_pos = self.get_current_position()
        net_pos = 0.0
        try:
            if symbol in dict_pos:
                net_pos = dict_pos[symbol]
            else:
                sb = symbol.encode('utf-8') if isinstance(symbol, str) else symbol
                if sb in dict_pos:
                    net_pos = dict_pos[sb]
        except Exception:
            net_pos = 0.0
        px = 0.0
        try:
            px = float(price)
        except Exception:
            px = 0.0
        if px <= 0.0:
            try:
                if self.data_type == "tick":
                    try:
                        px = float(self.account_manager.dict_last_tick[symbol].last_price)
                        logging.info("[client_api][px_pick] src=tick last_price=%.6f", float(px))
                    except Exception:
                        px = 0.0
                else:
                    if self.account_manager.deal_type == "close":
                        if self.is_option:
                            try:
                                px = float(getattr(self.account_manager.dict_last_bar[symbol], "option_close"))
                                logging.info("[client_api][px_pick] src=bar option_close=%.6f", float(px))
                            except Exception:
                                try:
                                    px = float(getattr(self.account_manager.dict_last_bar[symbol], "option_pre_close"))
                                    logging.info("[client_api][px_pick] src=bar option_pre_close=%.6f", float(px))
                                except Exception:
                                    px = 0.0
                        else:
                            try:
                                px = float(getattr(self.account_manager.dict_last_bar[symbol], "close"))
                                logging.info("[client_api][px_pick] src=bar close=%.6f", float(px))
                            except Exception:
                                try:
                                    px = float(getattr(self.account_manager.dict_last_bar[symbol], "pre_close"))
                                    logging.info("[client_api][px_pick] src=bar pre_close=%.6f", float(px))
                                except Exception:
                                    px = 0.0
                    else:
                        if self.is_option:
                            try:
                                px = float(getattr(self.account_manager.dict_last_bar[symbol], "option_open"))
                                logging.info("[client_api][px_pick] src=bar option_open=%.6f", float(px))
                            except Exception:
                                try:
                                    px = float(getattr(self.account_manager.dict_last_bar[symbol], "option_pre_close"))
                                    logging.info("[client_api][px_pick] src=bar option_pre_close=%.6f", float(px))
                                except Exception:
                                    px = 0.0
                        else:
                            try:
                                px = float(getattr(self.account_manager.dict_last_bar[symbol], "open"))
                                logging.info("[client_api][px_pick] src=bar open=%.6f", float(px))
                            except Exception:
                                try:
                                    px = float(getattr(self.account_manager.dict_last_bar[symbol], "pre_close"))
                                    logging.info("[client_api][px_pick] src=bar pre_close=%.6f", float(px))
                                except Exception:
                                    px = 0.0
            except Exception:
                px = 0.0
        try:
            logging.info("[client_api][risk_pre] symbol=%s side=%d size=%f price=%.6f net_pos=%.6f", str(symbol), int(side), float(size), float(px), float(net_pos))
        except Exception:
            pass
        if not self.account_manager.risk_check(symbol, side, size, px, net_pos):
            logging.info("[send_order] [%s] fail, user:%s, symbol:%s, size:%f, side:%d, price:%f" % 
                        (datetime.datetime.now().isoformat(), self.user_name, symbol, size, side, px))
            return -1
        try:
            logging.info("[client_api][risk_pass] symbol=%s side=%d size=%f price=%.6f net_pos=%.6f", str(symbol), int(side), float(size), float(px), float(net_pos))
        except Exception:
            pass
        order_id = self.match_engine.place_order(symbol, size, side, 0.0)
        logging.info("[send_order] [%s] user:%s, symbol:%s, size:%f, side:%d, id:%d, price:%f" % 
                    (datetime.datetime.now().isoformat(), self.user_name, symbol, size, side, order_id, 0.0))
        ret = self.match_engine.get_order(order_id)
        if not (ret is None):
            self.account_manager.on_place_order(ret, net_pos)
        return order_id

    # 撤单
    def cancel_order(self, order_id):
        self.match_engine.cancel_order(order_id)
        return

    # 订单发出回报
    # ack 订单发出回报信息
    def on_ack(self, ord, ack):
        logging.info("[client_api][on_ack] [%s] oid:%d, symbol:%s, mkt_time:%s" % 
                    (datetime.datetime.now().isoformat(), ack.order_id, ord.symbol.decode(), ord.place_time.decode()))
        return

    # 订单确认回报
    # conf 订单确认回报信息
    def on_conf(self, ord, conf):
        logging.info("[client_api][on_conf] [%s] oid:%d, symbol:%s, mkt_time:%s" % 
                    (datetime.datetime.now().isoformat(), conf.order_id, ord.symbol.decode(), ord.place_time.decode()))
        return

    # 订单成交回报
    # fill 订单成交回报信息
    def on_fill(self, ord, fill):
        logging.info("[client_api][on_fill] [%s] oid:%d, fill_sz:%f, fill_px:%f, symbol:%s, mkt_time:%s" % 
                    (datetime.datetime.now().isoformat(), fill.order_id, fill.filled_sz, fill.filled_px, ord.symbol.decode(), ord.place_time.decode()))
        self.account_manager.on_fill(ord, fill)
        return

    #订单撤单成功回报
    # cxl 订单撤单成功回报信息
    def on_cxl(self, ord, cxl):
        logging.info("[client_api][on_cxl] [%s] oid:%d, cxl_sz:%f, symbol:%s, mkt_time:%s" % 
                    (datetime.datetime.now().isoformat(), cxl.order_id, cxl.cxl_sz, ord.symbol.decode(), ord.place_time.decode()))
        self.account_manager.on_cxl(cxl)
        return

    # 订单撤单失败回报
    # cxl_rej 订单撤单失败回报信息
    def on_cxl_rej(self, ord, cxl_rej):
        logging.info("[client_api][on_cxl_rej] [%s] oid:%d, reason:%d, rej_msg:%s, symbol:%s, mkt_time:%s" % 
                    (datetime.datetime.now().isoformat(), cxl_rej.order_id, cxl_rej.reason, cxl_rej.rej_msg.decode(), ord.symbol.decode(), ord.place_time.decode()))
        return

    #订单拒绝回报
    # rej 订单拒绝回报信息
    def on_rej(self, ord, rej):
        logging.info("[client_api][on_rej] [%s] oid:%d, reason:%d, rej_msg:%s, symbol:%s, mkt_time:%s" % 
                    (datetime.datetime.now().isoformat(), rej.order_id, rej.reason, rej.rej_msg.decode(), ord.symbol.decode(), ord.place_time.decode()))
        self.account_manager.on_rej(rej)
        return

    # 判断是否夜盘时段（用于过滤夜盘数据）
    # time 形如 "HH:MM:SS"
    def is_night_time_section(self, time):
        if time >= "20:00:00" or time <= "06:00:00":
            return True
        return False

    # 判断是否属于成交允许时段（避开清算与非交易时间）
    # time 形如 "HH:MM:SS"
    def is_trading_time(self, time):
        if (time >= "15:30:00" and time <= "20:59:00") or (time >= "03:00:00" and time <= "08:59:00"):
            return False
        return True

    # 将行数据映射为产品ID（期货用 symbol，期权用 option_code）
    def convert_product_id(self, row):
        product_id = ""
        symbol = row["symbol"]
        if self.is_option:
            symbol = row["option_code"]
        if str(symbol) != "nan":
            product_id = self.dict_product_id[symbol]
        return product_id

    # 计算成交均价 vwap（成交额/成交量/合约乘数），期权走 option 字段
    def convert_vwap(self, row, trading_day):
        vwap = 0.0
        symbol = row["symbol"]
        volume = row["volume"]
        turnover = row["turnover"]
        if self.is_option:
            symbol = row["option_code"]
            volume = row["option_volume"]
            turnover = row["option_amt"]
        if trading_day in self.dict_daily_multiplier.keys() and symbol in self.dict_daily_multiplier[trading_day].keys():
            if volume > 0.0 and self.dict_daily_multiplier[trading_day][symbol] > 0.0:
                vwap = float(turnover) / float(volume) / float(self.dict_daily_multiplier[trading_day][symbol])
            else:
                vwap = 0.0
        else:
            vwap = float(row["close"]) if not self.is_option else float(row.get("option_close", 0.0))
        return vwap

    # 取上一时刻隐含波动率（若无则使用当前 imp_vol）
    def convert_pre_imp_vol(self, row):
        pre_imp_vol = 0
        symbol = row["option_code"]
        if symbol in self.dict_pre_imp_vol.keys():
            pre_imp_vol = self.dict_pre_imp_vol[symbol]
        else:
            pre_imp_vol = row["imp_vol"]
            self.dict_pre_imp_vol[symbol] = row["imp_vol"]
        return pre_imp_vol
    
    # 按订阅品种筛选当日可交易合约列表
    # 返回 list[str]
    def get_daily_symbols(self, trading_day):
        ret = []
        symbols = []
        try:
            import logging
            logging.debug("[get_daily_symbols] trading_day=%s", trading_day)
            logging.debug("[get_daily_symbols] subscribed_roots=%s", str(self.list_sub_symbol_root))
            logging.debug("[get_daily_symbols] available_days=%s", str(list(self.dict_daily_multiplier.keys()))[:256])
            if trading_day not in self.dict_daily_multiplier:
                logging.debug("[get_daily_symbols] no multiplier for day=%s, will fallback if empty", trading_day)
        except Exception:
            pass
        if trading_day in self.dict_daily_multiplier:
            symbols = list(self.dict_daily_multiplier[trading_day].keys())
        try:
            import logging
            logging.debug("[get_daily_symbols] symbols_of_day(count=%d)=%s", len(symbols), str(symbols)[:512])
        except Exception:
            pass
        for symbol in symbols:
            root = self.get_symbol_root(symbol)
            if root in self.list_sub_symbol_root:
                ret.append(symbol)
            elif symbol in self.dict_product_id.keys() and self.dict_product_id[symbol] in self.list_sub_symbol_root:
                ret.append(symbol)
        try:
            import logging
            logging.debug("[get_daily_symbols] selected(count=%d)=%s", len(ret), str(ret)[:512])
            if len(ret) == 0:
                logging.debug("[get_daily_symbols] empty after filtering by roots, attempting dataset fallback")
        except Exception:
            pass
        # 本地兜底：当 dict_daily_multiplier 缺失或当日列表为空时，按目录扫描生成
        try:
            if len(ret) == 0:
                roots = list(self.list_sub_symbol_root or [])
                scan_data_type = self.data_type if str(self.data_type).lower() != "5m" else "1m"
                tmp = []
                bases = [os.path.join(self.cwd, "data", scan_data_type), os.path.join(self.cwd, "data", "data", scan_data_type)]
                try:
                    logging.debug("[get_daily_symbols][dataset_fallback] bases=%s roots=%s day=%s", str(bases), str(roots), str(trading_day))
                except Exception:
                    pass
                for r in roots:
                    try:
                        ex = r.split(".")[-1] if "." in r else ""
                        for base in bases:
                            dirp = os.path.join(base, ex, r, str(trading_day))
                            if os.path.exists(dirp) and os.path.isdir(dirp):
                                for name in os.listdir(dirp):
                                    fp = os.path.join(dirp, name)
                                    if os.path.isfile(fp):
                                        sym = name
                                        tmp.append(sym)
                                        self.dict_product_id[sym] = r
                                        if trading_day not in self.dict_daily_multiplier:
                                            self.dict_daily_multiplier[trading_day] = {}
                                        if sym not in self.dict_daily_multiplier[trading_day]:
                                            self.dict_daily_multiplier[trading_day][sym] = 1.0
                                break
                    except Exception:
                        continue
                if tmp:
                    ret = tmp
                    try:
                        logging.debug("[get_daily_symbols][dataset_fallback] selected(count=%d)=%s", len(ret), str(ret)[:512])
                    except Exception:
                        pass
                else:
                    try:
                        logging.debug("[get_daily_symbols][dataset_fallback] no files found under bases for day=%s", str(trading_day))
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            pm = self.get_pm()
            pos_map = pm.get_all_pos()
            for s_b in list(pos_map.keys()):
                try:
                    s = s_b.decode() if hasattr(s_b, "decode") else str(s_b)
                except Exception:
                    s = str(s_b)
                r = self.get_symbol_root(s)
                if (r in self.list_sub_symbol_root) or (s in ret):
                    if s not in ret:
                        ret.append(s)
                    self.dict_product_id[s] = r
                    if trading_day not in self.dict_daily_multiplier:
                        self.dict_daily_multiplier[trading_day] = {}
                    if s not in self.dict_daily_multiplier[trading_day]:
                        self.dict_daily_multiplier[trading_day][s] = 1.0
        except Exception:
            pass
        return ret

    # 加载当日期权与标的合并数据（分钟或日）
    # 返回 DataFrame，包含 option_* 与标的字段
    def _load_option_data_(self, trading_day):
        str_trading_day = str(trading_day).split(' ')[0]
        logging.info("[load_option_data] [%s] start trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
        list_symbols = self.get_daily_symbols(str_trading_day)
        daily_data = pd.DataFrame()
        dict_daily_future_data = {}
        underlying_columns =["symbol","trading_day","date_time","open",
                           "high","low","close","turnover","volume",
                           "open_interest","pre_close"]
        result_columns =["option_code","trade_day","rt_time","option_open","option_high","option_low",
                        "option_close","option_amt","option_volume",
                        "delta","gamma","vega","theta","imp_vol",
                        "symbol","trading_day","date_time",
                        "open","high","low","close",
                        "turnover","volume","pre_close"]

        #if self.data_type =="1d"
        dict_symbol_root = {}
        dict_symbol_root_to_exchange = {}
        for symbol in list_symbols:
            exchange = symbol.split('.')[-1]
            symbol_root = self.get_symbol_root(symbol)
            dict_symbol_root[symbol_root] = symbol_root
            dict_symbol_root_to_exchange[symbol_root] = exchange
        is_all_data_file_exist = True
        # dataset 模式下，如当日合约列表为空，则按目录扫描兜底生成列表
        try:
            is_dataset_mode = False
            try:
                is_dataset_mode = (os.path.basename(self.cwd).lower() == "dataset")
            except Exception:
                is_dataset_mode = False
            if is_dataset_mode and (not list_symbols or len(list_symbols) == 0):
                # 依据订阅的根品种扫描 dataset/data/<data_type>/<exchange>/<root>/<day>/ 下的所有合约文件
                roots = list(self.list_sub_symbol_root or [])
                tmp_symbols = []
                for r in roots:
                    try:
                        ex = r.split(".")[-1] if "." in r else ""
                        scan_data_type = self.data_type if str(self.data_type).lower() != "5m" else "1m"
                        bases = [os.path.join(self.cwd, "data", scan_data_type), os.path.join(self.cwd, "data", "data", scan_data_type)]
                        for base in bases:
                            dirp = os.path.join(base, ex, r, str_trading_day)
                            if os.path.exists(dirp) and os.path.isdir(dirp):
                                for name in os.listdir(dirp):
                                    fp = os.path.join(dirp, name)
                                    if os.path.isfile(fp):
                                        if ("." not in name) or (name.split(".")[-1] != ex):
                                            try:
                                                logging.warning("[load_option_data][dataset] invalid symbol filename=%s dir=%s", name, dirp)
                                            except Exception:
                                                pass
                                            continue
                                        tmp_symbols.append(name)
                                        self.dict_product_id[name] = r
                                        if str_trading_day not in self.dict_daily_multiplier:
                                            self.dict_daily_multiplier[str_trading_day] = {}
                                        if name not in self.dict_daily_multiplier[str_trading_day]:
                                            self.dict_daily_multiplier[str_trading_day][name] = 1.0
                                break
                    except Exception:
                        pass
                if tmp_symbols:
                    list_symbols = tmp_symbols
        except Exception:
            pass
        for symbol_root, v in dict_symbol_root.items():
            bases = [os.path.join(self.cwd, "data", self.data_type), os.path.join(self.cwd, "data", "data", self.data_type)]
            found = False
            for base in bases:
                filep = os.path.join(base, dict_symbol_root_to_exchange[symbol_root], symbol_root, str_trading_day)
                fn = os.path.join(filep, symbol_root)
                if os.path.exists(fn):
                    found = True
                    break
            if not found:
                is_all_data_file_exist = False
                break
        if is_all_data_file_exist:
            for symbol_root, v in dict_symbol_root.items():
                bases = [os.path.join(self.cwd, "data", self.data_type), os.path.join(self.cwd, "data", "data", self.data_type)]
                df_t = pd.DataFrame()
                for base in bases:
                    filep = os.path.join(base, dict_symbol_root_to_exchange[symbol_root], symbol_root, str_trading_day)
                    fn = os.path.join(filep, symbol_root)
                    if os.path.exists(fn):
                        try:
                            df_t = pd.read_csv(fn)
                        except Exception:
                            try:
                                df_t = pd.read_table(fn)
                            except Exception:
                                df_t = pd.DataFrame()
                        break
                daily_data = pd.concat([daily_data, df_t], ignore_index=True)
            logging.info("[load_option_data][cache] [%s] sort trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
            daily_data = daily_data.sort_values(by=["trade_day", "rt_time"], ascending=[True, True])
            daily_data.drop_duplicates(subset=["option_code", "trade_day","rt_time"], keep="last", inplace=True)
            daily_data.ffill(inplace=True)
            daily_data.bfill(inplace=True)
            daily_data = daily_data.reset_index(drop=True)
            logging.info("[load_option_data][cache] [%s] data ready, trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
            return daily_data
        
        for symbol in list_symbols:
            exchange = symbol.split('.')[-1]
            symbol_root = self.get_symbol_root(symbol)
            file_path = os.path.join(self.cwd, "data", self.data_type, exchange, symbol_root, str_trading_day)
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            filename = os.path.join(file_path, symbol)
            if os.path.exists(filename):
                df = pd.read_csv(filename)
                if df.shape[0] == 0 or df["option_volume"].sum() == 0:
                    continue
                daily_data = pd.concat([daily_data, df], ignore_index=True)
            else:
                option_df = self.get_option_bar(str_trading_day, self.data_type, exchange, symbol)
                if option_df is None or option_df.shape[0] == 0:
                    # 建立空文件占位，避免后续读取失败
                    option_df = pd.DataFrame([], columns=result_columns+["product_id", "vwap","pre_imp_vol"])
                    option_df.to_csv(filename, index=False)
                    continue
                underlying_code = self.dict_underlying_code[symbol]
                underlying_symbol = underlying_code
                if exchange not in underlying_symbol:
                    underlying_symbol = underlying_symbol + '.' + exchange
                underlying_df = pd.DataFrame([], columns=underlying_columns)
                if underlying_symbol != symbol:
                    future_filename = os.path.join(self.cwd, "data", self.data_type, exchange, self.get_symbol_root(symbol), str_trading_day, underlying_symbol)
                    if future_filename not in dict_daily_future_data.keys():
                        underlying_df = self.get_future_bar(str_trading_day, self.data_type, exchange, underlying_symbol)
                        dict_daily_future_data[future_filename] = underlying_df
                    else:
                        underlying_df = dict_daily_future_data[future_filename]
                try:
                    if self.data_type != "1d":
                        df = pd.merge(option_df, underlying_df, how='outer', left_on='rt_time', right_on='date_time')
                    else:
                        df = pd.merge(option_df, underlying_df, how='outer', left_on='trade_day', right_on='trading_day')
                except Exception as e:
                    pass
                df.dropna(inplace=True)
                df = df[result_columns]
                if df.shape[0] > 0:
                    df["product_id"] = df.apply(self.convert_product_id, axis=1)
                    df["vwap"] = df.apply(self.convert_vwap, axis=1, args=(str_trading_day,))
                    df["pre_imp_vol"] = df.apply(self.convert_pre_imp_vol, axis=1)
                else:
                    df["product_id"] = self.dict_product_id[symbol]
                    df["vwap"] = df["option_close"]
                    df["pre_imp_vol"] = 0
                df.to_csv(filename, index=False)
                daily_data = pd.concat([daily_data, df], ignore_index=True)
        
        if daily_data.shape[0] == 0:
            return daily_data
        logging.info("[load_option_data] [%s] sort trading day :%s" % (datetime.datetime.now().isoformat(), str_trading_day))
        daily_data = daily_data.sort_values(by=["trade_day", "rt_time"], ascending=[True, True])
        daily_data.drop_duplicates(subset=["option_code", "trade_day","rt_time"], keep="last", inplace=True)
        daily_data.ffill(inplace=True)
        daily_data.bfill(inplace=True)
        daily_data = daily_data.reset_index(drop=True)
        daily_data_grp = daily_data.groupby(by="option_code")
        for symbol, daily_data_df in daily_data_grp:
            exchange = symbol.split('.')[-1]
            symbol_root = self.get_symbol_root(symbol)
            filep = os.path.join(self.cwd, "data", self.data_type, dict_symbol_root_to_exchange[symbol_root], symbol_root, str_trading_day)
            fn = os.path.join(filep, symbol_root)
            if not os.path.exists(fn):
                daily_data_df.to_csv(fn, index=False, header=True)
            else:
                daily_data_df.to_csv(fn, index=False, header=False, mode='a')
        logging.info("[load_option_data] [%s] data ready, trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
        return daily_data

    def _load_data_(self, trading_day):
        str_trading_day = str(trading_day).split(' ')[0]
        logging.info("[load_data] [%s] start trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
        list_symbols = self.get_daily_symbols(str_trading_day)
        try:
            logging.info("[load_data] daily_symbols count=%d", len(list_symbols))
            if not list_symbols or len(list_symbols) == 0:
                logging.info("[load_data] no symbols for day=%s, checking dataset cache", str_trading_day)
        except Exception:
            pass
        daily_data = pd.DataFrame()
        dict_daily_future_data = {}
        dict_symbol_root = {}
        dict_symbol_root_to_exchange = {}
        for symbol in list_symbols:
            exchange = symbol.split('.')[-1]
            symbol_root = self.get_symbol_root(symbol)
            dict_symbol_root[symbol_root] = symbol_root
            dict_symbol_root_to_exchange[symbol_root] = exchange
        try:
            logging.debug("[load_data] roots=%s", str(list(dict_symbol_root.keys()))[:512])
        except Exception:
            pass
        # 判断是否全部已有本地缓存
        is_dataset_mode = False
        try:
            is_dataset_mode = (os.path.basename(self.cwd).lower() == "dataset")
        except Exception:
            is_dataset_mode = False
        orig_data_type = str(self.data_type)
        read_data_type = orig_data_type if orig_data_type.lower() != "5m" else "1m"
        try:
            logging.debug("[load_data] dataset_mode=%s orig_type=%s read_type=%s", str(is_dataset_mode), str(orig_data_type), str(read_data_type))
        except Exception:
            pass
        is_all_data_file_exist = True
        if is_dataset_mode:
            # 如当日合约列表为空，按目录扫描兜底生成
            if not list_symbols or len(list_symbols) == 0:
                roots = list(self.list_sub_symbol_root or [])
                tmp_symbols = []
                for r in roots:
                    try:
                        ex = r.split(".")[-1] if "." in r else ""
                        bases = [os.path.join(self.cwd, "data", read_data_type), os.path.join(self.cwd, "data", "data", read_data_type)]
                        for base in bases:
                            dirp = os.path.join(base, ex, r, str_trading_day)
                            if os.path.exists(dirp) and os.path.isdir(dirp):
                                for name in os.listdir(dirp):
                                    fp = os.path.join(dirp, name)
                                    if os.path.isfile(fp):
                                        if ("." not in name) or (name.split(".")[-1] != ex):
                                            try:
                                                logging.warning("[load_data][dataset] invalid symbol filename=%s dir=%s", name, dirp)
                                            except Exception:
                                                pass
                                            continue
                                        tmp_symbols.append(name)
                                        self.dict_product_id[name] = r
                                        if str_trading_day not in self.dict_daily_multiplier:
                                            self.dict_daily_multiplier[str_trading_day] = {}
                                        if name not in self.dict_daily_multiplier[str_trading_day]:
                                            self.dict_daily_multiplier[str_trading_day][name] = 1.0
                                break
                    except Exception:
                        continue
                if tmp_symbols:
                    list_symbols = tmp_symbols
                else:
                    try:
                        logging.debug("[load_data][dataset] no files found under bases for day=%s", str_trading_day)
                    except Exception:
                        pass
            for symbol in list_symbols:
                exchange = symbol.split('.')[-1]
                symbol_root = self.get_symbol_root(symbol)
                bases = [os.path.join(self.cwd, "data", read_data_type), os.path.join(self.cwd, "data", "data", read_data_type)]
                found = False
                for base in bases:
                    fn = os.path.join(base, exchange, symbol_root, str_trading_day, symbol)
                    if os.path.exists(fn):
                        found = True
                        break
                if not found:
                    is_all_data_file_exist = False
                    try:
                        logging.debug("[load_data][dataset] missing cache file symbol=%s day=%s", symbol, str_trading_day)
                    except Exception:
                        pass
                    break
            if is_all_data_file_exist:
                for symbol in list_symbols:
                    exchange = symbol.split('.')[-1]
                    symbol_root = self.get_symbol_root(symbol)
                    bases = [os.path.join(self.cwd, "data", read_data_type), os.path.join(self.cwd, "data", "data", read_data_type)]
                    df_t = pd.DataFrame()
                    for base in bases:
                        fn = os.path.join(base, exchange, symbol_root, str_trading_day, symbol)
                        if os.path.exists(fn):
                            try:
                                df_t = pd.read_csv(fn)
                            except Exception:
                                try:
                                    df_t = pd.read_table(fn)
                                except Exception:
                                    df_t = pd.DataFrame()
                            try:
                                if ("date_time" not in df_t.columns) and ("data_time" in df_t.columns):
                                    df_t["date_time"] = df_t["data_time"]
                            except Exception:
                                pass
                            break
                    daily_data = pd.concat([daily_data, df_t], ignore_index=True)
                logging.info("[load_data][cache] [%s] sort trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
                if daily_data.shape[0] > 0:
                    # 如需 5m，先合成，再排序
                    if orig_data_type.lower() == "5m":
                        try:
                            cols = ["symbol","trading_day","date_time","open","high","low","close","turnover","volume","open_interest","pre_close"]
                            daily_data["date_time"] = pd.to_datetime(daily_data["date_time"])
                            # 统一列存在性
                            for col in ["turnover","open_interest","pre_close"]:
                                if col not in daily_data.columns:
                                    daily_data[col] = 0.0
                            def _agg_func(g):
                                return pd.Series({
                                    "symbol": g["symbol"].iloc[0],
                                    "trading_day": g["trading_day"].iloc[0],
                                    "date_time": g["date_time"].max(),
                                    "open": g["open"].iloc[0],
                                    "high": g["high"].max(),
                                    "low": g["low"].min(),
                                    "close": g["close"].iloc[-1],
                                    "turnover": g["turnover"].sum(),
                                    "volume": g["volume"].sum(),
                                    "open_interest": g["open_interest"].iloc[-1],
                                    "pre_close": g["pre_close"].iloc[-1]
                                })
                            daily_data["bucket"] = daily_data["date_time"].dt.floor("5min")
                            grouped = daily_data.groupby(["symbol","bucket"], as_index=False)
                            daily_data = grouped.apply(_agg_func)
                            if "bucket" in daily_data.columns:
                                daily_data = daily_data.drop(columns=["bucket"])
                        except Exception:
                            pass
                    daily_data = daily_data.sort_values(by=["trading_day", "date_time"], ascending=[True, True])
                    daily_data.ffill(inplace=True)
                    daily_data.bfill(inplace=True)
                    daily_data = daily_data.reset_index(drop=True)
                logging.debug("[load_data][cache] daily_data.shape=%s", str(daily_data.shape))
                logging.info("[load_data][cache] [%s] data ready, trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
                return daily_data
        else:
            for symbol in list_symbols:
                exchange = symbol.split('.')[-1]
                symbol_root = self.get_symbol_root(symbol)
                fn = os.path.join(self.cwd, "data", self.data_type, exchange, symbol_root, str_trading_day, symbol)
                if not os.path.exists(fn):
                    is_all_data_file_exist = False
                    break
            if is_all_data_file_exist:
                for symbol in list_symbols:
                    exchange = symbol.split('.')[-1]
                    symbol_root = self.get_symbol_root(symbol)
                    fn = os.path.join(self.cwd, "data", self.data_type, exchange, symbol_root, str_trading_day, symbol)
                    try:
                        df_t = pd.read_csv(fn)
                    except Exception:
                        try:
                            df_t = pd.read_table(fn)
                        except Exception:
                            df_t = pd.DataFrame()
                    try:
                        if ("date_time" not in df_t.columns) and ("data_time" in df_t.columns):
                            df_t["date_time"] = df_t["data_time"]
                    except Exception:
                        pass
                    daily_data = pd.concat([daily_data, df_t], ignore_index=True)
                logging.info("[load_data][cache] [%s] sort trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
                if daily_data.shape[0] > 0:
                    daily_data = daily_data.sort_values(by=["trading_day", "date_time"], ascending=[True, True])
                    daily_data.ffill(inplace=True)
                    daily_data.bfill(inplace=True)
                    daily_data = daily_data.reset_index(drop=True)
                logging.debug("[load_data][cache] daily_data.shape=%s", str(daily_data.shape))
                logging.info("[load_data][cache] [%s] data ready, trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
                return daily_data
        
        for symbol in list_symbols:
            exchange = symbol.split('.')[-1]
            symbol_root = self.get_symbol_root(symbol)
            file_path = os.path.join(self.cwd, "data", self.data_type, exchange, symbol_root, str_trading_day)
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            filename = os.path.join(file_path, symbol)
            if os.path.exists(filename):
                try:
                    df = pd.read_csv(filename)
                except Exception:
                    try:
                        df = pd.read_table(filename)
                    except Exception:
                        df = pd.DataFrame()
                if df.shape[0] == 0:
                    continue
                daily_data = pd.concat([daily_data, df], ignore_index=True)
            else:
                filename = os.path.join(self.cwd, "data", self.data_type, exchange, self.get_symbol_root(symbol), str_trading_day, symbol)
                if is_dataset_mode and (not getattr(self, "dataset_allow_download", False)):
                    df = pd.DataFrame()
                else:
                    if self.is_option:
                        df = self.get_option_bar(str_trading_day, self.data_type, exchange, symbol)
                    else:
                        if exchange == "SH" or exchange == "SZ":
                            df = self.get_stock_index_minute_bar(str_trading_day, symbol)
                        else:
                            df = self.get_future_bar(str_trading_day, self.data_type, exchange, symbol)
                    try:
                        df.to_csv(filename, index=False)
                    except Exception:
                        pass
                if df.shape[0] > 0:
                    daily_data = pd.concat([daily_data, df], ignore_index=True)
        logging.debug("[load_data] appended %s shape=%s total_shape=%s", symbol, str(df.shape) if 'df' in locals() else "(n/a)", str(daily_data.shape))
        
        if daily_data.shape[0] == 0:
            logging.warning("[load_data] no data for %s", str_trading_day)
            return daily_data
        logging.info("[load_data] [%s] sort trading day :%s" % (datetime.datetime.now().isoformat(), str_trading_day))
        # 如需 5m，先合成，再排序
        if is_dataset_mode and orig_data_type.lower() == "5m":
            try:
                for col in ["turnover","open_interest","pre_close"]:
                    if col not in daily_data.columns:
                        daily_data[col] = 0.0
                daily_data["date_time"] = pd.to_datetime(daily_data["date_time"])
                def _agg_func2(g):
                    return pd.Series({
                        "symbol": g["symbol"].iloc[0],
                        "trading_day": g["trading_day"].iloc[0],
                        "date_time": g["date_time"].max(),
                        "open": g["open"].iloc[0],
                        "high": g["high"].max(),
                        "low": g["low"].min(),
                        "close": g["close"].iloc[-1],
                        "turnover": g["turnover"].sum(),
                        "volume": g["volume"].sum(),
                        "open_interest": g["open_interest"].iloc[-1],
                        "pre_close": g["pre_close"].iloc[-1]
                    })
                daily_data["bucket"] = daily_data["date_time"].dt.floor("5min")
                grouped2 = daily_data.groupby(["symbol","bucket"], as_index=False)
                daily_data = grouped2.apply(_agg_func2)
                if "bucket" in daily_data.columns:
                    daily_data = daily_data.drop(columns=["bucket"])
            except Exception:
                pass
        try:
            if ("date_time" not in daily_data.columns) and ("data_time" in daily_data.columns):
                daily_data["date_time"] = daily_data["data_time"]
        except Exception:
            pass
        daily_data = daily_data.sort_values(by=["trading_day", "date_time"], ascending=[True, True])
        daily_data.ffill(inplace=True)
        daily_data.bfill(inplace=True)
        daily_data = daily_data.reset_index(drop=True)
        try:
            logging.debug("[load_data] final daily_data.shape=%s", str(daily_data.shape))
            if daily_data.shape[0] > 0:
                head_dt = str(daily_data.iloc[0]["date_time"])
                tail_dt = str(daily_data.iloc[-1]["date_time"])
                logging.debug("[load_data] dt_range=%s ~ %s", head_dt, tail_dt)
        except Exception:
            pass
        logging.info("[load_data] [%s] data ready, trading day :%s" % (datetime.datetime.now().isoformat(), str_trading_day))
        return daily_data

    def replay(self):
        for trading_day in self.trading_date_section:
            try:
                if not self.running:
                    break
                str_trading_day = str(trading_day).split(' ')[0]
                self._last_nav_ = None
                self._stable_nav_count_ = 0
                self._last_section_time_ = ""
                self._section_count_ = 0
                self.match_engine.bod(str_trading_day)
                self.account_manager.on_bod(str_trading_day)
                self.on_bod(str_trading_day)
                daily_data = pd.DataFrame()
                if not self.is_option:
                    daily_data = self._load_data_(trading_day)
                else:
                    daily_data = self._load_option_data_(trading_day)
                logging.debug("[replay] %s daily_data.shape=%s", str_trading_day, str(daily_data.shape))
                try:
                    daily_syms = self.get_daily_symbols(str_trading_day)
                    logging.info("[replay][day_begin] tday=%s symbols=%d avail=%.2f", str_trading_day, len(daily_syms), float(self.get_available()))
                except Exception:
                    daily_syms = []
                try:
                    if not self.is_option and ("symbol" in daily_data.columns) and ("date_time" in daily_data.columns):
                        tmp = daily_data.groupby("symbol")["date_time"].last().reset_index()
                        logging.info("[debug][day_begin][bar_tail] tday=%s count=%d sample=%s", str_trading_day, int(tmp.shape[0]), str(tmp.head(10).values.tolist()))
                    if self.is_option and ("option_code" in daily_data.columns) and ("rt_time" in daily_data.columns):
                        tmp2 = daily_data.groupby("option_code")["rt_time"].last().reset_index()
                        logging.info("[debug][day_begin][option_bar_tail] tday=%s count=%d sample=%s", str_trading_day, int(tmp2.shape[0]), str(tmp2.head(10).values.tolist()))
                except Exception:
                    pass
                try:
                    syms_in_data = set()
                    if self.is_option:
                        try:
                            if "option_code" in daily_data.columns:
                                syms_in_data = set([str(x) for x in daily_data["option_code"].unique().tolist()])
                        except Exception:
                            syms_in_data = set()
                    else:
                        try:
                            if "symbol" in daily_data.columns:
                                syms_in_data = set([str(x) for x in daily_data["symbol"].unique().tolist()])
                        except Exception:
                            syms_in_data = set()
                    if not syms_in_data:
                        syms_in_data = set(daily_syms or [])
                    try:
                        logging.info("[debug][day_begin][syms] tday=%s in_data=%d daily_syms=%d", str_trading_day, len(syms_in_data), len(daily_syms))
                    except Exception:
                        pass
                    self.match_engine.force_close_unavailable_futures(syms_in_data, self.account_manager, str_trading_day)
                except Exception:
                    pass
                if daily_data.shape[0] == 0:
                    self.match_engine.eod(str_trading_day)
                    self.account_manager.on_eod(str_trading_day, self.match_engine.get_position_manager())
                    self.on_eod(str_trading_day)
                    logging.info("[replay] [%s] done trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
                    continue
                section_bar = []
                last_bar_tm = "60:60:60"
                if self.data_type == "1d":
                    daily_data["date_time"] = "15:00:00"
                for tup in daily_data.itertuples():
                    mkt_time = "15:00:00"
                    rt_time = getattr(tup, "date_time")
                    if self.data_type != "1d":
                        mkt_time = rt_time.split(' ')[-1]
                    self.match_engine.on_bar(tup)
                    if len(mkt_time) == 0 or (not self.night_trade and self.is_night_time_section(mkt_time)) or (not self.is_trading_time(mkt_time)):
                        logging.info("[replay][skip_bar] tday=%s time=%s night=%s trading=%s", str_trading_day, mkt_time, str(self.night_trade), str(self.is_trading_time(mkt_time)))
                        continue
                    self.account_manager.on_bar(tup)
                    self.on_bar(tup)
                    if last_bar_tm == "60:60:60":
                        last_bar_tm = mkt_time
                    if last_bar_tm != mkt_time:
                        self.on_section_bar(section_bar)
                        try:
                            try:
                                logging.info("[debug][stable_nav][gate] tday=%s last_tm=%s cur_tm=%s sections=%d size=%d", str_trading_day, last_bar_tm, mkt_time, int(getattr(self, "_section_count_", 0)), int(len(section_bar)))
                            except Exception:
                                pass
                            cur_pos = self.get_current_position()
                            cur_bal = float(self.account_manager.calculate_pnl(self.match_engine.get_position_manager()))
                            base_bal = float(self.account_manager.init_balance) if abs(self.account_manager.init_balance) > 1e-6 else 1.0
                            nav = cur_bal / base_bal
                            logging.info("[replay][section_end] tday=%s time=%s pos_count=%d avail=%.2f cur_balance=%.2f nav=%.6f", str_trading_day, mkt_time, len(cur_pos), float(self.get_available()), cur_bal, nav)
                            try:
                                if len(cur_pos) > 0:
                                    if (self._last_nav_ is not None) and (abs(nav - self._last_nav_) < 1e-12):
                                        self._stable_nav_count_ += 1
                                    else:
                                        self._stable_nav_count_ = 0
                                    self._last_nav_ = nav
                                    self._last_section_time_ = mkt_time
                                    if self._stable_nav_count_ >= 3 and int(len(section_bar)) >= 5:
                                        missing_syms = []
                                        late_syms = []
                                        for s_b, net_b in cur_pos.items():
                                            try:
                                                s = s_b.decode() if hasattr(s_b, "decode") else str(s_b)
                                            except Exception:
                                                s = str(s_b)
                                            bar = self.account_manager.dict_last_bar.get(s, None)
                                            if bar is None:
                                                missing_syms.append(s)
                                                continue
                                            try:
                                                btday = self._norm_day(getattr(bar, "trading_day"))
                                                btime = str(getattr(bar, "date_time")).split(' ')[-1]
                                            except Exception:
                                                btday = ""
                                                btime = ""
                                            sec_cur = self._tm_to_sec(mkt_time)
                                            sec_bar = self._tm_to_sec(btime)
                                            lag_ok = (sec_cur >= 0 and sec_bar >= 0 and (sec_cur >= sec_bar) and (sec_cur - sec_bar) <= 18000)
                                            if btday != self._norm_day(str_trading_day) or (not lag_ok):
                                                late_syms.append(f"{s}:{btday} {btime} lag={sec_cur - sec_bar}")
                                            try:
                                                logging.info("[debug][stable_nav][late_eval] sym=%s btday=%s btime=%s cur_day=%s cur_time=%s lag=%s lag_ok=%s", s, btday, btime, self._norm_day(str_trading_day), mkt_time, str(sec_cur - sec_bar if (sec_cur>=0 and sec_bar>=0) else None), str(bool(lag_ok)))
                                            except Exception:
                                                pass
                                        msg = "[warn_break][no_price_update] tday=%s time=%s pos_count=%d nav=%.6f stable_count=%d missing=%s late=%s" % (
                                            str_trading_day, mkt_time, len(cur_pos), nav, self._stable_nav_count_, ",".join(missing_syms), ",".join(late_syms)
                                        )
                                        logging.warning(msg)
                                        try:
                                            det = []
                                            for s_b, net_b in cur_pos.items():
                                                s = s_b.decode() if hasattr(s_b, "decode") else str(s_b)
                                                bar = self.account_manager.dict_last_bar.get(s, None)
                                                btday = ""
                                                btime = ""
                                                if bar is not None:
                                                    btday = self._norm_day(getattr(bar, "trading_day"))
                                                    btime = str(getattr(bar, "date_time")).split(' ')[-1]
                                                sec_cur2 = self._tm_to_sec(mkt_time)
                                                sec_bar2 = self._tm_to_sec(btime)
                                                det.append([s, float(net_b), btday, btime, self._norm_day(str_trading_day), mkt_time, (sec_cur2 - sec_bar2 if (sec_cur2>=0 and sec_bar2>=0) else None)])
                                            logging.info("[debug][stable_nav][pos_detail] tday=%s time=%s stable_count=%d last_nav=%.6f detail=%s", str_trading_day, mkt_time, self._stable_nav_count_, self._last_nav_ if self._last_nav_ is not None else float('nan'), str(det))
                                        except Exception:
                                            pass
                                        try:
                                            self.match_engine.force_close_stale_price_futures(mkt_time, self.account_manager, str_trading_day)
                                        except Exception:
                                            pass
                            except Exception:
                                pass
                        except Exception:
                            pass
                        logging.info("[replay] section size=%d time=%s", len(section_bar), mkt_time)
                        self.match_engine.on_bar(tup)
                        last_bar_tm = mkt_time
                        section_bar.clear()
                        try:
                            self._section_count_ = int(getattr(self, "_section_count_", 0)) + 1
                        except Exception:
                            self._section_count_ = 1
                    section_bar.append(tup)
                if self.data_type == "1d":
                    self.match_engine.on_bar(tup)
                self.on_section_bar(section_bar)
                try:
                    cur_pos2 = self.get_current_position()
                    cur_bal2 = float(self.account_manager.calculate_pnl(self.match_engine.get_position_manager()))
                    base_bal2 = float(self.account_manager.init_balance) if abs(self.account_manager.init_balance) > 1e-6 else 1.0
                    nav2 = cur_bal2 / base_bal2
                    logging.info("[replay][final_section_end] tday=%s pos_count=%d avail=%.2f cur_balance=%.2f nav=%.6f", str_trading_day, len(cur_pos2), float(self.get_available()), cur_bal2, nav2)
                    try:
                        if len(cur_pos2) > 0 and (self._last_nav_ is not None) and (abs(nav2 - self._last_nav_) < 1e-12):
                            missing2 = []
                            late2 = []
                            for s_b, net_b in cur_pos2.items():
                                try:
                                    s = s_b.decode() if hasattr(s_b, "decode") else str(s_b)
                                except Exception:
                                    s = str(s_b)
                                bar = self.account_manager.dict_last_bar.get(s, None)
                                if bar is None:
                                    missing2.append(s)
                                    continue
                                try:
                                    btday = self._norm_day(getattr(bar, "trading_day"))
                                    btime = str(getattr(bar, "date_time")).split(' ')[-1]
                                except Exception:
                                    btday = ""
                                    btime = ""
                                sec_cur3 = self._tm_to_sec(self._last_section_time_)
                                sec_bar3 = self._tm_to_sec(btime)
                                lag_ok2 = (sec_cur3 >= 0 and sec_bar3 >= 0 and (sec_cur3 >= sec_bar3) and (sec_cur3 - sec_bar3) <= 18000)
                                if btday != self._norm_day(str_trading_day) or (not lag_ok2):
                                    late2.append(f"{s}:{btday} {btime} lag={sec_cur3 - sec_bar3}")
                                try:
                                    logging.info("[debug][final_section][late_eval] sym=%s btday=%s btime=%s cur_day=%s cur_time=%s lag=%s lag_ok=%s", s, btday, btime, self._norm_day(str_trading_day), self._last_section_time_, str(sec_cur3 - sec_bar3 if (sec_cur3>=0 and sec_bar3>=0) else None), str(bool(lag_ok2)))
                                except Exception:
                                    pass
                            msg2 = "[warn_break][final_no_price_update] tday=%s time=%s pos_count=%d nav=%.6f missing=%s late=%s" % (
                                str_trading_day, self._last_section_time_, len(cur_pos2), nav2, ",".join(missing2), ",".join(late2)
                            )
                            logging.warning(msg2)
                            try:
                                det2 = []
                                for s_b, net_b in cur_pos2.items():
                                    s = s_b.decode() if hasattr(s_b, "decode") else str(s_b)
                                    bar = self.account_manager.dict_last_bar.get(s, None)
                                    btday = ""
                                    btime = ""
                                    if bar is not None:
                                        btday = self._norm_day(getattr(bar, "trading_day"))
                                        btime = str(getattr(bar, "date_time")).split(' ')[-1]
                                    sec_cur4 = self._tm_to_sec(self._last_section_time_)
                                    sec_bar4 = self._tm_to_sec(btime)
                                    det2.append([s, float(net_b), btday, btime, self._norm_day(str_trading_day), self._last_section_time_, (sec_cur4 - sec_bar4 if (sec_cur4>=0 and sec_bar4>=0) else None)])
                                logging.info("[debug][final_section][pos_detail] tday=%s time=%s detail=%s", str_trading_day, self._last_section_time_, str(det2))
                            except Exception:
                                pass
                            try:
                                self.match_engine.force_close_stale_price_futures(self._last_section_time_, self.account_manager, str_trading_day)
                            except Exception:
                                pass
                    except Exception:
                        pass
                except Exception:
                    pass
                logging.info("[replay] final section size=%d", len(section_bar))
                self.match_engine.eod(str_trading_day)
                self.account_manager.on_eod(str_trading_day, self.match_engine.get_position_manager())
                self.on_eod(str_trading_day)
                logging.info("[replay] [%s] done trading day:%s" % (datetime.datetime.now().isoformat(), str_trading_day))
            except Exception as e:
                try:
                    logging.warning("[replay] [%s] exception %s" % (datetime.datetime.now().isoformat(), str(e)))
                    tc = traceback.format_exc()
                    logging.warning("[replay] traceback:%s" % (tc))
                    return
                except Exception:
                    return

    #回测，数据回放
    def process(self):
        self._is_done_ = False
        self.replay()
        self.account_manager.on_done(self.match_engine.get_position_manager())
        self.on_result(self.account_manager.get_result())
        self._is_done_ = True
        return

    # 等待回测结束
    def join(self):
        self.th.join()
        return

    # 结束回测
    def stop(self):
        self.running = False
        self.th.join()
        return

    def start(self):
        # 初始化撮合与账户依赖信息，确保期权交易所需字典已设置
        try:
            self.account_manager.set_product_id(self.dict_product_id)
            self.account_manager.set_daily_multiplier(self.dict_daily_multiplier)
            self.match_engine.set_call_or_put_info(self.dict_call_or_put)
            self.match_engine.set_daily_strike_price(self.dict_daily_strike_price)
            self.match_engine.set_expired_date(self.dict_expired_date)
        except Exception:
            pass
        self.running = True
        self.th = threading.Thread(target=self.process)
        self.th.start()
        return

    def is_done(self):
        return self._is_done_

    def req_msg_pack(self, msg_type):
        req_msg = protocol.msg_header()
        req_msg.session_id = self.cur_uuid.encode('utf-8')
        req_msg.user_name = self.user_name.encode('utf-8')
        req_msg.user_pwd = self.user_pwd.encode('utf-8')
        req_msg.msg_type = msg_type
        req_msg = struct.pack("<i36s16s16s", req_msg.msg_type, req_msg.session_id, req_msg.user_name, req_msg.user_pwd)
        return req_msg

    # 获取交易日列表
    # 返回 DataFrame
    def get_trading_day(self):
        if self.df_trading_day.shape[0] > 0:
            return self.df_trading_day
        req_msg = self.req_msg_pack(5000)
        rsp_msg = self._safe_req([req_msg], tag="get_trading_day")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if isinstance(dt, pd.DataFrame):
                self.df_trading_day = dt
                return dt
            else:
                return None
        return None

    def tdaysoffset(self, n, trade_day):
        if self.df_trading_day.shape[0] == 0:
            self.df_trading_day = self.get_trading_day()
        if type(self.df_trading_day) != type(b'None') and (not self.date_time_util.is_init()):
            self.date_time_util.init(list(self.df_trading_day["trade_day"]))
        days = n
        ret = trade_day
        while days > 0:
            ret = self.date_time_util.get_next_trading_day(trading_day=ret)
            days = days - 1
        while days < 0:
            ret = self.date_time_util.get_pre_trading_day(trading_day=ret)
            days = days + 1
        return ret

    #获取每日合约列表
    #返回DataFrame
    def get_daily_symbol(self):
        if self.df_daily_symbol.shape[0] > 0:
            return self.df_daily_symbol
        if not self.remote_enabled:
            return None
        req_msg = self.req_msg_pack(5001)
        rsp_msg = self._safe_req([req_msg, self.begin_date.encode(), self.end_date.encode()], tag="get_daily_symbol")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if type(dt) == type('None'):
                return None
            else:
                self.df_daily_symbol = dt
                cols = list(dt.columns)
                # choose column names robustly
                def pick_col(candidates):
                    for c in candidates:
                        if c in cols:
                            return c
                        # case-insensitive match
                        for col in cols:
                            if col.lower() == c.lower():
                                return col
                    return None
                col_trade_day = pick_col(["trade_day", "trading_day", "TradingDay"]) or "trade_day"
                col_symbol = pick_col(["symbol", "inst", "instrument", "InstrumentId"]) or "symbol"
                col_pid = pick_col(["product_id", "product", "ProductId"]) or "product_id"
                col_multiplier = pick_col(["multiplier", "multi"]) or "multiplier"

                def normalize_day(s):
                    s = str(s).strip()
                    if len(s) == 8 and s.isdigit():
                        return s[0:4] + '-' + s[4:6] + '-' + s[6:8]
                    return s.replace('_', '-')

                try:
                    import logging
                    logging.info("[get_daily_symbol] cols=%s", str(cols))
                except Exception:
                    pass
                for tup in dt.itertuples():
                    trading_day = normalize_day(getattr(tup, col_trade_day))
                    symbol = getattr(tup, col_symbol)
                    product_id = getattr(tup, col_pid)
                    try:
                        symbol = str(symbol)
                        product_id = str(product_id)
                    except Exception:
                        pass
                    # append exchange suffix if missing, based on product_id like 'CU.SHFE'
                    try:
                        if ('.' not in symbol) and ('.' in product_id):
                            exid = product_id.split('.')[-1]
                            if exid:
                                symbol = f"{symbol}.{exid}"
                    except Exception:
                        pass
                    if trading_day not in self.dict_daily_symbol.keys():
                        self.dict_daily_symbol[trading_day] = {}
                    self.dict_daily_symbol[trading_day][symbol] = symbol
                    self.dict_product_id[symbol] = product_id
                    if trading_day not in self.dict_daily_multiplier.keys():
                        self.dict_daily_multiplier[trading_day] = {}
                    try:
                        m = float(getattr(tup, col_multiplier))
                        if (m != m) or (m <= 0.0) or (abs(m) < 1e-6) or (abs(m) > 1e6):
                            m = 1.0
                        self.dict_daily_multiplier[trading_day][symbol] = m
                    except Exception:
                        self.dict_daily_multiplier[trading_day][symbol] = 1.0
                    # 对订阅的股票指数根项（SH/SZ），按当日写入到 daily_symbol 与 product_id
                    # 仅在订阅列表中存在时添加，避免污染期货合约日表
                    try:
                        for s in self.list_sub_symbol_root:
                            ex = str(s).split('.')[-1]
                            if ex in ("SH", "SZ"):
                                self.dict_daily_symbol[trading_day][s] = s
                                self.dict_product_id[s] = s
                    except Exception:
                        pass
                try:
                    import logging
                    logging.info("[get_daily_symbol] days=%d sample_day=%s sym_count=%d", len(self.dict_daily_symbol.keys()), (list(self.dict_daily_symbol.keys())[0] if len(self.dict_daily_symbol)>0 else "None"), (len(self.dict_daily_symbol.get(list(self.dict_daily_symbol.keys())[0], {})) if len(self.dict_daily_symbol)>0 else 0))
                except Exception:
                    pass
                try:
                    self.account_manager.set_daily_multiplier(self.dict_daily_multiplier)
                    self.account_manager.set_product_id(self.dict_product_id)
                except Exception:
                    pass
                return dt
        return None

    def get_symbol_root(self, symbol):
        try:
            base, exchange = symbol.split('.', 1)
        except Exception:
            return symbol
        prefix = ""
        for ch in base:
            if ch.isdigit():
                break
            prefix += ch
        if prefix == "":
            prefix = base
        return prefix + "." + exchange

    def get_stock_symbol_root(self, symbol):
        try:
            base, exchange = symbol.split('.', 1)
        except Exception:
            return symbol
        prefix = ""
        for ch in base:
            if ch.isdigit():
                break
            prefix += ch
        if prefix == "":
            prefix = base
        return prefix + "." + exchange

    def _root_day_dir(self, symbol_root, day):
        try:
            ex = symbol_root.split(".")[-1] if "." in symbol_root else ""
        except Exception:
            ex = ""
        scan_type = self.data_type if str(self.data_type).lower() != "5m" else "1m"
        return os.path.join(self.cwd, "data", scan_type, ex, symbol_root, day)

    def compute_missing_days_by_root(self, symbol_root):
        days = list(self.trading_date_section or [])
        if not isinstance(days, list):
            days = []
        head_ok = False
        tail_ok = False
        missing = []
        if len(days) > 0:
            first_day = days[0]
            last_day = days[-1]
            fp1 = self._root_day_dir(symbol_root, first_day)
            fp2 = self._root_day_dir(symbol_root, last_day)
            try:
                if os.path.isdir(fp1):
                    for n in os.listdir(fp1):
                        if os.path.isfile(os.path.join(fp1, n)):
                            head_ok = True
                            break
                if os.path.isdir(fp2):
                    for n in os.listdir(fp2):
                        if os.path.isfile(os.path.join(fp2, n)):
                            tail_ok = True
                            break
            except Exception:
                head_ok = False
                tail_ok = False
        for d in days:
            dp = self._root_day_dir(symbol_root, d)
            ok = False
            try:
                if os.path.isdir(dp):
                    for n in os.listdir(dp):
                        if os.path.isfile(os.path.join(dp, n)):
                            ok = True
                            break
            except Exception:
                ok = False
            if not ok:
                missing.append(d)
        return {"head_ok": head_ok, "tail_ok": tail_ok, "missing_days": missing}

    def stand_date_format(self, row, name):
        date = str(row[name])
        return date.replace('-', '')

    # 获取期权列表信息
    #返回 DataFrame
    def get_option_info(self):
        logging.info("[get_option_info] [%s] start" % (datetime.datetime.now().isoformat()))
        if self.df_option_info.shape[0] > 0:
            logging.info("[get_option_info][%s] end" % (datetime.datetime.now().isoformat()))
            return self.df_option_info
        self.df_trading_day = self.get_trading_day()
        if self.df_trading_day is not None and getattr(self.df_trading_day, "shape", [0])[0] > 0:
            self.date_time_util.init(list(self.df_trading_day["trade_day"]))
        req_msg = self.req_msg_pack(5002)
        rsp_msg = self._safe_req([req_msg, self.begin_date.encode("utf-8"), self.end_date.encode("utf-8"), str(self.list_sub_symbol_root).encode("utf-8")], tag="get_option_info")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if type(dt) == type('None'):
                return None
            else:
                self.df_option_info = dt
                logging.info("[get_option_info] [%s] end" % (datetime.datetime.now().isoformat()))
                self.df_option_info.drop_duplicates(subset=["option_code", "trade_day"], keep='last', inplace=True)
                self.df_option_info['exp_date'] = self.df_option_info.apply(self.stand_date_format, axis=1, args=('exp_date',))
                for tup in self.df_option_info.itertuples():
                    td_raw = str(getattr(tup, "trade_day"))
                    trade_day = td_raw.replace('_', '-') if '-' in td_raw or '_' in td_raw else (td_raw[0:4] + '-' + td_raw[4:6] + '-' + td_raw[6:])
                    symbol = getattr(tup, "option_code")
                    if '.' not in symbol:
                        continue
                    option_type = getattr(tup, "option_type")
                    exp_date = getattr(tup, "exp_date")
                    try:
                        s_exp = str(exp_date)
                        if len(s_exp) == 8 and s_exp.isdigit():
                            exp_norm = s_exp[0:4] + "-" + s_exp[4:6] + "-" + s_exp[6:8]
                        else:
                            exp_norm = s_exp.replace("_", "-")
                    except Exception:
                        exp_norm = str(exp_date)
                    strike_px = getattr(tup, "strike")
                    multi = float(getattr(tup, "multi"))
                    underlying_code = getattr(tup, "underlying_code")
                    if "Io" in underlying_code:
                        underlying_code = underlying_code.replace("Io", "IF")
                    elif "Mo" in underlying_code:
                        underlying_code = underlying_code.replace("Mo", "IM")
                    elif "Ho" in underlying_code:
                        underlying_code = underlying_code.replace("Ho", "IH")
                    product_id = getattr(tup, "product_id")
                    if trade_day not in self.dict_daily_symbol:
                        self.dict_daily_symbol[trade_day] = {}
                    self.dict_daily_symbol[trade_day][symbol] = symbol
                    if trade_day not in self.dict_daily_multiplier:
                        self.dict_daily_multiplier[trade_day] = {}
                    self.dict_daily_multiplier[trade_day][symbol] = multi
                    self.dict_expired_date[symbol] = exp_norm
                    self.dict_call_or_put[symbol] = option_type
                    if trade_day not in self.dict_daily_strike_price:
                        self.dict_daily_strike_price[trade_day] = {}
                    self.dict_daily_strike_price[trade_day][symbol] = strike_px
                    self.dict_underlying_code[symbol] = underlying_code
                    self.dict_product_id[symbol] = product_id
                try:
                    self.account_manager.set_daily_multiplier(self.dict_daily_multiplier)
                    self.account_manager.set_product_id(self.dict_product_id)
                except Exception:
                    pass
                return self.df_option_info
        return None

    # 获取当前 bar 的期权列表
    # 返回 DataFrame（仅包含与 bar 同期的期权基本信息）
    def get_option_info_by_bar(self, bar):
        list_data = []
        for tup in self.df_option_info.itertuples():
            if bar.option_code == getattr(tup, "option_code") and bar.trade_day == getattr(tup, "trade_day"):
                list_data.append(list(tup[1:]))
        if len(list_data) > 0:
            return pd.DataFrame(list_data, columns=["option_code", "option_name", "underlying_code", "option_type", "strike", "exp_date", "multi"])
        return None

    #获取期权分钟数据
    # date 数据日期
    # data_type 数据类型：1m：分钟；1d：日
    # exchange 交易所 CFFEX：中金所；SHFE：上期所；DCE：大商所；CZCE：郑商所；SH：上交所；SZ：深交所
    # symbol 期权合约，如果为空，则返回当天所有合约数据
    # 返回 DataFrame
    def get_option_bar(self, date, data_type, exchange, symbol = ""):
        symbol_root = self.get_symbol_root(symbol)
        option_filename = os.path.join(self.cwd, "data", data_type, exchange, symbol_root, date, symbol)
        columns=["option_code","rt_time","trade_day","option_open","option_high","option_low","option_close",
                "option_amt","option_volume","option_open_interest","option_pre_close",
                "delta","gamma","theta","vega","imp_vol"]
        option_df = pd.DataFrame([], columns=columns)
        if os.path.exists(option_filename) and os.path.getsize(option_filename) > 0:
            try:
                option_df = pd.read_csv(option_filename)
            except Exception:
                try:
                    option_df = pd.read_table(option_filename)
                except Exception:
                    option_df = pd.DataFrame([], columns=columns)
            return option_df
        dir_path = os.path.join(self.cwd, "data", data_type, exchange, symbol_root, date)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        req_msg = protocol.msg_header()
        if data_type == "1m":
            req_msg = self.req_msg_pack(100)
        elif data_type == "1d":
            req_msg = self.req_msg_pack(101)
        else:
            return option_df
        rsp_msg = self._safe_req([req_msg, date.encode('utf-8'), exchange.encode('utf-8'), symbol.encode('utf-8')], tag="get_option_bar")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if type(dt) == type('None') or type(dt) == type(b'None'):
                return option_df
            elif dt.shape[0] == 0:
                    return option_df
            else:
                    if type(dt) == type(pd.DataFrame()):
                        if data_type == "1d":
                            if "open_time" in dt.columns and "close_time" in dt.columns:
                                dt.drop(["open_time", "close_time"], axis=1, inplace=True)
                            if "delta" not in dt.columns:
                                dt["delta"] = 0
                                dt["gamma"] = 0
                                dt["theta"] = 0
                                dt["vega"] = 0
                                dt["imp_vol"] = 0
                        if len(dt.columns) == 0:
                            option_df.to_csv(option_filename, header=True, index=False)
                            return option_df
                        dt.columns = columns
                        try:
                            dt.to_csv(option_filename, header=True, index=False)
                        except Exception:
                            pass
                    else:
                        dt = pd.DataFrame([],columns=columns)
                        logging.warning("[get_option_bar] [%s] load %s failed" % (datetime.datetime.now().isoformat(), option_filename))
                    return dt
        return option_df

    #获取期货分钟数据
    # date 数据日期
    # data_type 数据类型：1m：分钟；1d：日
    # exchange 交易所 CFFEX:中金所； SHFE:上期所； DCE：大商所； CZCE：郑商所
    # symbol 合约，如果为空，则返回当天所有合约数据
    # 返回 DataFrame
    def get_future_bar(self, date, data_type, exchange, symbol = ""):
        symbol_root = self.get_symbol_root(symbol)
        if len(symbol_root) == 0:
            symbol_root = self.get_stock_symbol_root(symbol)
        def _try_read(paths):
            for fn in paths:
                try:
                    if os.path.exists(fn) and os.path.getsize(fn) > 0:
                        try:
                            df = pd.read_csv(fn)
                        except Exception:
                            try:
                                df = pd.read_table(fn)
                            except Exception:
                                df = pd.DataFrame()
                        if isinstance(df, pd.DataFrame):
                            return df
                except Exception:
                    continue
            return None
        scan_type = data_type if str(data_type).lower() != "5m" else "1m"
        local_candidates = []
        local_candidates.append(os.path.join(self.cwd, "data", scan_type, exchange, symbol_root, date, symbol))
        local_candidates.append(os.path.join(self.cwd, "data", "data", scan_type, exchange, symbol_root, date, symbol))
        try:
            ds_base = os.path.join(os.getcwd(), "dataset")
            local_candidates.append(os.path.join(ds_base, "data", scan_type, exchange, symbol_root, date, symbol))
            local_candidates.append(os.path.join(ds_base, "data", "data", scan_type, exchange, symbol_root, date, symbol))
        except Exception:
            pass
        df_local = _try_read(local_candidates)
        if isinstance(df_local, pd.DataFrame):
            return df_local
        dest_dir = os.path.join(self.cwd, "data", data_type, exchange, symbol_root, date)
        if not os.path.exists(dest_dir):
            try:
                os.makedirs(dest_dir)
            except Exception:
                pass
        future_filename = os.path.join(dest_dir, symbol)
        req_msg = protocol.msg_header()
        if data_type == "1m":
            req_msg = self.req_msg_pack(200)
        elif data_type == "1d":
            req_msg = self.req_msg_pack(201)
        else:
            return pd.DataFrame()
        rsp_msg = self._safe_req([req_msg, date.encode('utf-8'), exchange.encode('utf-8'), symbol.encode('utf-8')], tag="get_future_bar")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if type(dt) == type('None') or dt is None:
                try:
                    df_fb = _try_read(local_candidates)
                    if isinstance(df_fb, pd.DataFrame):
                        return df_fb
                except Exception:
                    pass
                logging.warning("[get_future_bar] [%s] empty data %s" % (datetime.datetime.now().isoformat(), future_filename))
                return pd.DataFrame()
            else:
                if type(dt) == type(pd.DataFrame()):
                    try:
                        dt.to_csv(future_filename, header=True, index=False)
                        logging.debug("[get_future_bar] [%s] saved %s rows=%d" % (datetime.datetime.now().isoformat(), future_filename, dt.shape[0]))
                    except Exception:
                        logging.warning("[get_future_bar] [%s] save failed %s" % (datetime.datetime.now().isoformat(), future_filename))
                    return dt
                else:
                    try:
                        df_fb2 = _try_read(local_candidates)
                        if isinstance(df_fb2, pd.DataFrame):
                            logging.warning("[get_future_bar] [%s] non-df from server, fallback local %s" % (datetime.datetime.now().isoformat(), future_filename))
                            return df_fb2
                    except Exception:
                        pass
                    logging.warning("[get_future_bar] [%s] non-df data for %s" % (datetime.datetime.now().isoformat(), future_filename))
                    return pd.DataFrame()
        try:
            df_fb3 = _try_read(local_candidates)
            if isinstance(df_fb3, pd.DataFrame):
                return df_fb3
        except Exception:
            pass
        return pd.DataFrame()

    # 获取期货原始分钟数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_datetime 开始时间，例如："20200101 09:00:00"
    # end_datetime 结束时间，例如："20240601 15:00:00"
    # data_type 数据类型，例如："main"、"sec"、"sec_fwd"
    # 返回 DataFrame
    def get_future_origin_minute_bar(self, symbol_root, start_datetime, end_datetime, data_type):
        future_df = pd.DataFrame()
        req_msg = protocol.msg_header()
        req_msg = self.req_msg_pack(300)
        rsp_msg = self._safe_req([req_msg, symbol_root.encode('utf-8'), start_datetime.encode('utf-8'), end_datetime.encode('utf-8'), data_type.encode('utf-8')], tag="get_future_origin_minute_bar")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if (not isinstance(dt, pd.DataFrame)) or dt is None:
                return future_df
            else:
                return dt
        return future_df

    # 获取期货原始日数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_date 开始日期，例如："20200101"
    # end_date 结束日期，例如："20240601"
    # data_type 数据类型，例如："main"、"sec"、"sec_fwd"
    # 返回 DataFrame
    def get_future_origin_daily_bar(self, symbol_root, start_date, end_date, data_type):
        future_df = pd.DataFrame()
        req_msg = protocol.msg_header()
        req_msg = self.req_msg_pack(301)
        rsp_msg = self._safe_req([req_msg, symbol_root.encode('utf-8'), start_date.encode('utf-8'), end_date.encode('utf-8'), data_type.encode('utf-8')], tag="get_future_origin_daily_bar")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if (not isinstance(dt, pd.DataFrame)) or dt is None:
                return future_df
            else:
                return dt
        return future_df

    # 获取期货特殊日数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_date 开始日期，例如："20200101"
    # end_date 结束日期，例如："20240601"
    # data_type 数据类型，例如："v3"
    # 返回 DataFrame
    def get_future_spec_daily_bar(self, symbol_root, start_date, end_date, data_type):
        future_df = pd.DataFrame()
        req_msg = protocol.msg_header()
        req_msg = self.req_msg_pack(302)
        rsp_msg = self._safe_req([req_msg, symbol_root.encode('utf-8'), start_date.encode('utf-8'), end_date.encode('utf-8'), data_type.encode('utf-8')], tag="get_future_spec_daily_bar")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if (not isinstance(dt, pd.DataFrame)) or dt is None:
                return future_df
            else:
                return dt
        return future_df

    # 获取期货特殊复权日数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_date 开始日期，例如："20200101"
    # end_date 结束日期，例如："20240601"
    # data_type 数据类型，例如："v3"
    # 返回 DataFrame
    def get_future_spec_adj_factor_daily_bar(self, symbol_root, start_date, end_date, data_type):
        future_df = pd.DataFrame()
        req_msg = protocol.msg_header()
        req_msg = self.req_msg_pack(303)
        rsp_msg = self._safe_req([req_msg, symbol_root.encode('utf-8'), start_date.encode('utf-8'), end_date.encode('utf-8'), data_type.encode('utf-8')], tag="get_future_spec_adj_factor_daily_bar")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if (not isinstance(dt, pd.DataFrame)) or dt is None:
                return future_df
            else:
                return dt
        return future_df

    # 获取期货复权分钟数据
    # symbol_root 品种，例如："A.DCE"、"CU.SHFE"、"IC.CFFEX"、"SC.INE"、"AP.CZCE"、"SI.GFEX"
    # start_datetime 开始时间，例如："20200101 09:00:00"
    # end_datetime 结束时间，例如："20240601 15:00:00"
    # 返回 DataFrame
    def get_future_adj_factor_minute_bar(self, symbol_root, start_datetime, end_datetime):
        future_df = pd.DataFrame()
        req_msg = protocol.msg_header()
        req_msg = self.req_msg_pack(304)
        rsp_msg = self._safe_req([req_msg, symbol_root.encode('utf-8'), start_datetime.encode('utf-8'), end_datetime.encode('utf-8')], tag="get_future_adj_factor_minute_bar")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if (not isinstance(dt, pd.DataFrame)) or dt is None:
                return future_df
            else:
                return dt
        return future_df

    # 获取期货主力合约表
    # symbol_root 品种，例如：'A.DCE'
    # 返回 DataFrame
    def get_main_symbol_info(self, symbol_root):
        df = pd.DataFrame()
        req_msg = protocol.msg_header()
        req_msg = self.req_msg_pack(5003)
        rsp_msg = self._safe_req([req_msg, symbol_root.encode('utf-8')], tag="get_main_symbol_info")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if (not isinstance(dt, pd.DataFrame)) or dt is None:
                return df
            else:
                return dt
        return df

    # 获取期货主力复权因子表
    # symbol_root 品种，例如：'A.DCE'
    # 返回 DataFrame
    def get_main_symbol_adj_factor_info(self, symbol_root):
        df = pd.DataFrame()
        req_msg = protocol.msg_header()
        req_msg = self.req_msg_pack(5004)
        rsp_msg = self._safe_req([req_msg, symbol_root.encode('utf-8')], tag="get_main_symbol_adj_factor_info")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if (not isinstance(dt, pd.DataFrame)) or dt is None:
                return df
            else:
                return dt
        return df

    #获取股票指数分钟数据
    # date 数据日期
    # symbol 合约
    # 返回 DataFrame
    def get_stock_index_minute_bar(self, date, symbol):
        data_type = "1m"
        exchange = symbol.split('.')[-1]
        symbol_root = self.get_symbol_root(symbol)
        if len(symbol_root) == 0:
            symbol_root = self.get_stock_symbol_root(symbol)
        filename = os.path.join(self.cwd, "data", data_type, exchange, symbol_root, date, symbol)
        try:
            if os.path.exists(filename):
                try:
                    df = pd.read_csv(filename)
                except Exception:
                    try:
                        df = pd.read_table(filename)
                    except Exception:
                        df = pd.DataFrame()
                return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        except Exception:
            pass
        dir_path = os.path.join(self.cwd, "data", data_type, exchange, symbol_root, date)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        req_msg = protocol.msg_header()
        if data_type == "1m":
            req_msg = self.req_msg_pack(1000)
        elif data_type == "1d":
            req_msg = self.req_msg_pack(1001)
        else:
            return pd.DataFrame()
        rsp_msg = self._safe_req([req_msg, symbol.encode('utf-8'), date.encode('utf-8')], tag="get_stock_index_minute_bar")
        if len(rsp_msg) >= 2:
            dt = pickle.loads(rsp_msg[-1])
            if isinstance(dt, pd.DataFrame):
                dt.to_csv(filename, header=True, index=False)
                return dt
            else:
                logging.warning("[get_stock_index_minute_bar] [%s] load %s failed" % (datetime.datetime.now().isoformat(), filename))
                return pd.DataFrame()
        return pd.DataFrame()

    # 发送目标仓位
    # dict_target_position 目标仓位，例如：{'CU2409.SHFE' : 0.1}
    def send_target_position(self, dict_target_position):
        return

    #获取期货实时分钟线原始数据
    def get_live_1m_data(self, list_symbol, begin_datetime, end_datetime, columns):
        return self.md_live_api.get_origin_data(list_symbol, begin_datetime, end_datetime, columns)

    #获取期货实时分钟线复权数据
    def get_live_1m_adj_data(self, list_symbol, begin_datetime, end_datetime, columns):
        return self.md_live_api.get_adj_factor_data(list_symbol, begin_datetime, end_datetime, columns)
