import sqlite3
import os
import pdb
import trace
import importlib_local
import logging

DB_PATH = os.path.join("db", "user_data.db")
UPLOAD_DIR = os.path.join("strategy", "source_code")
LOCAL_STRAT_PATH = os.path.join("strategy", "source_code")


class StrategyManager(object):
    def __init__(self) -> None:
        """
        策略管理器
        - _dict_strat_: {user_name: {strat_filename: strat_instance}}
        - _api_: 平台API引用
        - _dict_target_position_: 聚合后的目标仓位（含资金权重）
        - dict_user_name: 用户名列表
        - acct_balance_ratio: 各策略资金分配比例
        - trading_session_by_strat: 策略交易时段
        - is_live_mode: 是否实盘
        """
        self._dict_strat_ = {}
        self._api_ = None
        self._dict_target_position_ = {}
        self.dict_user_name = {}
        self.acct_balance_ratio = {}
        self.trading_session_by_strat = {}
        self.is_live_mode = False
        return

    def init(self, api, dict_user_name, acct_balance_ratio, trading_session_by_strat, is_live_mode):
        """
        初始化策略管理器并加载策略
        """
        self._api_ = api
        self.dict_user_name = dict_user_name
        self.acct_balance_ratio = acct_balance_ratio
        self.trading_session_by_strat = trading_session_by_strat
        self.is_live_mode = is_live_mode
        self.load_strategy_from_db()
        return

    def load_strategy_from_db(self):
        """
        加载策略
        - 优先从本地路径加载（DB 不存在）
        - 否则从数据库 uploads 列表加载
        """
        if not os.path.exists(DB_PATH):
            for user_name in self.dict_user_name.keys():
                for root, dirs, files in os.walk(os.path.join(LOCAL_STRAT_PATH, user_name)):
                    for f in files:
                        if ".py" not in f or f not in self.trading_session_by_strat.keys():
                            continue
                        fn = os.path.join(root, f)
                        with open(fn, 'rb') as strat_f:
                            strat_code = strat_f.read()
                        exec_code = compile(strat_code, fn, "exec")
                        scope = {}
                        exec(exec_code, scope)
                        self.add_strategy(user_name, f, scope)
                        print("[strategy_manager] load strategy {}".format(fn))
            return

        uploads_all = []
        try:
            for user_name in self.dict_user_name.keys():
                with sqlite3.connect(DB_PATH) as conn:
                    cn = conn.cursor()
                    cn.execute('SELECT * FROM uploads WHERE username = ? ORDER BY upload_time DESC', (user_name,))
                    uploads_all += cn.fetchall()
        except Exception as e:
            print(str(e))

        for upload in uploads_all:
            file_id, username, filename, is_active, upload_time = upload
            if username not in self.dict_user_name.keys() or is_active == "no" or (".py" not in filename and ".pyd" not in filename):
                continue
            filepath = os.path.join(UPLOAD_DIR, username, filename)
            if os.path.exists(filepath):
                if "py" in filename.split('.')[-1]:
                    with open(filepath, 'rb') as strat_f:
                        strat_code = strat_f.read()
                    exec_code = compile(strat_code, filepath, "exec")
                    scope = {}
                    exec(exec_code, scope)
                    self.add_strategy(username, filename, scope)
                elif "pyd" in filename.split('.')[-1]:
                    if username not in self._dict_strat_.keys():
                        self._dict_strat_[username] = {}
                    strat = importlib_local.path_import(filepath)
                    self._dict_strat_[username][filename] = getattr(strat, filename.split('.')[0])()
                    self._dict_strat_[username][filename].init(self._api_)
        return

    def add_strategy(self, user_name, strat_name, strat):
        """
        将策略加入管理器，并进行初始化
        """
        if user_name not in self._dict_strat_.keys():
            self._dict_strat_[user_name] = {}
        self._dict_strat_[user_name][strat_name] = strat.get(strat_name.split('.')[0])()
        self._dict_strat_[user_name][strat_name].init(self._api_)
        return

    def on_bod(self, trading_day):
        self.trading_day = trading_day
        logging.debug("[strategy_manager][bod] trading_day=%s", trading_day)
        for user_name, dict_strat in self._dict_strat_.items():
            for strat_name, strat in dict_strat.items():
                if hasattr(strat, "on_bod"):
                    strat.on_bod(trading_day)
        return

    def on_section_bar(self, list_bar):
        """
        截面bar处理
        - 聚合各策略目标仓位，并乘以资金分配比例
        - 返回聚合后目标仓位与是否变化标志
        """
        self._dict_target_position_.clear()
        is_pos_changed = False
        logging.debug("[strategy_manager][section] bars=%d", len(list_bar))
        for user_name, dict_strat in self._dict_strat_.items():
            for strat_name, strat in dict_strat.items():
                if self.is_live_mode:
                    dict_target_pos, pos_changed = strat.calculate_target_position(list_bar)
                elif strat_name in self.trading_session_by_strat.keys():
                    dict_target_pos, pos_changed = strat.calculate_target_position(list_bar)
                else:
                    continue
                try:
                    logging.info("[strategy_manager][section] strat=%s changed=%s targets=%s", strat_name, str(pos_changed), str(dict_target_pos))
                except Exception:
                    pass
                if pos_changed:
                    is_pos_changed = True
                for symbol, target_pos in dict_target_pos.items():
                    if user_name not in self._dict_target_position_.keys():
                        self._dict_target_position_[user_name] = {}
                    if symbol not in self._dict_target_position_[user_name].keys():
                        self._dict_target_position_[user_name][symbol] = 0
                    try:
                        ratio = float(self.acct_balance_ratio.get(strat_name, 1.0))
                    except Exception:
                        ratio = 1.0
                    agg_val = target_pos * ratio
                    self._dict_target_position_[user_name][symbol] += agg_val
                    try:
                        logging.debug("[strategy_manager][section] user=%s symbol=%s raw=%.6f ratio=%.4f agg=%.6f", user_name, symbol, float(target_pos), float(ratio), float(self._dict_target_position_[user_name][symbol]))
                    except Exception:
                        pass
        logging.info("[strategy_manager][section] target_pos=%s changed=%s", str(self._dict_target_position_), str(is_pos_changed))
        try:
            for user_name, pos_map in self._dict_target_position_.items():
                logging.info("[strategy_manager][section][agg_summary] user=%s symbols=%d live=%s", str(user_name), int(len(pos_map.keys())), str(self.is_live_mode))
        except Exception:
            pass
        return self._dict_target_position_, is_pos_changed

    def on_bar(self, bar):
        """
        单根bar处理（预留）
        """
        return
