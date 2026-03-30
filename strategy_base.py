import cta_platform_api
import strategy_manager
import copy
import pdb
import logging
import datetime

class strategy_base(object):
    def __init__(self) -> None:
        """
        策略基类
        - api/_raw_api_: 平台API与底层API引用
        - cur_trading_day: 当前交易日
        - last_dict_target_position: 上次目标仓位记录（按用户与合约）
        - dict_user_name: 用户名列表
        """
        self.api = 0
        self._raw_api_ = None
        self.cur_trading_day = None
        self.last_dict_target_position = {}
        self.dict_user_name = {}
        self._acted_today = {}
    
    def init(self, api, dict_user_name, acct_balance_ratio, trading_session_by_strat):
        """
        初始化策略基类
        :param api: 平台API底层对象
        :param dict_user_name: 用户名字典
        :param acct_balance_ratio: 账户资金分配比例
        :param trading_session_by_strat: 各策略的交易时段
        """
        self.dict_user_name = dict_user_name
        self._raw_api_ = api
        self.api = cta_platform_api.cta_platform_api()
        self.api.init(api)
        self.strat_manager = strategy_manager.StrategyManager()
        self.strat_manager.init(self.api, dict_user_name, acct_balance_ratio, trading_session_by_strat, self._raw_api_.get_live_mode())
        return
    
    def on_bod(self, trading_day):
        """
        交易日开始
        """
        self.cur_trading_day = trading_day
        try:
            try:
                s = str(trading_day).split(" ")[0]
            except Exception:
                s = str(trading_day)
            setattr(self.api, "cur_trading_day", s)
        except Exception:
            pass
        self._acted_today = {}
        self.strat_manager.on_bod(trading_day)
        logging.info("[strategy_base][on_bod] ----------begin of day---------- %s", trading_day)
        return
    
    def on_eod(self, trading_day):
        """
        交易日结束
        """
        logging.info("[strategy_base][on_eod] ----------end of day---------- %s", trading_day)
        return
    
    def handle_section_bar(self, list_bar):
        """
        处理截面bar数据
        - 非实盘模式：直接按目标仓位下单
        - 实盘模式：推送目标仓位与时间戳
        - 目标仓位支持两种语义：
          1) 小数（-1.0~1.0）：表示资金权重，按余额/乘数/收盘价换算为手数
          2) 整数：直接表示目标持仓手数（正多负空）
        """
        dict_target_position, is_pos_changed = self.strat_manager.on_section_bar(list_bar)
        logging.debug("[strategy_base][section] bars=%d target=%s changed=%s", len(list_bar), str(dict_target_position), str(is_pos_changed))
        if not is_pos_changed:
            try:
                for cur_user_name in self.dict_user_name.keys():
                    if self._acted_today.get(cur_user_name, "") == self.cur_trading_day:
                        try:
                            self.last_dict_target_position[cur_user_name] = copy.deepcopy(dict_target_position.get(cur_user_name, {}))
                        except Exception:
                            pass
                        logging.info("[strategy_base][daily_gate] day=%s user=%s skip further actions", str(self.cur_trading_day), str(cur_user_name))
                        return
                for cur_user_name in self.dict_user_name.keys():
                    dict_cur_pos = self._raw_api_.get_current_position()
                    user_targets = dict_target_position.get(cur_user_name, {})
                    need_act = False
                    reason = "none"
                    for symbol, target_pos in user_targets.items():
                        cur_net = dict_cur_pos.get(symbol.encode('utf-8'), 0)
                        if int(round(cur_net)) != int(round(target_pos)):
                            need_act = True
                            reason = "mismatch"
                            break
                    if not need_act:
                        for symbol_b, net_pos_b in dict_cur_pos.items():
                            if symbol_b.decode('utf-8') not in user_targets.keys() and abs(net_pos_b) != 0:
                                need_act = True
                                reason = "stray"
                                break
                    try:
                        logging.info("[strategy_base][unchanged_check] day=%s user=%s need_act=%s reason=%s targets=%s cur_pos=%s", str(self.cur_trading_day), str(cur_user_name), str(bool(need_act)), reason, str(user_targets), str({k.decode('utf-8'): v for k, v in dict_cur_pos.items()}))
                    except Exception:
                        pass
                    if not need_act:
                        self.last_dict_target_position = copy.deepcopy(dict_target_position)
                        return
            except Exception:
                self.last_dict_target_position = copy.deepcopy(dict_target_position)
                return
        
        if not self._raw_api_.get_live_mode():
            # cur_user_name = self._raw_api_.user_name
            for cur_user_name in self.dict_user_name.keys():
                user_targets = dict_target_position.get(cur_user_name, {})
                try:
                    exp_map = dict(getattr(self._raw_api_.account_manager, "dict_expired_date", {}))
                    td = str(self.cur_trading_day)
                    for k in list(user_targets.keys()):
                        exp_raw = exp_map.get(k, None)
                        try:
                            base = k.split('.')[0]
                            digits = "".join(ch for ch in base if ch.isdigit())
                            exp_ym_auto = None
                            if len(digits) >= 4:
                                y = 2000 + int(digits[0:2])
                                m = int(digits[2:4])
                                exp_ym_auto = y * 100 + m
                            cur_ym_auto = int(td[0:4]) * 100 + int(td[5:7])
                        except Exception:
                            exp_ym_auto = None
                            cur_ym_auto = None
                        if exp_raw is not None:
                            exp_norm = str(exp_raw).replace("_", "-")
                            if len(exp_norm) == 8 and exp_norm.isdigit():
                                exp_norm = exp_norm[0:4] + "-" + exp_norm[4:6] + "-" + exp_norm[6:8]
                            if td >= exp_norm:
                                user_targets[k] = 0
                                try:
                                    logging.info("[strategy_base][expiry_block_open] user=%s symbol=%s exp=%s day=%s", str(cur_user_name), str(k), exp_norm, td)
                                except Exception:
                                    pass
                        if exp_ym_auto is not None and cur_ym_auto is not None and cur_ym_auto >= exp_ym_auto:
                            user_targets[k] = 0
                            try:
                                logging.info("[strategy_base][delivery_month_block_open] user=%s symbol=%s ym=%d cur_ym=%d", str(cur_user_name), str(k), int(exp_ym_auto), int(cur_ym_auto))
                            except Exception:
                                pass
                except Exception:
                    pass
                if self._acted_today.get(cur_user_name, "") == self.cur_trading_day:
                    try:
                        self.last_dict_target_position[cur_user_name] = copy.deepcopy(user_targets)
                    except Exception:
                        pass
                    continue
                
                balance = self._raw_api_.get_balance()
                dict_cur_pos = self._raw_api_.get_current_position()
                logging.debug("[strategy_base][section] cur_pos=%s balance=%.2f", str(dict_cur_pos), balance)
                dict_close_price = {}
                act_attempts = 0
                act_success = 0
                close_success = 0
                act_fail = 0
                
                for bar in list_bar:
                    if self._raw_api_.get_is_option():
                        dict_close_price[str(getattr(bar, "option_code"))] = float(getattr(bar, "option_close"))
                    else:
                        dict_close_price[str(getattr(bar, "symbol"))] = float(getattr(bar, "close"))
                
                # close positions not in targets first to release margin
                for symbol_b, net_pos_b in dict_cur_pos.items():
                    str_symbol_b = symbol_b.decode('utf-8')
                    exp_norm2 = None
                    try:
                        exp_raw2 = getattr(self._raw_api_.account_manager, "dict_expired_date", {}).get(str_symbol_b, None)
                        if exp_raw2 is not None:
                            exp_norm2 = str(exp_raw2).replace("_", "-")
                            if len(exp_norm2) == 8 and exp_norm2.isdigit():
                                exp_norm2 = exp_norm2[0:4] + "-" + exp_norm2[4:6] + "-" + exp_norm2[6:8]
                    except Exception:
                        exp_norm2 = None
                    try:
                        base2 = str_symbol_b.split('.')[0]
                        digits2 = "".join(ch for ch in base2 if ch.isdigit())
                        cur_ym2 = int(str(self.cur_trading_day)[0:4]) * 100 + int(str(self.cur_trading_day)[5:7])
                        exp_ym2 = None
                        if len(digits2) >= 4:
                            y2 = 2000 + int(digits2[0:2])
                            m2 = int(digits2[2:4])
                            exp_ym2 = y2 * 100 + m2
                    except Exception:
                        exp_ym2 = None
                        cur_ym2 = None
                    need_close_exp = (exp_norm2 is not None and str(self.cur_trading_day) >= exp_norm2 and abs(net_pos_b) != 0)
                    need_close_dm = (exp_ym2 is not None and cur_ym2 is not None and cur_ym2 >= exp_ym2 and abs(net_pos_b) != 0)
                    if str_symbol_b not in user_targets.keys() or need_close_exp or need_close_dm:
                        if abs(net_pos_b) != 0:
                            try:
                                act_attempts += 1
                                oid_close = self._raw_api_.send_order(str_symbol_b, 1, -net_pos_b)
                                if oid_close == -1:
                                    logging.error("[strategy_base][close_pre][perm_fail] symbol=%s sz=%d dir=%d", str_symbol_b, 1, 1 if net_pos_b>0 else 0)
                                    act_fail += 1
                                    continue
                                else:
                                    close_success += 1
                            except Exception as e:
                                try:
                                    logging.warning("[strategy_base][close_pre][retry] symbol=%s err=%s", str_symbol_b, str(e))
                                    act_attempts += 1
                                    oid_close = self._raw_api_.send_order(str_symbol_b, 1, -net_pos_b)
                                    if oid_close == -1:
                                        logging.error("[strategy_base][close_pre][perm_fail] symbol=%s sz=%d dir=%d", str_symbol_b, 1, 1 if net_pos_b>0 else 0)
                                        act_fail += 1
                                        continue
                                except Exception as e2:
                                    logging.error("[strategy_base][close_pre][fail] symbol=%s err=%s", str_symbol_b, str(e2))
                                    act_fail += 1
                                    continue
                            logging.debug("[strategy_base][close_pre] symbol=%s sz=%d side=%d", str_symbol_b, 1, 1 if net_pos_b>0 else 0)
                
                for symbol, target_pos in user_targets.items():
                    last_target_pos = -1000
                    if cur_user_name in self.last_dict_target_position.keys():
                        if symbol in self.last_dict_target_position[cur_user_name].keys():
                            last_target_pos = self.last_dict_target_position[cur_user_name][symbol]
                    
                    if abs(target_pos - last_target_pos) < 0.000001 and (symbol.encode('utf-8') not in dict_cur_pos.keys()):
                        continue
                    
                    multiplier = 1.0
                    if hasattr(self._raw_api_, "dict_daily_multiplier") and \
                       self.cur_trading_day in getattr(self._raw_api_, "dict_daily_multiplier", {}) and \
                       symbol in self._raw_api_.dict_daily_multiplier[self.cur_trading_day].keys():
                        multiplier = self._raw_api_.dict_daily_multiplier[self.cur_trading_day][symbol]
                    try:
                        m = float(multiplier)
                        if (m != m) or (m <= 0.0) or (abs(m) < 1e-6) or (abs(m) > 1e6):
                            m = 1.0
                        multiplier = m
                    except Exception:
                        multiplier = 1.0
                    
                    if abs(target_pos) < 1:
                        if symbol in dict_close_price.keys() and dict_close_price[symbol] > 0:
                            try:
                                available = float(getattr(self._raw_api_, "get_available")())
                            except Exception:
                                available = float(balance)
                            try:
                                mdp = float(getattr(getattr(self._raw_api_, "match_engine", object()), "max_deal_pct", 0.2))
                            except Exception:
                                mdp = 0.2
                            try:
                                mr = float(getattr(getattr(self._raw_api_, "account_manager", object()), "margin_rate", 0.2))
                            except Exception:
                                mr = 0.2
                            cap_total = available * mdp
                            denom = multiplier * dict_close_price[symbol]
                            qty = 1
                            target_pos = qty if target_pos >= 0 else -qty
                            try:
                                logging.info("[strategy_base][fractional] symbol=%s target_raw=%.6f qty=%d avail=%.2f mdp=%.2f mr=%.2f mult=%.6f price=%.6f", symbol, float(target_pos), int(abs(qty)), float(available), float(mdp), float(mr), float(multiplier), float(dict_close_price[symbol]))
                            except Exception:
                                pass
                            if abs(target_pos) == 0:
                                try:
                                    logging.warning("[strategy_base][section] zero lots computed: symbol=%s available=%.2f mdp=%.2f mr=%.2f denom=%.6f cap_total=%.2f", symbol, available, mdp, mr, denom, cap_total)
                                except Exception:
                                    pass
                                continue
                        else:
                            logging.warning("[strategy_base][section] invalid close price for %s, skip fractional target", symbol)
                            continue
                    
                    # if abs(target_pos) == 0:
                    #     continue
                    
                    if symbol.encode('utf-8') not in dict_cur_pos.keys():
                        if abs(target_pos) != 0:
                            try:
                                logging.info("[strategy_base][pre_action] user=%s symbol=%s action=open lots=%d", str(cur_user_name), symbol, int(abs(target_pos)))
                            except Exception:
                                pass
                            try:
                                act_attempts += 1
                                oid = self._raw_api_.send_order(symbol, 1, target_pos)
                                if oid == -1:
                                    logging.error("[strategy_base][send_order][perm_fail] symbol=%s sz=%d dir=%d", symbol, 1, 0 if target_pos>=0 else 1)
                                    act_fail += 1
                                    continue
                                else:
                                    act_success += 1
                            except Exception as e:
                                try:
                                    logging.warning("[strategy_base][send_order][retry] symbol=%s err=%s", symbol, str(e))
                                    act_attempts += 1
                                    oid = self._raw_api_.send_order(symbol, 1, target_pos)
                                    if oid == -1:
                                        logging.error("[strategy_base][send_order][perm_fail] symbol=%s sz=%d dir=%d", symbol, 1, 0 if target_pos>=0 else 1)
                                        act_fail += 1
                                        continue
                                except Exception as e2:
                                    logging.error("[strategy_base][send_order][fail] symbol=%s err=%s", symbol, str(e2))
                                    act_fail += 1
                                    continue
                            logging.debug("[strategy_base][send_order] symbol=%s sz=%d side=%d", symbol, 1, 0 if target_pos>=0 else 1)
                    else:
                        cur_net = dict_cur_pos[symbol.encode('utf-8')]
                        diff_pos = target_pos - cur_net
                        if abs(diff_pos) != 0:
                            # 方向相反时先平再开，避免保证金不足
                            if (cur_net > 0 and target_pos < 0) or (cur_net < 0 and target_pos > 0):
                                try:
                                    logging.info("[strategy_base][pre_action] user=%s symbol=%s action=reverse cur=%d target=%d plan=[close->open]", str(cur_user_name), symbol, int(cur_net), int(target_pos))
                                except Exception:
                                    pass
                                # close current to zero
                                try:
                                    act_attempts += 1
                                    oid1 = self._raw_api_.send_order(symbol, 1, -cur_net)
                                    if oid1 == -1:
                                        logging.error("[strategy_base][close][perm_fail] symbol=%s sz=%d dir=%d", symbol, 1, 1 if cur_net>0 else 0)
                                    else:
                                        close_success += 1
                                        logging.debug("[strategy_base][close] symbol=%s sz=%d side=%d", symbol, 1, 1 if cur_net>0 else 0)
                                except Exception as e:
                                    logging.error("[strategy_base][close][fail] symbol=%s err=%s", symbol, str(e))
                                    # 尝试一次重试
                                    try:
                                        act_attempts += 1
                                        oid1 = self._raw_api_.send_order(symbol, 1, -cur_net)
                                        if oid1 == -1:
                                            logging.error("[strategy_base][close][perm_fail] symbol=%s sz=%d dir=%d", symbol, 1, 1 if cur_net>0 else 0)
                                            act_fail += 1
                                    except Exception as e2:
                                        logging.error("[strategy_base][close][fail2] symbol=%s err=%s", symbol, str(e2))
                                        act_fail += 1
                                # open new in target direction
                                try:
                                    act_attempts += 1
                                    oid2 = self._raw_api_.send_order(symbol, 1, target_pos)
                                    if oid2 == -1:
                                        logging.error("[strategy_base][open][perm_fail] symbol=%s sz=%d dir=%d", symbol, 1, 0 if target_pos>=0 else 1)
                                    else:
                                        act_success += 1
                                        logging.debug("[strategy_base][open] symbol=%s sz=%d side=%d", symbol, 1, 0 if target_pos>=0 else 1)
                                except Exception as e:
                                    logging.error("[strategy_base][open][fail] symbol=%s err=%s", symbol, str(e))
                                    try:
                                        act_attempts += 1
                                        _ = self._raw_api_.send_order(symbol, 1, target_pos)
                                    except Exception as e2:
                                        logging.error("[strategy_base][open][fail2] symbol=%s err=%s", symbol, str(e2))
                                        act_fail += 1
                            else:
                                # 同向或减仓，直接按差额下单
                                try:
                                    logging.info("[strategy_base][pre_action] user=%s symbol=%s action=adjust cur=%d target=%d diff=%d", str(cur_user_name), symbol, int(cur_net), int(target_pos), int(diff_pos))
                                except Exception:
                                    pass
                                try:
                                    act_attempts += 1
                                    oid = self._raw_api_.send_order(symbol, 1, 1 if diff_pos>=0 else -1)
                                    if oid == -1:
                                        logging.error("[strategy_base][send_order][perm_fail] symbol=%s sz=%d dir=%d", symbol, 1, 0 if diff_pos>=0 else 1)
                                        act_fail += 1
                                        continue
                                except Exception as e:
                                    try:
                                        logging.warning("[strategy_base][send_order][retry] symbol=%s err=%s", symbol, str(e))
                                        act_attempts += 1
                                        oid = self._raw_api_.send_order(symbol, 1, 1 if diff_pos>=0 else -1)
                                        if oid == -1:
                                            logging.error("[strategy_base][send_order][perm_fail] symbol=%s sz=%d dir=%d", symbol, 1, 0 if diff_pos>=0 else 1)
                                            act_fail += 1
                                            continue
                                    except Exception as e2:
                                        logging.error("[strategy_base][send_order][fail] symbol=%s err=%s", symbol, str(e2))
                                        act_fail += 1
                                        continue
                                logging.debug("[strategy_base][send_order] symbol=%s sz=%d side=%d", symbol, 1, 0 if diff_pos>=0 else 1)
                
                for symbol, net_pos in dict_cur_pos.items():
                    str_symbol = symbol.decode('utf-8')
                    if str_symbol not in user_targets.keys():
                        if abs(net_pos) != 0:
                            try:
                                act_attempts += 1
                                oid = self._raw_api_.send_order(symbol, 1, -net_pos)
                                if oid == -1:
                                    logging.error("[strategy_base][close][perm_fail] symbol=%s sz=%d dir=%d", str_symbol, 1, 1 if net_pos>0 else 0)
                                    act_fail += 1
                                    continue
                                else:
                                    close_success += 1
                            except Exception as e:
                                try:
                                    logging.warning("[strategy_base][close][retry] symbol=%s err=%s", str_symbol, str(e))
                                    act_attempts += 1
                                    oid = self._raw_api_.send_order(symbol, 1, -net_pos)
                                    if oid == -1:
                                        logging.error("[strategy_base][close][perm_fail] symbol=%s sz=%d dir=%d", str_symbol, 1, 1 if net_pos>0 else 0)
                                        act_fail += 1
                                        continue
                                except Exception as e2:
                                    logging.error("[strategy_base][close][fail] symbol=%s err=%s", str_symbol, str(e2))
                                    act_fail += 1
                                    continue
                            logging.debug("[strategy_base][close] symbol=%s sz=%d side=%d", str_symbol, 1, 1 if net_pos>0 else 0)
                
                # 记录本次目标仓位
                self.last_dict_target_position[cur_user_name] = copy.deepcopy(user_targets)
                try:
                    logging.info("[strategy_base][act_summary] day=%s user=%s attempts=%d placed=%d closes=%d fails=%d", str(self.cur_trading_day), str(cur_user_name), int(act_attempts), int(act_success), int(close_success), int(act_fail))
                except Exception:
                    pass
                if (act_success + close_success) > 0:
                    self._acted_today[cur_user_name] = self.cur_trading_day
        else:
            dict_pos = {}
            dict_pos["pos"] = dict_target_position
            dict_pos["trading_day"] = self.cur_trading_day
            
            for bar in list_bar:
                cur_datetime = getattr(bar, "date_time").replace('-', "")
                cur_dt = datetime.datetime.strptime(cur_datetime, "%Y%m%d %H:%M:%S") + datetime.timedelta(minutes=1)
                cur_datetime = str(cur_dt).split('.')[0]
                dict_pos["cur_datetime"] = cur_datetime
                break
            
            self.send_target_pos(dict_pos)
            return
    
    def handle_bar(self, bar):
        """
        推送bar数据（可在子类中实现）
        """
        return
    
    def send_target_pos(self, dict_target_pos):
        logging.info("send_target_pos: %s", str(dict_target_pos))
        return
