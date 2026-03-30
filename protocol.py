import struct
from ctypes import Structure, c_int, c_char

class msg_header(Structure):
    """
    消息头
    - msg_type: 消息类型
    - session_id: 会话标识
    - user_name: 用户名
    - user_pwd: 用户密码
    """
    _fields_ = [
        ("msg_type", c_int),    # 消息类型
        # 1: 登录
        # 100: 获取期权分钟bar数据
        # 101: 获取期权日数据
        # 200: 获取期货分钟bar数据
        # 201: 获取期货日数据
        # 300: 获取期货原始分钟数据
        # 301: 获取期货原始日数据
        # 302: 获取期货特殊日数据
        # 303: 获取期货复权日数据
        # 304: 获取期货复权分钟数据
        # 5000: 获取交易日
        # 5001: 获取每日合约
        # 5002: 获取期权基础信息
        # 5003: 获取期货主力合约表
        # 5004: 获取期货主力复权因子表
        ("session_id", c_char * 36),
        ("user_name", c_char * 16),
        ("user_pwd", c_char * 16),
    ]
