from ctypes import Structure, c_int, c_double, c_char

class order_event_type(Structure):
    """
    订单事件类型
    0: NEW; 1: ACK; 2: CONFIRM; 3: FILLED; 4: CXLLED; 5: CXL_REJECTED; 6: REJECTED
    """
    _fields_ = [
        ("type", c_int)
    ]

    def __init__(self):
        self.type = 0

    def set_type(self, type):
        self.type = type
        return

class order_event_base(Structure):
    """
    订单事件基础结构
    - order_id: 订单号
    - type: 事件类型
    """
    _fields_ = [
        ("order_id", c_int),
        ("type", order_event_type)
    ]

    def __init__(self) -> None:
        self.order_id = -1
        self.type = order_event_type()
        self.type.type = 0

class order_ack_evt(order_event_base):
    """
    订单确认事件（ack）
    """
    def __init__(self):
        super().__init__()
        self.type.type = 1

class order_conf_evt(order_event_base):
    """
    订单生效事件（confirm）
    """
    def __init__(self):
        super().__init__()
        self.type.type = 2

class order_filled_evt(order_event_base):
    """
    订单成交事件（filled）
    - filled_sz: 成交数量
    - filled_px: 成交价格
    """
    _fields_ = order_event_base._fields_ + [
        ("filled_sz", c_double),
        ("filled_px", c_double)
    ]
    def __init__(self) -> None:
        super().__init__()
        self.type.type = 3
        self.filled_sz = 0.0
        self.filled_px = 0.0

class order_cxlled_evt(order_event_base):
    """
    订单撤单完成事件（cancelled）
    - cxl_sz: 撤单数量
    """
    _fields_ = order_event_base._fields_ + [
        ("cxl_sz", c_double)
    ]
    def __init__(self) -> None:
        super().__init__()
        self.type.type = 4
        self.cxl_sz = 0.0

class order_cxl_rej_evt(order_event_base):
    """
    订单撤单拒绝事件（cxl_rejected）
    - reason: 拒绝原因码
    - rej_msg: 拒绝信息
    """
    _fields_ = order_event_base._fields_ + [
        ("reason", c_int),
        ("rej_msg", c_char * 256)
    ]

    def __init__(self) -> None:
        super().__init__()
        self.type.type = 5
        self.reason = 0
        self.rej_msg = b""

class order_reject_evt(order_event_base):
    """
    下单被拒事件（rejected）
    - reason: 拒绝原因码
    - rej_msg: 拒绝信息
    """
    _fields_ = order_event_base._fields_ + [
        ("reason", c_int),
        ("rej_msg", c_char * 256)
    ]

    def __init__(self) -> None:
        super().__init__()
        self.type.type = 6
        self.reason = 0
        self.rej_msg = b""
