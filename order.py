from ctypes import Structure, c_int, c_double, c_char

class order_status(Structure):
    _fields_ = [
        ("status", c_int)
        # 0: NEW; 1: SENT; 2: OPENED; 3: CLOSED; 4: REJECTED
    ]
    def __init__(self):
        self.status = 0
        pass
    def set_status(self, status):
        self.status = status
        return

class order(Structure):
    _fields_ = [
        ("symbol", c_char * 32),
        ("price", c_double),
        ("size", c_double),
        ("side", c_int),
        # 买卖方向 0：买入；1：卖出
        ("price_type", c_int),
        # 报单类型 0：限价单；1：市价单
        ("order_id", c_int),
        ("place_time", c_char * 32),
        ("filled_so_far", c_double),
        ("filled_value", c_double),
        ("cxl_sz", c_double),
        ("status", order_status),
        ("open_close", c_int)
        # 0：开仓；1：平仓
    ]

    def init(self, symbol, size, side, price_type, order_id, place_time, price = 0.0):
        if type(symbol) == type("string"):
            self.symbol = symbol.encode()
        else:
            self.symbol = symbol
        self.size = size
        self.side = side
        self.price_type = price_type
        self.order_id = order_id
        self.set_place_time(place_time)
        self.price = price
        self.status = order_status()
    def set_order_id(self, order_id) :
        self.order_id = order_id
        return
    def set_place_time(self, place_time):
        if type(place_time) == type("string"):
            self.place_time = place_time.encode()
        else:
            self.place_time = place_time
        return
    def on_order_evt(self, evt):
        if evt.type.type == 1:
            self.status.status = 1
        elif evt.type.type == 2:
            self.status.status = 2
        elif evt.type.type == 3:
            self.status.status = 2
            self.filled_value += evt.filled_px * evt.filled_sz
            self.filled_so_far += evt.filled_sz
            if abs(self.size - self.filled_so_far) < 0.0000001:
                self.status.status = 3
        elif evt.type.type == 4:
            self.status.status = 3
            self.cxl_sz = evt.cxl_sz
        elif evt.type.type == 5:
            self.status.status = 4
        elif evt.type.type == 6:
            # 下单被拒
            self.status.status = 4
        return self.status.status
