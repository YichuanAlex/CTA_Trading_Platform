import order
import order_event
import position_manager

class order_manager(object):
    def __init__(self):
        """
        订单管理器
        - 管理订单ID与订单字典
        - 同步订单事件到持仓管理器
        """
        self.order_id = 0
        self.dict_all_orders = {}
        self.pm = position_manager.position_manager()
    
    def init(self):
        return
    
    def get_position_manager(self):
        """
        获取持仓管理器
        """
        return self.pm
    
    def create_order(self):
        """
        创建订单并分配唯一ID
        :return: order 对象
        """
        ord = order.order()
        self.order_id += 1
        ord.set_order_id(self.order_id)
        return ord
    
    def on_bod(self, trading_day):
        """
        盘前初始化，清空订单字典
        """
        self.dict_all_orders.clear()
        return
    
    def on_place_order(self, ord):
        """
        下单事件，记录订单并通知持仓管理器
        :param ord: 订单对象
        """
        self.dict_all_orders[ord.order_id] = ord
        self.pm.on_place_order(ord.symbol, ord.size, ord.side)
        return
    
    def on_ack(self, evt):
        return
    
    def on_conf(self, evt):
        return
    
    def on_fill(self, evt):
        """
        成交事件，更新持仓
        :param evt: order_event.order_filled_evt
        """
        ord = self.dict_all_orders[evt.order_id]
        self.pm.on_fill(ord.symbol, evt.filled_sz, evt.filled_px, ord.side)
        return
    
    def on_cxl(self, evt):
        return
    
    def on_cxl_rej(self, evt):
        return
    
    def on_rej(self, evt):
        return
