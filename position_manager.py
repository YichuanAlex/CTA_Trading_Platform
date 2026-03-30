import position

class position_manager(object):
    def __init__(self):
        """
        持仓管理器
        - dict_positions: 每个合约的持仓对象字典
        - dict_net_pos: 每个合约的净持仓字典
        """
        self.dict_positions = {}
        self.dict_net_pos = {}
    
    def on_place_order(self, symbol, size, side):
        """
        下单占位更新
        :param symbol: 合约代码（bytes 或 str）
        :param size: 下单数量
        :param side: 方向，0 买；1 卖
        """
        if symbol not in self.dict_positions.keys():
            self.dict_positions[symbol] = position.position()
        self.dict_positions[symbol].on_place_order(size, side)
        return
    
    def on_fill(self, symbol, filled_sz, filled_px, side):
        """
        成交更新持仓与净持仓
        """
        if symbol not in self.dict_positions.keys():
            self.dict_positions[symbol] = position.position()
        self.dict_positions[symbol].on_fill(filled_sz, filled_px, side)
        self.dict_net_pos[symbol] = self.get_net_pos(symbol)
        return
    
    def get_long_pos(self, symbol):
        """
        获取多头持仓
        """
        if symbol not in self.dict_positions.keys():
            return 0
        return self.dict_positions[symbol].long
    
    def get_short_pos(self, symbol):
        """
        获取空头持仓
        """
        if symbol not in self.dict_positions.keys():
            return 0
        return self.dict_positions[symbol].short
    
    def get_net_pos(self, symbol):
        """
        获取净持仓（多头-空头）
        """
        if symbol not in self.dict_positions.keys():
            return 0
        return self.dict_positions[symbol].long - self.dict_positions[symbol].short
    
    def get_all_pos(self):
        """
        获取全部合约持仓字典
        """
        return self.dict_positions
    
    def get_pos(self, symbol):
        """
        获取指定合约持仓对象
        """
        if symbol not in self.dict_positions.keys():
            return position.position()
        return self.dict_positions[symbol]
    
    def reset(self, symbol):
        """
        重置指定合约持仓为 0
        """
        if symbol not in self.dict_positions.keys():
            self.dict_positions[symbol] = position.position()
        self.dict_positions[symbol].reset()
        return
