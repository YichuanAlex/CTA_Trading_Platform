from ctypes import Structure, c_double

class position(Structure):
    _fields_ = [
        ("long", c_double),
        ("short", c_double),
        ("long_cost_basis", c_double),
        ("short_cost_basis", c_double),
        ("long_total_placed", c_double),
        ("short_total_placed", c_double),
        ("long_total_filled", c_double),
        ("short_total_filled", c_double)
    ]
    
    def on_place_order(self, size, side):
        if side == 0:
            #买
            self.long_total_placed += size
        else:
            #卖
            self.short_total_placed += size
        return
    
    def on_fill(self, filled_sz, filled_px, side):
        if side == 0:
            #买
            self.long_cost_basis += filled_px * filled_sz
            self.long_total_filled += filled_sz
            self.long += filled_sz
        else:
            #卖
            self.short_cost_basis += filled_px * filled_sz
            self.short_total_filled += filled_sz
            self.short += filled_sz
        return
    
    def reset(self):
        self.long = 0
        self.short = 0
        self.long_cost_basis = 0
        self.short_cost_basis = 0
        self.long_total_placed = 0
        self.short_total_placed = 0
        self.long_total_filled = 0
        self.short_total_filled = 0
