import sys
import os
import logging
from datetime import datetime
import types

def main():
    sys.path.append(os.getcwd())
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    from strategy.source_code.test.test_sample_strategy import test_sample_strategy as Strategy
    api = types.SimpleNamespace(
        trading_date_section=['2024-12-10', '2024-12-11', '2024-12-12'],
        cur_trading_day='2024-12-11',
        list_sub_symbol_root=['CU.SHFE'],
        remote_enabled=True
    )
    s = Strategy()
    s.api = api
    bars_ok = [types.SimpleNamespace(
        symbol='CU2401.SHFE',
        trading_day='2024-12-11',
        date_time='2024-12-11 09:15:00',
        datetime=datetime(2024, 12, 11, 9, 15, 0)
    )]
    bars_early = [types.SimpleNamespace(
        symbol='CU2401.SHFE',
        trading_day='2024-12-11',
        date_time='2024-12-11 09:01:00',
        datetime=datetime(2024, 12, 11, 9, 1, 0)
    )]
    td = s._get_trading_day(bars_ok)
    print("tday", td.strftime("%Y-%m-%d"))
    print("need_ok", s._need_rebalance(td, bars_ok))
    print("need_early", s._need_rebalance(td, bars_early))
    from datetime import datetime as DT2
    s.last_rebalance_day = DT2(2026, 1, 1)
    print("need_future_reset", s._need_rebalance(td, bars_ok))
    api.trading_date_section = ['2024-12-09']
    s.last_rebalance_day = None
    print("need_idx_neg", s._need_rebalance(td, bars_ok))

if __name__ == "__main__":
    main()
