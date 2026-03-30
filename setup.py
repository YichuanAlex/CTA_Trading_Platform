from distutils.core import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize([
        "client_api.py",
        "protocol.py",
        "account_manager.py",
        "date_time_util.py",
        "market_data_type.py",
        "match_engine.py",
        "order.py",
        "order_event.py",
        "order_manager.py",
        "position.py",
        "position_manager.py",
        "strategy_base.py",
    ])
)

# python setup.py build_ext
