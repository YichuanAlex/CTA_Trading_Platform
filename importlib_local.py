import importlib
import importlib.util
import os
import sys

def path_import(file):
    """
    动态加载指定路径的模块（支持 .py/.pyd）
    :param file: 模块文件的绝对或相对路径
    :return: 已加载的模块对象
    """
    print("\n开始动态加载模块")
    try:
        mod_name = os.path.basename(file).split(".")[0]
        spec = importlib.util.spec_from_file_location(mod_name, file)
        if spec is None or spec.loader is None:
            raise ImportError("spec 加载失败")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        print("导入成功 path_import()：", module)
        print("检查sys中是否包含了此模块：", module in sys.modules)
        print("动态加载模块完成")
        return module
    except Exception as e:
        print("动态加载失败：", str(e))
        raise
