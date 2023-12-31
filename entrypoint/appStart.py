# 把父目录放入path， 父目录就是包。 这个需要自己调整
import os
import sys

root_model_path = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.append(root_model_path)

# 在这里导入自己的serviceApplication 和 start_rest_service
from addressSearch.entrypoint.applicationStarter import serviceApplication
from addressSearch.controller.queryController import start_rest_service

if __name__ == '__main__':
    # 启动app
    serviceApplication.run(True)

    # 启动rest服务
    start_rest_service()
