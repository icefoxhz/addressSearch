import os
import sys

from pySimpleSpringFramework.spring_core.applicationStarter import ApplicationStarter
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import ComponentScan, ConfigDirectories

# 把父目录放入path， 父目录就是包。 这个需要自己调整
root_model_path = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.append(root_model_path)


# 基于 root_model_path 的相对的位置， 因为 root_model_path 就是包
@ComponentScan("../../addressSearch/service",
               "../../addressSearch/mapping",
               "../../addressSearch/entity",
               "../../addressSearch/controller",
               "../../addressSearch/resolver",
               )
# 这里修改成自己的配置文件位置（相对当前这个启动文件的位置）
@ConfigDirectories("../../config")
class ServiceApplication(ApplicationStarter):
    def __init__(self):
        super().__init__()

    def main(self):
        pass


serviceApplication = ServiceApplication()

if __name__ == '__main__':
    print("root_model_path=", root_model_path)
    serviceApplication.run(True)
