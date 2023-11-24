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

    def __reduce__(self):
        return self.__class__, ()

    def parseTest(self):
        lacModelManager = self.application_context.get_bean("lacModelManager")
        addressParseRunner = self.application_context.get_bean("addressParseRunner")
        address_parser = self.application_context.get_bean("addressParser")
        with lacModelManager as model:
            result = addressParseRunner.run(address_parser, model, "无锡市新吴区梅村街道梅里社区吴音水岸街区333－211号")
            print(result)

    def do_parse_table(self):
        service = self.application_context.get_bean("resolveToDBService")
        return service.start_by_thread()

    def do_post_to_es(self):
        service = self.application_context.get_bean("postDataToEsService")
        service.start_by_thread()

    def main(self):

        # self.parseTest()

        # self.do_parse_table()

        # self.do_post_to_es()

        if self.do_parse_table():
            self.do_post_to_es()


serviceApplication = ServiceApplication()

if __name__ == '__main__':
    print("root_model_path=", root_model_path)
    serviceApplication.run(True)
