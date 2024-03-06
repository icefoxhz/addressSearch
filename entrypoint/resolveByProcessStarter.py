import os
import sys
from time import sleep

from pySimpleSpringFramework.spring_core.applicationStarter import ApplicationStarter
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import ComponentScan, ConfigDirectories

from addressSearch.utils.Util import delete_old_files

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
        self._address_table = None
        self._address_parsed_table = None
        self._address_mapping = None
        self._application_environment = None
        self._configService = None

    def clearLacCustomDict(self):
        dict_dir = self._application_environment.get("lac.dict_dir")
        delete_old_files(dict_dir)

    def truncate_address_table(self):
        parsed_address_table = self._configService.get_addr_cnf("data_table_parsed")
        self._address_mapping.truncate_table(parsed_address_table)

    def get_address_data_count(self):
        data = self._address_mapping.get_data_count(self._address_table)
        return data.iloc[0, 0]

    def get_parsed_address_data_count(self):
        data = self._address_mapping.get_data_count(self._address_parsed_table)
        return data.iloc[0, 0]

    def set_all_waiting_completed(self):
        # 防止 flag=8的没更新， 每次启动先把 flag=8 的更新成 9
        self._address_table = self._configService.get_addr_cnf("data_table")
        self._address_mapping.set_all_waiting_completed(self._address_table)

        self._address_parsed_table = self._configService.get_addr_cnf("data_table_parsed")
        self._address_mapping.set_all_waiting_completed(self._address_parsed_table)

    def do_parse_table(self, start_row, end_row):
        service = self.application_context.get_bean("resolveToDBService")
        service.start_by_process(start_row, end_row)

    def do_post_to_es(self):
        service = self.application_context.get_bean("postDataToEsService")
        service.start_by_thread()

    def main(self):
        self._application_environment = self.application_context.get_bean("applicationEnvironment")
        self._address_mapping = self.application_context.get_bean("addressMapping")
        self._configService = self.application_context.get_bean("configService")


def task_parse(start_row, end_row):
    app = ServiceApplication()
    app.run()
    app.do_parse_table(start_row, end_row)


def parse_process(app):
    data_count = app.get_address_data_count()
    if data_count == 0:
        return

    executorTaskManager = app.application_context.get_bean("executorTaskManager")
    process_count = executorTaskManager.core_num

    min_batch = 50
    if data_count <= min_batch * process_count:
        process_count = int(data_count / min_batch if data_count % min_batch == 0 else data_count / min_batch + 1)
        process_count = 1 if process_count == 0 else process_count
        batch_size = int(data_count / process_count + 1)  # 直接 + 1 省的判断了
    else:
        batch_size = data_count / process_count
        batch_size = int(batch_size if data_count % process_count == 0 else batch_size + 1)
    # app.truncate_address_table()

    for i in range(process_count):
        start = i * batch_size
        end = start + batch_size
        # print(start, end)
        executorTaskManager.submit(task_parse, True, None, start, end)
    executorTaskManager.wait_completed()


def post_to_es(app):
    data_count = app.get_parsed_address_data_count()
    if data_count == 0:
        return
    app.do_post_to_es()


if __name__ == '__main__':
    print("root_model_path=", root_model_path)

    serviceApplication = ServiceApplication()
    serviceApplication.run(debug=True)

    serviceApplication.clearLacCustomDict()
    serviceApplication.set_all_waiting_completed()

    while True:
        # ================= 解析更新数据库
        parse_process(serviceApplication)
        # ================ 更新es库
        post_to_es(serviceApplication)
        sleep(5)
