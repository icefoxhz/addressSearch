import os
import platform
import sys

# from multiprocessing import freeze_support

# 把父目录放入path， 父目录就是包。 这个需要自己调整
root_model_path = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.append(root_model_path)

from time import sleep
from pySimpleSpringFramework.spring_core.applicationStarter import ApplicationStarter
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import ComponentScan, ConfigDirectories
from addressSearch.utils.commonTool import CommonTool
from datetime import datetime

__LIMIT_SCALE = 10 if platform.system() == "Windows" else 1


# 基于 root_model_path 的相对的位置， 因为 root_model_path 就是包
@ComponentScan("../../addressSearch/service",
               "../../addressSearch/mapping",
               "../../addressSearch/entity",
               "../../addressSearch/controller",
               )
# 这里修改成自己的配置文件位置（相对当前这个启动文件的位置）
@ConfigDirectories("../../config")
class ServiceApplication(ApplicationStarter):
    def __init__(self):
        super().__init__()
        self._esInitService = None
        self._address_table = None
        self._address_parsed_table = None
        self._address_mapping = None
        self._application_environment = None
        self._configService = None

    def clearLacCustomDict(self):
        dict_dir = self._application_environment.get("project.lac.dict_dir")
        CommonTool.delete_old_files(dict_dir)

    def get_address_data_limit(self, limit_size):
        data = self._address_mapping.get_address_data_limit(self._address_table, limit_size)
        return data

    def get_parsed_address_data_limit(self, limit_size):
        data = self._address_mapping.get_parsed_data_limit(self._address_parsed_table, limit_size)
        return data

    def do_parse_table_limit(self, df):
        service = self.application_context.get_bean("resolveToDBService")
        service.start_by_process_df(df)

    def do_post_to_es_limit(self, df):
        service = self.application_context.get_bean("postDataToEsService")
        service.start_by_thread_df(df)

    def main(self):
        self._application_environment = self.application_context.get_bean("applicationEnvironment")
        self._address_mapping = self.application_context.get_bean("addressMapping")
        self._configService = self.application_context.get_bean("configService")
        self._esInitService = self.application_context.get_bean("esInitService")
        self._esInitService.create_scripts()

        self._address_table = self._configService.get_addr_cnf("data_table")
        self._address_parsed_table = self._configService.get_addr_cnf("data_table_parsed")


def task_parse_limit(df):
    app = ServiceApplication()
    app.run()
    app.do_parse_table_limit(df)


def parse_process_limit(app):
    executorTaskManager = app.application_context.get_bean("executorTaskManager")
    process_count = executorTaskManager.core_num

    applicationEnvironment = app.application_context.get_bean("applicationEnvironment")
    min_size = applicationEnvironment.get("project.tables.batch_size")

    limit_size = process_count * min_size

    data = app.get_address_data_limit(limit_size * __LIMIT_SCALE)
    data_count = len(data)
    if data_count == 0:
        return data_count

    if data_count <= limit_size:
        process_count = 1

    ls_df = CommonTool.split_dataframe(data, process_count)
    for df in ls_df:
        # print(start, end)
        executorTaskManager.submit(task_parse_limit, True, None, df)
    print(f"\n============ 开始分词解析, 当前需要处理数量: {data_count} , 请等待 ============\n")
    executorTaskManager.wait_completed()
    del ls_df
    return data_count


def post_to_es_limit(app):
    executorTaskManager = app.application_context.get_bean("executorTaskManager")
    process_count = executorTaskManager.core_num

    applicationEnvironment = app.application_context.get_bean("applicationEnvironment")
    min_size = applicationEnvironment.get("project.tables.batch_size")

    limit_size = process_count * min_size

    data = app.get_parsed_address_data_limit(limit_size * __LIMIT_SCALE)
    data_count = len(data)
    if data_count == 0:
        return data_count

    app.do_post_to_es_limit(data)
    return data_count


if __name__ == '__main__':
    CommonTool.write_pid("resolve_pid", pid=os.getpid())

    # freeze_support()  # pyinstaller 打包后的进程支持，必须加，不然无法使用进程
    print("root_model_path=", root_model_path)

    serviceApplication = ServiceApplication()
    serviceApplication.run(debug=True)

    serviceApplication.clearLacCustomDict()

    count_parsed_count = 0
    count_to_es_count = 0
    while True:
        try:
            # 分词和解析
            count_parsed = parse_process_limit(serviceApplication)
            # 更新es
            count_to_es = post_to_es_limit(serviceApplication)

            tm_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if count_parsed > 0 or count_to_es > 0:
                count_parsed_count += count_parsed
                count_to_es_count += count_to_es
                print(f"===== {tm_now} 分词总量: {count_parsed_count}, ES总量: {count_to_es_count} =====\n")

            if count_parsed == 0 and count_to_es == 0 and count_parsed_count > 0:
                count_parsed_count = 0
                count_to_es_count = 0
                print(f">>>>> {tm_now} 当前需处理数据全部完成 <<<<<\n\n")

            sleep(5)
        except Exception as e:
            print(str(e))
