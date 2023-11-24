import time

from pySimpleSpringFramework.spring_core.task.executorTaskManager import ExecutorTaskManager
from tqdm import tqdm
import pandas as pd
from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired, Value
from pySimpleSpringFramework.spring_orm.databaseManager import DatabaseManager

from addressSearch.mapping.addressMapping import AddressMapping
from addressSearch.resolver.addressParseRunner import AddressParseRunner


@Component
class ResolveToDBService:
    @Value({
        "project.tables.batch_size": "_batch_size",
        "project.tables.address_table": "_address_table",
        "project.tables.parsed_address_table": "_parsed_address_table",
        "task.execution.pool.max_size": "_thread_pool_max_size",
    })
    def __init__(self):
        # 自己注入自己，为了可以调用 Sync
        self._self = None

        self._databaseManager = None
        self._lacModelManager = None
        self._addressParseRunner = None
        self._addressMapping = None
        self._applicationContext = None

        self._executorTaskManager = None

        self._batch_size = None
        self._address_table = None
        self._parsed_address_table = None
        self._thread_pool_max_size = None

    def __reduce__(self):
        # 在序列化过程中排除线程锁
        return self.__class__, ()

    @Autowired
    def set_params(self,
                   lacModelManager,
                   resolveToDBService,
                   applicationContext,
                   addressMapping: AddressMapping,
                   addressParseRunner: AddressParseRunner,
                   databaseManager: DatabaseManager,
                   executorTaskManager: ExecutorTaskManager
                   ):
        self._lacModelManager = lacModelManager
        self._self = resolveToDBService
        self._applicationContext = applicationContext
        self._addressMapping = addressMapping
        self._addressParseRunner = addressParseRunner
        self._databaseManager = databaseManager
        self._executorTaskManager = executorTaskManager

    def _insert_parsed_result(self, data):
        df = pd.DataFrame(data)
        self._databaseManager.execute_by_df(df, self._parsed_address_table)

    def do_run(self, df, progress_bar=None):
        with self._lacModelManager as model:
            data = []
            for _, row in df.iterrows():
                # t_id = row["id"]
                full_name = row["fullname"]
                x = row["x"]
                y = row["y"]
                address_parser = self._applicationContext.get_bean("addressParser")
                resultList, cutListStr = self._addressParseRunner.run(address_parser, model, full_name, x, y)
                for result in resultList:
                    result["fullname"] = full_name
                    result["parseResult"] = cutListStr
                    data.append(result)
            self._insert_parsed_result(data)
        # print(">>>>> 当前入库总量: ", len(df))
        if progress_bar is not None:
            progress_bar.update(len(df))

    def start_by_thread(self):
        self._addressMapping.truncate_table(self._parsed_address_table)
        progress_bar = tqdm(total=0, position=0, leave=True, desc="地名地址解析后生成解析表, 当前完成 ", unit=" 条")
        try:
            page = 0
            while True:
                df = self._addressMapping.get_address_data(self._address_table, self._batch_size,
                                                           page * self._batch_size)
                if df is None or len(df) == 0:
                    break

                self._executorTaskManager.submit(self._self.do_run,
                                                 False,
                                                 None,
                                                 df,
                                                 progress_bar)
                page += 1

            self._executorTaskManager.wait_completed()
            return True
        except Exception as e:
            log.error(str(e))

        return False

    def start_by_process(self, start_row, end_row):
        progress_bar = tqdm(total=0, position=0, leave=True, desc="地名地址解析后生成解析表, 当前完成 ", unit=" 条")

        start = start_row
        end = end_row
        try:
            while True:
                page_size = self._batch_size
                not_do_count = end - start
                if self._batch_size > not_do_count:
                    page_size = not_do_count

                df = self._addressMapping.get_address_data(self._address_table, page_size, start)
                if df is None or len(df) == 0:
                    break
                self._self.do_run(df, progress_bar)
                start += self._batch_size

                if start >= end:
                    break
        except Exception as e:
            log.error(str(e))

    # def callback_function(self, future):
    #     future.result()
