from datetime import datetime

import pandas as pd
from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.task.executorTaskManager import ExecutorTaskManager
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value, Autowired
from pySimpleSpringFramework.spring_core.type.annotationType import Propagation
from pySimpleSpringFramework.spring_orm.annoation.dataSourceAnnotation import Transactional
from tqdm import tqdm

from addressSearch.enums.dbOperator import DBOperator
from addressSearch.es.elasticsearchManger import ElasticsearchManger
from addressSearch.es.schemas import schemaMain
from addressSearch.mapping.addressMapping import AddressMapping
from addressSearch.service.configService import ConfigService


@Component
class PostDataToEsService:
    @Value({
        "project.tables.batch_size": "_batch_size",
    })
    def __init__(self):
        self._self = None
        self._addressMapping = None
        self._configService = None
        self._parsed_address_table = None
        self._db_name = None
        self._ip = None
        self._port = None
        self._batch_size = None
        self._executorTaskManager = None

    @Autowired
    def set_params(self,
                   addressMapping: AddressMapping,
                   executorTaskManager: ExecutorTaskManager,
                   postDataToEsService,
                   configService: ConfigService
                   ):
        self._addressMapping = addressMapping
        self._self = postDataToEsService
        self._executorTaskManager = executorTaskManager
        self._configService = configService

    def _es_init(self):
        self._createESSchema()

    def _after_init(self):
        self._parsed_address_table = self._configService.get_addr_cnf("data_table_parsed")
        self._db_name = self._configService.get_es_cnf("db_name_address")
        self._ip = self._configService.get_es_cnf("ip")
        self._port = int(self._configService.get_es_cnf("port"))
        self._es_init()

    @Transactional()
    def do_run(self, df, progress_bar=None):
        df.columns = df.columns.str.lower()

        es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
        if es is None:
            log.error("当前线程连接elasticSearch服务器失败")
            return

        ids = []
        for row in df.itertuples():
            flag = int(getattr(row, "op_flag"))

            dataId = getattr(row, "id")
            ids.append(dataId)

            # 删除
            if flag == DBOperator.DELETE.value:
                es.delete(dataId)
                continue

            # 新增或更新
            if flag == DBOperator.INSERT.value or flag == DBOperator.UPDATE.value:
                # 新增 和 修改 是一样的
                data_dict = {}
                for fieldName in schemaMain["mappings"]["properties"].keys():
                    fieldName = fieldName.lower()
                    if (fieldName == "location"
                            and hasattr(row, "x") and getattr(row, "x") is not None
                            and hasattr(row, "y") and getattr(row, "y") is not None):
                        data_dict["location"] = {
                            "lat": getattr(row, "y"),
                            "lon": getattr(row, "x")
                        }
                        continue

                    if hasattr(row, fieldName):
                        val = getattr(row, fieldName)
                        if pd.isna(val) or val == "":
                            continue

                        data_dict[fieldName] = str(getattr(row, fieldName.lower()))

                if len(data_dict) == 0:
                    continue

                # 去掉无值字段
                delKeyList = []
                for k, v in data_dict.items():
                    if v is None or str(v) == "":
                        delKeyList.append(k)

                for k in delKeyList:
                    del data_dict[k]

                # 入库
                es.insert(dataId, data_dict)
                continue

        if len(ids) > 0:
            self._self.set_waiting_completed(ids)

        if progress_bar is not None:
            progress_bar.update(len(df))

        return len(df)

    @Transactional(propagation=Propagation.REQUIRES_NEW)
    def set_waiting_completed(self, ids):
        for tId in ids:
            self._addressMapping.set_waiting_completed(self._parsed_address_table, tId)

    def start_by_thread(self):
        progress_bar = tqdm(total=0, position=0, leave=True,
                            desc="从解析表读取数据后写入到ElasticSearch库, 当前完成 ", unit=" 条")

        try:
            page = 0
            futures = []
            while True:
                df = self._addressMapping.get_parsed_data(self._parsed_address_table,
                                                          self._batch_size,
                                                          page * self._batch_size)
                if df is None or len(df) == 0:
                    break

                future = self._executorTaskManager.submit(self._self.do_run,
                                                          False,
                                                          self.callback_function,
                                                          df,
                                                          progress_bar)
                if future is not None:
                    futures.append(future)
                page += 1
            self._executorTaskManager.waitUntilComplete(futures)
            # 把完成的状态更新  op_flag=9
            self._addressMapping.set_all_waiting_completed(self._parsed_address_table)

            progress_bar.close()
            if len(futures) > 0:
                print("========== {} 本次操作完成 ==========\n\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            futures.clear()

            return True
        except Exception as e:
            # print(str(e))
            log.error("start_by_thread => " + str(e))

        return False

    def _createESSchema(self):
        try:
            es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
            es.create(self._db_name)
            es.close()
        except Exception as e:
            print("create_es_index => " + str(e))

    # def _deleteDB(self):
    #     es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
    #     es.deleteIndex(self._db_name)
    #     es.close()

    # def do_run_by_process(self, df, progress_bar=None):
    #     self.do_run(df, progress_bar)

    @staticmethod
    def callback_function(future):
        future.result()
