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
from addressSearch.es.schemas import schemaMain, es_fullname_field
from addressSearch.mapping.addressMapping import AddressMapping
from addressSearch.service.configService import ConfigService
from addressSearch.utils.commonTool import CommonTool


@Component
class PostDataToEsService:
    @Value({
        "project.tables.batch_size": "_batch_size",
        "task.execution.pool.max_size": "_max_core",
        "project.standard.x_field_name": "_x_field_name",
        "project.standard.y_field_name": "_y_field_name",
        "project.standard.address_field_name": "_address_field_name",
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
        self._max_core = 2
        self._x_field_name = None
        self._y_field_name = None
        self._ID_FIELD_NAME = "id"
        self._address_field_name = None
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

            dataId = getattr(row, self._ID_FIELD_NAME)
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
                            and hasattr(row, self._x_field_name) and getattr(row, self._x_field_name) is not None
                            and hasattr(row, self._y_field_name) and getattr(row, self._y_field_name) is not None):
                        data_dict["location"] = {
                            self._y_field_name: getattr(row, self._y_field_name),
                            self._x_field_name: getattr(row, self._x_field_name)
                        }
                        continue

                    if fieldName == es_fullname_field:
                        fieldName = str(self._address_field_name).lower()

                    if hasattr(row, fieldName):
                        val = getattr(row, fieldName)
                        if pd.isna(val) or val == "":
                            continue

                        esFieldName = fieldName
                        if fieldName == str(self._address_field_name).lower():
                            esFieldName = es_fullname_field

                        data_dict[esFieldName] = getattr(row, fieldName.lower())

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

        if len(ids) > 0:
            self._self.set_waiting_completed(ids)

        if progress_bar is not None:
            progress_bar.update(len(df))

        return len(df)

    @Transactional(propagation=Propagation.REQUIRES_NEW)
    def set_waiting_completed(self, ids):
        for tId in ids:
            self._addressMapping.set_completed(self._parsed_address_table, tId)

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

            progress_bar.close()
            if len(futures) > 0:
                print("========== {} 本次操作完成 ==========".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            futures.clear()

            return True
        except Exception as e:
            # print(str(e))
            log.error("start_by_thread => " + str(e))

        return False

    def start_by_thread_df(self, df):
        try:
            batch = int(len(df) / self._batch_size)
            batch += int(0 if len(df) % self._batch_size == 0 else 1)

            ls_df = CommonTool.split_dataframe(df, batch)
            futures = []
            for df_tmp in ls_df:
                future = self._executorTaskManager.submit(self._self.do_run,
                                                          False,
                                                          self.callback_function,
                                                          df_tmp,
                                                          None)
                if future is not None:
                    futures.append(future)

            self._executorTaskManager.waitUntilComplete(futures)
            futures.clear()
            del ls_df
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
