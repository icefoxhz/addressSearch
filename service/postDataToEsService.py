import time

import pandas as pd
from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.task.executorTaskManager import ExecutorTaskManager
# from pySimpleSpringFramework.spring_core.task.annoation.taskAnnotation import Sync, WaitForAllCompleted
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value, Autowired
from pySimpleSpringFramework.spring_core.type.annotationType import Propagation
from pySimpleSpringFramework.spring_orm.annoation.dataSourceAnnotation import Transactional
from tqdm import tqdm

from addressSearch.enums.dbOperator import DBOperator
from addressSearch.es.elasticsearchManger import ElasticsearchManger
from addressSearch.es.schemas import schemaMain
from addressSearch.mapping.addressMapping import AddressMapping


@Component
class PostDataToEsService:
    @Value({
        "project.tables.batch_size": "_batch_size",
        "project.tables.parsed_address_table": "_parsed_address_table",
        "project.elasticsearch.db.db_name": "_db_name",
        "project.elasticsearch.db.ip": "_ip",
        "project.elasticsearch.db.port": "_port",
        "task.execution.pool.max_size": "_thread_pool_max_size",
    })
    def __init__(self):
        self._self = None
        self._addressMapping = None
        self._parsed_address_table = None
        self._db_name = None
        self._ip = None
        self._port = None
        self._thread_pool_max_size = None
        self._batch_size = None
        self._executorTaskManager = None
        self.__isESSchemaCreated = False

    @Autowired
    def set_params(self,
                   addressMapping: AddressMapping,
                   executorTaskManager: ExecutorTaskManager,
                   postDataToEsService,
                   ):
        self._addressMapping = addressMapping
        self._self = postDataToEsService
        self._executorTaskManager = executorTaskManager

    # def do_run(self, df, progress_bar=None):
    #     df.columns = df.columns.str.lower()
    #
    #     es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
    #     for row in df.itertuples():
    #         try:
    #             data_dict = {
    #                 "id": row.id if hasattr(row, 'id') else None,
    #                 "fullname": row.fullname if hasattr(row, 'fullname') else None,
    #                 "province": row.province if hasattr(row, 'province') else None,
    #                 "city": row.city if hasattr(row, 'city') else None,
    #                 "region": row.region if hasattr(row, 'region') else None,
    #                 "street": row.street if hasattr(row, 'street') else None,
    #                 "community": row.community if hasattr(row, 'community') else None,
    #                 "group_number": row.group_number if hasattr(row, 'group_number') else None,
    #                 "natural_village": row.natural_village if hasattr(row, 'natural_village') else None,
    #                 "road": row.road if hasattr(row, 'road') else None,
    #                 "address_number": row.address_number if hasattr(row, 'address_number') else None,
    #                 "building_site": row.building_site if hasattr(row, 'building_site') else None,
    #                 "unit": row.unit if hasattr(row, 'unit') else None,
    #                 "floor": row.floor if hasattr(row, 'floor') else None,
    #                 "room": row.room if hasattr(row, 'room') else None,
    #                 "courtyard": row.courtyard if hasattr(row, 'courtyard') else None,
    #                 "building_name": row.building_name if hasattr(row, 'building_name') else None,
    #                 "company": row.company if hasattr(row, 'company') else None,
    #                 "location":
    #                     {
    #                         "lat": float(row.y) if hasattr(row, 'y') else None,
    #                         "lon": float(row.x) if hasattr(row, 'x') else None
    #                     }
    #                     if row.x is not None and row.y is not None and row.x != "" and row.y != ""
    #                     else None
    #             }
    #
    #             # 去掉无值字段
    #             delKeyList = []
    #             for k, v in data_dict.items():
    #                 if v is None or str(v) == "":
    #                     delKeyList.append(k)
    #
    #             for k in delKeyList:
    #                 del data_dict[k]
    #
    #             # 入库
    #             es.insert(row.id, data_dict)
    #         except Exception as e:
    #             log.error(str(e))
    #
    #     if progress_bar is not None:
    #         progress_bar.update(len(df))

    def do_run(self, df, progress_bar=None):
        df.columns = df.columns.str.lower()

        es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)

        ids = []
        for row in df.itertuples():
            flag = int(getattr(row, "flag"))

            dataId = getattr(row, "id")
            ids.append(dataId)

            # 删除
            if flag == DBOperator.DELETE.value:
                es.delete(dataId)
                continue

            if flag == DBOperator.INSERT.value or flag == DBOperator.UPDATE.value:
                # 新增 和 修改 是一样的
                data_dict = {}
                for fieldName in schemaMain["mappings"]["properties"].keys():
                    fieldName = fieldName.lower()
                    if fieldName == "location":
                        data_dict["location"] = {
                            "lat": getattr(row, "y") if hasattr(row, "y") else None,
                            "lon": getattr(row, "x") if hasattr(row, "x") else None
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
            self._self.set_completed(ids)

        if progress_bar is not None:
            progress_bar.update(len(df))

        return len(df)

    @Transactional(propagation=Propagation.REQUIRES_NEW)
    def set_completed(self, ids):
        for tId in ids:
            self._addressMapping.set_completed(self._parsed_address_table, tId)

    def start_by_thread(self):
        progress_bar = None
        try:
            if not self.__isESSchemaCreated:
                self._createESSchema()
                self.__isESSchemaCreated = True

            page = 0
            futures = []
            while True:
                df = self._addressMapping.get_parsed_data(self._parsed_address_table,
                                                          self._batch_size,
                                                          page * self._batch_size)
                if df is None or len(df) == 0:
                    break

                if progress_bar is None:
                    progress_bar = tqdm(total=0, position=0, leave=True,
                                        desc="从解析表读取数据后写入到ElasticSearch库, 当前完成 ", unit=" 条")

                future = self._executorTaskManager.submit(self._self.do_run,
                                                 False,
                                                 self.callback_function,
                                                 df,
                                                 progress_bar)
                futures.append(future)
                page += 1
            self._executorTaskManager.waitUntilComplete(futures)
            return True
        except Exception as e:
            log.error(str(e))

        return False

    def _createESSchema(self):
        es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
        es.create(self._db_name)
        es.close()

    # def do_run_by_process(self, df, progress_bar=None):
    #     self.do_run(df, progress_bar)

    @staticmethod
    def callback_function(future):
        future.result()
