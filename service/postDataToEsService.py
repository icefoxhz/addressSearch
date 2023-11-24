import time

from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.task.executorTaskManager import ExecutorTaskManager
# from pySimpleSpringFramework.spring_core.task.annoation.taskAnnotation import Sync, WaitForAllCompleted
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value, Autowired
from tqdm import tqdm

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

    @Autowired
    def set_params(self,
                   addressMapping: AddressMapping,
                   executorTaskManager: ExecutorTaskManager,
                   postDataToEsService,
                   ):
        self._addressMapping = addressMapping
        self._self = postDataToEsService
        self._executorTaskManager = executorTaskManager

    def do_run(self, df, progress_bar=None):
        df.columns = df.columns.str.lower()

        es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
        for row in df.itertuples():
            try:
                data_dict = {
                    "id": row.id if hasattr(row, 'id') else None,
                    "fullname": row.fullname if hasattr(row, 'fullname') else None,
                    "province": row.province if hasattr(row, 'province') else None,
                    "city": row.city if hasattr(row, 'city') else None,
                    "region": row.region if hasattr(row, 'region') else None,
                    "street": row.street if hasattr(row, 'street') else None,
                    "community": row.community if hasattr(row, 'community') else None,
                    "group_number": row.group_number if hasattr(row, 'group_number') else None,
                    "natural_village": row.natural_village if hasattr(row, 'natural_village') else None,
                    "road": row.road if hasattr(row, 'road') else None,
                    "address_number": row.address_number if hasattr(row, 'address_number') else None,
                    "building_site": row.building_site if hasattr(row, 'building_site') else None,
                    "unit": row.unit if hasattr(row, 'unit') else None,
                    "floor": row.floor if hasattr(row, 'floor') else None,
                    "room": row.room if hasattr(row, 'room') else None,
                    "courtyard": row.courtyard if hasattr(row, 'courtyard') else None,
                    "building_name": row.building_name if hasattr(row, 'building_name') else None,
                    "company": row.company if hasattr(row, 'company') else None,
                    "location":
                        {
                            "lat": float(row.y) if hasattr(row, 'y') else None,
                            "lon": float(row.x) if hasattr(row, 'x') else None
                        }
                        if row.x is not None and row.y is not None and row.x != "" and row.y != ""
                        else None
                }

                # 去掉无值字段
                delKeyList = []
                for k, v in data_dict.items():
                    if v is None or str(v) == "":
                        delKeyList.append(k)

                for k in delKeyList:
                    del data_dict[k]

                # 入库
                es.insert(row.id, data_dict)
            except Exception as e:
                log.error(str(e))

        if progress_bar is not None:
            progress_bar.update(len(df))

    def start_by_thread(self):
        progress_bar = tqdm(total=0, position=0, leave=True, desc="从解析表读取数据后写入到ElasticSearch库, 当前完成 ",
                            unit=" 条")

        try:
            page = 0
            while True:
                df = self._addressMapping.get_parsed_data(self._parsed_address_table,
                                                          self._batch_size,
                                                          page * self._batch_size)
                if df is None or len(df) == 0:
                    break

                self._executorTaskManager.submit(self._self.do_run,
                                                 False,
                                                 self._self.callback_function,
                                                 df,
                                                 progress_bar)
                page += 1
            self._executorTaskManager.wait_completed()
            return True
        except Exception as e:
            log.error(str(e))

        return False

    def do_run_by_process(self, df, progress_bar=None):
        self.do_run(df, progress_bar)

    def callback_function(self, future):
        future.result()
