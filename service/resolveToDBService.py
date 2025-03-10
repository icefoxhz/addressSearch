import pandas as pd
from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.task.executorTaskManager import ExecutorTaskManager
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired, Value
from pySimpleSpringFramework.spring_core.type.annotationType import Propagation
from pySimpleSpringFramework.spring_orm.annoation.dataSourceAnnotation import Transactional
from pySimpleSpringFramework.spring_orm.databaseManager import DatabaseManager
from tqdm import tqdm
from addressSearch.enums.dbOperator import DBOperator, RestRet
from addressSearch.mapping.addressMapping import AddressMapping
from addressSearch.service.configService import ConfigService
from addressSearch.service.aiModelService import AiModelService
from addressSearch.utils.commonTool import CommonTool


@Component
class ResolveToDBService:
    @Value({
        "project.tables.batch_size": "_batch_size",
        "task.execution.pool.max_size": "_thread_pool_max_size",
        "project.standard.x_field_name": "_x_field_name",
        "project.standard.y_field_name": "_y_field_name",
        "project.standard.address_field_name": "_address_field_name",
        "project.big_region.region_field": "_region_field",
        "project.big_region.street_field": "_street_field",
        "project.big_region.multi_region": "_multi_region",
        "project.local_config.address_ex_fields": "_address_ex_fields"
    })
    def __init__(self):
        # 自己注入自己，为了可以调用 Sync
        self._self = None

        self._databaseManager = None
        self._aiModelService = None
        self._addressMapping = None
        self._applicationContext = None
        self._configService = None

        self._executorTaskManager = None

        self._x_field_name = None
        self._y_field_name = None
        self._address_field_name = None
        self._ID_FIELD_NAME = "id"
        self._O_ID_FIELD_NAME = "o_id"
        self._batch_size = None
        self._address_table = None
        self._parsed_address_table = None
        self._thread_pool_max_size = None
        self._region_field = None
        self._street_field = None
        self._multi_region = False
        self._address_ex_fields = []

    def __reduce__(self):
        # 在序列化过程中排除线程锁
        return self.__class__, ()

    @Autowired
    def set_params(self,
                   aiModelService: AiModelService,
                   resolveToDBService,
                   applicationContext,
                   configService: ConfigService,
                   addressMapping: AddressMapping,
                   databaseManager: DatabaseManager,
                   executorTaskManager: ExecutorTaskManager
                   ):
        self._configService = configService
        self._aiModelService = aiModelService
        self._self = resolveToDBService
        self._applicationContext = applicationContext
        self._addressMapping = addressMapping
        self._databaseManager = databaseManager
        self._executorTaskManager = executorTaskManager

    def _after_init(self):
        self._address_table = self._configService.get_addr_cnf("data_table")
        self._parsed_address_table = self._configService.get_addr_cnf("data_table_parsed")

    def _do_parsed_result(self, data, if_exists='append'):
        df = pd.DataFrame(data)
        self._databaseManager.execute_by_df(df, self._parsed_address_table, if_exists)

    @Transactional(propagation=Propagation.REQUIRES_NEW)
    def delete_data(self, ids):
        for tId in ids:
            self._addressMapping.delete_data(self._parsed_address_table, tId)

    @Transactional()
    def do_run(self, df, is_participle_continue=False, progress_bar=None):
        is_parsed = False

        do_count = 0
        ids_insert = []
        ids_update = []
        ids_delete = []
        ids_unable_parsed = []

        data_insert = []
        data_modify = []

        try:
            for _, row in df.iterrows():
                full_name = row[self._address_field_name]
                if full_name is None or full_name == "":
                    continue

                x = row[self._x_field_name]
                y = row[self._y_field_name]

                op_flag = row["op_flag"]
                is_del = row["is_del"]
                flag = int(op_flag) if op_flag is not None else 0
                is_del = int(is_del) if is_del is not None else 0

                t_id = row[self._ID_FIELD_NAME]
                o_t_id = t_id
                if is_participle_continue:
                    t_id = t_id + "_" + str(1)

                if flag == DBOperator.INSERT.value:
                    if is_del == 1:  # 如果这条记录已经删除了。（删除后重新新增, 实际是更新)
                        ids_update.append(t_id)
                    else:
                        ids_insert.append(t_id)
                elif flag == DBOperator.UPDATE.value:
                    ids_update.append(t_id)
                elif flag == DBOperator.DELETE.value:
                    ids_delete.append(t_id)
                    continue
                else:
                    raise Exception("flag未知的数字: {} ！0=新增  1=更新  2=删除  9=完成".format(t_id))

                result = self._aiModelService.run(full_name)
                if result["code"] == RestRet.FAILED.value:
                    log.error("命名实体识别失败, full_name= " + full_name)
                    continue

                result["op_flag"] = flag
                result[self._ID_FIELD_NAME] = t_id
                result[self._O_ID_FIELD_NAME] = o_t_id
                result[self._address_field_name] = full_name
                result[self._x_field_name] = x
                result[self._y_field_name] = y
                if flag == DBOperator.INSERT.value:
                    if is_del == 1:  # 删除后重新新增实际是更新
                        data_modify.append(result)
                    else:
                        data_insert.append(result)
                    do_count += 1
                if flag == DBOperator.UPDATE.value:
                    data_modify.append(result)
                    do_count += 1

                # 附加字段
                for ex_field in self._address_ex_fields:
                    result[ex_field] = row[ex_field]

            # 新增
            if len(data_insert) > 0:
                self._do_parsed_result(data=data_insert)
                for tId in ids_insert:
                    self._addressMapping.set_notDelete_and_completed(self._address_table, tId)

            # 修改
            if len(data_modify) > 0:
                # 修改太麻烦，这里使用先删再插。
                self._self.delete_data(ids_update)
                self._do_parsed_result(data=data_modify)

                for tId in ids_update:
                    self._addressMapping.set_notDelete_and_completed(self._address_table, tId)

            # 删除
            if len(ids_delete) > 0:
                for tId in ids_delete:
                    self._addressMapping.set_deleted(self._parsed_address_table, tId)
                    self._addressMapping.set_delete_and_completed(self._address_table, tId)

            # 无法解析
            if len(ids_unable_parsed) > 0:
                for tId in ids_unable_parsed:
                    self._addressMapping.set_unable_parsed(self._address_table, tId)

            is_parsed = True
        except Exception as e:
            # 违反唯一性约束， 主键重复了.  这里直接删掉那个记录
            if str(e).find('psycopg2.errors.UniqueViolation') >= 0:
                for data in data_insert:
                    tId = data[self._ID_FIELD_NAME]
                    self._addressMapping.delete_row(self._parsed_address_table, tId)
            else:
                log.error("ResolveToDBService do_run error => " + str(e))

                ls = []
                for data in data_insert:
                    ls.append(data[self._ID_FIELD_NAME])
                log.error("ResolveToDBService error ids => " + str(ls))

                raise Exception(e)

        if progress_bar is not None and is_parsed:
            progress_bar.update(do_count)
        return is_parsed

    # def start_by_process_linux(self, start_row, end_row):
    #     progress_bar = tqdm(total=0, position=0, leave=True, desc="地名地址解析后生成解析表, 当前完成 ", unit=" 条")
    #
    #     start = start_row
    #     end = end_row
    #     try:
    #         page_size = self._batch_size
    #         while True:
    #             # 保证不会多查数据
    #             remain_size = end - start
    #             if remain_size < 0:
    #                 break
    #
    #             page_size = remain_size if page_size > remain_size else page_size
    #             df = self._addressMapping.get_address_data(self._address_table, page_size, start)
    #             if df is None or len(df) == 0:
    #                 break
    #             is_parsed = self._self.do_run(df, False, progress_bar)
    #             if is_parsed:
    #                 self._self.do_run(df, True, progress_bar)
    #
    #             start += page_size
    #
    #             if start >= end:
    #                 break
    #
    #         progress_bar.close()
    #     except Exception as e:
    #         # print(str(e))
    #         log.error("start_by_process => " + str(e))

    def start_by_process_df(self, df):
        try:
            batch = int(len(df) / self._batch_size)
            batch += int(0 if len(df) % self._batch_size == 0 else 1)

            ls_df = CommonTool.split_dataframe(df, batch)
            for df_tmp in ls_df:
                is_parsed = self._self.do_run(df_tmp, False, None)
                if is_parsed:
                    self._self.do_run(df_tmp, True, None)
                del df_tmp
            del df
        except Exception as e:
            # print(str(e))
            log.error("start_by_process_df => " + str(e))

    # def callback_function(self, future):
    #     future.result()
