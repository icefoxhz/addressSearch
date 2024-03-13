from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
import pandas as pd
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired
from pySimpleSpringFramework.spring_orm.databaseManager import DatabaseManager

from addressSearch.service.configService import ConfigService


@Component
class FileImportService:
    def __init__(self):
        self._databaseManager = None
        self._configService = None

    @Autowired
    def _set_params(self, databaseManager: DatabaseManager, configService: ConfigService):
        self._databaseManager = databaseManager
        self._configService = configService

    def run(self, file_path: str, table_name=None):
        df = None
        table = self._configService.get_addr_cnf("data_table") if table_name is None else table_name
        if table is None or table == "":
            log.error("未获取data_table名称")
            return

        try:
            if file_path.lower().endswith(".xlsx"):
                df = pd.read_excel(file_path)
            elif file_path.lower().endswith(".csv"):
                df = pd.read_csv(file_path)

            if df is not None:
                df.to_sql(table,
                          self._databaseManager.engine,
                          if_exists='append',
                          chunksize=500,
                          index=False
                          )
            return True
        except Exception as e:
            log.error(str(e))
        return False
