from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired
from pySimpleSpringFramework.spring_orm.databaseManager import DatabaseManager

from addressSearch.mapping.configMapping import ConfigMapping


@Component
class ConfigService:
    def __init__(self):
        self._configMapping = None
        self._databaseManager = None
        self.__addr_dict = {}
        self.__es_dict = {}

    @Autowired
    def set_params(self, configMapping: ConfigMapping, databaseManager: DatabaseManager,):
        self._configMapping = configMapping
        self._databaseManager = databaseManager

    def _after_init(self):
        self._databaseManager.switch_datasource("sourceConfig")
        df_addr = self._configMapping.get_address_search_config()
        df_es = self._configMapping.get_es_config()
        self._databaseManager.switch_datasource("sourceData")
        if df_addr is None or df_addr.empty or df_es is None or df_es.empty:
            return

        for index, row in df_addr.iterrows():
            config_name = row['config_name']
            config_value = row['config_value']
            self.__addr_dict[config_name] = config_value

        for index, row in df_es.iterrows():
            config_name = row['config_name']
            config_value = row['config_value']
            self.__es_dict[config_name] = config_value

    def get_addr_cnf(self, key):
        return self.__addr_dict.get(key, None)

    def get_es_cnf(self, key):
        return self.__es_dict.get(key, None)
