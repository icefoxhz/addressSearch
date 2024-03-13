from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired, Value
from pySimpleSpringFramework.spring_orm.databaseManager import DatabaseManager

from addressSearch.mapping.configMapping import ConfigMapping


@Component
class ConfigService:

    @Value({
        "project.local_config.enable": "_local_config_enable",
        "project.local_config.synonyms_table": "_synonyms_table",
        "project.local_config.dict_table": "_dict_table",
        "project.local_config.data_table": "_data_table",
        "project.local_config.data_table_parsed": "_data_table_parsed",
        "project.local_config.ip": "_ip",
        "project.local_config.port": "_port",
        "project.local_config.db_name_address": "_db_name",
        "project.local_config.address_max_return": "_address_max_return",
        "project.local_config.point_buffer_distance": "_point_buffer_distance"
    })
    def __init__(self):
        self._configMapping = None
        self._databaseManager = None

        self._local_config_enable = False
        self._synonyms_table = None
        self._dict_table = None
        self._data_table = None
        self._data_table_parsed = None
        self._ip = None
        self._port = None
        self._db_name = None
        self._address_max_return = None
        self._point_buffer_distance = None

        self.__addr_dict = {}
        self.__es_dict = {}

    @Autowired
    def set_params(self, configMapping: ConfigMapping, databaseManager: DatabaseManager, ):
        self._configMapping = configMapping
        self._databaseManager = databaseManager

    def _load_db_config(self):
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

    def _load_local_config(self):
        self.__addr_dict = {
            "synonyms_table": self._synonyms_table,
            "dict_table": self._dict_table,
            "data_table": self._data_table,
            "data_table_parsed": self._data_table_parsed
        }

        self.__es_dict = {
            "ip": self._ip,
            "port": int(self._port),
            "db_name_address": self._db_name,
            "address_max_return": int(self._address_max_return),
            "point_buffer_distance": self._point_buffer_distance
        }

    def _after_init(self):
        if self._local_config_enable:
            self._load_local_config()
        else:
            self._load_db_config()

    def get_addr_cnf(self, key):
        return self.__addr_dict.get(key, None)

    def get_es_cnf(self, key):
        return self.__es_dict.get(key, None)
