from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired
from pySimpleSpringFramework.spring_orm.databaseManager import DatabaseManager

from addressSearch.mapping.configMapping import ConfigMapping
from addressSearch.service.configService import ConfigService


@Component
class ThesaurusService:
    """
    同义词字典
    """
    def __init__(self):
        self._configMapping = None
        self._configService = None
        self._databaseManager = None
        self.s2t = {}
        self.t2s = {}

    @Autowired
    def set_params(self, configMapping: ConfigMapping,
                   configService: ConfigService,
                   databaseManager: DatabaseManager,
                   ):
        self._configMapping = configMapping
        self._configService = configService
        self._databaseManager = databaseManager

    def after_init(self):
        synonyms_table = self._configService.get_addr_cnf("synonyms_table")

        self._databaseManager.switch_datasource("sourceConfig")
        df = self._configMapping.get_address_thesaurus(synonyms_table)
        self._databaseManager.switch_datasource("sourceData")

        if df is None or df.empty:
            return

        for index, row in df.iterrows():
            s = row['sword']
            t = row['tword']

            if s not in self.s2t.keys():
                self.s2t[s] = [t]
            else:
                self.s2t[s].append(t)

            if t not in self.t2s.keys():
                self.t2s[t] = [s]
            else:
                self.t2s[t].append(s)

