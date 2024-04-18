from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired, Value

from addressSearch.es.elasticsearchManger import ElasticsearchManger
from addressSearch.es.schemas import schemaMain
from addressSearch.es.scripts import SEARCH_SCORE_SCRIPT
from addressSearch.service.configService import ConfigService


@Component
class EsInitService:
    @Value({
        "project.score_script_id": "_score_script_id"
    })
    def __init__(self):
        self._configService = None
        self._score_script_id = None

    @Autowired
    def set_params(self, configService: ConfigService):
        self._configService = configService

    def create_scripts(self):
        ip = self._configService.get_es_cnf("ip")
        port = int(self._configService.get_es_cnf("port"))
        db_name_address = self._configService.get_es_cnf("db_name_address")

        es_cli = ElasticsearchManger(db_name_address, schemaMain, ip, port)
        with es_cli as es_conn:
            if es_conn is None:
                return
        succeed = es_cli.put_script(self._score_script_id, SEARCH_SCORE_SCRIPT)
        print("是否成功创建ES脚本: ", succeed)
