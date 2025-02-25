import copy

from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component, Scope
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired, Value

from addressSearch.es.elasticsearchManger import ElasticsearchManger
from addressSearch.es.schemas import schemaMain, es_fullname_field
from addressSearch.mapping.addressMapping import AddressMapping
from addressSearch.service.addressParseService import AddressParseService
from addressSearch.service.configService import ConfigService
from addressSearch.service.aiModelService import AiModelService
from addressSearch.service.thesaurusService import ThesaurusService
from addressSearch.utils.commonTool import CommonTool


@Component
@Scope("prototype")
class EsSearchService:
    @Value({
        "project.print_debug": "_print_debug",
        "project.blur_search": "_blur_search",
        "project.score_script_id": "_score_script_id",
        "project.local_config.address_max_return": "_address_max_return"
    })
    def __init__(self):
        self._print_debug = False
        self._blur_search = 0
        self._score_script_id = None
        self._address_table = None
        self._parsed_address_table = None
        self._ip = None
        self._port = None
        self._username = None
        self._password = None
        self._db_name_address = None
        self._max_distance = None
        self._max_return = None
        self._addressParseService = None
        self._aiModelService = None
        self._thesaurusService = None
        self._addressMapping = None
        self._configService = None
        self._address_max_return = 20
        self._return_multi = False
        self._es_cli = None
        self._multi_region = False
        self._region_field = None
        self._street_field = None
        self._build_number_tolerance = 20  # 前后n栋的来去

    def _after_init(self):
        self._address_table = self._configService.get_addr_cnf("data_table")
        self._parsed_address_table = self._configService.get_addr_cnf("data_table_parsed")
        self._ip = self._configService.get_es_cnf("ip")
        self._port = int(self._configService.get_es_cnf("port"))
        self._username = self._configService.get_es_cnf("username")
        self._password = self._configService.get_es_cnf("password")
        self._db_name_address = self._configService.get_es_cnf("db_name_address")
        self._max_distance = self._configService.get_es_cnf("max_point_buffer_distance")
        self._max_return = int(self._configService.get_es_cnf("address_max_return"))
        self.__conn_es()

    def __conn_es(self):
        # 添加额外字段.  深拷贝一份，不能修改老的
        # schemaMainNew = copy_schema()
        # add_schema_field(schemaMainNew, "ex_val")
        # self._es_cli = ElasticsearchManger(self._db_name_address, schemaMainNew, self._ip, self._port)

        self._es_cli = ElasticsearchManger(self._db_name_address, schemaMain, self._ip, self._port, self._username,
                                           self._password)
        with self._es_cli as es_conn:
            if es_conn is None:
                return

    def close(self):
        if self._es_cli is not None:
            self._es_cli.close()

    def set_return_multi(self):
        self._return_multi = True

    @Autowired
    def set_params(self,
                   addressMapping: AddressMapping,
                   configService: ConfigService,
                   aiModelService: AiModelService,
                   thesaurusService: ThesaurusService
                   ):
        self._addressMapping = addressMapping
        self._configService = configService
        self._aiModelService = aiModelService
        self._thesaurusService = thesaurusService

    # def reset(self):
    #     # 清空数据表
    #     self._addressMapping.truncate_table(self._address_table)
    #     self._addressMapping.truncate_table(self._parsed_address_table)
    #
    #     # 删了重建es库
    #     es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
    #     es.deleteIndex(self._db_name)
    #     es.create(self._db_name)
    #     es.close()

    def _run_address_search_not_by_thesaurus(self, address_string, use_session=False):
        """
        不使用同义词通过地名地址匹配
        :param address_string:
        :param use_session:
        :return:
        """
        if self._print_debug:
            print("\n-----------------------------------------------------\n")

        if use_session:
            result = self._aiModelService.run(address_string)
        else:
            result = self._aiModelService.run_by_session(address_string)

        # 生成查询参数
        search_params = self.__create_address_search_params(result)
        # 搜索
        succeed, result = self.__address_search(search_params)

        return succeed, result

    def _run_address_search_by_thesaurus(self, address_string):
        """
        使用同义词通过地名地址匹配    
        """
        ls = [self._thesaurusService.s2t, self._thesaurusService.t2s]

        for d in ls:
            for k, words in d.items():
                if address_string.find(k) >= 0:
                    for word in words:
                        # 一对同义词出现在一个地址中。 比如: 机场路133号格林东方酒店6楼。  机场路133号 -> 格林东方酒店
                        if address_string.find(word) >= 0:
                            address_string = address_string.replace(k, "")
                        # 同义词互换
                        else:
                            address_string = address_string.replace(k, word)
                        succeed, result = self._run_address_search_not_by_thesaurus(address_string)
                        if succeed:
                            return succeed, result
        return False, {}

    @staticmethod
    def __real_succeed(score):
        return score >= 70

    def run_address_search(self, address_string, is_participle_continue=False, remove_last=False):
        succeed, result = self._run_address_search_not_by_thesaurus(address_string, is_participle_continue)
        # 还是未找到的話，使用同义词
        if not succeed:
            succeed, result = self._run_address_search_by_thesaurus(address_string)
        return succeed, result

    def run_address_search_by_score(self, address_string):
        # 只要返回最高分数的那条
        self._address_max_return = 1

        succeed, result = self.run_address_search(address_string)
        score = 0
        if succeed:
            score = result["score"]
        if not self.__real_succeed(score):
            succeed2, result2 = self.run_address_search(address_string, True)
            if succeed2:
                score2 = result2["score"]
                if score2 > score:
                    result = result2
        return succeed, result

    def __create_address_search_params(self, result):
        """
        生成搜索参数
        """
        search_params = None
        return search_params

    def __address_search(self, search_params):
        succeed = False
        search_result = {}

        if len(search_params) == 0:
            return succeed, search_result

        for search_param in search_params:
            if self._print_debug:
                # print("======", str(search_param))
                # print("search_param :" + str(search_param["query"]["function_score"]["query"]))
                print("search_param :" + str(search_param))
            succeed, search_result = self._do_address_search(search_param)
            if succeed:
                break
        return succeed, search_result

    def _do_address_search(self, search_param):
        try:
            search_result = self._es_cli.query(search_param)
            return self._get_query_result(search_result)
        except Exception as e:
            log.error(str(e))
            return False, {}

    def _get_query_dict(self, search_list1, search_list2, search_list3):
        search_query = {}
        if self._blur_search == 0:
            return search_query

        # 匹配到3段得 81分
        search_query["81"] = search_list1
        if self._blur_search == 1:
            return search_query

        if len(search_list1) != len(search_list2):
            # 匹配到2段得 61分
            search_query["61"] = search_list2

        if self._blur_search == 2:
            return search_query

        if len(search_list2) != len(search_list3):
            # 匹配到1段得 51分
            search_query["51"] = search_list3

        return search_query

    def _make_point_search_param(self, x, y, buff_distance):
        """
        坐标查询
        :param x:
        :param y:
        :param buff_distance:
        :return:
        """
        buff_distance = buff_distance if buff_distance <= self._max_distance else self._max_distance
        return {
            "query": {
                "geo_distance": {
                    "distance": str(float(buff_distance) / 1000) + "km",
                    "distance_type": "arc",
                    "location": {
                        "lat": float(y),
                        "lon": float(x)
                    }
                }
            },
            "sort": [
                {
                    "_geo_distance": {
                        "location": {
                            "lat": float(y),
                            "lon": float(x)
                        },
                        "order": "asc",
                        "unit": "m"
                    }
                }
            ],
            "size": int(self._address_max_return)
        }

    def run_search_by_point(self, points_string: str, buff_distance: int):
        """
        通过坐标匹配
        :return:
        """
        points = points_string.split(",")
        x = str(points[0]).strip()
        y = str(points[1]).strip()

        search_param = self._make_point_search_param(x=x, y=y, buff_distance=buff_distance)
        if self._print_debug:
            print("search_param = ", search_param)

        search_result = self._es_cli.query(search_param)
        result = self._get_query_result(search_result)

        return result

    def _get_query_result(self, search_result):
        """
        重新组织es返回的结果
        """
        if search_result is None:
            return False, {}

        search_count = int(search_result.get("hits").get("total").get("value"))
        if self._print_debug:
            print("找到数量 = " + str(search_count))
        if search_count <= 0:
            return False, {}

        items = search_result.get("hits").get("hits")
        # 返回多个
        if self._return_multi:
            results = []
            for item in items:
                result = item.get("_source")
                result["score"] = item.get("_score")
                result["id"] = item.get("_id")
                results.append(result)
            return True, results

        # 返回1个， 第1個分數最高
        result = items[0].get("_source")
        result["score"] = items[0].get("_score")
        result["id"] = items[0].get("_id")
        return True, result

    def _get_score_script(self, region, street, sections_fir, sections_mid, sections_last, building_number):
        if sections_fir is None:
            sections_fir = {}
        if sections_mid is None:
            sections_mid = {}
        if sections_last is None:
            sections_last = {}

        script = {
            "script_score": {
                "script": {
                    "id": self._score_script_id,
                    "params": {
                        "multi_region": 1 if self._multi_region else 0,
                        "region_field": self._region_field,
                        "region_value": region if region is not None else "",
                        "street_field": self._street_field,
                        "street_value": street if street is not None else "",
                        "query_fields_fir": es_schema_fields_fir,
                        "query_fields_mid": es_schema_fields_mid,
                        "query_fields_last": es_schema_fields_last,
                        "query_field_building_number": es_schema_field_building_number,
                        "query_value_building_number": building_number,
                        "query_value_fir": list(sections_fir.values()),
                        "query_value_mid": list(sections_mid.values()),
                        "query_value_last": list(sections_last.values())
                    }
                }
            }
        }
        return script
