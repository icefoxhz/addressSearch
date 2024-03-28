from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component, Scope
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired, Value

from addressSearch.es.elasticsearchManger import ElasticsearchManger
from addressSearch.es.schemas import es_schema_fields_fir, es_schema_fields_main, es_schema_fields_mid, \
    copy_schema, add_schema_field, es_schema_field_building_number, es_fullname_field
from addressSearch.mapping.addressMapping import AddressMapping
from addressSearch.service.lacModelManageService import LacModelManageService
from addressSearch.service.addressParseService import AddressParseService
from addressSearch.service.configService import ConfigService
from addressSearch.service.thesaurusService import ThesaurusService


@Component
@Scope("prototype")
class EsSearchService:
    @Value({
        "project.print_debug": "_print_debug",
        "project.blur_search": "_blur_search",
        "project.local_config.db_name_address": "_db_name_address",
        "project.local_config.address_max_return": "_address_max_return",
    })
    def __init__(self):
        self._print_debug = False
        self._blur_search = False
        self._address_table = None
        self._parsed_address_table = None
        self._ip = None
        self._port = None
        self._db_name_address = None
        self._distance = None
        self._max_return = None
        self._addressParseService = None
        self._lacModelManageService = None
        self._thesaurusService = None
        self._addressMapping = None
        self._configService = None
        self._address_max_return = 20
        self._return_multi = False
        self._es_cli = None
        self._build_number_tolerance = 15

    def _after_init(self):
        self._address_table = self._configService.get_addr_cnf("data_table")
        self._parsed_address_table = self._configService.get_addr_cnf("data_table_parsed")
        self._ip = self._configService.get_es_cnf("ip")
        self._port = int(self._configService.get_es_cnf("port"))
        self._distance = self._configService.get_es_cnf("point_buffer_distance")
        self._max_return = int(self._configService.get_es_cnf("address_max_return"))
        self.__conn_es()

    def __conn_es(self):
        # 添加额外字段.  深拷贝一份，不能修改老的
        schemaMainNew = copy_schema()
        add_schema_field(schemaMainNew, "ex_val")

        self._es_cli = ElasticsearchManger(self._db_name_address, schemaMainNew, self._ip, self._port)
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
                   lacModelManageService: LacModelManageService,
                   addressParseService: AddressParseService,
                   thesaurusService: ThesaurusService
                   ):
        self._addressMapping = addressMapping
        self._configService = configService
        self._lacModelManageService = lacModelManageService
        self._addressParseService = addressParseService
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

    def _run_address_search_not_by_thesaurus(self, address_string):
        """
        不使用同义词通过地名地址匹配
        :param address_string:
        :return:
        """
        if self._print_debug:
            print("\n-----------------------------------------------------\n")
        # 分词
        with self._lacModelManageService as model:
            succeed, sections_fir, sections_main, sections_mid, sections_building_number = self._addressParseService.run(
                model, address_string)

        if not succeed or sections_main is None or len(sections_main) == 0:
            log.error("分詞失敗，地址: " + address_string)
            return False, {}

        # 生成查询参数
        search_params = self.__create_address_search_params(sections_fir, sections_main, sections_mid,
                                                            sections_building_number)

        # 搜索
        succeed, result = self.__address_search(search_params)

        # 还是未找到的話，尝试模糊查询
        if not succeed and self._blur_search:
            succeed, result = self.search_by_like(sections_fir, sections_main, sections_mid)

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
                        address_string = address_string.replace(k, word)
                        succeed, result = self._run_address_search_not_by_thesaurus(address_string)
                        if succeed:
                            return succeed, result

        return False, {}

    def run_address_search(self, address_string):
        succeed, result = self._run_address_search_not_by_thesaurus(address_string)
        # 还是未找到的話，使用同义词
        if not succeed:
            succeed, result = self._run_address_search_by_thesaurus(address_string)

        return succeed, result

    def __create_address_search_params(self, sections_fir, sections_main, sections_mid, sections_building_number):
        """
        生成搜索参数, 尝试多次搜索，每次都减少搜索条件，但是 main必须包含
        """
        d_fir = None
        d_main = None
        d_mid = None

        all_value_list = list(dict(sections_fir | sections_main | sections_mid).values())
        all_search_field_list = es_schema_fields_fir + es_schema_fields_main + es_schema_fields_mid

        # 评分函数
        script = self._get_score_script(all_search_field_list, all_value_list)

        # 组织主体前部分的搜索
        if sections_fir is not None and len(sections_fir) > 0:
            d_fir = {"bool": {"should": [], "minimum_should_match": "0%"}}  # 0% 是要求至少匹配一个
            for field, val in sections_fir.items():
                d_fir["bool"]["should"].append(
                    {
                        "multi_match": {
                            "query": str(val),
                            # 只找前面5個，定義大于5個是為了容錯的
                            "fields": [es_schema_fields_fir[i] for i in range(5)]
                        }
                    }
                )

        # 组织主体部分的搜索
        if sections_main is not None and len(sections_main) > 0:
            d_main = {"bool": {"should": [], "minimum_should_match": "100%"}}  # 主体必须要完全匹配到
            for field, val in sections_main.items():
                d_main["bool"]["should"].append(
                    {
                        "multi_match": {
                            "query": str(val),
                            "fields": es_schema_fields_main
                        }
                    })

        # 组织主体后部分的搜索
        if sections_mid is not None and len(sections_mid) > 0:
            d_mid = {"bool": {"should": [], "minimum_should_match": "0%"}}  # 0% 是要求至少匹配一个
            for field, val in sections_mid.items():
                d_mid["bool"]["should"].append(
                    {
                        "multi_match": {
                            "query": str(val),
                            "fields": es_schema_fields_mid
                        }
                    }
                )

            val = sections_building_number[es_schema_field_building_number]
            if val > 0:
                d_mid["bool"]["should"].append(
                    {
                        "range": {
                            es_schema_field_building_number: {
                                "gte": val - self._build_number_tolerance,
                                "lte": val + self._build_number_tolerance
                            }
                        }
                    }
                )

        # 创建搜索组合
        if d_fir is not None and d_mid is not None:
            lss = [[d_fir, d_main, d_mid], [d_main, d_mid], [d_fir, d_main], [d_main]]
        elif d_fir is not None and d_mid is None:
            lss = [[d_fir, d_main], [d_main]]
        elif d_fir is None and d_mid is not None:
            lss = [[d_main, d_mid], [d_main]]
        else:
            lss = [[d_main]]

        search_params = []
        for ls in lss:
            search_param = {
                "query": {
                    "function_score": {
                        "score_mode": "sum",
                        "boost_mode": "replace",
                        "functions": [script],
                        "query": {
                            "bool": {
                                "must": ls
                            }
                        }
                    }
                },
                "size": int(self._address_max_return)
            }
            search_params.append(search_param)

        return search_params

    def __address_search(self, search_params):
        succeed = False
        search_result = {}

        if len(search_params) == 0:
            return succeed, search_result

        for search_param in search_params:
            if self._print_debug:
                print("search_param :" + str(search_param["query"]["function_score"]["query"]))
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

    def search_by_like(self, sections_fir, sections_main, sections_mid):
        """
        模糊匹配
        """

        # 主体之前和主体
        search_list = list(sections_fir.values()) + list(sections_main.values())

        all_search_list = list(sections_fir.values()) + list(sections_main.values()) + list(sections_mid.values())
        all_search_field_list = es_schema_fields_fir + es_schema_fields_main + es_schema_fields_mid
        script = self._get_score_script(all_search_field_list, all_search_list)

        search_string = "*".join(search_list)
        search_param = {
            "query": {
                "function_score": {
                    "score_mode": "sum",
                    "boost_mode": "replace",
                    "functions": [script],
                    "query": {
                        "bool": {
                            "must": [{
                                "query_string": {
                                    "default_field": es_fullname_field,
                                    "query": "*" + search_string + "*"
                                }
                            }]
                        }
                    }
                }
            },
            "size": int(self._address_max_return)
        }

        if self._print_debug:
            print(">>>>>>>>>> 模糊查詢 <<<<<<<<<<")
            print("search_param :" + str(search_param["query"]["function_score"]["query"]))

        search_result = self._es_cli.query(search_param)
        result = self._get_query_result(search_result)

        return result

    def _make_point_search_param(self, x, y):
        """
        坐标查询
        :param x:
        :param y:
        :return:
        """
        return {
            "query": {
                "geo_distance": {
                    "distance": self._distance,
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

    def run_search_by_point(self, points_string: str):
        """
        {
            "1": "119.87630533652268,31.31180405900834"
        }

        通过坐标匹配
        :return:
        """
        points = points_string.split(",")
        x = points[0]
        y = points[1]

        search_param = self._make_point_search_param(x=x, y=y)
        if self._print_debug:
            print("search_param = ", search_param)

        search_result = self._es_cli.query(search_param)
        result = self._get_query_result(search_result)

        return result

    def _get_query_result(self, search_result):
        """
        重新组织es返回的结果
        """
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
                result["id"] = int(item.get("_id"))
                results.append(result)
            return True, results

        # 返回1个， 第1個分數最高
        result = items[0].get("_source")
        result["score"] = items[0].get("_score")
        result["id"] = int(items[0].get("_id"))
        return True, result

    @staticmethod
    def _get_score_script(all_search_field_list, all_value_list):
        script = {
            "script_score": {
                "script": {
                    "source": """
                                   double score = 0;
                                    for (int i = 0; i < params.query_field.length; i++) {
                                      if (doc.containsKey(params.query_field[i]) && doc[params.query_field[i]].size() > 0) {
                                        for (int j = 0; j < params.query_value.length; j++) {
                                          if (doc[params.query_field[i]].value == params.query_value[j]) {
                                            score += 1; // 匹配度加1
                                            break; // 如果有匹配，则跳出内层循环
                                          }
                                        }
                                      }
                                    }
                                    return score / params.query_value.length;

                                  """,
                    "lang": "painless",
                    "params": {
                        "query_field": all_search_field_list,
                        "query_value": all_value_list
                    }
                }
            }
        }
        return script
