import re
from copy import deepcopy

from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component, Scope
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired, Value
from shapely import wkt
from shapely.wkt import loads

from addressSearch.es.elasticsearchManger import ElasticsearchManger
from addressSearch.es.schemas import schemaMain, es_schema_fields_fir, es_schema_fields_main, es_schema_fields_mid
from addressSearch.mapping.addressMapping import AddressMapping
from addressSearch.resolver.addressParseRunner import AddressParseRunner
from addressSearch.resolver.lacModelManager import LacModelManager
from addressSearch.service.addressParseService import AddressParseService
from addressSearch.service.configService import ConfigService
from addressSearch.service.thesaurusService import ThesaurusService


@Component
@Scope("prototype")
class EsSearchService:
    @Value({
        "project.print_debug": "_print_debug",
        "CUT_WORDS": "_cut_words",
    })
    def __init__(self):
        self._print_debug = False
        self._address_table = None
        self._parsed_address_table = None
        self._db_name = None
        self._ip = None
        self._port = None
        self._distance = None
        self._max_return = None
        self._addressParseService = None
        self._lacModelManager = None
        self._thesaurusService = None
        self._addressMapping = None
        self._configService = None

    def _after_init(self):
        self._address_table = self._configService.get_addr_cnf("data_table")
        self._parsed_address_table = self._configService.get_addr_cnf("data_table_parsed")
        self._db_name = self._configService.get_es_cnf("db_name_address")
        self._ip = self._configService.get_es_cnf("ip")
        self._port = int(self._configService.get_es_cnf("port"))
        self._distance = self._configService.get_es_cnf("point_buffer_distance")
        self._max_return = int(self._configService.get_es_cnf("address_max_return"))

        self._es_cli = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
        with self._es_cli as es_conn:
            if es_conn is None:
                return

    def close(self):
        if self._es_cli is not None:
            self._es_cli.close()

    @Autowired
    def set_params(self,
                   addressMapping: AddressMapping,
                   configService: ConfigService,
                   lacModelManager: LacModelManager,
                   addressParseService: AddressParseService,
                   thesaurusService: ThesaurusService
                   ):
        self._addressMapping = addressMapping
        self._configService = configService
        self._lacModelManager = lacModelManager
        self._addressParseService = addressParseService
        self._thesaurusService = thesaurusService

    def reset(self):
        # 清空数据表
        self._addressMapping.truncate_table(self._address_table)
        self._addressMapping.truncate_table(self._parsed_address_table)

        # 删了重建es库
        es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
        es.deleteIndex(self._db_name)
        es.create(self._db_name)
        es.close()

    def run_address_search(self, address_string):
        """
        通过地名地址匹配
        :param address_string:
        :return:
        """
        # 分词
        with self._lacModelManager as model:
            succeed, sections_fir, sections_main, sections_mid = self._addressParseService.run(model, address_string)

        if not succeed or sections_main is None or len(sections_main) == 0:
            log.error("分詞失敗，地址: " + address_string)
            return False, {}

        # 生成查询参数
        search_param = self.__create_address_search_param(sections_fir, sections_main, sections_mid)

        # 搜索
        succeed, result = self.__address_search(search_param)
        # 还是未找到的話，模糊查询
        if not succeed:
            succeed, result = self.search_by_like(sections_fir, sections_main, sections_mid)

        return succeed, result

    @staticmethod
    def __create_address_search_param(sections_fir, sections_main, sections_mid):
        """
        生成搜索参数
        """
        search_param = {
            "query": {
                "bool": {
                    "must": []
                }
            }
        }

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
            search_param["query"]["bool"]["must"].append(d_fir)

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
            search_param["query"]["bool"]["must"].append(d_main)

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
            search_param["query"]["bool"]["must"].append(d_mid)

        return search_param

    def __address_search(self, search_param):
        print("search_param :", search_param)
        succeed, search_result = self._do_address_search(search_param)

        must_list = search_param["query"]["bool"]["must"]
        if len(must_list) <= 0:
            return False, {}

        # 未找到的話，去掉楼栋后的数字，即去掉 mid部分
        if not succeed and len(must_list) > 2:
            must_list.pop(2)
            succeed, search_result = self._do_address_search(search_param)

        # 还是未找到的話，去掉主体前部分，即 fir部分
        if not succeed and len(must_list) > 1:
            must_list.pop(0)
            succeed, search_result = self._do_address_search(search_param)
        # print("search_result: ", search_result)
        return succeed, search_result

    def _do_address_search(self, search_param, return_multi=False):
        try:
            search_result = self._es_cli.query(search_param)
            return self._get_query_result(search_result, return_multi)
        except Exception as e:
            log.error(str(e))
            return False, {}

    def _get_query_result(self, search_result, return_multi=False):
        search_count = int(search_result.get("hits").get("total").get("value"))
        if self._print_debug:
            print("找到数量 = ", search_count)
        if search_count <= 0:
            return False, {}
        items = search_result.get("hits").get("hits")
        if return_multi:
            results = []
            for item in items:
                result = item.get("_source")
                result["score"] = item.get("_score")
                result["id"] = int(item.get("_id"))
                results.append(result)
            return True, results
        # 第1個分數最高
        result = items[0].get("_source")
        result["score"] = items[0].get("_score")
        result["id"] = int(items[0].get("_id"))
        return True, result

    def search_by_like(self, sections_fir, sections_main, sections_mid):
        """
        模糊匹配
        """
        s = "*".join(list(sections_main.values()))
        search_param = {
            "query": {
                "bool": {
                    "must": []
                }
            }
        }
        search_param["query"]["bool"]["must"].append({
            "query_string": {
                "default_field": es_schema_fields_main[0],
                "query": "*" + s + "*"
            }
        })

        if self._print_debug:
            print(">>>>>>>>>> 模糊查詢 <<<<<<<<<<")
            print(search_param)

        search_result = self._es_cli.query(search_param)
        result = self._get_query_result(search_result)

        return result

    def _gen_location_search_param(self, json_param):
        """
        坐标查询
        :param json_param:
        :return:
        """
        search_param_list = []
        for dataId, point_str in json_param.items():
            pts = point_str.split(",")
            param = {
                str(dataId): {
                    "query": {
                        "geo_distance": {
                            "distance": self._distance,
                            "distance_type": "arc",
                            "location": {
                                "lat": float(pts[1]),
                                "lon": float(pts[0])
                            }
                        }
                    }
                }
            }
            search_param_list.append(param)
        return search_param_list

    @staticmethod
    def _get_distance(x1, y1, x2, y2):
        """
        空间算子获取
        :param x1:
        :param y1:
        :param x2:
        :param y2:
        :return:
        """
        geo1 = wkt.loads("point({} {})".format(x1, y1))
        geo2 = wkt.loads("point({} {})".format(x2, y2))
        return geo1.distance(geo2)

    def _do_location_search(self, search_param, es, return_multi=False):
        search_result = es.query(search_param)
        search_count = int(search_result.get("hits").get("total").get("value"))
        items = search_result.get("hits").get("hits")

        if self._print_debug:
            print("找到数量 = ", search_count)

        if return_multi:
            val = []
            for item in items:
                val.append(item.get("_source"))
            return val

        if search_count == 1:
            return items[0].get("_source")

        val = ""
        if search_count > 1:
            # 取距离最近的
            location_o = search_param.get("query").get("geo_distance").get("location")
            lon_o = location_o.get("lon")
            lat_o = location_o.get("lat")

            dList = []
            for i in range(len(items)):
                location = items[i].get("_source").get("location")
                lon_d = location.get("lon")
                lat_d = location.get("lat")
                # 获取2点之间距离
                distance = EsSearchService._get_distance(lon_o, lat_o, lon_d, lat_d)
                dList.append(distance)
            # 获取距离最近的下标，这个下标和items下标是一致的
            maxVal = min(dList)
            idx = dList.index(maxVal)
            return items[idx].get("_source")
        return val

    def search_by_point(self, json_param, return_multi=False):
        """
        {
            "1": "119.87630533652268,31.31180405900834"
        }

        通过坐标匹配
        :return:
        """

        jsonParamData = json_param
        # print(jsonParam)

        searchResultAll = {}
        SearchParamList = self._gen_location_search_param(jsonParamData)
        for param in SearchParamList:
            for dataId, searchParam in param.items():
                val = self._do_location_search(searchParam, self._es_cli, return_multi)
                if val is not None and len(str(val)) > 0:
                    searchResultAll[dataId] = val
                else:
                    searchResultAll[dataId] = None
        return searchResultAll
