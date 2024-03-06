import math
from copy import deepcopy

import ujson as json
from shapely.wkt import loads
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component, Scope
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired, Value
from shapely import wkt

from addressSearch.es.elasticsearchManger import ElasticsearchManger
from addressSearch.es.schemas import schemaMain
from addressSearch.mapping.addressMapping import AddressMapping
from addressSearch.resolver.addressParseRunner import AddressParseRunner
from addressSearch.resolver.lacModelManager import LacModelManager
from addressSearch.service.thesaurusService import ThesaurusService


@Component
@Scope("prototype")
class EsSearchService:
    @Value({
        "project.print_debug": "_print_debug",
        "project.tables.address_table": "_address_table",
        "project.tables.parsed_address_table": "_parsed_address_table",
        "project.search_key": "_search_key",
        "project.elasticsearch.db.db_name": "_db_name",
        "project.elasticsearch.db.ip": "_ip",
        "project.elasticsearch.db.port": "_port",
        "project.elasticsearch.point.distance": "_distance",
        "project.elasticsearch.address.max_return": "_max_return",
        "CUT_WORDS": "_cut_words",
    })
    def __init__(self):
        self._print_debug = False
        self._address_table = None
        self._parsed_address_table = None
        self._search_key = None
        self._db_name = None
        self._ip = None
        self._port = None
        self._distance = None
        self._cut_words = None
        self._max_return = None
        self._applicationContext = None
        self._addressParseRunner = None
        self._lacModelManager = None
        self._thesaurusService = None
        self._addressMapping = None

        self._searchMain = None

        # 权重
        self._searchSection2Boost = {
            "region": 5,
            "street": 20,
            "community": 20,
            "natural_village": 20,
            "road": 20,
            "courtyard": 20,
            "group_number": 5,
            "address_number": 5,
            "building_name": 20,
            "building_site": 5,
            "unit": 1,
            "floor": 1,
            "room": 1,
            "company": 1
        }

        self._matchSectionListFront = ["region", "street", "community", "natural_village", "road", "courtyard",
                                       "group_number", "address_number", "building_name", "building_site", "company"]

        # 第1次去掉后面部分先不要去 building_site
        self._matchSectionListBackFirst = ["unit", "floor", "room", "company"]

    def _after_init(self):
        self._searchMain = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
        with self._searchMain as es_conn:
            if es_conn is None:
                return

    def close(self):
        if self._searchMain is not None:
            self._searchMain.close()

    @Autowired
    def set_params(self,
                   addressMapping: AddressMapping,
                   lacModelManager: LacModelManager,
                   addressParseRunner: AddressParseRunner,
                   thesaurusService: ThesaurusService,
                   applicationContext):
        self._addressMapping = addressMapping
        self._lacModelManager = lacModelManager
        self._addressParseRunner = addressParseRunner
        self._thesaurusService = thesaurusService
        self._applicationContext = applicationContext

    def reset(self):
        # 清空数据表
        self._addressMapping.truncate_table(self._address_table)
        self._addressMapping.truncate_table(self._parsed_address_table)

        # 删了重建es库
        es = ElasticsearchManger(self._db_name, schemaMain, self._ip, self._port)
        es.deleteIndex(self._db_name)
        es.create(self._db_name)
        es.close()

    @staticmethod
    def getAliasName(fullName, startStr, endStr):
        fullNameList = []
        if startStr in fullName and endStr in fullName:
            startIdx = fullName.index(startStr)
            endIdx = fullName.index(endStr)
            # 去掉 startStr 到 endStr 之间的部分
            fullnameTemp = fullName[:startIdx] + fullName[endIdx + 1:]
            fullNameList.append(fullnameTemp)
        return fullNameList

    def parse(self, jsonRequest):
        resultDict = {}
        for dataId, fullName in jsonRequest.items():
            fullNameAllList = [fullName]
            for cutList in self._cut_words:
                startStr = cutList[0]
                endStr = cutList[1]
                fullNameList = self.getAliasName(fullName, startStr, endStr)
                fullNameAllList = fullNameAllList + fullNameList

            with self._lacModelManager as model:
                i = 1
                for address in fullNameAllList:
                    addressParser = self._applicationContext.get_bean("addressParser")
                    address = address.replace(" ", "")
                    resultList, _ = self._addressParseRunner.run(addressParser, model, address)
                    resultDict[str(dataId) + "^" + str(i)] = resultList
                    i += 1
        return resultDict

    @staticmethod
    def removeKeyWord(resultDict):
        ls = ["province", "city"]
        for key in ls:
            if key in dict(resultDict).keys():
                resultDict.pop(key)

    def searchByAddress(self, resultListDict, isAccurate=True, returnMulti=False):
        """
        通过地名地址匹配
        :param resultListDict:
        :param isAccurate:   是否精准匹配
        :param returnMulti:  是否返回多条
        :return:
        """
        succeed = False
        searchResultAll = {}
        for dataId, resultList in resultListDict.items():
            # print("resultList = ", resultList)
            # 可能被分了多个形式，其中一个搜索到就算ok
            dataId = str(dataId).split("^")[0]
            searchResultAll[dataId] = {}
            if len(searchResultAll[dataId]) > 0:
                continue

            if len(resultList) > 0:
                resultDict = resultList[0]
                # for resultDict in resultList:
                self.removeKeyWord(resultDict)

                # 全匹配
                is_find = False
                match_percents = ["100%", "90%", "80%", "70%", "60%", "50%", "40%"]
                current_match_percent = None
                for match_percent in match_percents:
                    current_match_percent = match_percent
                    searchParam = self.__genAddressSearchParam(resultDict, isAccurate, match_percent)
                    if self._print_debug:
                        print(">>>>>>>>>> 全匹配({}) <<<<<<<<<<".format("精准" if isAccurate else "模糊"))
                        print("searchParam = ", searchParam)
                    val = self._doAddressSearch(searchParam, self._searchMain, returnMulti)

                    if val is not None and val != "" and len(val) > 0:
                        searchResultAll[str(dataId)] = val
                        succeed = True
                        is_find = True
                        break
                # 高精度匹配到的就直接用
                if is_find and current_match_percent in ["100%", "90%"]:
                    continue

                # 低精度下， 能后面匹配到就用后面的
                # 第1次去除back后匹配
                val = self.searchCutBack(self._matchSectionListBackFirst, isAccurate, resultDict, returnMulti)
                if val is not None and val != "" and len(val) > 0:
                    searchResultAll[str(dataId)] = val
                    succeed = True
                    print("第1次去除back后匹配 = ", val)
                    continue

                # # 去除back后匹配
                # val = self.searchCutBack(self._matchSectionListBack, isAccurate, isRtnMulti, resultDict, val)
                # if val is not None and val != "" and len(val) > 0:
                #     searchResultAll[str(dataId)] = val
                #     continue

            if len(searchResultAll[dataId]) == 0:
                searchResultAll[dataId] = None

        return succeed, searchResultAll

    def searchCutBack(self, cutBackList, isAccurate, resultDict, returnMulti=False):
        val = None
        resultDictTemp = deepcopy(resultDict)
        oldLen = len(resultDict)
        for sec in cutBackList:
            if sec in resultDict.keys():
                resultDict.pop(sec)

        # 只有后半部分， 去掉了就没了
        onlyBack = len(resultDict) == 0
        if len(resultDict) == 0:
            resultDict = resultDictTemp

        if oldLen > len(resultDict) or onlyBack:
            searchParam = self.__genAddressSearchParam(resultDict, isAccurate)
            if self._print_debug:
                print(">>>>>>>>>> 去除back后匹配({}) <<<<<<<<<<".format("精准" if isAccurate else "模糊"))
                print("searchParam = ", searchParam)
            val = self._doAddressSearch(searchParam, self._searchMain, returnMulti)
        return val

    def _doAddressSearch(self, searchParam, es, returnMulti=False):
        searchResult = es.query(searchParam)
        searchCount = int(searchResult.get("hits").get("total").get("value"))
        val = []
        if searchCount > 0:
            if self._print_debug:
                print("找到数量 = ", searchCount)
            items = searchResult.get("hits").get("hits")
            maxScore = items[0].get("_score")

            for item in items:
                if maxScore == item.get("_score"):
                    val.append(item.get("_source"))

                if len(val) >= self._max_return or item.get("_score") < maxScore:
                    break

        if returnMulti:
            return val

        rtnVal = ""
        # 找到多个的情况下， 取最短的那个
        if len(val) > 0:
            keyCount = 999
            for val in val:
                if len(val.keys()) < keyCount:
                    rtnVal = val
                    keyCount = len(val.keys())
        return rtnVal

    def __genAddressSearchParam(self, parseResultValue, isAccurate=True, match_percent="100%"):
        """
        生成搜索的参数
        :param parseResultValue:
        :param isAccurate: 是否精确查找， 分词结果是哪个 section 就找哪个 section
        :return:
        """

        # 获取设置的权重
        vDict = {}
        for sectionName, boost in self._searchSection2Boost.items():
            sectionValue = parseResultValue.get(sectionName, None)
            if sectionValue is not None:
                vDict[sectionValue] = boost

        rtn = {
            "size": self._max_return + 1,
            "query": {
                "bool": {
                    "must": []
                }
            }
        }

        # ---------------- 前半段必须先匹配成功的
        self.__genAddressSearchParamFront(isAccurate, parseResultValue, rtn, vDict, match_percent)

        # ---------------- 前半段匹配到的情况下才继续匹配后半段
        self.__genAddressSearchParamBack(isAccurate, parseResultValue, rtn, vDict)

        return rtn

    def __genAddressSearchParamFront(self, isAccurate, parseResultValue, rtn, vDict, match_percent="100%"):
        ct, param = self.__paramCommon(isAccurate, parseResultValue, vDict, self._matchSectionListFront, True,
                                       match_percent)

        # 2个词以内必须全匹配
        # if ct <= 2:
        #     param["bool"]["minimum_should_match"] = "100%"

        if ct > 0:
            rtn["query"]["bool"]["must"].append(param)

    def __genAddressSearchParamBack(self, isAccurate, parseResultValue, rtn, vDict):
        ct, param = self.__paramCommon(isAccurate, parseResultValue, vDict, self._matchSectionListBackFirst)

        if ct > 0:
            rtn["query"]["bool"]["must"].append(param)

    @staticmethod
    def __paramCommon(isAccurate, parseResultValue, vDict, sectionList, isFront=False, match_percent="100%"):
        """
        :param isAccurate:
        :param parseResultValue:
        :param vDict:
        :param sectionList:
        :return:
        """
        param = {
            "bool": {
                "should": [],
                "minimum_should_match": "50%" if not isAccurate and not isFront else match_percent
            }
        }
        ct = 0
        for sectionName in sectionList:
            sectionValue = parseResultValue.get(sectionName, None)
            if sectionValue is None:
                continue

            # # 前半段在模糊查询的时候，把数字判断去掉， 一般都是分词误差导致
            # if isFront and not isAccurate and isNumLetters(sectionValue) and len(parseResultValue) > 3:
            #     continue

            ct += 1

            tmpDict = {
                "multi_match":
                    {
                        "query": sectionValue,
                        "fields": sectionList if not isAccurate else [sectionName],
                        "boost": vDict[sectionValue]
                    }
            }
            param["bool"]["should"].append(tmpDict)
        return ct, param

    def _genLocationSearchParam(self, jsonParam):
        """
        坐标查询
        :param jsonParam:
        :return:
        """
        SearchParamList = []
        for dataId, pointStr in jsonParam.items():
            pts = pointStr.split(",")
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
            SearchParamList.append(param)
        return SearchParamList

    @staticmethod
    def _getDistance(x1, y1, x2, y2):
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

    def _doLocationSearch(self, searchParam, es, returnMulti=False):
        searchResult = es.query(searchParam)
        searchCount = int(searchResult.get("hits").get("total").get("value"))
        items = searchResult.get("hits").get("hits")

        if self._print_debug:
            print("找到数量 = ", searchCount)

        if returnMulti:
            val = []
            for item in items:
                val.append(item.get("_source"))
            return val

        if searchCount == 1:
            return items[0].get("_source")

        val = ""
        if searchCount > 1:
            # 取距离最近的
            location_o = searchParam.get("query").get("geo_distance").get("location")
            lon_o = location_o.get("lon")
            lat_o = location_o.get("lat")

            dList = []
            for i in range(len(items)):
                location = items[i].get("_source").get("location")
                lon_d = location.get("lon")
                lat_d = location.get("lat")
                # 获取2点之间距离
                distance = EsSearchService._getDistance(lon_o, lat_o, lon_d, lat_d)
                dList.append(distance)
            # 获取距离最近的下标，这个下标和items下标是一致的
            maxVal = min(dList)
            idx = dList.index(maxVal)
            return items[idx].get("_source")
        return val

    def searchByPoint(self, jsonParam, returnMulti=False):
        """
        {
            "1": "119.87630533652268,31.31180405900834",
            "2": "120.23387066537168,31.646691535452955",
            "3": "120.51393079234518,31.546471419507913",
            ...
        }

        通过坐标匹配
        :return:
        """

        jsonParamData = jsonParam
        # print(jsonParam)

        searchResultAll = {}
        SearchParamList = self._genLocationSearchParam(jsonParamData)
        for param in SearchParamList:
            for dataId, searchParam in param.items():
                val = self._doLocationSearch(searchParam, self._searchMain, returnMulti)
                if val is not None and len(str(val)) > 0:
                    searchResultAll[dataId] = val
                else:
                    searchResultAll[dataId] = None
        return searchResultAll

    def commonSearch(self, jsonParam):
        """
        point radius 和 wkt 取其一

        {
           "key": "name",                   字段名
           "point": "120.29 31.92",         示例：120.29 31.92
           "radius": "100m",
           "wkt": "POLYGON((120.33569335900006 31.545104980000076,120.34569335900007 31.545104980000076,120.32569335900006 31.645104980000077,120.33569335900006 31.545104980000076))",
           "start": 0,                      用于分页，查询结果索引起始值，默认0
           "rows": 0                        用于分页，查询结果返回记录数，默认0，最大值500
        }
        :param jsonParam:
        :return:
        """
        try:
            searchParam = self.generateCommonSearchParam(self._search_key, jsonParam)
            if self._print_debug:
                print("searchParam = ", searchParam)
            if searchParam is None:
                raise Exception("必须包含查询关键字")

            searchResult = self._searchMain.query(jsonQuery=searchParam)

            searchCount = int(searchResult.get("hits").get("total").get("value"))
            items = searchResult.get("hits").get("hits")

            searchList = []
            for item in items:
                searchList.append(item["_source"])

            return {
                "count": searchCount,
                "result": searchList,
                "code": 1,
                "msg": "success"
            }
        except Exception as e:
            return {
                "code": 0,
                "msg": str(e)
            }

    @staticmethod
    def generateCommonSearchParam(keyField, jsonParam):
        searchParam = {
            "query": {
                "bool": {
                    "must": []
                }
            }
        }

        searchKey = str(jsonParam["key"]).strip()

        #  字段模糊查询（必须）
        if "key" not in jsonParam or jsonParam["key"] is None or searchKey == "":
            return None

        """
        特殊字符要转义， 比如 万家防水(墙宅路) => 万家防水\\(墙宅路\\)
        keywordList = [":", "{", "}", "[", "]", "\"", "(", ")", "*", "?","+"]
        """

        keywordList = [":", "{", "}", "[", "]", "\"", "(", ")", "*", "?", "+"]
        for k in keywordList:
            if k in searchKey:
                searchKey = searchKey.replace(k, "\\" + k)
        print(searchKey)

        searchParam["query"]["bool"]["must"].append({
            "query_string": {
                "default_field": keyField,
                "query": "*" + searchKey + "*"
            }
        })

        # 分页
        if "start" in jsonParam and "rows" in jsonParam and int(jsonParam["rows"]) > 0:
            searchParam["from"] = int(jsonParam["start"])
            size = int(jsonParam["rows"])
            searchParam["size"] = size if size <= 50 else 50

        #  空间查询
        if ("point" in jsonParam and "radius" in jsonParam and jsonParam["point"] is not None
                and jsonParam["radius"] is not None):
            points = str(jsonParam["point"]).split(" ")
            if len(points) == 2:
                searchParam["query"]["bool"]["must"].append({
                    "geo_distance": {
                        "distance": str(jsonParam["radius"]) + "km",
                        "distance_type": "arc",
                        "location": {
                            "lat": float(points[1].strip()),
                            "lon": float(points[0].strip())
                        }
                    }
                })
        elif "wkt" in jsonParam and jsonParam["wkt"] is not None:
            geometry = loads(jsonParam["wkt"])

            points = []
            geoPoints = list(geometry.exterior.coords)
            for geoPoint in geoPoints:
                points.append({
                    "lon": geoPoint[0],
                    "lat": geoPoint[1]
                })
            searchParam["query"]["bool"]["must"].append({
                "geo_polygon": {
                    "location": {
                        "points": points
                    }
                }
            })

        return searchParam
