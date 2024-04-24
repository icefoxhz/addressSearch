import copy
import re
import threading

import jieba
from LAC import LAC
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value

from addressSearch.es.schemas import es_schema_field_building_number, es_schema_fields_fir, es_schema_fields_main, \
    es_schema_fields_mid, es_schema_fields_last
from addressSearch.utils.commonTool import CommonTool


@Component
class AddressParseService:
    __local_obj = threading.local()

    @Value({
        "project.print_debug": "_print_debug",
        "project.big_region.province": "_provinces",
        "project.big_region.city": "_cities",
        "project.big_region.region": "_regions",
        "project.big_region.street": "_streets",
    })
    def __init__(self):
        self._print_debug = False

        # --------------------------------
        self._LAST_MAX_LEN = 6
        self._provinces = None
        self._cities = None
        self._regions = None
        self._streets = None

        # --------------------------------
        # 比如，龙湖·星悦荟,  把这些符号去掉
        self._common_symbol = ["·"]

        # --------------------------------
        # 特殊楼栋判断。  震泽路18号B座狮子座
        # self._special_building_chinese_words = ["狮子座", "巨蟹座", "双子座", "白羊座", "金牛座", "射手座", "水瓶座",
        #                                         "处女座", "凤凰座", "海豚座", "鲸鱼座",  "天蝎座", "摩羯座", "双鱼座",
        #                                         "天鹅座", "飞鱼座", "杜鹃座", ]
        self._special_building_chinese_words = []

        # 通用楼栋判断
        self._building_chinese_words = ["幢", "栋", "号楼", "楼", "座",
                                        "号厂区", "号东厂区", "号南厂区", "号西厂区", "号北厂区",
                                        ]

        self._courtyard_chinese_words = ["期", "区"]

        # 新安花苑第二社区  =>  新安花苑2。 （新安花苑第二社区和新安花苑 都在字典表中，进行2次处理）
        self._extract_again_chinese_words = ["社区", "期", "区"]

        # 最后一个词满足条件，就删掉最后一个字。  "fir_3": "国道路" => "fir_3": "国道"
        self._remove_last_words = ["道路"]

        # --------------------------------
        self._CONJUNCTION = "-"
        self._conjunction_symbols = ["－", "—", "～", "~", "#", "/", "、", ",", "，", " ",
                                     "内东", "内南", "内西", "内北",
                                     "东边", "南边", "西边", "北边",
                                     "东面", "南面", "西面", "北面",
                                     "东侧", "南侧", "西侧", "北侧",
                                     ]

        self._conjunction_re_patterns_get_front = [r'正(.*?)方向(.*?)米',
                                                   r'斜(.*?)方向(.*?)米',
                                                   r'东(.*?)方向(.*?)米',
                                                   r'西(.*?)方向(.*?)米',
                                                   r'南(.*?)方向(.*?)米',
                                                   r'北(.*?)方向(.*?)米',
                                                   r'向(.*?)米',
                                                   r'东(.*?)米',
                                                   r'西(.*?)米',
                                                   r'南(.*?)米',
                                                   r'北(.*?)米',
                                                   ]

        self._conjunction_re_patterns_get_behind = [r'路与(.*?)路交叉口',
                                                    r'路与(.*?)路交界处',
                                                    r'路与(.*?)路交汇处',
                                                    ]

        # --------------------------------
        self._extra_symbols = [["(", ")"],
                               ["（", "）"],
                               ["[", "]"],
                               ["【", "】"]
                               ]

    def __print(self, msg):
        if self._print_debug:
            print(msg)

    def run(self, model: LAC, addr_string: str, is_participle_continue=False):
        """
        :param model:
        :param addr_string:
        :param is_participle_continue:
        :return:
        """
        try:
            if not self.acceptAddress(addr_string):
                return False, None, None, None, None, None, None, None

            addr_string = self.removeStartWordsIfNecessary(addr_string)
            addr_string = self.removeExtra(addr_string)
            self.__print("移除无用信息: " + str(addr_string))

            _, addr_string, last_string = self.cutAddress(model, addr_string)

            # 分詞並處理
            cut_list = self.participleAndProcess(model, addr_string)
            # 每个分词再次判断处理
            if is_participle_continue:
                cut_list = self.participleContinue(model, cut_list)
                self.__print("二次分词结果: " + str(cut_list))

            if cut_list is None:
                return False, None, None, None, None, None, None, None

            # 找主体
            body_idx = self.findMainBodyIndex(model, addr_string, cut_list)
            if body_idx == -1:
                return False, None, None, None, None, None, None, None
            # 处理并生成 sections
            succeed, [address_section_first, address_section_main, address_section_mid, address_section_last,
                      address_section_build_number] = self.create_sections(
                cut_list, body_idx, model, last_string)

            #  -------------
            if not succeed:
                # 找主体
                body_idx = self.findMainBodyIndexReverse(model, addr_string, cut_list)
                if body_idx == -1:
                    return False, None, None, None, None, None, None, None
                # 处理并生成 sections
                succeed, [address_section_first, address_section_main, address_section_mid, address_section_last,
                          address_section_build_number] = self.create_sections(
                    cut_list, body_idx, model, last_string)

            region = self.__local_obj.region if hasattr(self.__local_obj, "region") else None
            street = self.__local_obj.street if hasattr(self.__local_obj, "street") else None

            return succeed, region, street, address_section_first, address_section_main, address_section_mid, address_section_last, address_section_build_number
        except Exception as e:
            self.__print(str(e))
            return False, None, None, None, None, None, None, None
        finally:
            self.__local_obj.region = None
            self.__local_obj.street = None

    def participleContinue(self, model: LAC, cut_list):
        word_list = cut_list[0]

        word_list_ret = copy.deepcopy(word_list)

        # 獲取加載的字典表
        model_dict = model.__getattribute__("model").custom.dictitem
        for i in range(len(word_list) - 1, -1, -1):
            word = word_list[i]
            if word in model_dict:
                _, word, _ = self.__cutToCourtyardByChineseWords(model, word, False)
                word_list_temp = jieba.lcut(word)

                if len(word_list_temp) > 1 and (word_list_temp[0] in model_dict or word_list_temp[-1] in model_dict):
                    for word2 in word_list_temp:
                        if word2 in model_dict:
                            word_list_ret.pop(i)
                            # 在word_list_ret的下标为i的位置插入cut_list_temp列表的元素
                            word_list_ret[i:0] = word_list_temp
                            break
        lac_list_ret = ["m" for _ in range(len(word_list_ret))]
        return [word_list_ret, lac_list_ret] if len(word_list_ret) != len(word_list) else None

    # def participleContinue(self, model: LAC, cut_list):
    #     word_list = cut_list[0]
    #     lac_list = cut_list[1]
    #
    #     word_list_ret = copy.deepcopy(word_list)
    #     lac_list_ret = copy.deepcopy(lac_list)
    #
    #     # 獲取加載的字典表
    #     model_dict = model.__getattribute__("model").custom.dictitem
    #     for i in range(len(word_list) - 1, -1, -1):
    #         word = word_list[i]
    #         if word in model_dict:
    #             _, word, _ = self.__cutToCourtyardByChineseWords(model, word, False)
    #             cut_list_temp = model.run(word)
    #             word_list_temp = cut_list_temp[0]
    #             lac_list_temp = cut_list_temp[1]
    #             if len(word_list_temp) > 1 and (word_list_temp[0] in model_dict or word_list_temp[-1] in model_dict):
    #                 for word2 in word_list_temp:
    #                     if word2 in model_dict:
    #                         word_list_ret.pop(i)
    #                         lac_list_ret.pop(i)
    #                         # 在word_list_ret的下标为i的位置插入cut_list_temp列表的元素
    #                         word_list_ret[i:0] = word_list_temp
    #                         lac_list_ret[i:0] = lac_list_temp
    #                         break
    #     return [word_list_ret, lac_list_ret] if len(word_list_ret) != len(word_list) else None

    def acceptAddress(self, addr_string: str):
        """
        太模糊的地址，无法处理
        :param addr_string:
        :return:
        """
        # 江苏省无锡市新吴区硕放街道机场南路与南开路交叉口东100米
        # patterns = [
        # ]
        # for pattern in patterns:
        #     match = re.search(pattern, addr_string)
        #     if match:
        #         self.__print("无法处理太模糊地址: " + addr_string)
        #         return False
        return True

    def removeStartWordsIfNecessary(self, addr_string: str):
        """
        移除省、市、区、街道
        :param addr_string:
        :return:
        """
        remove_list = [self._provinces, self._cities]
        for remove_words in remove_list:
            for remove_word in remove_words:
                if addr_string.startswith(remove_word):
                    addr_string = addr_string.replace(remove_word, "", 1)

        for remove_word in self._regions:
            if addr_string.startswith(remove_word):
                self.__local_obj.region = remove_word
                addr_string = addr_string.replace(remove_word, "", 1)
                break

        for remove_word in self._streets:
            if addr_string.startswith(remove_word):
                self.__local_obj.street = remove_word
                addr_string = addr_string.replace(remove_word, "", 1)
                break

        return addr_string

    def removeExtra(self, addr_string: str):
        """
         去掉 () 相关的这种额外的东西
        :param addr_string:
        :return:
        """
        # 1.
        for extra_ls in self._extra_symbols:
            start_symbol = extra_ls[0]
            end_symbol = extra_ls[1]
            if start_symbol in addr_string and end_symbol in addr_string:
                s_idx = addr_string.index(start_symbol)
                e_idx = addr_string.index(end_symbol)
                addr_string = addr_string[:s_idx] + addr_string[e_idx + 1:]
                # print("去除extra后: ", addr_string)
            elif start_symbol in addr_string and end_symbol not in addr_string:
                addr_string = addr_string.replace(start_symbol, "")
            elif start_symbol not in addr_string and end_symbol in addr_string:
                addr_string = addr_string.replace(end_symbol, "")

        # 2.
        for symbol in self._common_symbol:
            addr_string = addr_string.replace(symbol, "")

        return addr_string

    def cutAddress(self, model: LAC, addr_string: str):
        """
        先截取楼栋， 如果截取不到，就尝试截取到小区院落
        :param model:
        :param addr_string:
        :return:
        """
        cut_succeed, addr_string, last_string = self.__cutToBuilding(model, addr_string)
        if not cut_succeed:
            cut_succeed, addr_string, last_string = self.__cutToCourtyardByChineseWords(model, addr_string)
        return cut_succeed, addr_string, last_string

    def __cutToBuilding(self, model: LAC, addr_string: str):
        """
        截取到楼栋的位置，楼栋之后的去掉
        :param model:
        :param addr_string:
        :return:
        """
        ret_succeed = False
        ret_last_string = ""

        # 交叉口这种先处理一下, 这步总是要处理
        _, addr_string = self.__cutToBuildingByJoinRePatterns(model,
                                                              addr_string,
                                                              self._conjunction_re_patterns_get_behind,
                                                              get_front=False)

        cut_succeed, addr_string, last_string = self.__cutToBuildingByChineseWords(addr_string)
        if cut_succeed:
            ret_succeed = cut_succeed
        if last_string is not None:
            ret_last_string = last_string + ret_last_string

        cut_succeed, addr_string = self.__cutToBuildingByJoinRePatterns(model,
                                                                        addr_string,
                                                                        self._conjunction_re_patterns_get_front,
                                                                        get_front=True)
        if cut_succeed:
            ret_succeed = cut_succeed

        cut_succeed, addr_string, last_string = self.__cutToBuildingByJoinSymbols(model, addr_string)
        if cut_succeed:
            ret_succeed = cut_succeed
        if last_string is not None:
            ret_last_string = last_string + ret_last_string

        return ret_succeed, addr_string, ret_last_string

    def __getCourtyardNumberByChineseWords(self, addr_string: str):
        """
        获取小区院落的数字
        :return:
        """
        number = -1
        # 1. 根据中文标识符判断
        for symbol in self._courtyard_chinese_words:
            re_strs = [
                r'第+\d+[A-Za-z]+区|\d+区|[A-Za-z]+区|[A-Za-z]+\d+区'.replace("区", symbol),  # 阿拉伯数字和字母的组合
                r'\d+[A-Za-z]+区|\d+区|[A-Za-z]+区|[A-Za-z]+\d+区'.replace("区", symbol),  # 阿拉伯数字和字母的组合
                r'(第+[一二三四五六七八九十〇壹贰叁肆伍陆柒捌玖拾百佰千仟万亿东南西北]+' + symbol + ')',  # 中文数字
                r'([一二三四五六七八九十〇壹贰叁肆伍陆柒捌玖拾百佰千仟万亿东南西北]+' + symbol + ')'  # 中文数字
            ]
            for re_str in re_strs:
                match = re.search(re_str, addr_string)
                if match:
                    result = match.group(0)
                    # 必须开头才行
                    if not addr_string.startswith(result):
                        continue
                    # print(result)
                    # 能找到就认为可以截取到楼栋
                    number = result[:0 - len(symbol)]
                    number = CommonTool.chinese_to_arabic(number)

        return number

    def __cutToCourtyardByChineseWords(self, model: LAC, addr_string: str, judge_in_before_building_words=True):
        """
        根据中文字符判断截取到小区院落的位置。
        :return:
        """
        addr_string_copy = copy.deepcopy(addr_string)
        cut_succeed = False
        find_result = None
        cut_list = []

        chinese_words = self._extract_again_chinese_words if not judge_in_before_building_words else self._courtyard_chinese_words

        # 1. 根据中文标识符判断
        for symbol in chinese_words:
            re_strs = [
                r'第+\d+[A-Za-z]+区|\d+区|[A-Za-z]+区|[A-Za-z]+\d+区'.replace("区", symbol),  # 阿拉伯数字和字母的组合
                r'\d+[A-Za-z]+区|\d+区|[A-Za-z]+区|[A-Za-z]+\d+区'.replace("区", symbol),  # 阿拉伯数字和字母的组合
                r'(第+[一二三四五六七八九十〇壹贰叁肆伍陆柒捌玖拾百佰千仟万亿东南西北]+' + symbol + ')',  # 中文数字
                r'([一二三四五六七八九十〇壹贰叁肆伍陆柒捌玖拾百佰千仟万亿东南西北]+' + symbol + ')'  # 中文数字
            ]
            for re_str in re_strs:
                match = re.search(re_str, addr_string)
                if match:
                    result = match.group(0)
                    # print(result)
                    find_result = result

                    is_in_words = False
                    # 当前找到的必须不在截取到楼栋后的字符串分词中才行
                    if judge_in_before_building_words:
                        cut_list = model.run(addr_string)[0]
                        for word in cut_list:
                            if result in word:
                                is_in_words = True
                                break

                    if not is_in_words:
                        # 能找到就认为可以截取到楼栋
                        number = result[:0 - len(symbol)]
                        number = CommonTool.chinese_to_arabic(number)
                        addr_split = addr_string.split(result)
                        # ls[1] 是楼栋之后的地址, 以后解析门牌会用到
                        addr_string = addr_split[0] + str(number)
                        cut_succeed = True

        # 獲取last_string
        last_string = None
        if find_result is not None:
            last_string = addr_string_copy.split(find_result)[1]

        self.__print("截取到楼栋: " + addr_string + ", last_string: " + str(last_string))
        return cut_succeed, addr_string, last_string

    def __cutToBuildingByChineseWords(self, addr_string: str):
        """
        根据中文字符判断截取到楼栋的位置，楼栋之后的去掉
        :param addr_string:
        :return:
        """
        addr_string_copy = copy.deepcopy(addr_string)
        cut_succeed = False
        find_result = None

        # 1. 根据中文标识符判断 (特殊标识)
        for symbol in self._special_building_chinese_words:
            find_idx = addr_string.find(symbol)
            if find_idx > 0:
                find_idx += len(symbol)
                return True, addr_string[:find_idx], addr_string[find_idx:]

        # 2. 根据中文标识符判断 (通用标识)
        for symbol in self._building_chinese_words:
            # if cut_succeed:
            #     break

            re_strs = [
                r'第+\d+[A-Za-z]+区|\d+区|[A-Za-z]+区|[A-Za-z]+\d+区'.replace("区", symbol),  # 阿拉伯数字和字母的组合
                r'\d+[A-Za-z]+区|\d+区|[A-Za-z]+区|[A-Za-z]+\d+区'.replace("区", symbol),  # 阿拉伯数字和字母的组合
                r'(第+[零一二三四五六七八九十〇壹贰叁肆伍陆柒捌玖拾百佰千仟万亿东南西北]+' + symbol + ')',  # 中文数字
                r'([零一二三四五六七八九十〇壹贰叁肆伍陆柒捌玖拾百佰千仟万亿东南西北]+' + symbol + ')'  # 中文数字
            ]
            for re_str in re_strs:
                match = re.search(re_str, addr_string)
                if match:
                    result = match.group(0)
                    find_result = result
                    # print(result)
                    # 能找到就认为可以截取到楼栋
                    number = result[:0 - len(symbol)]
                    number = CommonTool.chinese_to_arabic(number)
                    addr_split = addr_string.split(result)
                    # ls[1] 是楼栋之后的地址, 以后解析门牌会用到
                    addr_string = addr_split[0] + str(number)
                    cut_succeed = True
                    # break

        # 獲取last_string
        last_string = None
        if find_result is not None:
            last_string = addr_string_copy.split(find_result)[1]

        self.__print("截取到楼栋: " + addr_string + ", last_string: " + str(last_string))
        return cut_succeed, addr_string, last_string

    def __cutToBuildingByJoinSymbols(self, model: LAC, addr_string: str):
        """
        根据符号判断截取到楼栋的位置，楼栋之后的去掉
        :param model:
        :param addr_string:
        :return:
        """
        last_string = None
        cut_succeed = False
        # 根据符号判断
        for num_symbol in self._conjunction_symbols:
            addr_string = addr_string.replace(num_symbol, self._CONJUNCTION)

        if self._CONJUNCTION in addr_string:
            ls = addr_string.split(self._CONJUNCTION)
            while "" in ls: ls.remove("")
            addr_string = self._CONJUNCTION.join(ls)

            # ls[1:] 是楼栋之后的地址, 以后解析门牌会用到
            cut_addr_string = ls[0]
            # 獲取 last_string
            last_string = self._CONJUNCTION.join(ls[1:])

            cut_list = model.run(addr_string)
            cut_words = cut_list[0]
            if self._CONJUNCTION in cut_words:
                idx_s = cut_words.index(self._CONJUNCTION)
                word = cut_words[idx_s - 1]
                word_d = cut_words[idx_s + 1]

                # 獲取 last_string
                if idx_s + 1 <= len(cut_words):
                    last_string = self._CONJUNCTION.join(cut_words[idx_s + 1:])

                # 獲取加載的字典表
                model_dict = model.__getattribute__("model").custom.dictitem
                if word in model_dict.keys():
                    join_symbol = "号" if CommonTool.is_last_char_number(word) else ""
                    cut_addr_string = cut_addr_string + join_symbol + CommonTool.remove_chinese_chars(word_d)

            addr_string = cut_addr_string
            self.__print("截取到楼栋: " + addr_string + ", last_string: " + str(last_string))
            cut_succeed = True

        return cut_succeed, addr_string, last_string

    @staticmethod
    def __cutToBuildingByJoinRePatterns(model: LAC, addr_string: str, re_list, get_front=True):
        """
        根据正则判断截取到楼栋的位置，楼栋之后的去掉
        :param model:
        :param addr_string:
        :param re_list:
        :param get_front:
        :return:
        """
        # 獲取加載的字典表
        model_dict = model.__getattribute__("model").custom.dictitem

        cut_succeed = False
        # 根据正则判断
        for pattern in re_list:
            match = re.search(pattern, addr_string)
            if match:
                result = match.group(0)
                # 不在字典内就分割出去
                if result not in model_dict.keys():
                    addr_string = addr_string.split(result)[0] if get_front else addr_string.split(result)[1]

        return cut_succeed, addr_string

    def participleAndProcess(self, model: LAC, addr_string: str):
        """
        分词，并进行一些处理
        :param model:
        :param addr_string:
        :return:
        """
        cut_list = model.run(addr_string)
        # print(cut_list)
        self.__print("分词并处理前: " + str(cut_list))

        self.removeBigRegions(cut_list)
        self.__print("分词并处理（removeBigRegions）: " + str(cut_list))

        self.removeSingleChinese(cut_list)
        self.__print("分词并处理（removeUselessWords）: " + str(cut_list))

        self.removeUselessWordsByLac(model, cut_list)
        self.__print("分词并处理（removeUselessWordsByLac）: " + str(cut_list))

        self.preProcess(cut_list)
        self.__print("分词并处理（preProcess）: " + str(cut_list))

        return cut_list

    def removeBigRegions(self, cut_list: list):
        """
        去掉分词是省、市、区、街道的， 和 去掉词中的省、市、区、街道
        :param cut_list:
        :return:
        """
        cut_words = cut_list[0]
        lac_words = cut_list[1]

        # 去掉分词是省、市、区、街道的
        remove_idx_ls = []
        for i in range(len(cut_words)):
            j_word = cut_words[i]
            if j_word in self._provinces:
                remove_idx_ls.insert(0, i)
            elif j_word in self._cities:
                remove_idx_ls.insert(0, i)
            elif j_word in self._regions:
                remove_idx_ls.insert(0, i)
            elif j_word in self._streets:
                remove_idx_ls.insert(0, i)
        for idx in remove_idx_ls:
            cut_words.pop(idx)
            lac_words.pop(idx)

        # 去掉词中的省、市、区、街道
        for i in range(len(cut_words)):
            j_word = cut_words[i]
            for w in self._provinces:
                j_word = j_word.replace(w, "")
            for w in self._cities:
                j_word = j_word.replace(w, "")
            for w in self._regions:
                j_word = j_word.replace(w, "")
            for w in self._streets:
                j_word = j_word.replace(w, "")
            cut_words[i] = j_word

    @staticmethod
    def removeSingleChinese(cut_list):
        # c_word_list = []
        cut_words = cut_list[0]
        lac_words = cut_list[1]

        remove_idx_ls = []
        for i in range(len(cut_words)):
            word = cut_words[i]
            # 只有1个汉字
            if len(word) == 1 and CommonTool.has_chinese_characters(word):
                remove_idx_ls.insert(0, i)
                continue

            # for c_word in c_word_list:
            #     if word == c_word:
            #         remove_idx_ls.insert(0, i)

        for idx in remove_idx_ls:
            cut_words.pop(idx)
            lac_words.pop(idx)

    @staticmethod
    def removeUselessWordsByLac(model: LAC, cut_list):
        """
        根據詞性去掉無用詞，但該詞不能在字典中， 比如： 的
        :param model:
        :param cut_list:
        :return:
        """
        # 獲取加載的字典表
        model_dict = model.__getattribute__("model").custom.dictitem

        cut_words = cut_list[0]
        lac_words = cut_list[1]

        # 根據詞性判斷,  去掉助詞、名詞、動詞
        # useless_lac_list = ["u", "n", "v"]
        useless_lac_list = ["u"]

        remove_idx_ls = []
        for i in range(len(lac_words)):
            for useless_lac in useless_lac_list:
                word = cut_words[i]
                # 詞性吻合並不在字典中
                if lac_words[i] == useless_lac and word not in model_dict.keys():
                    remove_idx_ls.insert(0, i)

        for idx in remove_idx_ls:
            cut_words.pop(idx)
            lac_words.pop(idx)

    def preProcess(self, cut_list: list):
        cut_words = cut_list[0]
        lac_words = cut_list[1]

        # 1. 字母或数字开头的，只要字母或数字。  555号 => 555     1层 => 1
        for i in range(len(cut_words)):
            word = cut_words[i]

            # 類似 13号公元九里
            if CommonTool.count_chinese_characters(word) >= 3:
                continue

            # 忽略非数字或字母开头的，有可能分词的时候会把数字或字母和中文分到一起，比如：新之城全生活广场A
            if not CommonTool.has_chinese_characters(word[0]):
                matches = re.findall(r'[a-zA-Z0-9]+', word)
                if len(matches) > 0:
                    cut_words[i] = matches[0]

        # 2. 特殊字符去掉
        idx_list = []
        for i in range(len(cut_words)):
            if cut_words[i] in (self._conjunction_symbols + [self._CONJUNCTION]):
                idx_list.append(i)
        idx_list.reverse()
        for i in idx_list:
            cut_words.pop(i)
            lac_words.pop(i)

        # 3. 获取分词中的courtyard的数字
        for i in range(len(cut_words)):
            word = cut_words[i]
            number = self.__getCourtyardNumberByChineseWords(word)
            if number > 0:
                cut_words[i] = str(number)

    def __last_process(self, section_first, section_main):
        ls = [section_first, section_main]
        for sect in ls:
            for k, v in sect.items():
                for word in self._remove_last_words:
                    v = str(v)
                    if v.endswith(word):
                        sect[k] = v[:len(v) - 1]

    @staticmethod
    def findMainBodyIndexByDict(model: LAC, cut_words, only_in_dict_return=False):
        """
        根据字典表，截取到楼栋的位置。（分词在字典中，且该词的后一个词是数字。 only_in_dict_return=True则，分词在字典中就返回）
        """
        model_dict = model.__getattribute__("model").custom.dictitem
        find_idx = -1

        for i in range(len(cut_words) - 1, -1, -1):
            word = cut_words[i]
            if only_in_dict_return:
                if word in model_dict:
                    find_idx = i
                    break
            else:
                if i < len(cut_words) - 1:
                    word_behind = cut_words[i + 1]
                    if word in model_dict and CommonTool.is_first_char_number_or_letter(word_behind):
                        find_idx = i
                        break

        return find_idx

    @staticmethod
    def findMainBodyIndexByJudge(cut_words):
        find_idx = -1
        for i in range(len(cut_words) - 1, -1, -1):
            word = cut_words[i]

            # 已经是中文开头了，应该就是 building
            if CommonTool.is_first_char_chinese(word):
                find_idx = i
                # building = word
                break

            # 看数字或字母前面的是不是 building
            prev_word = cut_words[i - 1]
            if (CommonTool.is_first_char_number_or_letter(word) and
                    not CommonTool.is_first_char_number_or_letter(prev_word)):
                find_idx = i - 1
                # building = prev_word
                break
        return find_idx

    def findMainBodyIndex(self, model: LAC, addr_string: str, cut_list: list):
        """
        找到主体， 这步非常重要
        :param model:
        :param addr_string:
        :param cut_list:
        :return:
        """
        cut_words = cut_list[0]
        # lac_words = cut_list[1]

        idx = -1
        try:
            # 第1种的方式找主体
            idx = self.findMainBodyIndexByDict(model, cut_words, False)
            if idx != -1:
                return idx

            # 第2种方式找主体
            idx = self.findMainBodyIndexByJudge(cut_words)
            if idx != -1:
                return idx

            # 第3种方式找主体
            idx = self.findMainBodyIndexByDict(model, cut_words, True)
            if idx != -1:
                return idx
        finally:
            self.__print("找到主体: " + str(cut_words[idx] if idx != -1 else None))

        return idx

    def findMainBodyIndexReverse(self, model: LAC, addr_string: str, cut_list: list):
        """
        找到主体， 这步非常重要
        :param model:
        :param addr_string:
        :param cut_list:
        :return:
        """
        cut_words = cut_list[0]
        # lac_words = cut_list[1]

        idx = -1
        try:
            # 第1种的方式找主体
            idx = self.findMainBodyIndexByDict(model, cut_words, True)
            if idx != -1:
                return idx

            # 第2种方式找主体
            idx = self.findMainBodyIndexByJudge(cut_words)
            if idx != -1:
                return idx

            # 第3种方式找主体
            idx = self.findMainBodyIndexByDict(model, cut_words, False)
            if idx != -1:
                return idx
        finally:
            self.__print("找到主体: " + str(cut_words[idx] if idx != -1 else None))

        return idx

    def __process_last_string(self, model: LAC, address_section_last, last_string):
        last_string_copy = copy.deepcopy(last_string)
        if last_string is not None and last_string != "":
            for num_symbol in self._conjunction_symbols:
                last_string = last_string.replace(num_symbol, self._CONJUNCTION)
            last_string = CommonTool.replace_chinese_to_symbol(last_string, self._CONJUNCTION)
            last_cut_list = last_string.split(self._CONJUNCTION)
            while "" in last_cut_list:
                last_cut_list.remove("")
            if len(last_cut_list) > self._LAST_MAX_LEN:
                last_cut_list = last_cut_list[:self._LAST_MAX_LEN]
            for i in range(len(last_cut_list)):
                field_name = "last_" + str(i + 1)
                address_section_last[field_name] = last_cut_list[i]

            # 后面部分可能有中文， 这个中文如果在字典表中，且还有位置放，则保留下来
            if self._LAST_MAX_LEN > len(last_cut_list):
                # 獲取加載的字典表
                model_dict = model.__getattribute__("model").custom.dictitem

                idx = len(last_cut_list)
                cut_list = model.run(last_string_copy)[0]
                for word in cut_list:
                    if word in model_dict:
                        field_name = "last_" + str(idx + 1)
                        address_section_last[field_name] = word
                        idx += 1
                    if idx >= self._LAST_MAX_LEN:
                        break

    def create_sections(self, cut_list, body_idx, model: LAC, last_string):
        cut_words = cut_list[0]

        # 主体前的部分
        address_section_first = {}
        # 主体
        address_section_main = {}
        # 主体后 到 楼栋之间的部分
        address_section_mid = {}
        # last部分
        address_section_last = {}

        # 赋值
        idx = 1
        for i in range(body_idx):
            field_name = "fir_" + str(idx)
            address_section_first[field_name] = cut_words[i]
            idx += 1

        address_section_main["f_main"] = cut_words[body_idx]

        idx = 1
        for i in range(body_idx + 1, len(cut_words)):
            field_name = "mid_" + str(idx)
            address_section_mid[field_name] = cut_words[i]
            idx += 1

        address_section_build_number = {es_schema_field_building_number: -99999}
        if len(address_section_mid) > 0:
            try:
                # 如果只有1个值，就是 mid_1 ,  如果有2个值，就是 mid_2
                key_name = "mid_" + str(len(address_section_mid))
                building_num = address_section_mid[key_name]
                building_num = CommonTool.convert_building_num(building_num)
                address_section_build_number[es_schema_field_building_number] = building_num
            except:
                pass

        # last_string 部分分词
        self.__process_last_string(model, address_section_last, last_string)

        if (len(address_section_first) > len(es_schema_fields_fir)
                or len(address_section_main) > len(es_schema_fields_main)
                or len(address_section_mid) > len(es_schema_fields_mid)
                or len(address_section_last) > len(es_schema_fields_last)):
            return False, [None, None, None, None, None]

        # 最后的处理
        self.__last_process(address_section_first, address_section_main)

        self.__print("=== sections ===")
        self.__print(str(address_section_first) + str(address_section_main) + str(address_section_mid) +
                     str(address_section_last) + str(address_section_build_number))
        return True, [address_section_first, address_section_main, address_section_mid, address_section_last,
                      address_section_build_number]
