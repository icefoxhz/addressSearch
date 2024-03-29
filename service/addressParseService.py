import os.path
import re
from LAC import LAC
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value

from addressSearch.utils.commonTool import CommonTool


@Component
class AddressParseService:
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
        self._provinces = None
        self._cities = None
        self._regions = None
        self._streets = None

        # --------------------------------
        self._building_chinese_words = ["幢", "栋", "区", "号楼", "楼",
                                        "号厂区", "号东厂区", "号南厂区", "号西厂区", "号北厂区",
                                        ]
        self._courtyard_chinese_words = ["期"]

        # --------------------------------
        self._JOIN_SYMBOL = "-"
        self._join_symbols = ["－", "—", "～", "~", "#", "/", "、", ",", "，", " ",
                              "内东", "内南", "内西", "内北",
                              "东边", "南边", "西边", "北边",
                              "东面", "南面", "西面", "北面",
                              "东侧", "南侧", "西侧", "北侧",
                              ]

        self._join_re_patterns_get_front = [r'向(.*?)米',
                                            r'东(.*?)米',
                                            r'西(.*?)米',
                                            r'南(.*?)米',
                                            r'北(.*?)米',
                                            ]

        self._join_re_patterns_get_behind = [r'路与(.*?)路交叉口',
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

    def run(self, model: LAC, addr_string: str):
        """
        :param model:
        :param addr_string:
        :return:
        """
        try:
            if not self.acceptAddress(addr_string):
                return False, None, None, None

            addr_string = self.removeStartWordsIfNecessary(addr_string)
            addr_string = self.removeExtra(addr_string)
            self.__print("移除无用信息: " + addr_string)

            _, addr_string = self.cutAddress(model, addr_string)

            # 分詞並處理
            cut_list = self.participleAndProcess(model, addr_string)

            # 找主体
            is_find_body, body_idx = self.findMainBodyIndex(addr_string, cut_list)
            if not is_find_body:
                return False, None, None, None

            # 处理并生成 sections
            address_section_first, address_section_main, address_section_mid = self.create_sections(cut_list, body_idx)

            return True, address_section_first, address_section_main, address_section_mid
        except Exception as e:
            self.__print(str(e))
            return False, None, None, None

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
        remove_list = [self._provinces, self._cities, self._regions, self._streets]
        for remove_words in remove_list:
            for remove_word in remove_words:
                if addr_string.startswith(remove_word):
                    addr_string = addr_string.replace(remove_word, "", 1)

        return addr_string

    def removeExtra(self, addr_string: str):
        """
         去掉 () 相关的这种额外的东西
        :param addr_string:
        :return:
        """
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

        return addr_string

    def cutAddress(self, model: LAC, addr_string: str):
        """
        先截取楼栋， 如果截取不到，就尝试截取到小区院落
        :param model:
        :param addr_string:
        :return:
        """
        cut_succeed, addr_string = self.__cutToBuilding(model, addr_string)
        if not cut_succeed:
            cut_succeed, addr_string = self.__cutToCourtyardByChineseWords(addr_string)
        return cut_succeed, addr_string

    def __cutToBuilding(self, model: LAC, addr_string: str):
        """
        截取到楼栋的位置，楼栋之后的去掉
        :param model:
        :param addr_string:
        :return:
        """

        # 交叉口这种先处理一下, 这步总是要处理
        _, addr_string = self.__cutToBuildingByJoinRePatterns(model,
                                                              addr_string,
                                                              self._join_re_patterns_get_behind,
                                                              get_front=False)

        cut_succeed, addr_string = self.__cutToBuildingByChineseWords(addr_string)

        if not cut_succeed:
            cut_succeed, addr_string = self.__cutToBuildingByJoinSymbols(addr_string)

        if not cut_succeed:
            cut_succeed, addr_string = self.__cutToBuildingByJoinRePatterns(model,
                                                                            addr_string,
                                                                            self._join_re_patterns_get_front,
                                                                            get_front=True)

        return cut_succeed, addr_string

    def __cutToCourtyardByChineseWords(self, addr_string: str):
        """
        根据中文字符判断截取到小区院落的位置。
        :return:
        """
        cut_succeed = False

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
                    # print(result)
                    # 能找到就认为可以截取到楼栋
                    number = result[:0 - len(symbol)]
                    number = CommonTool.chinese_to_arabic(number)
                    addr_split = addr_string.split(result)
                    # ls[1] 是楼栋之后的地址, 以后解析门牌会用到
                    addr_string = addr_split[0] + str(number)

        self.__print("截取到小区院落: " + addr_string)
        return cut_succeed, addr_string

    def __cutToBuildingByChineseWords(self, addr_string: str):
        """
        根据中文字符判断截取到楼栋的位置，楼栋之后的去掉
        :param addr_string:
        :return:
        """
        cut_succeed = False

        # 1. 根据中文标识符判断
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
                    # print(result)
                    # 能找到就认为可以截取到楼栋
                    number = result[:0 - len(symbol)]
                    number = CommonTool.chinese_to_arabic(number)
                    addr_split = addr_string.split(result)
                    # ls[1] 是楼栋之后的地址, 以后解析门牌会用到
                    addr_string = addr_split[0] + str(number)

                    # cut_succeed = True
                    # break
        self.__print("截取到楼栋: " + addr_string)
        return cut_succeed, addr_string

    def __cutToBuildingByJoinSymbols(self, addr_string: str):
        """
        根据符号判断截取到楼栋的位置，楼栋之后的去掉
        :param addr_string:
        :return:
        """
        cut_succeed = False
        # 根据符号判断
        for num_symbol in self._join_symbols:
            addr_string = addr_string.replace(num_symbol, self._JOIN_SYMBOL)

        if self._JOIN_SYMBOL in addr_string:
            ls = addr_string.split(self._JOIN_SYMBOL)
            # ls[1:] 是楼栋之后的地址, 以后解析门牌会用到
            addr_string = ls[0]
            self.__print("截取到楼栋: " + addr_string)
            cut_succeed = True

        return cut_succeed, addr_string

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
        去掉分词中的 省、市、区、街道
        :param cut_list:
        :return:
        """
        cut_words = cut_list[0]
        lac_words = cut_list[1]

        remove_idx_ls = []
        for i in range(len(cut_words)):
            if cut_words[i] in self._provinces:
                remove_idx_ls.insert(0, i)
            elif cut_words[i] in self._cities:
                remove_idx_ls.insert(0, i)
            elif cut_words[i] in self._regions:
                remove_idx_ls.insert(0, i)
            elif cut_words[i] in self._streets:
                remove_idx_ls.insert(0, i)

        for idx in remove_idx_ls:
            cut_words.pop(idx)
            lac_words.pop(idx)

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
        useless_lac_list = ["u", "n", "v"]

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

    @staticmethod
    def preProcess(cut_list: list):
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

        # 这个判断不准确，有时候准确的词也会被处理掉
        # # 2. 判断是否还有词性是 LOC、ORG的存在. 如果没有说明无用了
        # if "LOC" not in lac_words and "ORG" not in lac_words:
        #     cut_words.clear()
        #     lac_words.clear()

    def findMainBodyIndex(self, addr_string: str, cut_list: list):
        """
        找到主体， 这步非常重要
        :param addr_string:
        :param cut_list:
        :return:
        """
        cut_words = cut_list[0]
        lac_words = cut_list[1]

        idx = -1
        # building = None
        try:
            # 1. 第一种方式找主体
            for i in range(len(cut_words) - 1, -1, -1):
                word = cut_words[i]

                # 已经是中文开头了，应该就是 building
                if CommonTool.is_first_char_chinese(word):
                    idx = i
                    # building = word
                    break

                # 看数字或字母前面的是不是 building
                prev_word = cut_words[i - 1]
                if CommonTool.is_first_char_number(word) and not CommonTool.is_first_char_number(prev_word):
                    idx = i - 1
                    # building = prev_word
                    break
        except:
            pass

        self.__print("找到主体: " + str(cut_words[idx] if idx != -1 else None))

        return idx != -1, idx

    def create_sections(self, cut_list, body_idx):
        cut_words = cut_list[0]

        # 主体前的部分
        address_section_first = {}
        # 主体
        address_section_main = {}
        # 主体后 到 楼栋之间的部分
        address_section_mid = {}

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

        self.__print("=== sections ===")
        self.__print(str(address_section_first) + str(address_section_main) + str(address_section_mid))
        return address_section_first, address_section_main, address_section_mid
