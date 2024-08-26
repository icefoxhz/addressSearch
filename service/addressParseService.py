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

        "common_symbol": "_common_symbol",
        "special_building_chinese_words": "_special_building_chinese_words",
        "special_words": "_special_words",
        "building_chinese_words": "_building_chinese_words",
        "courtyard_chinese_words": "_courtyard_chinese_words",
        "extract_again_chinese_words": "_extract_again_chinese_words",
        "remove_last_words": "_remove_last_words",
        "CONJUNCTION": "_CONJUNCTION",
        "conjunction_symbols": "_conjunction_symbols",
        "conjunction_re_patterns_get_front": "_conjunction_re_patterns_get_front",
        "conjunction_re_patterns_get_behind": "_conjunction_re_patterns_get_behind",
        "extra_symbols": "_extra_symbols"
    })
    def __init__(self):
        self._print_debug = False

        # --------------------------------
        self._LAST_MAX_LEN = 6
        self._provinces = None
        self._cities = None
        self._regions = None
        self._streets = None

        self._common_symbol = None
        self._special_building_chinese_words = None
        self._special_words = None
        self._building_chinese_words = None
        self._courtyard_chinese_words = None
        self._extract_again_chinese_words = None
        self._remove_last_words = None
        self._CONJUNCTION = None
        self._conjunction_symbols = None
        self._conjunction_re_patterns_get_front = None
        self._conjunction_re_patterns_get_behind = None
        self._extra_symbols = None

    def __print(self, msg):
        if self._print_debug:
            print(msg)

    @staticmethod
    def __fail_ret():
        return False, None, None, None, None, None, None, None

    def run(self, model: LAC, addr_string: str, is_participle_continue=False):
        """
        :param model:
        :param addr_string:
        :param is_participle_continue:
        :return:
        """
        if not hasattr(self.__local_obj, "model_dict"):
            self.__local_obj.model_dict = model.__getattribute__("model").custom.dictitem

        try:
            addr_string = self.prepareAddress(addr_string)

            if not self.acceptAddress(addr_string):
                return self.__fail_ret()

            addr_string = self.removeStartWordsIfNecessary(model, addr_string)
            addr_string = self.removeExtra(addr_string)
            self.__print("移除无用信息: " + str(addr_string))

            _, addr_string, last_string = self.cutAddress(model, addr_string)

            if addr_string is None or addr_string == "":
                return self.__fail_ret()

            # 分詞並處理
            cut_list = self.participleAndProcess(model, addr_string)
            # 每个分词再次判断处理
            if is_participle_continue:
                cut_list = self.participleContinue(model, cut_list)
                self.__print("二次分词结果: " + str(cut_list))

            if cut_list is None:
                return self.__fail_ret()

            # 找主体
            body_idx = self.findMainBodyIndex(model, addr_string, cut_list)
            if body_idx == -1:
                return self.__fail_ret()
            # 处理并生成 sections
            succeed, [address_section_first, address_section_main, address_section_mid, address_section_last,
                      address_section_build_number] = self.create_sections(
                cut_list, body_idx, model, last_string)

            #  -------------
            if not succeed:
                # 找主体
                body_idx = self.findMainBodyIndexReverse(model, addr_string, cut_list)
                if body_idx == -1:
                    return self.__fail_ret()
                # 处理并生成 sections
                succeed, [address_section_first, address_section_main, address_section_mid, address_section_last,
                          address_section_build_number] = self.create_sections(
                    cut_list, body_idx, model, last_string)

            region = self.__local_obj.region if hasattr(self.__local_obj, "region") else None
            street = self.__local_obj.street if hasattr(self.__local_obj, "street") else None

            return succeed, region, street, address_section_first, address_section_main, address_section_mid, address_section_last, address_section_build_number
        except Exception as e:
            self.__print(str(e))
        finally:
            self.__local_obj.region = None
            self.__local_obj.street = None
        return self.__fail_ret()

    def participleContinue(self, model: LAC, cut_list):
        word_list = cut_list[0]

        word_list_ret = copy.deepcopy(word_list)

        # 獲取加載的字典表
        model_dict = self.__local_obj.model_dict
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

    @staticmethod
    def acceptAddress(addr_string: str):
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

    @staticmethod
    def prepareAddress(addr_string: str):
        """
        预处理
        """
        # 全角转半角
        addr_string = CommonTool.full_to_half(addr_string)
        # 去掉空格
        addr_string = CommonTool.remove_spaces(addr_string)
        return addr_string

    def removeStartWordsIfNecessary(self, model: LAC, addr_string: str):
        """
        移除 省、市、区、街道
        :param model:
        :param addr_string:
        :return:
        """
        # 如果第1个分词就在字典表里，说明不需要处理这一步了。 因为 省、市、区、街道 不放在字典中
        model_dict = self.__local_obj.model_dict
        cut_list = model.run(addr_string)[0]
        first_word = cut_list[0]
        if first_word in model_dict.keys():
            return addr_string

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
            ret_last_string = last_string + ("" if ret_last_string is None else (self._CONJUNCTION + ret_last_string))

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
            if find_idx >= 0:
                find_idx += len(symbol)
                return True, addr_string[:find_idx], addr_string[find_idx:]

        # 2. 根据中文标识符判断 (通用标识)
        for symbol in self._building_chinese_words:
            # if cut_succeed:
            #     break

            # 特殊判斷
            is_go = False
            for w in self._special_words:
                if w.find(symbol) >= 0 and addr_string.find(w) >= 0:
                    is_go = True
            if is_go:
                continue

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
                model_dict = self.__local_obj.model_dict
                if word in model_dict.keys():
                    join_symbol = "号" if CommonTool.is_last_char_number(word) else ""
                    cut_addr_string = cut_addr_string + join_symbol + CommonTool.remove_chinese_chars(word_d)

            addr_string = cut_addr_string
            self.__print("截取到楼栋: " + addr_string + ", last_string: " + str(last_string))
            cut_succeed = True

        return cut_succeed, addr_string, last_string

    def __cutToBuildingByJoinRePatterns(self, model: LAC, addr_string: str, re_list, get_front=True):
        """
        根据正则判断截取到楼栋的位置，楼栋之后的去掉
        :param model:
        :param addr_string:
        :param re_list:
        :param get_front:
        :return:
        """
        # 獲取加載的字典表
        model_dict = self.__local_obj.model_dict

        # # 帶方向的名稱判斷。  无锡市江阴市周庄镇长南村培元路107号北3米平房 ， 這裡有個“长南村”，有個“南”字， 就把它分成： 无锡市江阴市周庄镇 和 培元路107号北3米平房，再分別判斷
        # for name in self._dir_rel_names:
        #     idx = addr_string.find(name)
        #     if idx >= 0:
        #         for d in self._dirs:
        #             addr_string_tmp = addr_string[:idx]
        #             if addr_string_tmp.find(d) >= 0:
        #                 find_addr_string = addr_string_tmp
        #                 break
        #             addr_string_tmp = addr_string[idx + len(name):]
        #             if addr_string_tmp.find(d) >= 0:
        #                 find_addr_string = addr_string_tmp
        #                 break
        #         break

        cut_succeed = False

        # 根据正则判断。 找最短的，因為比如：无锡市江阴市周庄镇长南村培元路107号北3米平房， 长南村也有南這個字
        result = None
        pattern_find = None
        for pattern in re_list:
            match = re.search(pattern, addr_string)
            if match:
                result_tmp = match.group(0)
                if result is None:
                    result = result_tmp
                    pattern_find = pattern
                else:
                    if len(result_tmp) < len(result):
                        result = result_tmp
                        pattern_find = pattern

        # 找到了
        if result is not None:
            cut_words_origin = model.run(addr_string)[0]
            while True:
                # 分词后看第1个词, 第1个词是涉及方向的，但是可能在字典中
                cut_words = model.run(result)[0]
                cut_first_word = cut_words[0]
                if cut_first_word not in model_dict.keys():
                    # 可能中间被截断，要获取整个词，判断是否在字典中。如果在就不要操作了。
                    # 比如: 东贤中路67号丁蜀中心幼儿园东行50米 , 会把整个都找到，但是"东贤中路"是字典
                    # 比如: 阳泉西路188号红星美凯龙3层西北方向70米， 会找到:西路188号红星美凯龙3层西北方向70米，但是"阳泉西路"是字典
                    for word in cut_words_origin:
                        # 找到截取对应的分词的那个词，并判断是否在字典中
                        if word.endswith(cut_first_word) and word not in model_dict.keys():
                            break

                s = "".join(cut_words[1:])
                match = re.search(pattern_find, s)
                if not match:
                    break
                result = match.group(0)

            # 不在字典内就分割出去
            if result not in model_dict.keys():
                cut_succeed = True
                addr_string = addr_string.split(result)[0] if get_front else addr_string.split(result)[1]
            self.__print("截取到楼栋: " + addr_string)
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

        self.removeBigRegions(model, cut_list)
        self.__print("分词并处理（removeBigRegions）: " + str(cut_list))

        self.removeSingleChinese(cut_list)
        self.__print("分词并处理（removeUselessWords）: " + str(cut_list))

        self.removeUselessWordsByLac(model, cut_list)
        self.__print("分词并处理（removeUselessWordsByLac）: " + str(cut_list))

        self.preProcess(cut_list)
        self.__print("分词并处理（preProcess）: " + str(cut_list))

        return cut_list

    def removeBigRegions(self, model: LAC, cut_list: list):
        """
        去掉分词是省、市、区、街道的， 和 去掉词中的省、市、区、街道
        :param model:
        :param cut_list:
        :return:
        """
        cut_words = cut_list[0]
        lac_words = cut_list[1]

        # 獲取加載的字典表
        model_dict = self.__local_obj.model_dict

        # 去掉分词是省、市、区、街道的
        remove_idx_ls = []
        for i in range(len(cut_words)):
            j_word = cut_words[i]
            if j_word in self._provinces and j_word not in model_dict.keys():
                remove_idx_ls.insert(0, i)
            elif j_word in self._cities and j_word not in model_dict.keys():
                remove_idx_ls.insert(0, i)
            elif j_word in self._regions and j_word not in model_dict.keys():
                remove_idx_ls.insert(0, i)
            elif j_word in self._streets and j_word not in model_dict.keys():
                remove_idx_ls.insert(0, i)
        for idx in remove_idx_ls:
            cut_words.pop(idx)
            lac_words.pop(idx)

        # 去掉词中的省、市、区、街道
        for i in range(len(cut_words)):
            j_word = cut_words[i]
            if j_word in model_dict.keys():
                continue
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

    def removeUselessWordsByLac(self, model: LAC, cut_list):
        """
        根據詞性去掉無用詞，但該詞不能在字典中， 比如： 的
        :param model:
        :param cut_list:
        :return:
        """
        # 獲取加載的字典表
        model_dict = self.__local_obj.model_dict

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

    def findMainBodyIndexByDict(self, model: LAC, cut_words, only_in_dict_return=False):
        """
        根据字典表，截取到楼栋的位置。（分词在字典中，且该词的后一个词是数字。 only_in_dict_return=True则，分词在字典中就返回）
        """
        model_dict = self.__local_obj.model_dict
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
                model_dict = self.__local_obj.model_dict

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

        address_section_build_number = {es_schema_field_building_number: 0}
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
            self.__print(f"解析出的长度超限制, cut_list = {cut_list}")
            return False, [None, None, None, None, None]

        # 最后的处理
        self.__last_process(address_section_first, address_section_main)

        self.__print("=== sections ===")
        self.__print(str(address_section_first) + str(address_section_main) + str(address_section_mid) +
                     str(address_section_last) + str(address_section_build_number))
        return True, [address_section_first, address_section_main, address_section_mid, address_section_last,
                      address_section_build_number]
