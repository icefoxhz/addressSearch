# -*- coding: utf-8 -*-
import copy
import re

from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component, Scope
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value


def isNumLetters(s):
    if s is None or s == "":
        return False

    if len(s) < 2:
        if re.match('^[0-9a-zA-Z]+$', s[0]):
            return True
        else:
            return False
    else:
        if re.match('^[0-9a-zA-Z]+$', s[0]) and re.match('^[0-9a-zA-Z_-]+$', s[1:]):
            return True
        else:
            return False


@Component
@Scope("prototype")
class AddressParser:
    """
    流程图： https://processon.com/diagraming/616f6ebee401fd16dd8ffdd6
    """

    @Value(
        {
            "project.print_debug": "_print_debug",
            "project.region.judge_dict_list": "_judge_dict_list",
            "project.region.province": "_province",
            "project.region.city": "_city",
            "JUDGE_HAS_WORDS": "_judge_has_words",
            "startChinese": "_start_chinese",
            "JUDGE_HAS_WORDS_MUST_NUMBER": "_judge_has_words_must_number",
            "NUM_JOIN_SYMBOLS": "_num_join_symbols",
            "JUDGE_HAS_WORDS_NOT_NUMBER": "_judge_has_words_not_number",
            "JUDGE_NOT_HAS_WORDS": "_judge_not_has_words",
            "MULTI_JOIN_SYMBOLS": "_multi_join_symbols",
            "WASH_UNIT_WORDS": "_wash_unit_words",
            "DIRECT_WORDS": "_direct_words",
            "DIRECT_WORDS_NOT_DROP": "_direct_words_not_drop",
            "STOP_WORD_LIST": "_stop_word_list",
            "JUDGE_JOIN_WORDS": "_judge_join_words",
        }
    )
    def __init__(self):
        self._print_debug = False
        self._judge_has_words = None
        self._province = None
        self._city = None
        self._judge_dict_list = None
        self._start_chinese = None
        self._judge_has_words_must_number = None
        self._judge_not_has_words = None
        self._num_join_symbols = None
        self._judge_has_words_not_number = None
        self._multi_join_symbols = None
        self._wash_unit_words = None
        self._direct_words = None
        self._direct_words_not_drop = None
        self._stop_word_list = None
        self._judge_join_words = None

        self._x = None
        self._y = None
        # 分词列表
        self._wordList = None
        # 词性列表
        self._wordLacList = None
        # 前半段
        self._frontWordList = None
        self._frontWordLacList = None
        # 后半段
        self._backWordList = None
        self._backWordLacList = None
        # 室 号 列表， 可能出现 、 分割的多个室。 这种要分成多条记录，一个室1条记录
        self._roomList = []
        # province - address_number  按顺序
        self._frontSectionList = ["region", "street", "community",
                                  "natural_village", "road", "courtyard", "group_number", "address_number"]
        # 用于前半部分判断查找。 区 不要参与
        self._frontFindSectionList = ["street", "community",
                                      "natural_village", "road", "courtyard", "group_number", "address_number"]
        # building_name - room
        self._backSectionList = ["building_name", "building_site", "unit", "floor", "room", "company"]

        #  去掉 省 市 区 之后的section中还出现了 省 市 区 关键字的值
        self._washSectionList = ["street", "community", "natural_village", "road", "courtyard", "building_name",
                                 "building_site", "company"]

        # 判断要和 building_site 合并的section
        self._joinSectionList = ["building_name", "courtyard", "road", "natural_village", "community", "street"]

        # 数字section
        self._numSectionList = ["group_number", "address_number", "building_site", "unit", "floor", "room"]
        self._numSections = []

        # 可能是多个室/门牌， 所以最终结果要以列表返回
        self._resultList = []
        self._result = None

    def _after_init(self):
        for section in self._numSectionList:
            self._numSections = self._numSections + self._judge_has_words[section]
        self._numSections = list(set(self._numSections))

        # 单个的结果
        self._result = {
            "fullname": None,
            "province": self._province,
            "city":  self._city,
            "region": None,
            "street": None,
            "community": None,
            "natural_village": None,
            "road": None,
            "courtyard": None,
            "group_number": None,
            "address_number": None,
            "building_name": None,
            "building_site": None,
            "unit": None,
            "floor": None,
            "room": None,
            "company": None,
            "x": None,
            "y": None,
        }

    def set_params(self, x, y, wordList, wordLacList):
        self._x = x
        self._y = y
        self._wordList = wordList
        self._wordLacList = wordLacList

    def _removeWord(self, idx):
        self._wordList.pop(idx)
        self._wordLacList.pop(idx)

    @staticmethod
    def _judgeByKeyWord(word, keyWordList, notKeyWordList):
        """
        # 通过关键字来判断。 比如 省、市、区 ...
        :param word:
        :param keyWordList:
        :return:
        """
        for jWord in keyWordList:
            # 是否关键字结尾
            if str(word).endswith(jWord):
                # 是否在以不允许的关键字结尾
                for jWord2 in notKeyWordList:
                    if str(word).endswith(jWord2):
                        return None
                return word
        return None

    @staticmethod
    def _judgeByDict(word, jDictList):
        """
        通过字典来判断。 比如  REGIONS = ["新吴", "锡山"]
        :param word:
        :param jDictList:
        :return:
        """
        for jWord in jDictList:
            if word.find(jWord) >= 0:
                return word
        return None

    def _parseInit(self):
        """
        开始解析之前的操作
        1. 把省、市 去掉， 因为是定好的
        2. 合并词性顺序是 'LOC', 'p', 'LOC', 'n' 这种的。 比如：'静慧寺东路', '与', '具区路', '交界处'
        :return:
        """
        # 开始2个词判断是否 省、市。如果是，要去除
        try:
            for i in range(1, -1, -1):
                word = self._wordList[i]
                if word in self._judge_dict_list["province"] or word in self._judge_dict_list["city"]:
                    self._wordList.pop(i)
                    self._wordLacList.pop(i)
        except:
            pass

        # 合并词性顺序是 'LOC', 'p', 'LOC', 'n' 这种的。 比如：'静慧寺东路', '与', '具区路', '交界处'
        # 合并词性顺序是 'm', 'w', 'LOC' 这种的。 比如：'3301', '-', '1室'

        # 为了下标不越界, 添加3个空值
        self._wordLacList.append("")
        self._wordLacList.append("")
        self._wordLacList.append("")
        self._wordList.append("")
        self._wordList.append("")
        self._wordList.append("")

        findPos = -1
        for i in range(len(self._wordLacList)):
            try:
                wordLac1 = self._wordLacList[i]
                wordLac2 = self._wordLacList[i + 1]
                wordLac3 = self._wordLacList[i + 2]
                wordLac4 = self._wordLacList[i + 3]
                if wordLac1 == "LOC" and wordLac2 == "p" and wordLac3 == "LOC" and wordLac4 == "n":
                    findPos = i
                    break

                if wordLac1 == "m" and wordLac2 == "w" and wordLac3 == "LOC":
                    findPos = i
                    break
            except:
                return

        if findPos >= 0:
            self._wordList[findPos] += self._wordList[findPos + 1]
            self._wordList[findPos] += self._wordList[findPos + 2]
            self._wordList[findPos] += self._wordList[findPos + 3]

            self._wordList.pop(findPos + 1)
            self._wordList.pop(findPos + 1)
            self._wordList.pop(findPos + 1)

            self._wordLacList.pop(findPos + 1)
            self._wordLacList.pop(findPos + 1)
            self._wordLacList.pop(findPos + 1)

    def _replaceAlias(self):
        """
        替换别名
        :return:
        """
        pass

    def _doWashWordList(self):
        self.washWordList(self._wordList, self._wordLacList)

    def _parseFront(self):
        """
        解析前半段。 从第1个word开始解析
        :return:
        """
        # self.washWordList(self._frontWordList, self._frontWordLacList)

        # 根据关键字匹配到的最后的section
        sectionFindList = []

        # 从第1个word开始解析
        # ct = 0
        for i in range(len(self._frontWordList)):
            try:
                word = self._frontWordList[i]
                # 数字和前面的词合并过的标记 =
                wordTemp = None
                if "=" in word:
                    wordTemp = word
                    split = word.split("=")
                    word = split[0]

                # wordLac = self._frontWordLacList[i]
                # print("当前解析词 = " + word)
                parseResult = None
                sectionNameTemp = None
                for j in range(len(self._frontSectionList)):
                    sectionName = self._frontSectionList[j]
                    parseResult = self.__simpleParser(word, sectionName, self._result)  # 根据关键字匹配成功

                    if parseResult is not None:
                        # 数字和前面的词合并过
                        if wordTemp is not None:
                            sectionNameTemp = sectionName
                            break

                        # ct += 1
                        # 当前还没有值，放到当前section
                        if self._result[sectionName] is None:
                            self._result[sectionName] = parseResult
                            self._reParseNumWords(sectionName, parseResult)
                        # 当前已经有值了，一直往下找，找到没值的section放
                        else:
                            idx = self._findNearestNoValueSectionName(self._frontFindSectionList)
                            sectionName = self._frontFindSectionList[idx]
                            self._result[sectionName] = parseResult
                            self._reParseNumWords(sectionName, parseResult)
                        sectionFindList.append(sectionName)
                        break
                    # 第1个词就根据关键字匹配不到，直接放到 street位置
                    elif parseResult is None and i == 0:
                        parseResult = word if wordTemp is None else wordTemp
                        self._result["street"] = parseResult

                # 数字和前面的词合并过
                if wordTemp is not None and sectionNameTemp is not None:
                    self._result[sectionNameTemp] = wordTemp
                    self._backWordList = self._frontWordList[i + 1:] + self._backWordList
                    self._backWordLacList = self._frontWordLacList[i + 1:] + self._backWordLacList
                    # frontWordList 已经无所谓了。这里不做操作也无所谓
                    self._frontWordList = self._frontWordList[: i + 1]
                    self._frontWordLacList = self._frontWordLacList[: i + 1]
                    return

                # 找不到匹配的，还原
                if wordTemp is not None and sectionNameTemp is None:
                    word = wordTemp

                # 查找后半段能否匹配到关键字, 如果能匹配到，说明前半段已经匹配完了，直接放入后半段，退出
                if parseResult is None:
                    for sectionName in self._backSectionList:
                        parseResult = self.__simpleParser(word, sectionName, self._result)
                        if parseResult is not None:
                            self._backWordList = self._frontWordList[i:] + self._backWordList
                            self._backWordLacList = self._frontWordLacList[i:] + self._backWordLacList
                            return

                #  前后段都解析不到，再次判断
                if parseResult is None:
                    # 解析不出, 找最近有值的sectionName
                    # 比如   无锡市滨湖区胡埭镇夏渎村石漕头4号南100米无锡市谊诚焊割设备有限公司  =>  石漕头
                    # 涉及后半段解析，必须找最近有值的sectionName,必须找最近有值的sectionName,必须找最近有值的sectionName
                    nearestHasValueIndex = self._findLastHasValueSectionName(self._frontFindSectionList)
                    # 找的到最近有值的sectionName
                    if nearestHasValueIndex >= 0:
                        # 如果是数字。 因为做完会删除，所有下标为0
                        if self._frontWordLacList[0] == "m":
                            # ct += 1
                            self._result["address_number"] = word
                            parseResult = word
                        else:
                            #  最后根据关键字匹配到的位置不是最后一个
                            if len(sectionFindList) > 0:
                                sectionFind = sectionFindList[-1]
                                if sectionFind in self._frontFindSectionList and \
                                        self._frontFindSectionList[-1] != sectionFind:
                                    # 找最后没值的，放上去
                                    idx = self._findLastNoValueSectionName(self._frontFindSectionList)
                                    if idx > 0:
                                        # ct += 1
                                        sectionName = self._frontFindSectionList[idx]
                                        self._result[sectionName] = word
                                        self._reParseNumWords(sectionName, word)
                                        parseResult = word

                # 还是解析不到， 在判断courtyard是否为空, 如果为空就放到courtyard
                if parseResult is None:
                    if self._result.get("courtyard", None) is None:
                        self._result["courtyard"] = word
                        parseResult = word

                # 还是解析不到，说明前半段已经匹配完了，放到后半段，退出
                if parseResult is None:
                    self._backWordList = self._frontWordList[i:] + self._backWordList
                    self._backWordLacList = self._frontWordLacList[i:] + self._backWordLacList
                    return
            finally:
                if self.dropWashWord():
                    sectionFindList = sectionFindList[0:-1]

    def dropWashWord(self):
        # # 去掉 省 市 区 之后的section中还出现了 省 市 区 关键字的值。
        # 比如： 云南省昆明市呈贡区马金铺街道办事处化城居民委员会博大路云南省昆明市呈贡新城雨花片区月角塘西侧月马路北侧广电苑礼园15幢1502号
        delKeys = []
        for key, val in dict(self._result).items():
            if key in self._washSectionList:
                if self._isEqualsWash(val):
                    delKeys.append(key)
        for k in delKeys:
            self._result[k] = None

        return len(delKeys) > 0

    def _isEqualsWash(self, val):
        if val is None:
            return False
        for _, wordList in self._judge_dict_list.items():
            if val in wordList:
                return True
        return False

    def _parseBack(self):
        """
        解析后半段
        :return:
        """
        # 数字后面的词的词性如果是 n, 就合并。比如：'6', '室'
        self._joinNumAndNLac(self._backWordList, self._backWordLacList)

        # 从第1个word开始解析
        canNotJudgeWordList = []  # 暂时无法判断的词
        for i in range(len(self._backWordList)):
            try:
                word = self._backWordList[i]
                wordLac = self._backWordLacList[i]

                # 只有1个字，直接pass
                if len(word) == 1:
                    continue

                # print("当前解析词 = " + word)
                parseResult = None
                for j in range(len(self._backSectionList)):
                    sectionName = self._backSectionList[j]
                    parseResult = self.__simpleParser(word, sectionName, self._result)
                    if parseResult is not None:
                        # 当前还没有值，放到当前section
                        if self._result[sectionName] is None:
                            self._result[sectionName] = parseResult
                            self._reParseNumWords(sectionName, parseResult)
                        break

                if parseResult is None:
                    if wordLac == "m" and len(self._roomList) == 0:
                        # 当前词性是数字，且室/门牌的列表没值
                        self._roomList.append(word)
                    elif wordLac == "LOC" and self._result["building_name"] is None:
                        # 当前词性是地址，且building_name还没值
                        self._result["building_name"] = word
                    elif self._result["building_name"] is not None:
                        # 剩下部分当它是公司之类的，可能被分词分开了，都合并到 company上去
                        if self._result["company"] is None:
                            self._result["company"] = word
                        else:
                            self._result["company"] += word
                    else:
                        canNotJudgeWordList.append(word)
            finally:
                self.dropWashWord()

        # 最后处理无法判断的词
        noneBuildName = self._result["building_name"] is None
        for word in canNotJudgeWordList:
            if noneBuildName:
                if self._result["building_name"] is None:
                    self._result["building_name"] = word
                else:
                    self._result["building_name"] += word
            else:
                # 剩下部分当它是公司之类的，可能被分词分开了，都合并到 company上去
                if self._result["company"] is None:
                    self._result["company"] = word
                else:
                    self._result["company"] += word

    def _swapNumWords(self):
        """
        调整数字的位置
        :return:
        """
        for k, v in self._result.items():
            if v is not None:
                self._reParseNumWords(k, v)

    def _doChineseAndNum(self):
        """
        第1 ， 第一  这种 把 第字去掉
        :return:
        """
        for result in self._resultList:
            for k, v in result.items():
                if str(k).lower() == "x" or str(k).lower() == "y":
                    continue
                if v is not None and v != "":
                    # 第一个字符合要求
                    if v[0] in self._start_chinese:
                        # 后面如果是数字
                        if isNumLetters(v[1:]):
                            result[k] = v[1:]

    # 这么做有问题
    # def _joinBuildingSite(self):
    #     """
    #     把 building_site 位置的词和前一个合并为一个词
    #     上可乐社区   5区，  这种合并为以个词
    #     :return:
    #     """
    #     for result in self._resultList:
    #         # building_site 有值
    #         buildingSiteVal = result.get("building_site", None)
    #         if buildingSiteVal is None or buildingSiteVal == "":
    #             continue
    #         if not isNumLetters(buildingSiteVal):
    #             continue
    #         # building_site 和前面一个词合并
    #         for section in self._joinSectionList:
    #             word = result.get(section, None)
    #             if word is None or word == "":
    #                 continue
    #             # 合并
    #             result[section] = word + buildingSiteVal
    #             result.pop("building_site")
    #             break

    def _parsePrevLast(self):
        """
        前半段和后半段解析完后的工作
        1. 多个室/门牌 要分解成多条结果
        :return:
        """
        self._result["x"] = self._x
        self._result["y"] = self._y

        # 去掉值是None的key
        popKeyList = []
        for k, v in self._result.items():
            if v is None:
                popKeyList.append(k)
        for k in popKeyList:
            self._result.pop(k)

        # 单个结果
        if len(self._roomList) == 0:
            self._resultList.append(self._result)
            return

        # 多个室/门票生成多条结果
        for room in self._roomList:
            result = copy.deepcopy(self._result)
            result["room"] = room
            self._resultList.append(result)

    def _reParseNumWords(self, section, val):
        """
        检查 JUDGE_HAS_WORDS_MUST_NUMBER  和  JUDGE_HAS_WORDS_NOT_NUMBER ， 重放到正确位置
        :return:
        """
        if section in self._judge_has_words_must_number:  # 必须数字
            val = self._parseDropSectionUnit(val)
            valTemp = val
            for symbol in self._num_join_symbols:
                valTemp = valTemp.replace(symbol, "")
            if not isNumLetters(valTemp) and not self.isContainsNum(valTemp):  # 如果不是数字
                if self._print_debug:
                    log.debug(str(section) + " => " + str(val) + " 不是数字")
                self.move2Section(section, val, self._judge_has_words_not_number)

        if section in self._judge_has_words_not_number:  # 必须不是数字
            val = self._parseDropSectionUnit(val)
            valTemp = val
            for symbol in self._num_join_symbols:
                valTemp = valTemp.replace(symbol, "")
            if isNumLetters(valTemp):  # 如果是数字
                if self._print_debug:
                    log.debug(str(section) + " => " + str(val) + " 是数字")
                self.move2Section(section, val, self._judge_has_words_must_number)

    def move2Section(self, section, val, checkList):
        find = False
        allSecList = list(self._judge_has_words.keys())
        for checkSec in allSecList:
            if not find:
                find = section == checkSec
                continue
            if checkSec not in checkList:
                continue

            if checkSec not in self._result.keys() or self._result.get(checkSec, None) is None:
                self._result[checkSec] = val
                self._result[section] = None
                break

    def _parseLast(self):
        """
        调整解析结果。 因为可能 "9栋" 分到了 "courtyard" 字段上这种，实际应该是 "building_site"
        :return:
        """
        tempDict = copy.deepcopy(self._judge_has_words)
        noJList = ["province", "city", "region", "x", "y"]
        for word in noJList:
            if word in tempDict.keys():
                tempDict.pop(word)

        for resultDict in self._resultList:
            addDict = {}
            delKeys = []
            for key, val in dict(resultDict).items():
                if key in noJList:
                    continue
                isFind = False
                keywordList = tempDict.get(key)
                for word in keywordList:
                    if str(val).endswith(word):
                        isFind = True
                        break
                if not isFind:
                    for jKey, jVal in tempDict.items():
                        isMove = False
                        for jWord in jVal:
                            if str(val).endswith(jWord) and dict(resultDict).get(jKey, None) is None:
                                addDict[jKey] = val
                                delKeys.append(key)
                                isMove = True
                                break
                        if isMove:
                            break

            for k in delKeys:
                resultDict.pop(k)
            for k, v in addDict.items():
                resultDict[k] = v

    def _parseDropSectionUnit(self, val):
        """
        把对应的判断关键字去掉, 因为可能不带关键字或者关键字描述不一样但又代表同含义:

        {'province': '云南省', 'city': '昆明市', 'building_name': '未来时空', 'building_site': '12栋', 'unit': '1单元'}
         =>
        {'province': '云南', 'city': '昆明', 'building_name': '未来时空', 'building_site': '12', 'unit': '1'}
        :return:
        """
        tempDict = copy.deepcopy(self._judge_has_words)
        doWord = None
        wordLen = 0
        for section in tempDict.keys():
            keywordList = tempDict.get(section)
            for word in keywordList:
                # 去掉最长的那个后缀
                if str(val).endswith(word) and len(word) > wordLen:
                    wordLen = len(word)
                    doWord = word

        if wordLen > 0:
            return str(val)[0:len(val) - len(doWord)]

        return val

    def _parseFinally(self):
        """
        收尾
        把对应的判断关键字去掉, 因为可能不带关键字或者关键字描述不一样但又代表同含义:

        {'province': '云南省', 'city': '昆明市', 'building_name': '未来时空', 'building_site': '12栋', 'unit': '1单元'}
         =>
        {'province': '云南', 'city': '昆明', 'building_name': '未来时空', 'building_site': '12', 'unit': '1'}
        :return:
        """

        tempDict = copy.deepcopy(self._judge_has_words)
        noJList = ["province", "city", "region", "x", "y"]
        for word in noJList:
            if word in tempDict.keys():
                tempDict.pop(word)
        # tempDict = collections.OrderedDict(tempDict)

        for resultDict in self._resultList:
            for key, val in dict(resultDict).items():
                if key in tempDict.keys():
                    find = False
                    for keyWordList in tempDict.values():
                        for word in keyWordList:
                            if str(val).endswith(word):
                                resultDict[key] = str(val)[0:len(val) - len(word)]
                                find = True
                                break
                        if find:
                            break

        # 去除空值 和 None值
        for resultDict in self._resultList:
            delKeys = []
            for key, val in resultDict.items():
                if val is None or val == "":
                    delKeys.append(key)

            for key in delKeys:
                resultDict.pop(key)

        self._numProcess()

        self._dropSection()

        self._doChineseAndNum()

        self._replaceSymbol()

    def _parseRoomsIfMulti(self):
        """
        判断有没有 MULTI_JOIN_SYMBOLS.  获取多个门牌/室
        :return:
        """
        # 获取所有分割符
        symbolPosList = []
        for i in range(len(self._wordList)):
            word = self._wordList[i]
            if word in self._multi_join_symbols:
                symbolPosList.append(i)

        # 获取分割符前后的值
        if len(symbolPosList) > 0:
            usedPosList = []
            # 获取每个符号前面的值
            for pos in symbolPosList:
                usedPosList.append(pos)
                if pos > 0:
                    # 符号前一个word,  比如 101室
                    word = self._wordList[pos - 1]
                    wordLac = self._wordLacList[pos - 1]
                    # 是数字
                    if (wordLac == "m" or wordLac == "LOC") and self.isContainsNum(word):
                        self._roomList.append(word)
                        usedPosList.append(pos - 1)

            # 获取最后一个符号后面的值
            pos = symbolPosList[-1]
            # 符号后面要有值
            if pos < len(self._wordList):
                word = self._wordList[pos + 1]
                wordLac = self._wordLacList[pos + 1]
                # 是数字
                if (wordLac == "m" or wordLac == "LOC") and self.isContainsNum(word):
                    self._roomList.append(word)
                    usedPosList.append(pos + 1)

            # 移除解析过的值（从大到小，倒序移除）
            self.__removeByPosList(usedPosList, self._wordLacList, self._wordList)

    @staticmethod
    def isContainsNum(word):
        """
        是否数字开头
        :param word:
        :return:
        """
        numList = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        numChList = ["一", "二", "三", "四", "五", "六", "七", "八", "九", "十"]
        # EnAlphabetListLow = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
        #                      "s", "t", "u", "v", "w", "x", "y", "z"]
        # EnAlphabetListHigh = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R",
        #                       "S", "T", "U", "V", "W", "X", "Y", "Z"]
        for num in numList:
            if str(word).find(str(num)) >= 0:
                return True

        for num in numChList:
            if str(word).find(str(num)) >= 0:
                return True

        # for num in EnAlphabetListLow:
        #     if str(word).find(str(num)) >= 0:
        #         return True
        #
        # for num in EnAlphabetListHigh:
        #     if str(word).find(str(num)) >= 0:
        #         return True
        return False

    def _findLastHasValueSectionName(self, sectionList):
        """
        找最后一个有值的SectionName
        :return:
        """
        for i in range(len(sectionList) - 1, -1, -1):
            sectionName = sectionList[i]
            if self._result.get(sectionName, None) is not None:
                return i
        return -1

    def _findNearestNoValueSectionName(self, sectionList):
        """
        从第1个有值的开始，找最近没值的SectionName
        :return:
        """
        start = -1
        for i in range(len(sectionList)):
            sectionName = sectionList[i]
            if self._result.get(sectionName, None) is not None:
                start = i
                break

        if start >= 0:
            for i in range(start, len(sectionList)):
                sectionName = sectionList[i]
                if self._result.get(sectionName, None) is None:
                    return i
        return -1

    def _findLastNoValueSectionName(self, sectionList):
        """
       找最后一个没值的SectionName
       :return:
       """
        for i in range(len(sectionList) - 1, -1, -1):
            sectionName = sectionList[i]
            if self._result.get(sectionName, None) is not None:
                # 已经是最后一个
                if i + 1 == len(sectionList):
                    return -1
                return i + 1
        # 全没值
        return 0

    def washWordList(self, wordList, wordLacList):
        """
         数据清洗
         1. 移除词性是f的
         2. 移除关键字结尾的词
        :param wordList:
        :param wordLacList:
        :return:
        """

        if wordList is None:
            return

        # 移除停用词
        stopPosList = []
        for stopWord in self._stop_word_list:
            for i in range(len(wordList)):
                word = wordList[i]
                if stopWord == word:
                    stopPosList.append(i)
        self.__removeByPosList(stopPosList, wordLacList, wordList)

        # 移除词性是f的
        fPosList = []
        for i in range(len(wordList)):
            wordLac = wordLacList[i]
            if wordLac == "f":
                fPosList.append(i)
        self.__removeByPosList(fPosList, wordLacList, wordList)

        # 移除关键字结尾的词
        fPosList.clear()
        for i in range(len(wordList)):
            wordLac = wordLacList[i]
            if wordLac == "m" or wordLac == "LOC":
                word = wordList[i]
                for jWord in self._wash_unit_words:
                    if word.endswith(jWord):
                        fPosList.append(i)
        self.__removeByPosList(fPosList, wordLacList, wordList)

        # 只有1个字，直接删除
        ls = self._multi_join_symbols + self._num_join_symbols  # 不应该去掉的词， 后面要用到
        fPosList.clear()
        for i in range(len(wordList)):
            word = wordList[i]
            if len(word) == 1 and word not in ls:
                fPosList.append(i)
        self.__removeByPosList(fPosList, wordLacList, wordList)

        # 去掉方向干扰
        fPosList.clear()
        for i in range(len(wordList)):
            word = wordList[i]
            for directWord in self._direct_words:
                if word.endswith(directWord) and word not in self._direct_words_not_drop:
                    wordList[i] = word[0:len(word) - len(directWord)]
                    break
                if word.startswith(directWord) and word not in self._direct_words_not_drop:
                    wordList[i] = word[len(directWord):]
                    break

    @staticmethod
    def __removeByPosList(fPosList, wordLacList, wordList):
        # 从大到小，倒序移除
        fPosList.sort(reverse=True)
        for pos in fPosList:
            wordList.pop(pos)
            wordLacList.pop(pos)

    @staticmethod
    def _joinNumAndNLac(wordList, wordLacList):
        """
        合并数字和名词

        数字后面的词的词性如果是 n, 就合并。比如：'6', '室'
        :return:
        """
        nPosList = []
        for i in range(len(wordList)):
            word = wordList[i]
            wordLac = wordLacList[i]
            if wordLac == "n":
                # 前面必须有值
                if i > 0:
                    frontWordLac = wordLacList[i - 1]
                    if frontWordLac == "LOC" or frontWordLac == "m":
                        nPosList.append(i)
                        wordList[i - 1] += word
        # 合并的，要去掉词性是n的词
        AddressParser.__removeByPosList(nPosList, wordLacList, wordList)

    def __simpleParser(self, word, sectionName, result):
        """
        通用解析： 1. 关键字判断   2. 找到不到关键字, 判断是否在对应字典中
        :param word:
        :param sectionName:
        :return:
        """
        # 关键字判断
        keyWordList = self._judge_has_words.get(sectionName, None)
        notKeyWordList = self._judge_not_has_words.get(sectionName, [])
        if keyWordList is not None:
            rtn = self._judgeByKeyWord(word, keyWordList, notKeyWordList)
            if rtn is not None:
                # 当前result中的 section没值，才返回。不然继续找
                if result[sectionName] is None:
                    return rtn

        # 找到不到关键字, 判断是否在对应字典中
        jList = self._judge_dict_list.get(sectionName, None)
        if jList is not None:
            rtn = self._judgeByDict(word, self._judge_dict_list[sectionName])
            if rtn is not None:
                # 当前result中的 section没值，才返回。不然继续找
                if result[sectionName] is None:
                    return rtn
        return None

    def _joinBuildingSite(self):
        """
        蓝光天骄城  第1期  合并为  蓝光天骄城1
        :return:
        """
        wordLen = len(self._wordLacList)
        if wordLen < 2:
            return

        flagIndex = []
        for i in range(wordLen - 1, -1, -1):
            if i == 0:
                break
            lacVal = self._wordLacList[i]
            lacValFront = self._wordLacList[i - 1]
            if (lacVal == "m" and lacValFront == "LOC") or (lacVal == "LOC" and lacValFront == "LOC"):
                val = self._wordList[i]
                ValFront = self._wordList[i - 1]
                for jWord in self._judge_join_words:
                    if val.endswith(jWord):

                        # 前一个词不能是数字section
                        frontFind = False
                        for jWordFront in self._numSections:
                            if ValFront.endswith(jWordFront):
                                ValFrontTemp = ValFront[:len(ValFront) - len(jWordFront)]
                                # 去掉 "第" 字
                                if ValFrontTemp[0] in self._start_chinese:
                                    ValFrontTemp = ValFrontTemp[1:]
                                # 是数字
                                if isNumLetters(ValFrontTemp):
                                    frontFind = True
                                    break
                        if frontFind:
                            break

                        # 去掉后缀判断词
                        val = val[:len(val) - len(jWord)]
                        # 去掉 "第" 字
                        if val[0] in self._start_chinese:
                            # 后面如果是数字
                            if isNumLetters(val[1:]):
                                val = val[1:]

                        # 最后的值判断一下
                        if not isNumLetters(val):
                            break

                        # 合并
                        self._wordList[i - 1] = ValFront + "=" + val
                        # i 放入删除列表
                        flagIndex.append(i)
                        break
        # 去掉
        for i in flagIndex:
            self._wordList.pop(i)
            self._wordLacList.pop(i)

    def _doSubSection_hasNum(self):
        """
        2. 判断数字后面是否数字连接符，如果是，要合并。  比如  '78'  '-'  '1'    或    'S10', '-', '12'
        3. 根据数据位置，分成前半段 和 后半段
        :return:
        """
        # 可能本来有数字， 但是调用 _parseRoomsIfMulti后就没了
        if "m" not in self._wordLacList:
            self._frontWordList = self._wordList
            self._frontWordLacList = self._wordLacList
            self._backWordList = []
            self._backWordLacList = []
            # return frontWordList, backWordList, frontWordLacList, backWordLacList
            return

        # 第1个数字的下标。 用于前半段解析（province - address_number 为止）
        firstNumPos1 = self._wordLacList.index("m")

        firstNumPos2 = -1
        if "nz" in self._wordLacList:
            firstNumPos2 = self._wordLacList.index("nz")

        if firstNumPos2 == -1:
            firstNumPos = firstNumPos1
        else:
            firstNumPos = min(firstNumPos1, firstNumPos2)
        nextPos = firstNumPos + 1

        # 必须后面还有值
        if nextPos < len(self._wordList):
            nextWord = self._wordList[nextPos]
            # 是否是NUM_JOIN_SYMBOLS内的连接符号
            if nextWord in self._num_join_symbols:
                self._removeWord(nextPos)
                self._wordList[firstNumPos] += nextWord

                # 上面移除了一个，所以还是 firstNumPos + 1
                nextPos = firstNumPos + 1
                if nextPos < len(self._wordLacList):
                    nextWord = self._wordList[nextPos]
                    nextWordLac = self._wordLacList[nextPos]
                    # 是否数字
                    if nextWordLac == "m" or nextWordLac == "nz":
                        self._removeWord(nextPos)
                        self._wordList[firstNumPos] += nextWord

        # 前半段
        self._frontWordList = self._wordList[:firstNumPos + 1]
        self._frontWordLacList = self._wordLacList[:firstNumPos + 1]
        # 后半段
        self._backWordList = self._wordList[firstNumPos + 1:]
        self._backWordLacList = self._wordLacList[firstNumPos + 1:]

    def _doSubSection_noNumButDir(self):
        """
        1. 判断数字后面是否数字连接符，如果是，要合并
        2. 根据数据位置，分成前半段 和 后半段
        :return:
        """

        firstPos = self.getDirWordPos(self._wordList, self._wordLacList)
        if firstPos == -1:
            return

        # 前半段
        # 方向字眼不要了，不需要 +1
        self._frontWordList = self._wordList[:firstPos]
        self._frontWordLacList = self._wordLacList[:firstPos]
        # 后半段
        self._backWordList = self._wordList[firstPos + 1:]
        self._backWordLacList = self._wordLacList[firstPos + 1:]

    @staticmethod
    def getDirWordPos(wordList, wordLacList):
        for i in range(len(wordList)):
            word = wordList[i]
            wordLac = wordLacList[i]
            if wordLac == "f" \
                    and \
                    (word.find("东") >= 0 or word.find("南") >= 0 or word.find("西") >= 0 or word.find("北") >= 0):
                return i
        return -1

    def _doSubSection_notAll(self):
        """
       不带数字的，无方向字眼。 认为就是前半段
        :return:
        """

        # 前半段
        self._frontWordList = self._wordList
        self._frontWordLacList = self._wordLacList
        # 后半段
        self._backWordList = []
        self._backWordLacList = []

    def _subSection(self):
        """
        分成前半段和后半段
        :return:
        """
        # 能找到数字的解析
        if "m" in self._wordLacList:
            self._doSubSection_hasNum()
        # 不带数字的，但有方向字眼
        elif self.getDirWordPos(self._wordList, self._wordLacList) > 0:
            self._doSubSection_noNumButDir()
        # 不带数字的，无方向字眼
        else:
            self._doSubSection_notAll()

    def _numProcess(self):
        """
        07  =>  7
        :return:
        """
        for result in self._resultList:
            for k, v in result.items():
                if str(k).lower() == "x" or str(k).lower() == "y":
                    continue
                try:
                    v = str(int(v))
                    result[k] = v
                except:
                    pass

    def _replaceSymbol(self):
        """
        去掉 = 号
        :return:
        """
        for result in self._resultList:
            for k, v in result.items():
                if str(k).lower() == "x" or str(k).lower() == "y":
                    continue
                v = v.replace("=", "")
                result[k] = v

    def _dropSection(self):
        """
        去掉省 市 区 的关键字
        :return:
        """
        for result in self._resultList:
            val = result.get("province", None)
            if val is not None and str(val).endswith("省"):
                result["province"] = val[0:len(val) - 1]

            val = result.get("city", None)
            if val is not None and str(val).endswith("市"):
                result["city"] = val[0:len(val) - 1]

            val = result.get("region", None)
            if val is not None and str(val).endswith("区"):
                result["region"] = val[0:len(val) - 1]

    def parse(self):
        self._parseInit()
        self._replaceAlias()
        self._doWashWordList()
        self._joinBuildingSite()
        self._parseRoomsIfMulti()
        self._subSection()
        self._parseFront()
        self._parseBack()
        self._swapNumWords()
        self._doChineseAndNum()
        self._parsePrevLast()
        self._parseLast()
        self._parseFinally()

        if self._print_debug:
            for result in self._resultList:
                print("解析结果: " + str(result))
        return self._resultList
