# -*- coding: utf-8 -*-

from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value

_numDict = {
    "一": "1",
    "二": "2",
    "三": "3",
    "四": "4",
    "五": "5",
    "六": "6",
    "七": "7",
    "八": "8",
    "九": "9",
    "十": "10",
    "壹": "1",
    "贰": "2",
    "叁": "3",
    "肆": "4",
    "伍": "5",
    "陆": "6",
    "柒": "7",
    "捌": "8",
    "玖": "9",
    "拾": "10",
}


@Component
class AddressParseRunner:
    @Value({
        "project.print_debug": "_print_debug",
        "DROP_WORDS": "_drop_words",
        "NUM_JOIN_SYMBOLS": "_num_join_symbols",

    })
    def __init__(self):
        self._print_debug = False
        self._drop_words = None
        self._num_join_symbols = None

    def run(self, address_parser, model, full_name, x=None, y=None):
        if model is None:
            raise Exception("分词模块实例为null")

        cutResult = model.run(full_name)
        if self._print_debug:
            log.debug("py库分词结果: " + str(cutResult))
        wordList = cutResult[0]
        wordLacList = cutResult[1]

        # 数字处理
        self._changeWord(wordList, wordLacList)
        if self._print_debug:
            log.debug("数字处理后结果: " + str(wordList))

        # wordListTemp = copy.deepcopy(wordList)
        wordListStr = str(wordList)

        # 连接符处理
        self._joinWord(wordList, wordLacList)
        if self._print_debug:
            log.debug("连接符处理后结果: " + str(wordList))

        address_parser.set_params(x, y, wordList, wordLacList)

        return address_parser.parse(), wordListStr
        # return None, None

    def _changeWord(self, wordList, wordLacList):
        """
        预处理
        :param wordList:
        :param wordLacList:
        :return:
        """
        nullIdxList = []
        for i in range(0, len(wordList)):
            # 去掉空格
            doWord = wordList[i]

            # 去掉干扰词
            for word in self._drop_words:
                doWord = str(doWord).replace(word, "")
            if doWord == "":
                nullIdxList.append(i)
                continue

            # 中文数字转阿拉伯数字
            # 这里暂时简单替换， 用正则表达式判断可能更准确
            for k, v in _numDict.items():
                doWord = str(doWord).replace(k, v)

            # 数字连接符替换成统一符号
            for symbol in self._num_join_symbols:
                doWord = doWord.replace(symbol, "-")
            wordList[i] = doWord

        # 去掉空格项
        if len(nullIdxList) > 0:
            nullIdxList = reversed(nullIdxList)
            for idx in nullIdxList:
                wordList.pop(idx)
                wordLacList.pop(idx)

    def _joinWord(self, wordList, wordLacList):
        """
        连接符处理
        :param wordList:
        :return:
        """
        for i in range(0, len(wordList)):
            try:
                # 去掉空格
                doWord = str(wordList[i])
                for symbol in self._num_join_symbols:
                    # 连接符开头，则连接前面的词
                    if len(doWord) > 1 and doWord.startswith(symbol):
                        wordList[i - 1] = wordList[i - 1] + doWord
                        wordLacList[i - 1] = "LOC"
                        wordList.pop(i)
                        wordLacList.pop(i)
                        self._joinWord(wordList, wordLacList)

                    # 连接符结尾，且不是最后一个词， 则连接后面的词
                    elif len(doWord) > 1 and (i != len(wordList) - 1) and doWord.endswith(symbol):
                        wordList[i] = doWord + wordList[i + 1]
                        wordLacList[i] = "LOC"
                        wordList.pop(i + 1)
                        wordLacList.pop(i + 1)
                        self._joinWord(wordList, wordLacList)

                    # 单独一个连接符，则连接前面和后面的词
                    elif len(doWord) == 1 and doWord == symbol:
                        wordList[i] = wordList[i - 1] + doWord + wordList[i + 1]
                        wordLacList[i] = "LOC"
                        wordList.pop(i + 1)
                        wordLacList.pop(i + 1)
                        wordList.pop(i - 1)
                        wordLacList.pop(i - 1)
                        self._joinWord(wordList, wordLacList)
            except:
                pass
