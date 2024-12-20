import os
import threading
import uuid

import jieba
import pandas as pd
from LAC import LAC
from numba import jit
from pySimpleSpringFramework.spring_core.log import log
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value, Autowired
from pySimpleSpringFramework.spring_orm.databaseManager import DatabaseManager

from addressSearch.mapping.configMapping import ConfigMapping
from addressSearch.service.configService import ConfigService


@Component
class LacModelManageService:
    __local_obj = threading.local()

    @Value({
        "project.big_region.province": "_provinces",
        "project.big_region.city": "_cities",
        "project.big_region.region": "_regions",
        "project.big_region.street": "_streets",
        "project.lac.model_path": "_model_path",
        "project.lac.dict_dir": "_dict_dir",
        "task.execution.pool.max_size": "_max_size",
        # "project.local_config.enable": "_local_config_enable"
    })
    def __init__(self):
        self._model_path = None
        self._dict_path = None
        self._dict_path_least = None
        self._dict_dir = None
        self._configMapping = None
        self._configService = None
        self._databaseManager = None
        self._max_size = 2

        self._provinces = None
        self._cities = None
        self._regions = None
        self._streets = None

        # self._local_config_enable = False

        self._workDir = os.path.abspath('../service')
        self._currentDir = os.path.dirname(__file__)

        self.__model = None

    @Autowired
    def _set_params(self, configMapping: ConfigMapping,
                    configService: ConfigService,
                    databaseManager: DatabaseManager,
                    ):
        self._configMapping = configMapping
        self._configService = configService
        self._databaseManager = databaseManager

    def _generateDict(self):
        log.info("=========== 开始加载字典表 ===========")
        self._dict_path = self._dict_dir + "/custom_" + str(uuid.uuid4()) + ".txt"
        self._dict_path_least = self._dict_dir + "/custom_least_" + str(uuid.uuid4()) + ".txt"
        # 判断文件是否存在
        if not os.path.exists(self._dict_path):
            # 如果文件不存在，则创建文件
            with open(self._dict_path, 'w'):
                pass

        dict_table = self._configService.get_addr_cnf("dict_table")

        self._databaseManager.switch_datasource("sourceConfig")
        data = self._configMapping.get_address_dict(dict_table)
        self._databaseManager.switch_datasource("sourceData")

        # 去掉配置文件中的 省、市、区、街道， 这些不能放到字典中，要影响判断算法
        remove_lss = [self._provinces, self._cities, self._regions, self._streets]
        for ls in remove_lss:
            for word in ls:
                # data.drop(data[data['dict_value'] == word].index, inplace=True)
                data = data[data['dict_value'] != word]

        data["dict_value"].to_csv(self._dict_path, index=False, header=False)
        # print("===== 重新下载分词字典完成 =====")

        dict_least_list = data["dict_value"].to_list()
        df_least = self._generate_least_word_dict(dict_least_list)
        df_least.to_csv(self._dict_path_least, index=False, header=False)

        log.info("=========== 加载字典表完成 ===========")

    # @staticmethod
    # @jit(nopython=True)
    # def generate_least_word_loop(dict_list):
    #     del_words = []
    #     for word1 in dict_list:
    #         for word2 in dict_list:
    #             if word1 == word2:
    #                 continue
    #             # 分词后只有1个字的就不要分了
    #             if (word2.startswith(word1) or word2.endswith(word1)) and len(word2.replace(word1, "")) > 1:
    #                 del_words.append(word2)
    #     return del_words

    @staticmethod
    @jit(nopython=True)
    def generate_least_word_loop(lst: list):
        # 用来存储要删除的元素
        del_words = set()
        # 遍历每个元素
        for i in range(len(lst)):
            for j in range(i + 1, len(lst)):
                i_val = lst[i]
                j_val = lst[j]
                # 如果后面的字符串包含当前的字符串, 分词后只有1个字的就不要分了
                if (i_val in j_val) and (len(j_val) > len(j_val) + 1):
                    del_words.add(j_val)
        return del_words

    @staticmethod
    def _generate_least_word_dict(dict_list):
        """
        只保留最小细度的字典。 比如： 中关村创新园、中关村、创新园 ， 那么 中关村创新园 要删除
        """
        if dict_list is None or len(dict_list) == 0:
            return pd.DataFrame([], columns=['dict_value'])

        dict_list = list(set(dict_list))
        # 按长度排序，短的字符串在前
        dict_list.sort(key=len)

        del_words = LacModelManageService.generate_least_word_loop(dict_list)

        # 返回没有被删除的元素
        result = [word for word in dict_list if word not in del_words]
        df = pd.DataFrame(result, columns=['dict_value'])
        return df

    def _after_init(self):
        # if self._local_config_enable:
        #     self._dict_path_least = self._dict_path = self._dict_dir + "/local_dict.txt"
        # else:
        #     self._generateDict()
        self._generateDict()
        self.__model = self.__generateModel()

        # 用jieba
        jieba.load_userdict(self._dict_path_least)

    def __generateModel(self):
        os.chdir(self._currentDir)
        model = LAC(model_path=self._model_path)
        model.load_customization(customization_file=self._dict_path)
        os.chdir(self._workDir)
        return model

    # 使用with的写法
    def __enter__(self):
        # self.__local_obj.model = copy.copy(self.__model)
        # return self.__local_obj.model
        return self.__model

    # 使用with的写法
    def __exit__(self, exc_type, exc_value, traceback):
        # if hasattr(self.__local_obj.model, "model") and self.__local_obj.model is not None:
        #     del self.__local_obj.model
        #     self.__local_obj.model = None

        # 如果在 with 语句块中出现异常，exc_type、exc_value 和 traceback 参数将包含异常信息
        if exc_type is not None:
            # raise Exception(f"出现异常,异常类型: {exc_type}, 异常信息: {exc_value}")
            log.error(f"LacModelManageService __exit__ 出现异常,异常类型: {exc_type}, 异常信息: {exc_value}")
            # 返回False则会让异常继续向上抛出
            return False
        return True
