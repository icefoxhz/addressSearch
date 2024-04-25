import collections
import os
import threading
import uuid

import jieba
import pandas as pd
from LAC import LAC
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
        "project.lac.model_path": "_model_path",
        "project.lac.dict_dir": "_dict_dir",
        "task.execution.pool.max_size": "_max_size",
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

        self._workDir = os.path.abspath('../service')
        self._currentDir = os.path.dirname(__file__)

        self._lock = threading.Lock()
        self.__q = collections.deque()

    @Autowired
    def _set_params(self, configMapping: ConfigMapping,
                    configService: ConfigService,
                    databaseManager: DatabaseManager,
                    ):
        self._configMapping = configMapping
        self._configService = configService
        self._databaseManager = databaseManager

    def _generateDict(self):
        self._dict_path = self._dict_dir + os.sep + "custom_" + str(uuid.uuid4()) + ".txt"
        self._dict_path_least = self._dict_dir + os.sep + "custom_least_" + str(uuid.uuid4()) + ".txt"
        # 判断文件是否存在
        if not os.path.exists(self._dict_path):
            # 如果文件不存在，则创建文件
            with open(self._dict_path, 'w'):
                pass

        dict_table = self._configService.get_addr_cnf("dict_table")

        self._databaseManager.switch_datasource("sourceConfig")
        data = self._configMapping.get_address_dict(dict_table)
        self._databaseManager.switch_datasource("sourceData")

        data["dict_value"].to_csv(self._dict_path, index=False, header=False)
        # print("===== 重新下载分词字典完成 =====")

        dict_least_list = data["dict_value"].to_list()
        df_least = self._generate_least_word_dict(dict_least_list)
        df_least.to_csv(self._dict_path_least, index=False, header=False)

    @staticmethod
    def _generate_least_word_dict(dict_list):
        """
        只保留最小细度的字典。 比如： 中关村创新园、中关村、创新园 ， 那么 中关村创新园 要删除
        """
        dict_list = list(set(dict_list))

        del_words = []
        for word1 in dict_list:
            for word2 in dict_list:
                if word1 == word2:
                    continue
                # 分词后只有1个字的就不要分了
                if (word2.startswith(word1) or word2.endswith(word1)) and len(word2.replace(word1, "")) > 1:
                    del_words.append(word2)

        del_words = list(set(del_words))
        for word in del_words:
            dict_list.remove(word)
        df = pd.DataFrame(dict_list, columns=['dict_value'])
        return df

    def _after_init(self):
        self._generateDict()
        for _ in range(self._max_size + 1):
            self.__q.append(self.__generateModel())

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
        model = None
        with self._lock:
            if len(self.__q) > 0:
                model = self.__q.popleft()

        # 万一__exit__ 出现异常导致模型没有还回去，就创建新的
        if model is None:
            model = self.__generateModel()
            log.info("===== 队列为空，重新创建模型 ======")

        self.__local_obj.model = model
        return model

    # 使用with的写法
    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self.__local_obj.model, "model") and self.__local_obj.model is not None:
            with self._lock:
                self.__q.append(self.__local_obj.model)

        # 如果在 with 语句块中出现异常，exc_type、exc_value 和 traceback 参数将包含异常信息
        if exc_type is not None:
            # raise Exception(f"出现异常,异常类型: {exc_type}, 异常信息: {exc_value}")
            log.error(f"LacModelManageService __exit__ 出现异常,异常类型: {exc_type}, 异常信息: {exc_value}")
            # 返回False则会让异常继续向上抛出
            return False
        return True

    # def take(self):
    #   with self._lock:
    #     return self.__q.popleft()
    #
    # def back(self, model):
    #   with self._lock:
    #     self.__q.append(model)
