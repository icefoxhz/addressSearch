import os
import collections
import threading

from LAC import LAC
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component, Scope
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value


@Component
class LacModelManager:
    __local_obj = threading.local()

    @Value({
        "lac.model_path": "_model_path",
        "lac.dict_path": "_dict_path",
        "task.execution.pool.max_size": "_max_size",
    })
    def __init__(self):
        self._model_path = None
        self._dict_path = None
        self._max_size = None

        self._workDir = os.path.abspath('.')
        self._currentDir = os.path.dirname(__file__)

        self._lock = threading.Lock()
        self.__q = collections.deque()

    def _after_init(self):
        for _ in range(self._max_size + 1):
            self.__q.append(self.__generateModel())

    def __generateModel(self):
        os.chdir(self._currentDir)
        model = LAC(model_path=self._model_path)
        model.load_customization(customization_file=self._dict_path)
        os.chdir(self._workDir)
        return model

    # 使用with的写法
    def __enter__(self):
        with self._lock:
            model = self.__q.popleft()
        self.__local_obj.model = model
        return model

    # 使用with的写法
    def __exit__(self, exc_type, exc_value, traceback):
        # 如果在 with 语句块中出现异常，exc_type、exc_value 和 traceback 参数将包含异常信息
        if exc_type is not None:
            raise Exception(f"出现异常,异常类型: {exc_type}, 异常信息: {exc_value}")

        if hasattr(self.__local_obj.model, "model") and self.__local_obj.model is not None:
            with self._lock:
                self.__q.append(self.__local_obj.model)
        return True

    # def take(self):
    #   with self._lock:
    #     return self.__q.popleft()
    #
    # def back(self, model):
    #   with self._lock:
    #     self.__q.append(model)
