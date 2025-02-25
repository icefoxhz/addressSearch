import threading

import requests
from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Value
from pySimpleSpringFramework.spring_core.log import log


@Component
class AiModelService:
    __local_obj = threading.local()

    @Value({
        "project.ai_interface": "_ai_rest_url"
    })
    def __init__(self):
        """
        use_session = True 为使用长连接
        """
        self._ai_rest_url = None
        self.__local_obj.s = None

    def run(self, address):
        r = requests.post(self._ai_rest_url, data={"1": address})
        return self._get_result(r)

    def run_by_session(self, address):
        if self.__local_obj.s is None:
            self.__local_obj.s = requests.Session()

        r = self.__local_obj.s.post(self._ai_rest_url, data={"1": address})
        if r.status_code == 401:
            log.info('session已过期')
            self.__local_obj.s = requests.Session()

        return self._get_result(r)

    def close(self):
        try:
            self.__local_obj.s.close()
        except:
            pass

    @staticmethod
    def _get_result(r):
        try:
            result = r.json()
            return result
        except Exception as e:
            log.error(str(e))
        return None
