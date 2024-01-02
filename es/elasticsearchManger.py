import threading
import time
from datetime import datetime

from elasticsearch import Elasticsearch
from pySimpleSpringFramework.spring_core.log import log


class ElasticsearchManger:
    __CONN_TIME_OUT = 120  # 单位秒
    __local_obj = threading.local()

    def __init__(self, indexName, indexSchema, ip, port):
        self.__indexName = indexName
        self._ip = ip
        self._port = port
        self.__indexSchema = indexSchema

    @property
    def index_name(self):
        return self.__indexName

    def is_time_out(self):
        last_do_time = self.__local_obj.last_do_time
        now_time = time.time()
        # 转换为datetime对象
        now_dt = datetime.fromtimestamp(now_time)
        last_do_dt = datetime.fromtimestamp(last_do_time)
        # 计算差值
        time_difference = now_dt - last_do_dt
        sec = time_difference.total_seconds()
        return sec > self.__CONN_TIME_OUT

    # noinspection PyUnusedLocal
    def insert(self, dataId, data):
        conn = self._get_conn()
        conn.index(index=self.__indexName, body=data, doc_type="_doc", id=dataId)
        self.__local_obj.last_do_time = time.time()

    def __conn(self):
        if hasattr(self.__local_obj, "es_conn") and self.__local_obj.es_conn is not None:
            if self.is_time_out():
                self.close()

        if not hasattr(self.__local_obj, "es_conn") or self.__local_obj.es_conn is None:
            es_conn = Elasticsearch(host=self._ip, port=int(self._port))
            self.__local_obj.es_conn = es_conn
            self.__local_obj.last_do_time = time.time()

        return self.__local_obj.es_conn

    def create(self, index_name):
        conn = self._get_conn()
        if not conn.indices.exists(index=index_name):
            # print("create index: ", index_name, " body: ", self.__indexSchema)
            conn.indices.create(index=index_name, body=self.__indexSchema)

    def close(self):
        try:
            if hasattr(self.__local_obj, "es_conn"):
                self.__local_obj.es_conn.close()
        except:
            pass
        finally:
            self.__local_obj.es_conn = None

    def _get_conn(self):
        es_conn = None
        try:
            es_conn = self.__conn()
        except Exception as e:
            log.error(str(e))
        return es_conn

    # 使用with的写法
    def __enter__(self):
        return self._get_conn()

    # 使用with的写法
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def deleteIndex(self):
        conn = self._get_conn()
        if conn.indices.exists(index=self.__indexName):
            conn.indices.delete(index=self.__indexName)
        self.__local_obj.last_do_time = time.time()

    def query(self, jsonQuery):
        conn = self._get_conn()
        self.__local_obj.last_do_time = time.time()
        return conn.search(body=jsonQuery, index=self.__indexName, doc_type="_doc")
