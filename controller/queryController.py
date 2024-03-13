import asyncio
import re
from typing import Dict
import uvicorn
from starlette.responses import JSONResponse
from uvicorn import Config
from fastapi import FastAPI, Request
from pySimpleSpringFramework.spring_core.log import log

from addressSearch.entrypoint.applicationStarter import serviceApplication

# 在这里导入自己的serviceApplication实例

# 设置最大并发数
semaphore = asyncio.Semaphore(1000)

rest_app = FastAPI()

RE_DO_REPLACE_SYMBOLS = {
    "-": "号"
}

_JUDGE_SCORE = 150


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


def _genRestResultOld(result):
    for k, v in result.items():
        result[k] = {
            "msg": "nothing" if v == "" or v is None or len(v) == 0 else "succeed",
            "data": {} if v == "" or v is None or len(v) == 0 else v,
            "code": 404 if v == "" or v is None or len(v) == 0 else 200
        }

    # return json.dumps(result, ensure_ascii=False)
    return result


def _genRestResult(resultDict, error):
    result = {
        "msg": str(error),
        "result": {},
        "code": 0
    }

    if error is None or str(error) == "":
        result["msg"] = "succeed"
        result["result"] = resultDict
        result["code"] = 1

    # return json.dumps(result, ensure_ascii=False)
    return result


def _doSearch(esSearchService, resultListDict, returnMulti=False):
    result = {}
    succeed = False
    error = None
    try:
        isAccurate_list = [True, False]
        for isAccurate in isAccurate_list:
            succeed, result = esSearchService.searchByAddress(resultListDict, isAccurate, returnMulti)
            if succeed:
                break
    except Exception as e:
        error = str(e)
        log.error("searchByAddress error =>" + str(e))
    return result, succeed, error


def _doSearchByAddress(jsonRequest, returnMulti=False):
    # 这里是获取bean的例子
    esSearchService = serviceApplication.application_context.get_bean("esSearchService")
    resultListDict = esSearchService.parse(jsonRequest)
    result, succeed = _doStep(esSearchService, jsonRequest, resultListDict, returnMulti)

    score = 0
    reqId = "1"
    if type(result[reqId]) == dict and "score" in result[reqId]["data"].keys():
        score = result[reqId]["data"]["score"]
    if not succeed or score < _JUDGE_SCORE:
        applicationEnvironment = serviceApplication.application_context.get_bean("applicationEnvironment")
        _JUDGE_DELETE_NUM = applicationEnvironment.get("JUDGE_DELETE_NUM")
        # 减少数字的分词
        for _, ls in resultListDict.items():
            for d in ls:
                for k in _JUDGE_DELETE_NUM:
                    if k in d.keys() and isNumLetters(d[k]):
                        d.pop(k)
                        result, succeed = _doStep(esSearchService, jsonRequest, resultListDict, returnMulti)
                        if type(result[reqId]) == dict and "score" in result[reqId]["data"].keys():
                            score = result[reqId]["data"]["score"]
                        if succeed and score > _JUDGE_SCORE:
                            break

    return result


def _doStep(esSearchService, jsonRequest, resultListDict, returnMulti):
    result, succeed = _doSearchByAddressWithoutThesaurus(esSearchService, jsonRequest, resultListDict, returnMulti)

    # 如果找不到或搜索结果得分太低的，则使用同义词重新搜索
    score = 0
    reqId = "1"
    if type(result[reqId]) == dict and "score" in result[reqId].keys():
        score = result[reqId]["score"]

    if not succeed or score < _JUDGE_SCORE:
        thesaurusService = serviceApplication.application_context.get_bean("thesaurusService")
        result_WithThesaurus, succeed = _doSearchByAddressWithThesaurus(thesaurusService,
                                                                        esSearchService,
                                                                        jsonRequest,
                                                                        returnMulti,
                                                                        score)
        if succeed:
            result = result_WithThesaurus

    # return _genRestResult(result, error)
    return _genRestResultOld(result), succeed


def _doSearchByAddressWithoutThesaurus(esSearchService, jsonRequest, resultListDict, returnMulti):
    result, succeed, error = _doSearch(esSearchService, resultListDict, returnMulti)
    if not succeed:
        should_do = False
        for symbol, v in RE_DO_REPLACE_SYMBOLS.items():
            for idx, address in jsonRequest.items():
                if address.find(symbol) >= 0:
                    jsonRequest[idx] = address.replace(symbol, v)
                    should_do = True
        if should_do:
            result, succeed, error = _doSearch(esSearchService, resultListDict, returnMulti)
    return result, succeed


def _doSearchByAddressWithThesaurus(thesaurusService, esSearchService, jsonRequest, returnMulti, score=0):
    newJsonRequest = {}
    reqId = "1"
    # 同义词 key => value   value => key
    ls = [thesaurusService.s2t, thesaurusService.t2s]

    for idx, address in jsonRequest.items():
        for d in ls:
            for k, words in d.items():
                if address.find(k) >= 0:
                    for word in words:
                        newJsonRequest[idx] = address.replace(k, word)
                        resultListDict = esSearchService.parse(newJsonRequest)
                        result, succeed = _doSearchByAddressWithoutThesaurus(esSearchService,
                                                                             newJsonRequest,
                                                                             resultListDict,
                                                                             returnMulti)
                        if succeed and result[reqId]["score"] > score:
                            return result, succeed
    return {}, False


def _doSearchByPoint(jsonRequest, returnMulti=False):
    result = {}
    try:
        # 这里是获取bean的例子
        esSearchService = serviceApplication.application_context.get_bean("esSearchService")
        result = esSearchService.searchByPoint(jsonRequest, returnMulti)
    except Exception as e:
        log.error("searchByAddress error =>" + str(e))
    # return _genRestResult(result, error)
    return _genRestResultOld(result)


@rest_app.middleware("http")
async def limit_concurrency(request: Request, call_next):
    async with semaphore:
        response = await call_next(request)
    return response


@rest_app.exception_handler(OSError)
async def handle_os_error(request, exc):
    log.error(f"OSError caught: {exc}")
    return JSONResponse(status_code=500, content={"message": "Internal server error"})


@rest_app.post("/searchByAddress")
async def appSearchByAddress(jsonRequest: Dict[int, str]):
    """
    参数格式
    {
        "1": "无锡市惠山区洛社镇五秦村强巷52号"
    }

    :param jsonRequest:
    :return:
    """
    return _doSearchByAddress(jsonRequest, False)


@rest_app.post("/searc6hByAddressEx")
async def appSearchByAddressEx(jsonRequest: Dict[int, str]):
    """
    参数格式
    {
        "1": "无锡市惠山区洛社镇五秦村强巷52号"
    }

    :param jsonRequest:
    :return:
    """
    return _doSearchByAddress(jsonRequest, True)


@rest_app.post("/searchByPoint")
async def appsearchByPoint(jsonRequest: Dict[int, str]):
    """
    参数格式
    {
        "1": "119.87630533652268,31.31180405900834",
        "2": "120.23387066537168,31.646691535452955",
        "3": "120.51393079234518,31.546471419507913",
        ...
    }
    :param jsonRequest:
    :return:
    """
    return _doSearchByPoint(jsonRequest, False)


@rest_app.post("/searchByPointEx")
async def appsearchByPointEx(jsonRequest: Dict[int, str]):
    """
    参数格式
    {
        "1": "119.87630533652268,31.31180405900834",
        "2": "120.23387066537168,31.646691535452955",
        "3": "120.51393079234518,31.546471419507913",
        ...
    }
    :param jsonRequest:
    :return:
    """
    return _doSearchByPoint(jsonRequest, True)


@rest_app.post("/reset")
async def addressReset():
    """
    重置
    """
    try:
        # 这里是获取bean的例子
        esSearchService = serviceApplication.application_context.get_bean("esSearchService")
        esSearchService.reset()
        return {
            "msg": "succeed",
            "code": 1
        }
    except Exception as e:
        log.error("reset error =>" + str(e))
        return {
            "msg": str(e),
            "code": 0
        }


@rest_app.post("/import_local_file")
async def addressCreateByFile(request: Request):
    try:
        jsonRequest = await request.json()
        file = jsonRequest["file"]

        table = None
        if "table" in jsonRequest.items():
            table = jsonRequest["table"]

        fileImportService = serviceApplication.application_context.get_bean("fileImportService")
        ret = fileImportService.run(file, table)
        return {
            "msg": "succeed" if ret else "failed",
            "code": 1 if ret else 0
        }

    except Exception as e:
        log.error("reset error =>" + str(e))
        return {
            "msg": str(e),
            "code": 0
        }


def start_rest_service():
    """
    启动
    """
    # 启动rest服务
    applicationEnvironment = serviceApplication.application_context.get_bean("applicationEnvironment")
    port = applicationEnvironment.get("project.http.rest_port")
    # uvicorn.run(rest_app, host="0.0.0.0", port=port, reload=False, workers=8)

    config = Config(app=rest_app, lifespan='off', host="0.0.0.0", port=port, reload=False)
    server = uvicorn.Server(config=config)
    server.run()
