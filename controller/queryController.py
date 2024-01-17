import ujson as json
from typing import Dict

import uvicorn
from fastapi import FastAPI, Request
from pySimpleSpringFramework.spring_core.log import log

from addressSearch.entrypoint.applicationStarter import serviceApplication

# 在这里导入自己的serviceApplication实例

rest_app = FastAPI()

RE_DO_REPLACE_SYMBOLS = {
    "-": "号"
}


def genRestResult(resultDict, error):
    result = {
        "msg": str(error),
        "result": {},
        "code": 0
    }

    if error is None or str(error) == "":
        result["msg"] = "success"
        result["result"] = resultDict
        result["code"] = 1

    # return json.dumps(result, ensure_ascii=False)
    return result


def __do_search(esSearchService, resultListDict, returnMulti=False):
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


def doSearchByAddress(jsonRequest, returnMulti=False):
    # 这里是获取bean的例子
    esSearchService = serviceApplication.application_context.get_bean("esSearchService")
    resultListDict = esSearchService.parse(jsonRequest)
    result, succeed, error = __do_search(esSearchService, resultListDict, returnMulti)
    if not succeed:
        should_do = False
        for symbol, v in RE_DO_REPLACE_SYMBOLS.items():
            for idx, address in jsonRequest.items():
                if address.find(symbol) >= 0:
                    jsonRequest[idx] = address.replace(symbol, v)
                    should_do = True
        if should_do:
            result, succeed, error = __do_search(esSearchService, resultListDict, returnMulti)
    return genRestResult(result, error)


def doSearchByPoint(jsonRequest, returnMulti=False):
    result = {}
    error = None
    try:
        # 这里是获取bean的例子
        esSearchService = serviceApplication.application_context.get_bean("esSearchService")
        result = esSearchService.searchByPoint(jsonRequest, returnMulti)
    except Exception as e:
        error = str(e)
        log.error("searchByAddress error =>" + str(e))
    return genRestResult(result, error)


@rest_app.post("/searchByAddress")
async def appSearchByAddress(jsonRequest: Dict[int, str]):
    """
    参数格式
    {
        "1": "无锡市惠山区洛社镇五秦村强巷52号",
        "2": "无锡市江阴市澄江街道天鹤社区人民东路二百九十九弄23号",
        "3": "无锡市江阴市周庄镇三房巷村三房巷278号",
        ...
    }

    :param jsonRequest:
    :return:
    """
    return doSearchByAddress(jsonRequest, False)


@rest_app.post("/searchByAddressEx")
async def appSearchByAddress(jsonRequest: Dict[int, str]):
    """
    参数格式
    {
        "1": "无锡市惠山区洛社镇五秦村强巷52号",
        "2": "无锡市江阴市澄江街道天鹤社区人民东路二百九十九弄23号",
        "3": "无锡市江阴市周庄镇三房巷村三房巷278号",
        ...
    }

    :param jsonRequest:
    :return:
    """
    return doSearchByAddress(jsonRequest, True)


@rest_app.post("/searchByPoint")
async def appSearchByAddress(jsonRequest: Dict[int, str]):
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
    return doSearchByPoint(jsonRequest, False)


@rest_app.post("/searchByPointEx")
async def appSearchByAddress(jsonRequest: Dict[int, str]):
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
    return doSearchByPoint(jsonRequest, True)


# 新加
@rest_app.post("/search")
async def appSearchByAddress(request: Request):
    result = {}
    try:
        jsonRequest = await request.json()

        # 这里是获取bean的例子
        esSearchService = serviceApplication.application_context.get_bean("esSearchService")
        result = esSearchService.commonSearch(jsonParam=jsonRequest)
    except Exception as e:
        log.error("searchByAddress error =>" + str(e))

    return result


def start_rest_service():
    # 启动rest服务
    applicationEnvironment = serviceApplication.application_context.get_bean("applicationEnvironment")
    port = applicationEnvironment.get("project.http.rest_port")
    uvicorn.run(rest_app, host="0.0.0.0", port=port, reload=False)
