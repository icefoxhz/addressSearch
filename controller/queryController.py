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


def do_search(esSearchService, resultListDict):
    result = {}
    succeed = False
    try:
        isAccurate_list = [True, False]
        for isAccurate in isAccurate_list:
            succeed, result = esSearchService.searchByAddress(resultListDict, isAccurate)
            if succeed:
                break
    except Exception as e:
        log.error("searchByAddress error =>" + str(e))
    return result, succeed


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
    # 这里是获取bean的例子
    esSearchService = serviceApplication.application_context.get_bean("esSearchService")
    resultListDict = esSearchService.parse(jsonRequest)

    result, succeed = do_search(esSearchService, resultListDict)
    if not succeed:
        should_do = False
        for symbol, v in RE_DO_REPLACE_SYMBOLS.items():
            for idx, address in jsonRequest.items():
                if address.find(symbol) >= 0:
                    jsonRequest[idx] = address.replace(symbol, v)
                    should_do = True
        if should_do:
            result, succeed = do_search(esSearchService, resultListDict)

    return result


def genRestResult(result):
    for k, v in result.items():
        result[k] = {
            "msg": "nothing" if v == "" or v is None or len(v) == 0 else "success",
            "data": {} if v == "" or v is None or len(v) == 0 else v,
            "code": 404 if v == "" or v is None or len(v) == 0 else 200
        }

    return json.dumps(result, ensure_ascii=False)


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
    result = {}
    try:
        # 这里是获取bean的例子
        esSearchService = serviceApplication.application_context.get_bean("esSearchService")
        result = esSearchService.searchByPoint(jsonRequest)
    except Exception as e:
        log.error("searchByAddress error =>" + str(e))

    return genRestResult(result)


@rest_app.post("/search")
async def appSearchByAddress(request: Request):
    result = {}
    try:
        jsonRequest = await request.json()

        # 这里是获取bean的例子
        esSearchService = serviceApplication.application_context.get_bean("esSearchService")
        result = esSearchService.commonSearch(jsonRequest)
    except Exception as e:
        log.error("searchByAddress error =>" + str(e))
        result["msg"] = str(e)
        result["code"] = 500

    # return json.dumps(result, ensure_ascii=False)
    return result


def start_rest_service(port):
    # 启动rest服务
    uvicorn.run(rest_app, host="0.0.0.0", port=port, reload=False)
