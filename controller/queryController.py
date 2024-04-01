import asyncio

import uvicorn
from fastapi import FastAPI, Request
from pySimpleSpringFramework.spring_core.log import log
from starlette.responses import JSONResponse
from uvicorn import Config

from addressSearch.entrypoint.applicationStarter import serviceApplication
from addressSearch.enums.dbOperator import RestRet

# 在这里导入自己的serviceApplication实例

# 设置最大并发数
semaphore = asyncio.Semaphore(10000)

rest_app = FastAPI()


def _make_rest_result(key, resultDict, error):
    result = {
        "msg": str(error),
        "data": {},
        "code": RestRet.FAILED.value
    }

    if error is None or str(error) == "":
        result["msg"] = "succeed"
        result["data"] = resultDict
        result["code"] = RestRet.SUCCEED.value

    # return json.dumps(result, ensure_ascii=False)
    return {key: result}


@rest_app.middleware("http")
async def limit_concurrency(request: Request, call_next):
    """
    限制並發
    """
    async with semaphore:
        response = await call_next(request)
    return response


@rest_app.exception_handler(OSError)
async def handle_os_error(request, exc):
    """
     異常處理
    """
    log.error(f"OSError caught: {exc}")
    return JSONResponse(status_code=500, content={"message": "Internal server error"})


# ================================================================================

@rest_app.post("/searchByAddress")
async def searchByAddress(request: Request):
    """
    参数格式
    {
        "1": "无锡市惠山区洛社镇五秦村强巷52号"
    }

    :param request:
    :return:
    """
    jsonRequest = await request.json()
    key = list(jsonRequest.keys())[0]
    address_string = list(jsonRequest.values())[0]

    esSearchService = serviceApplication.application_context.get_bean("esSearchService")
    succeed, result = esSearchService.run_address_search(address_string)
    return _make_rest_result(key, result, "未找到" if not succeed else None)


@rest_app.post("/searchByAddressEx")
async def searchByAddressEx(request: Request):
    """
    参数格式
    {
        "1": "无锡市惠山区洛社镇五秦村强巷52号"
    }

    :param request:
    :return:
    """
    jsonRequest = await request.json()
    key = list(jsonRequest.keys())[0]
    address_string = list(jsonRequest.values())[0]

    esSearchService = serviceApplication.application_context.get_bean("esSearchService")
    esSearchService.set_return_multi()
    succeed, result = esSearchService.run_address_search(address_string)
    return _make_rest_result(key, result, "未找到" if not succeed else None)


@rest_app.post("/searchByPoint")
async def searchByPoint(request: Request):
    """
    参数格式
    {
        "1": "119.87630533652268,31.31180405900834"
    }
    :param request:
    :return:
    """
    jsonRequest = await request.json()
    key = list(jsonRequest.keys())[0]
    points_string = list(jsonRequest.values())[0]

    esSearchService = serviceApplication.application_context.get_bean("esSearchService")
    succeed, result = esSearchService.run_search_by_point(points_string)
    return _make_rest_result(key, result, "未找到" if not succeed else None)


@rest_app.post("/searchByPointEx")
async def searchByPointEx(request: Request):
    """
    参数格式
    {
        "1": "119.87630533652268,31.31180405900834"
    }
    :param request:
    :return:
    """
    jsonRequest = await request.json()
    key = list(jsonRequest.keys())[0]
    points_string = list(jsonRequest.values())[0]

    esSearchService = serviceApplication.application_context.get_bean("esSearchService")
    esSearchService.set_return_multi()
    succeed, result = esSearchService.run_search_by_point(points_string)
    return _make_rest_result(key, result, "未找到" if not succeed else None)


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
            "code": RestRet.SUCCEED.value
        }
    except Exception as e:
        log.error("reset error =>" + str(e))
        return {
            "msg": str(e),
            "code": RestRet.FAILED.value
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
            "code": RestRet.SUCCEED.value if ret else RestRet.FAILED.value
        }

    except Exception as e:
        log.error("reset error =>" + str(e))
        return {
            "msg": str(e),
            "code": RestRet.FAILED.value
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
