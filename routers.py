import asyncio
import json
from io import BytesIO
from typing import Dict

import pandas as pd
from fastapi import APIRouter, UploadFile, File, Query
from fastapi.responses import StreamingResponse, JSONResponse

from consts import RECOMMENDATIONS, EXEC_DOCUMENT_MOTION
from endpoints import _get_30_seconds_predictions, _get_give_promise_predictions, _get_keep_promise_predictions, \
    _convert_images, _send_fis_request, _csbi_send_data, _csbi_check_package, _csbi_get_data, \
    _get_claim_motion_recommendation, _get_fssp_department_ldc

router = APIRouter()


@router.post("/phone_30_seconds_predictions", tags=["ML"])
async def phone_30_seconds_predictions(file: UploadFile = File(...)) -> StreamingResponse:
    """
    Get phone success predictions
    :param file: file
    :return: Predictions like
       {
          "35146": {
            "0": 0.9870931870846715,
            "1": 0.012906812915328485
          },
          "35208": {
            "0": 0.9819173452768191,
            "1": 0.01808265472318095
          },
          "35217": {
            "0": 0.9913129540467149,
            "1": 0.008687045953285098
          }
        }
    """
    contents = file.file.read()
    buffer = BytesIO(contents)
    df = pd.read_excel(buffer, engine='openpyxl')
    return await _get_30_seconds_predictions(df['ClaimID'].tolist())


@router.post("/give_promise_predictions", tags=["ML"])
async def give_promise_predictions(file: UploadFile = File(...)) -> Dict[str, Dict[str, float]]:
    """
    Get contact prediction
    :param file: excel file
    :return: Predictions like
       {
          "35146": {
            "0": 0.9870931870846715,
            "1": 0.012906812915328485
          },
          "35208": {
            "0": 0.9819173452768191,
            "1": 0.01808265472318095
          },
          "35217": {
            "0": 0.9913129540467149,
            "1": 0.008687045953285098
          }
        }
    """
    contents = file.file.read()
    buffer = BytesIO(contents)
    df = pd.read_excel(buffer, engine='openpyxl')
    return await _get_give_promise_predictions(df['ClaimID'].tolist())


@router.post("/keep_promise_predictions", tags=["ML"])
async def keep_promise_predictions(file: UploadFile = File(...)) -> Dict[str, Dict[str, float]]:
    """
    Get contact prediction
    :param file:excel file
    :return: Predictions like
       {
          "35146": {
            "0": 0.9870931870846715,
            "1": 0.012906812915328485
          },
          "35208": {
            "0": 0.9819173452768191,
            "1": 0.01808265472318095
          },
          "35217": {
            "0": 0.9913129540467149,
            "1": 0.008687045953285098
          }
        }
    """
    contents = file.file.read()
    buffer = BytesIO(contents)
    df = pd.read_excel(buffer, engine='openpyxl')
    return await _get_keep_promise_predictions(df['ClaimID'].tolist())


@router.post("/convert_images", tags=["OTHER"])
async def convert_images(path_to_folder: str):
    """
    Get contact prediction
    :param path_to_folder: full path to folder
    """
    asyncio.ensure_future(_convert_images(path_to_folder))


@router.post("/send_fis_request", tags=["FIS"])
async def send_fis_request(
        batch_uuid: str,
        endpoint: str = Query(default=EXEC_DOCUMENT_MOTION, enum=[EXEC_DOCUMENT_MOTION, RECOMMENDATIONS])
) -> JSONResponse:
    """
    Sent fis request
    :param endpoint: target endpoint
    :param batch_uuid: UUID like 4E13FF89-D125-47B2-A380-AE0F49BF8B32
    :return: Status code int
    """
    res = await _send_fis_request(batch_uuid, endpoint) if endpoint == RECOMMENDATIONS \
        else json.dumps(await _send_fis_request(batch_uuid, endpoint), ensure_ascii=False)
    return JSONResponse(content=res)


@router.post("/csbi_send_data", tags=["CSBI"])
async def csbi_send_data(
        file: UploadFile = File(...),
        target: str = Query("target", enum=["COURT", "BAILIFF", "MAGISTRATE"])
) -> Dict:
    """
    Sent batch to csbi
    :param target: Enum=["COURT", "BAILIF"]
    :param file: Excel file with ID and AddressValue columns
    """
    contents = file.file.read()
    buffer = BytesIO(contents)
    df = pd.read_excel(buffer, engine='openpyxl')
    return await _csbi_send_data(df, target)


@router.post("/csbi_check_package", tags=["CSBI"])
async def csbi_check_package(package_id: str) -> Dict:
    """
    Check package id status
    :param package_id: Package id like 65728
    """
    return await _csbi_check_package(package_id)


@router.post("/csbi_get_data", tags=["CSBI"])
async def csbi_get_data(package_id: str) -> StreamingResponse:
    """
    Get data by package id
    :param package_id: Package id like 65728
    :return StreamingResponse to .csv
    """

    df = await _csbi_get_data(package_id)

    response = StreamingResponse(
        iter([df.to_csv(index=False)]),
        media_type="application/octet-stream",
    )
    response.headers["Content-Disposition"] = f"attachment; filename=csbi_{package_id}.csv"

    return response


@router.post("/get_claim_motion_recommendation", tags=["CSBI"])
async def get_claim_motion_recommendation(file: UploadFile = File(...)) -> StreamingResponse:
    """ Excel with claim_id column """
    contents = file.file.read()
    buffer = BytesIO(contents)
    df = pd.read_excel(buffer, engine='openpyxl')
    result = await _get_claim_motion_recommendation(df)

    response = StreamingResponse(
        iter([result.to_csv(index=False)]),
        media_type="application/octet-stream",
    )
    response.headers["Content-Disposition"] = f"attachment; filename=claim_motion_recommendation.csv"

    return response


@router.post("/get_fssp_department_ldc", tags=["FSSP"])
async def get_fssp_department_ldc(file: UploadFile = File(...)) -> StreamingResponse:
    """ """
    contents = file.file.read()
    buffer = BytesIO(contents)
    df = pd.read_excel(buffer, engine='openpyxl')

    result = await _get_fssp_department_ldc(df)

    response = StreamingResponse(
        iter([result.to_csv(index=False)]),
        media_type="application/octet-stream",
    )
    response.headers["Content-Disposition"] = f"attachment; filename=fssp_department_ldc.csv"

    return response


@router.get("/")
async def ok():
    return True
