import asyncio
from io import BytesIO
from typing import Dict, List
import json
import pandas as pd
from fastapi import APIRouter, UploadFile, File, Query
from fastapi.responses import StreamingResponse, JSONResponse

from endpoints import _get_30_seconds_predictions, _get_give_promise_predictions, _get_keep_promise_predictions, \
    _convert_images, _send_fis_request, _csbi_send_data

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
async def send_fis_request(batch_uuid: str) -> JSONResponse:
    """
    Sent fis request
    :param batch_uuid: UUID like 4E13FF89-D125-47B2-A380-AE0F49BF8B32
    :return: Status code int
    """
    res = await _send_fis_request(batch_uuid)
    res = json.dumps(res, default=str, ensure_ascii=False)
    return JSONResponse(content=res)


@router.post("/csbi_send_data", tags=["CSBI"])
async def csbi_send_data(file: UploadFile = File(...), target: str = Query("target", enum=["COURT", "BAILIF"])) -> Dict:
    """
    Sent batch to csbi
    :param target: enum=["COURT", "BAILIF"]
    :param file: excel file
    """
    contents = file.file.read()
    buffer = BytesIO(contents)
    df = pd.read_excel(buffer, engine='openpyxl')
    return await _csbi_send_data(df, target)


@router.get("/")
async def ok():
    return True
