import asyncio
from io import BytesIO
from typing import Dict, List

import pandas as pd
from fastapi import APIRouter, UploadFile, File
from fastapi.responses import StreamingResponse

from endpoints import _get_30_seconds_predictions, _get_give_promise_predictions, _get_keep_promise_predictions, \
    _convert_images, _claim_motion_recommendation

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


@router.post("/convert_images", tags=["Other"])
async def convert_images(path_to_folder: str):
    """
    Get contact prediction
    :param path_to_folder: full path to folder
    """
    asyncio.ensure_future(_convert_images(path_to_folder))


@router.post("/claim_motion_recommendation", tags=["Other"])
async def claim_motion_recommendation(claim_ids: List[int]) -> List[Dict]:
    """
    Claim motion recommendation placeholder
    :param claim_ids:
    :return:
    """
    return await _claim_motion_recommendation(claim_ids)


@router.get("/")
async def ok():
    return True
