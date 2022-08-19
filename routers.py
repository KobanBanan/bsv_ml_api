from typing import List, Dict

from fastapi import APIRouter

from endpoints import _get_30_seconds_predictions, _get_give_promise_predictions, _get_keep_promise_predictions

router = APIRouter()


@router.post("/phone_30_seconds_predictions/")
async def phone_30_seconds_predictions(ids: List[int]) -> Dict[str, Dict[str, float]]:
    """
    Get phone success predictions
    :param ids: List[35146,35208,35217,... n]
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
    return await _get_30_seconds_predictions(ids)


@router.post("/give_promise_predictions/")
async def give_promise_predictions(ids: List[int]) -> Dict[str, Dict[str, float]]:
    """
    Get contact prediction
    :param ids: List[35146,35208,35217,... n]
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
    return await _get_give_promise_predictions(ids)


@router.post("/keep_promise_predictions/")
async def keep_promise_predictions(ids: List[int]) -> Dict[str, Dict[str, float]]:
    """
    Get contact prediction
    :param ids: List[35146,35208,35217,... n]
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
    return await _get_keep_promise_predictions(ids)


@router.get("/")
async def ok():
    return True
