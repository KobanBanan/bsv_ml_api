from typing import List, Dict

from fastapi import APIRouter

from endpoints import _get_phone_success_predictions, _get_contact_predictions

router = APIRouter()


@router.post("/phone_success_predictions/")
async def phone_success_predictions(ids: List[int]) -> Dict[str, Dict[str, float]]:
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
    return await _get_phone_success_predictions(ids)


@router.post("/contact_predictions/")
async def contact_predictions(ids: List[int]) -> Dict[str, Dict[str, float]]:
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
    return await _get_contact_predictions(ids)
