from typing import List, Dict
import asyncio
from fastapi import APIRouter

from endpoints import _get_phone_success_predictions, _get_contact_predictions

router = APIRouter()


@router.get("/phone_success_predictions/")
async def phone_success_predictions(ids: List[int]) -> Dict:
    return await _get_phone_success_predictions(ids, asyncio.get_event_loop())


@router.get("/contact_predictions/")
async def contact_predictions(ids: List[int]) -> Dict:
    return await _get_contact_predictions(ids, asyncio.get_event_loop())
