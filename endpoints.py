from functools import partial
from typing import List

import aioodbc
import pandas as pd
from catboost import CatBoostClassifier
import asyncio
from datasets import SuccessInterface, ContactInterface, CLAIM_ID
from utils import predict

dsn = 'Driver=SQL Server Native Client 11.0;Server=10.168.4.148;Database=ML;UID=fronzilla;PWD=GP8_4z8%8r++'
connect = partial(aioodbc.connect, dsn=dsn, echo=True, autocommit=True)


async def _get_phone_success_predictions(
        ids: List[int], model: CatBoostClassifier = CatBoostClassifier().load_model('models/success.cbm')
):
    """
    Get phone success predictions
    :param ids: List[1,2,3,4]
    :param loop_:
    :return:
    """
    async with connect(loop=asyncio.get_event_loop()) as conn:
        async with conn.cursor() as cur:
            with open('sql/ContactPersonNotebook.sql') as f:
                sql = f.read().format(tuple(ids))

            await cur.execute(sql)
            val = await cur.fetchall()

            return predict(pd.DataFrame.from_records(val, columns=SuccessInterface.columns()), model).set_index(
                CLAIM_ID).T.to_dict()


async def _get_contact_predictions(
        ids: List[int], model: CatBoostClassifier = CatBoostClassifier().load_model('models/contact.cbm')
):
    """
    Get contact predictions
    :param ids: List[1,2,3,4]
    :param loop_:
    :return:
    """
    async with connect(loop=asyncio.get_event_loop()) as conn:
        async with conn.cursor() as cur:
            with open('sql/ContactPersonNotebook.sql') as f:
                sql = f.read().format(tuple(ids))

            await cur.execute(sql)
            val = await cur.fetchall()

            return predict(pd.DataFrame.from_records(val, columns=ContactInterface.columns()), model).set_index(
                CLAIM_ID).T.to_dict()
