import asyncio
from functools import partial, wraps
from typing import List

import aioodbc
import pandas as pd
from catboost import CatBoostClassifier

from consts import DEFAULT_COLUMNS, PERSON_ID
from utils import predict

dsn = 'Driver=SQL Server Native Client 11.0;Server=10.168.4.148;Database=ML;UID=fronzilla;PWD=GP8_4z8%8r++'
connect = partial(aioodbc.connect, dsn=dsn, echo=True, autocommit=True)


def clear_buffer_table():
    def wrapper(func):
        @wraps(func)
        async def wrapped(*args):
            async with connect(loop=asyncio.get_event_loop()) as conn:
                async with conn.cursor() as cur:
                    await cur.execute('DELETE FROM ML.buffer.ClaimListAPI')
                    return await func(*args)

        return wrapped

    return wrapper


@clear_buffer_table()
async def _get_30_seconds_predictions(
        ids: List[int], model: CatBoostClassifier = CatBoostClassifier().load_model('models/m30s.cbm')
):
    """
    Get phone 30 seconds call success predictions
    :param ids: List[1,2,3,4]
    :return:
    """
    async with connect(loop=asyncio.get_event_loop()) as conn:
        async with conn.cursor() as cur:
            with open('sql/m30s.sql') as f:
                sql = f.read()

            ids = [[x] for x in ids]
            await cur.executemany('INSERT INTO ML.buffer.ClaimListAPI (ClaimID) VALUES (?);', ids)
            await cur.execute(sql)
            val = await cur.fetchall()

            return predict(
                pd.DataFrame.from_records(val, columns=DEFAULT_COLUMNS).dropna(), model
            ).set_index(PERSON_ID).T.to_dict()


@clear_buffer_table()
async def _get_give_promise_predictions(
        ids: List[int], model: CatBoostClassifier = CatBoostClassifier().load_model('models/mgp.cbm')
):
    """
    Get contact predictions
    :param ids: List[1,2,3,4]
    :return:
    """
    async with connect(loop=asyncio.get_event_loop()) as conn:
        async with conn.cursor() as cur:
            with open('sql/mgp.sql') as f:
                sql = f.read()

            ids = [[x] for x in ids]
            await cur.executemany('INSERT INTO ML.buffer.ClaimListAPI (ClaimID) VALUES (?);', ids)
            await cur.execute(sql)
            val = await cur.fetchall()

            return predict(
                pd.DataFrame.from_records(val, columns=DEFAULT_COLUMNS).dropna(), model
            ).set_index(PERSON_ID).T.to_dict()


@clear_buffer_table()
async def _get_keep_promise_predictions(
        ids: List[int], model: CatBoostClassifier = CatBoostClassifier().load_model('models/mkp.cbm')
):
    """
    Get contact predictions
    :param ids: List[1,2,3,4]
    :return:
    """

    async with connect(loop=asyncio.get_event_loop()) as conn:
        async with conn.cursor() as cur:
            with open('sql/mkp.sql') as f:
                sql = f.read()

            ids_ = [[x] for x in ids]
            await cur.executemany('INSERT INTO ML.buffer.ClaimListAPI (ClaimID) VALUES (?);', ids_)
            await cur.execute(sql)
            val = await cur.fetchall()

            df = pd.DataFrame.from_records(val, columns=DEFAULT_COLUMNS).dropna()
            give_promise_predictions = await _get_give_promise_predictions(ids)
            df['MGPProbeValue'] = df['PersonID'].apply(lambda x: give_promise_predictions.get(x, {}).get('1', 0))
            return predict(df, model).set_index(PERSON_ID).T.to_dict()
