import asyncio
import io
from functools import partial, wraps
from typing import List

import aioodbc
import pandas as pd
from catboost import CatBoostClassifier
from fastapi.responses import StreamingResponse

from consts import DEFAULT_COLUMNS
from utils import predict, batch_iterable

dsn = 'Driver=ODBC Driver 18 for SQL Server;Server=10.168.4.148;Database=ML;UID=fronzilla;PWD=GP8_4z8%8r++;TrustServerCertificate=yes'
connect = partial(aioodbc.connect, dsn=dsn, echo=True, autocommit=True)


def clear_buffer_table(file_name: str, return_response=True):
    def wrapper(func):
        @wraps(func)
        async def wrapped(*args):
            async with connect(loop=asyncio.get_event_loop()) as conn:
                async with conn.cursor() as cur:
                    await cur.execute('DELETE FROM ML.buffer.ClaimListAPI')
                    result = await func(*args)
                    if not return_response:
                        return result
                    print('Streaming response')
                    response = StreamingResponse(io.StringIO(result.to_csv(index=False)), media_type="text/csv")
                    response.headers["Content-Disposition"] = f"attachment; filename={file_name}.csv"
                    return response

        return wrapped

    return wrapper


@clear_buffer_table('30_seconds_predictions')
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
            print('Preparing batches')
            batches = batch_iterable(ids, 1000)
            b = 0
            for bath in batches:
                query = f"INSERT INTO ML.buffer.ClaimListAPI VALUES {' ,'.join([f'({b})' for b in bath])}"
                await cur.execute(query)
                b += 1
                print(f'Inserted batch number: {b}')
            print('Executing select query')
            await cur.execute(sql)
            val = await cur.fetchall()
            print('Predicting')
            return predict(pd.DataFrame.from_records(val, columns=DEFAULT_COLUMNS).dropna(), model)


async def _promise_predictions(
        ids: List[int], model: CatBoostClassifier = CatBoostClassifier().load_model('models/mgp.cbm')
):
    """
    Get contact predictions
    :param model:
    :param ids: List[1,2,3,4]
    :return:
    """
    async with connect(loop=asyncio.get_event_loop()) as conn:
        async with conn.cursor() as cur:
            with open('sql/mgp.sql') as f:
                sql = f.read()
            print('Preparing batches')
            batches = batch_iterable(ids, 1000)
            b = 0
            for bath in batches:
                query = f"INSERT INTO ML.buffer.ClaimListAPI VALUES {' ,'.join([f'({b})' for b in bath])}"
                await cur.execute(query)
                b += 1
                print(f'Inserted batch number: {b}')
            print('Executing select query')
            await cur.execute(sql)
            val = await cur.fetchall()
            print('Predicting')
            return predict(pd.DataFrame.from_records(val, columns=DEFAULT_COLUMNS).dropna(), model)


@clear_buffer_table('give_promise_predictions')
async def _get_give_promise_predictions(
        ids: List[int],
        model: CatBoostClassifier = CatBoostClassifier().load_model('models/mgp.cbm')
):
    return await _promise_predictions(ids, model)


@clear_buffer_table('give_promise_predictions', False)
async def _get_give_promise_predictions_no_response(
        ids: List[int],
        model: CatBoostClassifier = CatBoostClassifier().load_model('models/mgp.cbm')
):
    return await _promise_predictions(ids, model)


@clear_buffer_table('keep_promise_predictions')
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

            print('Preparing batches')
            batches = batch_iterable(ids, 1000)
            b = 0
            for bath in batches:
                query = f"INSERT INTO ML.buffer.ClaimListAPI VALUES {' ,'.join([f'({b})' for b in bath])}"
                await cur.execute(query)
                b += 1
                print(f'Inserted batch number: {b}')
            print('Executing select query')
            await cur.execute(sql)
            val = await cur.fetchall()

            df = pd.DataFrame.from_records(val, columns=DEFAULT_COLUMNS).dropna()
            print('Predicting for give promise model')
            give_promise_predictions = await _get_give_promise_predictions_no_response(ids)
            df['MGPProbeValue'] = df['PersonID'].apply(lambda x: give_promise_predictions.get(x, {}).get('1', 0))
            print('Predicting')
            return predict(df, model)
