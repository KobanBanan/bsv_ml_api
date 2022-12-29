import asyncio
import datetime
import io
import os
import random
import shutil
from functools import partial, wraps
from pathlib import Path
from typing import List, Dict

import aioodbc
import pandas as pd
from PIL import Image, UnidentifiedImageError
from catboost import CatBoostClassifier
from fastapi.responses import StreamingResponse
from tqdm import tqdm

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


async def _convert_images(image_path: str):
    """
    Convert images to jpeg in given folder
    :param image_path: full path to images
    :return:
    """

    def get_date():
        year = str(datetime.datetime.now().year)
        month = str(datetime.datetime.now().month)
        day = str(datetime.datetime.now().day)

        minute = str(datetime.datetime.now().minute)
        second = str(datetime.datetime.now().second)
        return day + '_' + month + '_' + year + ' ' + minute + ':' + second

    image_path = '/PortfolioUpload' + image_path.split('PortfolioUpload')[1]

    if not os.path.exists(image_path):
        raise 'Переданный путь не существует!'

    path = Path(image_path)
    path_to_root_folder = str(path.parent.absolute())
    path_to_original_images = os.path.join(path_to_root_folder, 'original')

    file_paths = []
    for path, subdirs, files in os.walk(image_path):
        for name in files:
            file_paths.append(os.path.join(path, name))

    logs_path = os.path.join(path_to_root_folder, 'logs')
    os.makedirs(logs_path, exist_ok=True)
    os.makedirs(path_to_original_images, exist_ok=True)

    pass_logs = {'Дата обработки': [], 'Путь к файлу': [], 'Исходное имя': [], 'Конечное имя': []}
    transform_logs = {'Дата обработки': [], 'Исходный путь': [], 'Формат исходного файла': [], 'Конечный путь': []}
    error_logs = {'Дата обработки': [], 'Исходный путь': []}

    for path in tqdm(file_paths):
        try:
            name_path, extension = os.path.splitext(path)
            old_name = path.split('/')[-1]

            new_path = name_path + '.jpg'

            if new_path in file_paths and extension != '.jpg':
                new_path = name_path + '_' + str(random.randint(1000, 9999)) + '.jpg'

            img = Image.open(path)

            if img.format not in ('JPEG', 'TIFF', 'GIF'):
                date = get_date()
                error_logs['Дата обработки'].append(date)
                error_logs['Исходный путь'].append(path)
                continue

            img.convert('RGB').save(new_path, "JPEG", quality=100)
            img.close()

            if extension != '.jpg':
                shutil.move(path, path_to_original_images)
                # os.remove(path)

            new_name = new_path.split('/')[-1]
            date = get_date()

            if img.format == 'JPEG':
                for col, value in zip(pass_logs.keys(), (date, path, old_name, new_name)):
                    pass_logs[col].append(value)
            else:
                for col, value in zip(transform_logs.keys(), (date, path, extension.replace(',', ''), new_path)):
                    transform_logs[col].append(value)

        except UnidentifiedImageError:
            date = get_date()
            error_logs['Дата обработки'].append(date)
            error_logs['Исходный путь'].append(path)

    pd.DataFrame(pass_logs).to_csv(os.path.join(logs_path, 'pass_logs.csv'))
    pd.DataFrame(transform_logs).to_csv(os.path.join(logs_path, 'transform_logs.csv'))
    pd.DataFrame(error_logs).to_csv(os.path.join(logs_path, 'error_logs.csv'))


async def _claim_motion_recommendation(claim_ids: List[int]) -> List[Dict]:
    """
    Returns claim motion recommendation placeholder
    :param claim_ids:
    :return:
    """
    easter_egg = {
        459326: {
            "claim_id": 459326,
            "claim_status": "БАНК",
            "bank_employer_details": {
                "bank_bik": "044525974"
            }
        },
        2750410: {
            "claim_id": 2750410,
            "claim_status": "РАБОТОДАТЕЛЬ",
            "bank_employer_details": {
                "employer_inn": "0326013767"
            }
        },
        3932859: {
            "claim_id": 3932859,
            "claim_status": "ФССП"
        }
    }

    res = []
    for claim_id in claim_ids:
        if easter_egg.get(claim_id):
            res.append(easter_egg.get(claim_id))
        else:
            res.append(
                {
                    "claim_id": claim_id,
                    "claim_status": "БЕЗ ВЗАИМОДЕЙСТВИЯ"
                }
            )

    return res
