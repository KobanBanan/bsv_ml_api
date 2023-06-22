import asyncio
import datetime
import io
import json
import os
import random
import shutil
from functools import partial, wraps
from pathlib import Path
from typing import List, Dict

import aioodbc
import pandas as pd
import requests
from PIL import Image, UnidentifiedImageError
from catboost import CatBoostClassifier
from fastapi.responses import StreamingResponse
from tqdm import tqdm

from consts import DEFAULT_COLUMNS, CSBI_HEADERS, CSBI_SEND_DATA_URL, CSBI_CHECK_PACKAGE, CSBI_GET_DATA
from utils import predict, batch_iterable, get_data, push_data

dsn = "Driver=ODBC Driver 18 for SQL Server;Server=10.168.4.148;Database=ML;UID=fronzilla;PWD=GP8_4z8%8r++;" \
      "TrustServerCertificate=yes"
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
async def _get_30_seconds_predictions(ids: List[int],
                                      model: CatBoostClassifier = CatBoostClassifier().load_model('models/m30s.cbm')):
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


async def _promise_predictions(ids: List[int],
                               model: CatBoostClassifier = CatBoostClassifier().load_model('models/mgp.cbm')):
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
async def _get_give_promise_predictions(ids: List[int],
                                        model: CatBoostClassifier = CatBoostClassifier().load_model('models/mgp.cbm')):
    return await _promise_predictions(ids, model)


@clear_buffer_table('give_promise_predictions', False)
async def _get_give_promise_predictions_no_response(ids: List[int],
                                                    model: CatBoostClassifier = CatBoostClassifier().load_model(
                                                        'models/mgp.cbm')):
    return await _promise_predictions(ids, model)


@clear_buffer_table('keep_promise_predictions')
async def _get_keep_promise_predictions(ids: List[int],
                                        model: CatBoostClassifier = CatBoostClassifier().load_model('models/mkp.cbm')):
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
                shutil.move(path, path_to_original_images)  # os.remove(path)

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


async def _send_fis_request(batch_uuid: str) -> Dict:
    df_ = get_data()
    df_ = df_.loc[df_['batch_uuid'] == batch_uuid]
    df_ = df_.applymap(lambda x: x.strip() if isinstance(x, str) else x).to_dict('records')

    if not df_:
        return {"UUID": batch_uuid, "timestamp": datetime.datetime.now(), "num_elements": 0,
                "status_code": 'no data by given UUID'}

    res = []
    for s in df_:

        d = {"qr_code": s["qr_code"], "claim_status": s["claim_status"],
             "judicial_case_details": {"court_remainder": s["court_remainder"]}}

        if s["claim_status"] == "РАБОТОДАТЕЛЬ":
            d.update({"employer_details": {"employer_inn": s["employer_inn"]}})

        if s["claim_status"] == "БАНК":
            d.update({"bank_details": {"bank_bik": s['bank_bik'],
                                       'bank_answer': {'bank_answer_mask_value': s.get('bank_answer_mask_value'),
                                                       'bank_answer_mask_date': s.get('bank_answer_mask_date')}},

                      })
        if s["claim_status"] == "ФССП":
            d.update({"FSSP_details": {"FSSP_department_FIS_id": str(s['FSSP_department_FIS_id'])}})

        if s["claim_status"] == "ПФР":
            d.update({"PFR_details": {"PFR_department_FIS_id": str(s['PFR_department_FIS_id'])}})
        if s["claim_status"] not in ("РАБОТОДАТЕЛЬ", 'БАНК', 'ФССП', 'ПФР'):
            d.update({"archive_storage_details": {"storage_type": s.get("archive_storage_type")}})

        res.append(d)
    print(f'sending request with {res}')
    json_data = json.dumps(res, ensure_ascii=False, default=str).encode('utf8').decode('utf8')
    batch_sent_datetime = datetime.datetime.now()
    # response = requests.post(
    response = requests.post('http://10.115.0.40:8080/platform/rs2/rest/endpoint/exec_document_motion',
                             headers={'Content-Type': 'application/json; charset=UTF-8'},
                             json=json.dumps(res, ensure_ascii=False, default=str).encode('utf8').decode('utf8'),
                             timeout=120

                             )

    answer_received_datetime = datetime.datetime.now()
    res = {"batch_uuid": [batch_uuid], "sent_count": [len(res)], "batch_sent_datetime": [batch_sent_datetime],
           "answer_code": [response.status_code], "answer_received_datetime": [answer_received_datetime],
           "json_data": [json_data]}
    push_data(res)

    return res


async def _csbi_send_data(df_: pd.DataFrame, target):
    """
    :param df_: DataFrame with ID and AddressValue
    :param target: ["COURT", "BAILIF"]
    :return:  {
        "status_code": ... ,
        "text": ...
    }
    """
    data_package = []
    for index, row in tqdm(df_.iterrows()):
        data_package.append({"TARGET": [target], "ID_CONTRACT": row['ID'], "ADDRESS": row['AddressValue']})
    data_packages = json.dumps(data_package, ensure_ascii=False).encode('utf8')
    req_package = requests.post(CSBI_SEND_DATA_URL, headers=CSBI_HEADERS, data=data_packages)
    return {
        "status_code": req_package.status_code,
        "text": req_package.text
    }


async def _csbi_check_package(package_id: str) -> Dict:
    """
    Check package status
    :param package_id:  Package ID
    :return: {
        "status_code": ... ,
        "text": ...
    }
    """
    data = json.dumps({"PACKAGE_ID": package_id})
    req_package = requests.post(CSBI_CHECK_PACKAGE, headers=CSBI_HEADERS, data=data)
    return {
        "status_code": req_package.status_code,
        "text": req_package.text
    }


async def _csbi_get_data(package_id):
    """
    Get CSBI data by package_id
    :param package_id: Package id like 65728
    :return: pd.DataFrame with package data
    """
    package_id = json.dumps({"PACKAGE_ID": package_id})
    req_package_id = requests.post(CSBI_GET_DATA, headers=CSBI_HEADERS, data=package_id)
    data = json.loads(req_package_id.text)
    data_df = pd.DataFrame(data)
    data_df['package_id'] = package_id
    return data_df
