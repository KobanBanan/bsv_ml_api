import concurrent
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

import pandas as pd
import pyodbc
import requests
from catboost import CatBoostClassifier

from consts import PERSON_ID, FSSP_DEPARTMENT_LDC

server = '10.115.0.64'
database = 'ML'
username = 'fronzilla'
password = 'GP8_4z8%8r++'


def predict(df_to_predict: pd.DataFrame, model: CatBoostClassifier, col=PERSON_ID) -> pd.DataFrame:
    result = pd.DataFrame({col: df_to_predict[col]})
    predictions = model.predict_proba(df_to_predict.drop(columns=[col]))
    result['1'] = predictions[:, 1]
    return result


def batch_iterable(iterable, batch_size):
    iterator = iter(iterable)
    while batch := list(islice(iterator, batch_size)):
        yield batch


def get_data():
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};TrustServerCertificate=yes;SERVER=' + server + ';DATABASE=' + database + ';ENCRYPT=yes;UID=' + username + ';PWD=' + password)

    sql = "SELECT * FROM buffer.FIS_push_data;"
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df


def push_data(data):
    data = pd.DataFrame(data)
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};TrustServerCertificate=yes;SERVER=' + server + ';DATABASE=' + database + ';ENCRYPT=yes;UID=' + username + ';PWD=' + password)
    curr = cnxn.cursor()
    for index, row in data.iterrows():
        curr.execute(
            "INSERT INTO buffer.FIS_push_log (batch_uuid,sent_count,batch_sent_datetime, answer_code,answer_received_datetime,json_data) values(?,?,?,?,?,?)",
            row.batch_uuid, row.sent_count, row.batch_sent_datetime, row.answer_code, row.answer_received_datetime,
            row.json_data)
    curr.commit()
    cnxn.close()


def make_api_request(id_value, address, debt_amount, api_key):
    headers = {'Content-Type': 'application/json'}
    payload = {
        'API ключ': api_key,
        'Адрес': address,
        'Сумма долга': debt_amount
    }

    response = requests.post(FSSP_DEPARTMENT_LDC, json=payload, headers=headers,  timeout=(60, 180))
    if response.status_code == 400:
        return id_value, None  # Or handle it in a way that fits the application logic

    return id_value, response.json()


def create_requests(df, api_key):
    # Convert DataFrame to a list of dictionaries for easier iteration
    records = df.to_dict('records')

    # List to hold final results
    results = []

    # Function to process each record in parallel
    def process_record(record):
        id_value, address, debt_amount = record['ID'], record['AddressValue'], record['court_remainder']
        return make_api_request(id_value, address, debt_amount, api_key)

    # Use ThreadPoolExecutor to make requests in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_record = {executor.submit(process_record, record): record for record in records}

        for future in concurrent.futures.as_completed(future_to_record):
            result = future.result()
            if result:
                results.append(result)

    # Creating a new DataFrame from results
    results_df = pd.DataFrame(results, columns=['ID', 'API_Result'])

    # Merging results back with the original DataFrame on ID
    merged_df = pd.merge(df, results_df, on='ID', how='left')

    return merged_df
