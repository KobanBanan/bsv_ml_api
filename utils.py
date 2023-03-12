from itertools import islice

import pandas as pd
import pyodbc
from catboost import CatBoostClassifier

from consts import PERSON_ID

server = '10.168.4.148'
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
