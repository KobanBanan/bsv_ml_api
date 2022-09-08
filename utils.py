from itertools import chain, islice

import pandas as pd
from catboost import CatBoostClassifier

from consts import PERSON_ID


def predict(df_to_predict: pd.DataFrame, model: CatBoostClassifier, col=PERSON_ID) -> pd.DataFrame:
    result = pd.DataFrame({col: df_to_predict[col]})
    predictions = model.predict_proba(df_to_predict.drop(columns=[col]))
    result['1'] = predictions[:, 1]
    return result


def batch_iterable(iterable, batch_size):
    iterator = iter(iterable)
    while batch := list(islice(iterator, batch_size)):
        yield batch
