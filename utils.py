import pandas as pd
from catboost import CatBoostClassifier


def predict(df_to_predict: pd.DataFrame, model: CatBoostClassifier) -> pd.DataFrame:
    result = pd.DataFrame({'ClaimID': df_to_predict['ClaimID']})
    result[['0', '1']] = model.predict_proba(df_to_predict.drop(columns=['ClaimID']))
    return result
