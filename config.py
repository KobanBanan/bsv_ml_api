import asyncio
import aioodbc
import pandas as pd
from catboost import CatBoostClassifier
import pandas
from utils import predict

loop = asyncio.get_event_loop()

from functools import partial

dsn = 'Driver=SQL Server Native Client 11.0;Server=10.168.4.148;Database=ML;UID=fronzilla;PWD=GP8_4z8%8r++'

cols = ['ClaimID',
         'PersonAge',
         'PersonSex',
         'PersonRegistrationRegionNumber',
         'BriefCaseName',
         'LoanClassName',
         'ContractTerm',
         'ContractAmount',
         'OtlNalPointID',
         'ContractRegion',
         'ContractCity',
         'InitialCreditorType',
         'TotalDebtAmount',
         'BKILoanCount',
         'BKILoanOverdueCount',
         'BKIOrganizationCount',
         'BKIDebtAmount']


model = CatBoostClassifier().load_model('models/contact.cbm')

# Sometimes you may want to reuse same connection parameters multiple times.
# This can be accomplished in a way below using partial function
connect = partial(aioodbc.connect, dsn=dsn, echo=True, autocommit=True)


async def test_example(loop=None):
    async with connect(loop=loop) as conn:
        async with conn.cursor() as cur:
            id_list = tuple([35146, 35208, 35217, 35229, 35286, 35307, 35321, 35357, 35364])
            with open('sql/ContactenPersonNotebook.sql') as f:
                sql = f.read().format(id_list)

            await cur.execute(sql)
            val = await cur.fetchall()

            return predict(pd.DataFrame.from_records(val, columns=cols), model).set_index('ClaimID').T.to_dict()


loop.run_until_complete(test_example(loop))
