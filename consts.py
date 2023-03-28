CLAIM_ID = 'ClaimID'
PERSON_ID = 'PersonID'

DEFAULT_COLUMNS = ['PersonID', 'PersonAge', 'PersonGenderMale', 'PersonRegionRegistrationNumber', 'PersonContractType',
                   'MonthBetweenFirstLastContract', 'OurPersonContractCount', 'OurMinMaxContractAmountRatio',
                   'OurMinMaxTotalDebtAmountRatio', 'BKILoanOverdueCountRatio', 'BKIDebtAmount', 'BKILastPaymentInfo']

CSBI_HEADERS = {'Content-type': 'application/json', 'Key': 'b49bbf90-59d0-444f-89f4-8ebec9578b67'}
CSBI_SEND_DATA_URL = 'https://svc.csbi.ru/api/v2/query1str'