CLAIM_ID = 'ClaimID'
PERSON_ID = 'PersonID'

DEFAULT_COLUMNS = ['PersonID', 'PersonAge', 'PersonGenderMale', 'PersonRegionRegistrationNumber', 'PersonContractType',
                   'MonthBetweenFirstLastContract', 'OurPersonContractCount', 'OurMinMaxContractAmountRatio',
                   'OurMinMaxTotalDebtAmountRatio', 'BKILoanOverdueCountRatio', 'BKIDebtAmount', 'BKILastPaymentInfo']

CSBI_HEADERS = {'Content-type': 'application/json', 'Key': 'b49bbf90-59d0-444f-89f4-8ebec9578b67'}
CSBI_SEND_DATA_URL = 'https://svc.csbi.ru/api/v2/query1str'
CSBI_CHECK_PACKAGE = 'https://svc.csbi.ru/api/v2/status'
CSBI_GET_DATA = 'https://svc.csbi.ru/api/v2/getdata'
RECOMMENDATIONS = 'https://collection.bsv.legal/api/ml/recommendations'
EXEC_DOCUMENT_MOTION = 'http://10.115.0.40:8080/platform/rs2/rest/endpoint/exec_document_motion'
FSSP_DEPARTMENT_LDC = 'https://legaldatacloud.ru:444/api/v2/df'
KEY = '8290e2a2-55ca-4106-9e56-90e003caa902'
