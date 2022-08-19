CLAIM_ID = 'ClaimID'
from dataclasses import dataclass
from typing import List

DEFAULT_COLUMNS = ['PersonID', 'PersonAge', 'PersonGenderMale', 'PersonRegionRegistrationNumber', 'PersonContractType',
                   'MonthBetweenFirstLastContract', 'OurPersonContractCount', 'OurMinMaxContractAmountRatio',
                   'OurMinMaxTotalDebtAmountRatio', 'BKILoanOverdueCountRatio', 'BKIDebtAmount', 'BKILastPaymentInfo']


@dataclass
class Phone30SecondsInterface:
    columns: List[str]

    def __post_init__(self):
        self.columns = DEFAULT_COLUMNS


@dataclass
class GivePromiseInterface:
    columns: List[str]

    def __post_init__(self):
        self.columns = DEFAULT_COLUMNS


@dataclass
class KeepPromiseInterface:
    columns: List[str]

    def __post_init__(self):
        self.columns = DEFAULT_COLUMNS
