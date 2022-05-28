CLAIM_ID = 'ClaimID'


class ContactInterface:
    @classmethod
    def columns(cls):
        return ['ClaimID', 'PersonAge', 'PersonSex', 'PersonRegistrationRegionNumber', 'BriefCaseName', 'LoanClassName',
                'ContractTerm', 'ContractAmount', 'OtlNalPointID', 'ContractRegion', 'ContractCity',
                'InitialCreditorType', 'TotalDebtAmount', 'BKILoanCount', 'BKILoanOverdueCount', 'BKIOrganizationCount',
                'BKIDebtAmount']


class SuccessInterface:
    @classmethod
    def columns(cls):
        return ['ClaimID', 'PersonAge', 'PersonSex', 'PersonRegistrationRegionNumber', 'BriefCaseName', 'LoanClassName',
                'ContractTerm', 'ContractAmount', 'OtlNalPointID', 'ContractRegion', 'ContractCity',
                'InitialCreditorType', 'TotalDebtAmount', 'BKILoanCount', 'BKILoanOverdueCount', 'BKIOrganizationCount',
                'BKIDebtAmount']
