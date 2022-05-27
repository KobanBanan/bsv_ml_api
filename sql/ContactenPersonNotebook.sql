WITH BriefCaseData AS (SELECT DISTINCT bc.ClaimID,
                                       DATEDIFF(YEAR, bc.PersonBirthDate, GETDATE())          PersonAge,
                                       IIF(bc.PersonSexMale = 1, N'М', N'Ж')                  PersonSex,
                                       bc.PersonRegistrationRegion,
                                       bc.PersonRegistrationRegionNumber,
                                       bc.BriefCaseName,
                                       ISNULL(bc.LoanClassName, 'NoName') AS                  [LoanClassName],
                                       DATEDIFF(DAY, bc.ContractDate,
                                                COALESCE(bc.ContractRepaymentDate,
                                                         bc_cred.ContractRepaymentDate,
                                                         DATEADD(DAY, 30, bc.ContractDate)))  [ContractTerm],
                                       bc.ContractAmount,
                                       ISNULL(bc.OtlNalPointID, 0)        AS                  [OtlNalPointID],
                                       ISNULL(bc.ContractRegion, bc.PersonRegistrationRegion) AS [ContractRegion],
                                       UPPER(bc.ContractCity)             AS                  ContractCity,
                                       bc.InitialCreditorType,
                                       bc.MainDebtAmount + bc.PercentAmount + bc.FineAmount +
                                       bc.StateTaxAmount                                      [TotalDebtAmount],
/*                                       LawsuitSentDate,
                                       LawsuitAmount,
                                       CourtCompensationAmount,
                                       CourtDecision,
                                       CourtDecisionDate,
                                       IIF(ISNULL(DATEDIFF(MONTH, OverdueDate, EnforcementOrderReceiveDate), 0) < 0, 0,
                                           DATEDIFF(MONTH, OverdueDate, EnforcementOrderReceiveDate)) [EnforcementOrderProcessPastOverdueTerm],*/
                                       bc.BKILoanCount,
                                       bc.BKILoanOverdueCount,
                                       bc.BKIOrganizationCount,
                                       bc.BKIDebtAmount,

                                       csm.CreditID,
                                       bc_cred.ContractRepaymentDate

                       FROM reference.BriefCase bc

	                            LEFT JOIN otlnal.CreditStrahMapping csm ON bc.StrahID = csm.StrahID
	                            LEFT JOIN reference.BriefCase bc_cred ON csm.CreditID = bc_cred.CreditID
)

SELECT bc.*
FROM BriefCaseData bc

	     INNER JOIN buffer.ClaimList cl ON bc.ClaimID = cl.ClaimID



