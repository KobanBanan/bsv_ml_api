WITH RawBriefCaseTable AS
	     (SELECT DISTINCT PersonID,
	                      CreditID,
	                      ThirdID,
	                      StrahID,
	                      ContractDate,
	                      ContractAmount,
	                      MainDebtAmount,
	                      PercentAmount,
	                      FineAmount,
	                      ISNULL(MainDebtAmount, 0) + ISNULL(PercentAmount, 0) +
	                      ISNULL(FineAmount, 0)                  [TotalDebtAmount],
	                      CAST(BKILoanCount AS DECIMAL(5, 1)) AS [BKILoanCount],
	                      BKILoanOverdueCount,
	                      BKIOrganizationCount,
	                      BKIDebtAmount,
	                      BKILastPaymentDate
	      FROM reference.BriefCase bc

		           INNER JOIN buffer.ClaimListAPI cl ON bc.ClaimID = cl.ClaimID

	      WHERE ContractAmount > 1
		    AND (ISNULL(MainDebtAmount, 0) + ISNULL(PercentAmount, 0) +
		         ISNULL(FineAmount, 0)) > 1),

     PersonContractInfo AS
	     (SELECT PersonID,
	             COUNT(CreditID) [CreditCount],
	             COUNT(ThirdID)  [ThirdCount],
	             COUNT(StrahID)  [InsuranceCount]

	      FROM RawBriefCaseTable bc

	      GROUP BY PersonID),

     PersonContractType AS
	     (SELECT PersonID,
	             CASE
		             WHEN CreditCount > 0 AND ThirdCount > 0 AND InsuranceCount > 0 AND ThirdCount != InsuranceCount
			             THEN 7 --'Mixed'

		             WHEN CreditCount > 0 AND InsuranceCount = 0 AND ThirdCount = 0 THEN 1 --'ON Only'
		             WHEN CreditCount = 0 AND ThirdCount > 0 AND InsuranceCount = 0 THEN 2 --'Third Only'
		             WHEN CreditCount = 0 AND InsuranceCount > 0 AND ThirdCount = InsuranceCount
			             THEN 3 --'Insurance Only'

		             WHEN CreditCount > 0 AND ThirdCount > 0 AND InsuranceCount = 0 THEN 4 --'ON And Third'
		             WHEN CreditCount > 0 AND InsuranceCount > 0 AND ThirdCount = InsuranceCount
			             THEN 5 --'ON And Insurance'

		             WHEN CreditCount = 0 AND ThirdCount > 0 AND InsuranceCount > 0 AND ThirdCount != InsuranceCount
			             THEN 6 --'Third And Insurance'

		             ELSE
			             999 --'HZ'
	             END [PersonContractType]

	      FROM PersonContractInfo),

     DataSet AS
	     (SELECT DISTINCT bc.PersonID,
	                      DATEDIFF(YEAR, pers.PersonBirthDate, GETDATE())                            [PersonAge],
	                      CAST(pers.PersonGenderMale AS TinyInt)                                  AS PersonGenderMale,
	                      pers.PersonRegionRegistrationNumber,
	                      pct.PersonContractType,

	                      DATEDIFF(MONTH, MIN(ContractDate), MAX(ContractDate))                      [MonthBetweenFirstLastContract],

	                      COUNT(*)                                                                   [OurPersonContractCount],

	                      MAX(ContractAmount) / MIN(ContractAmount)                                  [OurMinMaxContractAmountRatio],

	                      MAX(TotalDebtAmount) / MIN(TotalDebtAmount)                                [OurMinMaxTotalDebtAmountRatio],

	                      CAST(IIF(SUM(BKILoanCount) = 0, 0, SUM(BKILoanOverdueCount) /
	                                                         SUM(BKILoanCount)) AS Decimal(7, 5)) AS [BKILoanOverdueCountRatio],

	                      SUM(BKIDebtAmount)                                                      AS [BKIDebtAmount],

	                      IIF(MAX(BKILastPaymentDate) IS NULL, 0, 1)                                 [BKILastPaymentInfo]

	      FROM RawBriefCaseTable bc

		           LEFT JOIN otlnal.Person pers ON bc.PersonID = pers.PersonID
		           LEFT JOIN PersonContractType pct ON bc.PersonID = pct.PersonID

	      GROUP BY bc.PersonID, pers.PersonBirthDate, pers.PersonGenderMale,
	               pers.PersonRegionRegistrationNumber,
	               pct.PersonContractType)

SELECT DISTINCT ds.* FROM DataSet ds ORDER BY PersonID
