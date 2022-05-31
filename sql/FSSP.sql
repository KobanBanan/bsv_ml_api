SELECT [ClaimID]
      ,dateadd(day,+7,getdate()) as first_fssp
      ,DATEDIFF(YEAR, [Дата рождения], dateadd(day,+7,getdate())) as age
      ,[Размер иска без ГП]
      ,[количество уникальных организаций у которых кредитуется по бки]
      ,[сумма задолжности по всем кредитам по бки]
      ,DATEDIFF(DAY, [Дата договора], [Дата возврата по договору]) as term
      ,DATEDIFF(DAY,  [Дата возврата по договору], dateadd(day,+7,getdate())) as late
      ,DATEDIFF(DAY, [Дата отправки иска в суд], dateadd(day,+7,getdate())) as overdue_judgement
      ,[ОД Текущий] as debt_sum
      ,DATEDIFF(DAY,  [Дата получения ИЛ], dateadd(day,+7,getdate())) as overdue_execution
      ,DATEDIFF(DAY, [Дата ухода в просрочку], dateadd(day,+7,getdate())) as overdue_day
      FROM [SmallData].[dbo].[BriefCase]
      where DATEDIFF(YEAR, [Дата рождения], getdate())>=18
      and DATEDIFF(DAY, [Дата договора], [Дата возврата по договору])>=1
      and DATEDIFF(DAY, [Дата ухода в просрочку], dateadd(day,+7,getdate()))>=1
      and ClaimID IN {}