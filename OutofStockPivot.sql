USE [QROI]
SELECT [store],
	   [sku],
       [sku/size],
       [Store group],
       [CurrUnitRetailPrice],
       [DaysinStock],
	   [OHUInstallDate],
       [OutofStockDate],
       [DaysinStockwSKUAvailable],
       [FromInstallToOOS],
       [DaysSinceInstall],
       [OutofStockDays],
       [TotalSales],
       [AvgSalesPerDay],
       [LostSalesOpportunity],
       [DaysTillMarkdown],
       [PredictedMarkdownLossUnits],
       CASE
           WHEN predictedmarkdownlossunits > 0 THEN (predictedmarkdownlossunits * currunitretailprice) * 0.2
           ELSE 0
       END AS 'PredictedMarkdownLossCost',
       DaysOOSAfterDCOOS,
       ROUND((DaysOOSAfterDCOOS * AvgSalesPerDay * CurrUnitRetailPrice),2) AS 'MissedOpportunity'
FROM
  (SELECT *,
          ROUND((CASE
               WHEN outofstockdays > 0 THEN avgsalesperday * currunitretailprice * outofstockdays
               ELSE 0
           END),2) AS 'LostSalesOpportunity',
          84 - dayssinceinstall AS DaysTillMarkdown,
          (CASE
               WHEN 84 - dayssinceinstall <= 0 THEN LastDayUnits
               WHEN LastDayUnits > 0
                    AND avgsalesperday = 0 THEN LastDayUnits
               WHEN LastDayUnits = 0 THEN 0
               ELSE (LastDayUnits - (avgsalesperday * (84 - dayssinceinstall)))
           END) AS 'PredictedMarkdownLossUnits'
   FROM
     (SELECT *,
             (CASE
                  WHEN daysinstock > 0 THEN DATEDIFF(DAY, T.[ohuinstalldate], '2018-05-09 00:00:00.000')
                  ELSE 0
              END) AS 'DaysSinceInstall',
             (CASE
                  WHEN OutofStockDate IS NOT NULL THEN frominstalltooos - daysinstockwskuavailable
                  ELSE (DATEDIFF(DAY, T.[ohuinstalldate], '2018-05-09 00:00:00.000') - DaysInStock)
              END) AS 'OutofStockDays',
             (CASE
                  WHEN daysinstock > 9 THEN totalsales / daysinstock
                  ELSE 0
              END) AS 'AvgSalesPerDay'
      FROM
        (SELECT [sku],
                [sku/size],
                [store group],
                [store],
                [curr unit retail price] AS CurrUnitRetailPrice,
                OutofStockDate,
                LastDayUnits,
                MIN(A.[date]) AS SKUStartDate,
                MIN(A.[ohuinstalldate]) AS [OHUInstallDate],
                COUNT(CASE
                          WHEN [store oh u] > 0 THEN 1
                      END) AS 'DaysInStock',
                SUM(CASE
                        WHEN [store oh u] > 0
                             AND A.date < OutofStockDate THEN 1
                        ELSE 0
                    END) AS 'DaysinStockwSKUAvailable',
                (CASE
                     WHEN DATEDIFF(DAY, [ohuinstalldate], OutofStockDate) > 0 THEN DATEDIFF(DAY, [ohuinstalldate], OutofStockDate)
                     ELSE 0
                 END) AS 'FromInstalltoOOS',
                COUNT(CASE
                          WHEN A.Date > OutofStockDate
                               AND [Store OH U] = 0 THEN 1
                      END) AS 'DaysOOSAfterDCOOS',
                SUM([sales u]) AS TotalSales
         FROM [dbo].[champs_qroi] A
         GROUP BY [sku],
                  [sku/size],
                  [store group],
                  [store],
                  [curr unit retail price],
                  [ohuinstalldate],
                  OutofStockDate,
                  LastDayUnits) AS T) AS R) AS FINAL