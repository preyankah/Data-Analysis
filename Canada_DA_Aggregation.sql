--Joins sales by postal code(centroid level) to Dissemeanation areas for Canada

SELECT 
	postalcode, 
	TotalCustomers, 
	TotalTransactions, 
	TotalSales,
	da.dauid as dissareaID,
	da.prname as province,
	da.geom 
	FROM 
(
	SELECT  pc.postalcode, TotalCustomers, TotalTransactions, TotalSales, geom
	FROM public.canada_postalcodes pc
	inner join (SELECT "POSTAL_CODE" as PostalCode,
				sum("CUSTOMERS") as TotalCustomers , 
				sum("TRANSACTIONS") as TotalTransactions, 
				sum("SALES") as TotalSales
	FROM public.loyaltydata
	GROUP BY "POSTAL_CODE") cust 
	on cust.PostalCode = pc.postalcode
 ) AS F 
LEFT JOIN canada_disseminationareas AS da
ON ST_Contains(da.geom, F.geom);