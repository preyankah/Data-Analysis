#---------------------------------------------------------------------------------------------------------------------------------#
# Name: Banner Movement Analysis
# Date: 04/20/2018
#---------------------------------------------------------------------------------------------------------------------------------#
from pyspark.sql import functions as F

# Purchases of known Customers with more than one purchase at any male banner
bannerOrders = sqlContext.sql('''SELECT distinct pft.final_individ, purchase_id, fiscal_year, com_company
                                    FROM customer360.purchase_fact_table pft                      
                                INNER JOIN (
                                SELECT * FROM (
                                SELECT distinct (pft.final_individ), count(distinct purchase_id) as numPurchases
                                    FROM customer360.purchase_fact_table pft                           
                                    WHERE order_type = 'OP' and pft.final_individ is not null
                                    AND com_company in (1,21,20,39)
                                    GROUP BY final_individ
                                ) AS T
                                WHERE T.numPurchases > 1) b ON  pft.final_individ = b.final_individ  
                                WHERE order_type = 'OP' and pft.final_individ is not null
                                AND com_company in (1,21,20,34) 
                                ORDER BY final_individ, fiscal_year
                             ''')

# Customers that made their first purchase on Eastbay since 2013
firstPurchaseEB = sqlContext.sql('''SELECT final_individ, date_entered, yearAcquired 
                                    FROM (
                                        SELECT final_individ, date_entered, com_company, fiscal_year as yearAcquired,
                                        ROW_NUMBER() OVER(PARTITION by final_individ ORDER BY date_entered ASC) AS Rank
                                        FROM customer360.purchase_fact_table
                                        WHERE final_individ is not null 
                                            AND date_entered is not null 
                                            AND order_type = 'OP'
                                        ) AS A
                                    WHERE Rank = 1 and com_company = 1 and yearAcquired > 2012
                                    ORDER BY A.final_individ, A.date_entered, A.com_company, A.yearAcquired
                             ''')

df = bannerOrders.join(firstPurchaseEB,"final_individ")

# Eastbay Customers that shopped at other banners- by year
cust_df = df.groupBy(["com_company","fiscal_year"]).agg(F.count("purchase_id").alias("orderCounts"),F.countDistinct("final_individ").alias("Customers"))
# Pivot unique customers by year and banner
cust_df = cust_df.groupBy("fiscal_year").pivot("com_company").sum("Customers").toPandas()
cust_df = cust_df.to_csv("EastbayAcq_Customers.csv", index = False)

#---------------------------------------------------------------------------------------------------------------------------------#
# Launch Purchase Analysis For Customers with more than one purchase at many male banners that were acquired at Eastbay
bannerOrders = sqlContext.sql('''SELECT distinct pft.final_individ, purchase_id, fiscal_year, com_company
                                    FROM customer360.purchase_fact_table pft                      
                                INNER JOIN (
                                SELECT * FROM (
                                SELECT distinct (pft.final_individ), count(distinct purchase_id) as numPurchases
                                    FROM customer360.purchase_fact_table pft                           
                                    WHERE order_type = 'OP' and pft.final_individ is not null
                                    AND com_company in (1,21,20,39)
                                    GROUP BY final_individ
                                ) AS T
                                WHERE T.numPurchases > 1) b ON  pft.final_individ = b.final_individ  
                                WHERE order_type = 'OP' and pft.final_individ is not null
                                AND com_company in (1,21,20,34) and launch_ind = true
                                ORDER BY final_individ, fiscal_year
                             ''')

# Customers that made their first purchase on Eastbay since 2013
firstPurchaseEB = sqlContext.sql('''SELECT final_individ, date_entered, yearAcquired 
                                    FROM (
                                        SELECT final_individ, date_entered, com_company, fiscal_year as yearAcquired,
                                        ROW_NUMBER() OVER(PARTITION by final_individ ORDER BY date_entered ASC) AS Rank
                                        FROM customer360.purchase_fact_table
                                        WHERE final_individ is not null 
                                            AND date_entered is not null 
                                            AND order_type = 'OP'
                                        ) AS A
                                    WHERE Rank = 1 and com_company = 1 and yearAcquired > 2012
                                    ORDER BY A.final_individ, A.date_entered, A.com_company, A.yearAcquired
                             ''')

df = bannerOrders.join(firstPurchaseEB,"final_individ")

# Eastbay Customers that shopped at other banners- by year
cust_df = df.groupBy(["com_company","fiscal_year"]).agg(F.countDistinct("final_individ").alias("Customers"))
# Pivot unique customers by year and banner
cust_df = cust_df.groupBy("fiscal_year").pivot("com_company").sum("Customers").toPandas()

cust_df = cust_df.to_csv("EastbayAcq_Customers_Launches.csv", index = False)
#---------------------------------------------------------------------------------------------------------------------------------#
# Total Count since 2013
cust_df = df.groupBy(["com_company"]).agg(F.countDistinct("final_individ").alias("Customers"))
