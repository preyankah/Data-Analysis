--Computes shortest line distance from each census tract with known Six:02 Online or Retail customers to all Six:02 stores
--Selects the closest store for each Tract
--Computed distance is in meters since the datasets use epsg:102004. Distance is divided by 1609.344 for conversion to miles
--Census Tracts of Alaska, Hawaii and Puerto Rico have been excluded from distance analysis
--Total Sales and Customers computed for each CT

SELECT * INTO CT_Store_ShortestDistance
FROM (
SELECT tract, store, shortestline, distance,
retailsale as retail_sales,
onlinesale as online_sales,
retailcust as retail_customers,
onlinecust as online_customers 
FROM (
SELECT *, 
row_number() over (partition by tract order by distance asc) as row_num
FROM (
SELECT ct as tract, "str name" as store, ST_ShortestLine(ct.geom, stores.geom) as shortestline, 
	ST_Length(ST_ShortestLine(ct.geom, stores.geom))/1609.344 distance
	FROM public.six02_ct_cust_op_rp_102004 ct, six02stores_102004 stores
	where geoid10 not like '02%' and   
		  geoid10 not like '15%' and  
		  geoid10 not like '72%'
	order by distance asc
	) AS T
) AS Final
inner join public.six02_ct_cust_op_rp_102004 sales on sales.ct = Final.tract
where row_num = 1
order by distance asc
) AS F