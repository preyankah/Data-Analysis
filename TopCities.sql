--Join Census Tracts to DMAs by joining the CT point to the market it lies in
--Aggregate scores at the DMA level
--Rank Markets
SELECT T.name, T.score, existingstores.TotalStores,
rank() OVER (ORDER BY score DESC) as Rank
FROM (
SELECT name, sum(ct.totalscore) as score
	FROM public.ct_totalscores ct
	JOIN public.dmas dma ON ST_Contains(dma.geom,  ST_PointOnSurface(ct.geom))
	GROUP BY name
	ORDER BY score desc 
	) AS T
LEFT JOIN (SELECT name, count(stores.gid) as TotalStores
	FROM public.six02stores_102004 stores
	JOIN public.dmas dma ON ST_Contains(dma.geom,  ST_Transform(stores.geom, 4326))
	GROUP BY name) existingstores on existingstores.name = T.name;