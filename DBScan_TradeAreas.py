#-----------------------------------------------------------------------#
# Name:    DBScan Cluster based Trade Areas
# Date:    07/12/2018
# Purpose: Updated logic for defining trade areas for retail stores
# Input:   Census tracts that account for 80% of customers for each store
#          Excluding tracts with singletons
# EPSG:    102004 (USA Contiguous Lambert Conformal Conic)
#-----------------------------------------------------------------------#
import psycopg2
from datetime import datetime
import traceback

# Execute queries
# Commit to confirm tranactions to db
def _postgresExecute(query):
    cur.execute(query)
    conn.commit()

startTime = datetime.now()
# data = pd.read_csv('LAstores.csv',header=0)
# stores = data['store_num'].tolist()
stores = ['29-57373']
for store in stores:
    try:
        conn = psycopg2.connect("dbname='MallTradeAreas' user='postgres' host='localhost' password='1234'")
        print "Connection Established"
        cur = conn.cursor()

        print "Creating Trade Areas For: %r" % (store)

        # Run DBScan clustering algorithm using a 100 meter distance with at least 8 census tracts per cluster
        _dbscanClusters = ('''SELECT * INTO public."DBSCAN_50M_%s"
                        FROM (
                        SELECT store_num, 
                            geoid, 
                            proportion,
                            ct.geom,
                            ST_ClusterDBSCAN(ST_Transform(ct.geom, 102004), eps := 100, minpoints := 8) over () AS cluster_id
                        FROM public."StoreCustomers_CT_80pct" cust
                        INNER JOIN uscensustracts_102004 ct on ct.geoid10 = cust.geoid
                        WHERE store_num = %r
                        ORDER BY cluster_id
                            ) as t;
                        ''') % (store,store)
        _postgresExecute(_dbscanClusters)

        # Get proportions of customers in each cluster
        # Remove 'NULL' census tracts that did not meet the criteria of a cluster
        # Divide % customers by area for normalization to exclude large CTs with low cust density
        # Order by proportion desc (More customers -> include)
        _cluster = ('''SELECT
                      * INTO public."DBclusters_%s"
                    FROM (SELECT
                      *,
                      (cust_proportion * 100) / area AS normalized
                    FROM (SELECT
                      store_num,
                      cluster_id,
                      SUM(proportion) AS cust_proportion,
                      (SUM(St_area(geom))) / 1609.34 AS area,
                      ST_UNION(geom) as geom
                    FROM public."DBSCAN_50M_%s"
                    WHERE cluster_id IS NOT NULL
                    GROUP BY cluster_id,
                             store_num) AS Y) AS T
                    WHERE normalized > .0001
                    ORDER BY normalized DESC''') % (store,store)
        _postgresExecute(_cluster)

        # Check spatial relationship between main cluster(largest area) and other clusters
        # If any clusters are disjoint(not connected to main cluster), decide if they should be separate TA polygon
        _indCluster = ('''SELECT *
                        FROM public."DBclusters_%s" db
                        WHERE st_disjoint((SELECT
                          geom
                        FROM public."DBclusters_%s"
                        ORDER BY area
                        LIMIT 1), db.geom) = TRUE''') % (store,store)
        cur.execute(_indCluster)
        _indCluster = cur.fetchall()

        # If none of the clusters are disjoint- create concave hull to enclose TA
        # Target % for concave hull: 97%
        if _indCluster == []:
            _concaveHull = ('''SELECT * INTO public."DBConcaveHull_%s"
                            FROM (
                            select store_num, ST_ConcaveHull(ST_Collect(geom), .97)
                            from public."DBclusters_%s"
                            group by store_num
                            ) AS T
                          ''') % (store,store)
            _postgresExecute(_concaveHull)

            # Spatially join the concave hull to census tracts that fall within it
            # Uses ST_PointOnSurface which is guaranteed to fall inside the CT
            # If CT point falls within hull, it gets picked up in the TA
            _ctjoin = ('''WITH Tract_TA AS (
                            select store_num, geoid10, tracts.geom
                            from  public."DBConcaveHull_%s" ch
                            LEFT JOIN uscensustracts_102004 AS tracts
                            ON ST_Contains(ch.st_concavehull,ST_PointOnSurface(tracts.geom))
                        ) 
                        INSERT INTO public."DBFinalTAs" SELECT * FROM Tract_TA;
                        ''') % (store)
            _postgresExecute(_ctjoin)

            # Drop store level tables since the
            # Computed TAs get appended to DBFinalTAs table
            _deletetables = ('''DROP TABLE public."DBSCAN_50M_%s", 
                                           public."DBclusters_%s",
                                           public."DBConcaveHull_%s";
                            ''') % (store,store,store)
            _postgresExecute(_deletetables)


        else:
            for cluster in _indCluster:
                _clusterID = int(cluster[1])
                print _clusterID

        # Dissolves all tracts for a store into a single polygon
        # Final TA boundary for store
        _dissolvedTA = ('''WITH Dissolved_Store_TA AS
                        (
                        SELECT store_num, ST_UNION(geom)
                        FROM public."DBFinalTAs"
                        GROUP BY store_num
                            )
                        INSERT INTO public.dbfinaldissolvedtas SELECT * FROM Dissolved_Store_TA;
                    ''')
        _postgresExecute(_dissolvedTA)
        conn.close()
    except:
        traceback.print_exc()
        print "Error/Unable to connect to the database"