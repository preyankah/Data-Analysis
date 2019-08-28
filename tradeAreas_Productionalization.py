# Databricks notebook source
# MAGIC %md
# MAGIC Trade Area Model v2
# MAGIC =============
# MAGIC The Trade Area model delineates the core customer base for all Foot Locker Inc. stores in the U.S. based on shopping patterns. It uses the PostgreSQL environment and its spatial extender PostGIS for the required spatial functions.
# MAGIC * Date Completed:    01/15/2019
# MAGIC * EPSG:    102004 (USA Contiguous Lambert Conformal Conic)

# COMMAND ----------

import psycopg2
from datetime import datetime
import traceback
import sys
from pyspark.sql import *
import pyspark.sql.functions as F
import random
import pandas as pd

# COMMAND ----------

pg_host = dbutils.secrets.get(scope = "postgresql", key = "hostname")
pg_port = dbutils.secrets.get(scope = "postgresql", key = "port")
pg_db = "market_planning"
pg_user = dbutils.secrets.get(scope = "postgresql", key = "username")
pg_pwd = dbutils.secrets.get(scope = "postgresql", key = "password")
pg_uri_ = "postgresql://{0}:{1}/{2}?user={3}&password={4}".format(pg_host, pg_port, pg_db, pg_user, pg_pwd)

pg_uri = "jdbc:postgresql://{0}:{1}/{2}".format(pg_host, pg_port, pg_db)
connectionProperties = {
  "user" : pg_user,
  "password" : pg_pwd,
  "ssl" : "true"
}

# COMMAND ----------

# MAGIC %md
# MAGIC -------
# MAGIC ### Porting Data into the PostGreSQL Database
# MAGIC -------
# MAGIC 1. Store data → customer360.store_info_retail (All Open Stores + lat/lon info)
# MAGIC 2. Customer data → work_priyanka.storecustomers_ct_80pct (Census tracts that account for 80% of sales for each store excluding tracts with singletons)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from work_priyanka.storecustomers_ct_80pct_2015 where store_num in ('03-08189')

# COMMAND ----------

# Code for updated customer data: https://eastus2.azuredatabricks.net/?o=8672838445587530#notebook/4132270837169616/command/4132270837169619
custdata = spark.sql("select * from work_priyanka.storecustomers_ct_80pct_2015")
(
    custdata
    .write
    .mode('overwrite')
    .jdbc(url=pg_uri, table="storecustomers_ct_80pct", properties=connectionProperties)
)

# COMMAND ----------

display(custdata.filter(F.col('store_num') == '03-08189'))

# COMMAND ----------

# TODO: get store information from Netezza
storedata = (spark
             .sql("""select CONCAT(division, '-', store) as store_num, store_latitude, store_longitude, state 
                          from customer360.store_info_retail where country = '001' """)                          
            )
storedata2 = (
               spark.sql("""select CONCAT(division_number, '-', store_number) as store_num, store_latitude, store_longitude,
                          store_state as state from customer360.store_fact_table where store_latitude is not null and store_longitude is not null""")
             )
storedata2 = (storedata2
              .join(storedata.select('store_num').distinct(), 'store_num', 'left_anti')
             )

storedata = (storedata
              .union(storedata2)
              .distinct()
             )

(
    storedata
    .write
    .mode('overwrite')
    .jdbc(url=pg_uri, table="store_info_retail", properties=connectionProperties)
)

# COMMAND ----------

display(storedata.select('store_num').groupby('store_num').count().filter(F.col('count') > 1))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from customer360.store_fact_table where store_number = '08189'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from customer360.store_info_retail

# COMMAND ----------

display(storedata.filter(F.col('store_num') == '03-08189'))

# COMMAND ----------

# MAGIC %md
# MAGIC -------
# MAGIC ### Static to Spatial Transformation
# MAGIC -------
# MAGIC Since Databricks does not support spatial data types, tabular data is brought into Postgres and converted into spatial data
# MAGIC 1. Store data → Using Latitude + Longitude coordinates
# MAGIC 2. Customer data → Census Tract file was uploaded to the PostgreSQL db in well known text format and converted to geometry. A tabular join with this table adds the necessary spatial information to the customer data

# COMMAND ----------

# Create geometry for Stores Layer
conn = psycopg2.connect(pg_uri_)
cur = conn.cursor()
cur.execute('''drop table if exists flincstores_102004''')
conn.commit()

cur.execute('''select * into flincstores_102004 
               from (
               select *, ST_Transform(ST_SetSRID(ST_MakePoint(store_longitude,store_latitude), 4326),102004) as geom from store_info_retail
               ) as stores''')
conn.commit()

# COMMAND ----------

# MAGIC %md
# MAGIC _____________________
# MAGIC ### Trade Area Creation: Functions
# MAGIC _____________________
# MAGIC The functions below incorporate a combination of PostGIS's spatial functions to define and verify trade areas.

# COMMAND ----------

# Execute queries
# Commit to confirm transactions to db
def _postgresExecute(query):
    cur.execute(query)
    conn.commit()

# COMMAND ----------

# Run DBScan clustering algorithm using a specified distance + minimum number of census tracts per cluster
# Cluster ID defined for tracts placed within a cluster
# 'NULL' for census tracts that did not meet the criteria of a cluster
def _dbscanClustering(store, distance, minpoints):
    _dbscanClusters = ('''
                    WITH dbscan_clusters AS (
                    SELECT store_num, 
                        cust.geoid10, 
                        proportion,
                        cumul_percent,
                        ct.geom,
                        ST_ClusterDBSCAN(ST_Transform(ct.geom, 102004), 
                        eps := %r, minpoints := %r) over () AS cluster_id
                    FROM storecustomers_ct_80pct cust
                    INNER JOIN uscensustracts_102004 ct on ct.geoid10::bigint = cust.geoid10
                    WHERE store_num = %r
                    ORDER BY cluster_id
                    ) 
                    INSERT INTO dbscan_clusters SELECT * FROM dbscan_clusters;
                    ''') % (distance, minpoints, store)
    _postgresExecute(_dbscanClusters)

# COMMAND ----------

# Verify if proportion of customers in clusters defined meets the 55% primary trade area criteria
def _clusterVerification(store):
    _clusterVerf = ('''SELECT store_num, sum(proportion) as prop
                       FROM dbscan_clusters
                       WHERE store_num = %r and cluster_id is not null
                       GROUP by store_num
                    ''') % (store)
    cur.execute(_clusterVerf)
    p = cur.fetchall()
    if list(p[0])[1] >= 0.55:
        return True
    else:
        return False

# COMMAND ----------

# Delete records from table
def _deletefromTable(store,table):
    deleteClusters = ('''DELETE from %s
                         WHERE store_num = %r
             ''') % (table,store)
    _postgresExecute(deleteClusters)

# COMMAND ----------

# Create concave Hull
def concaveHull(store,target,clusters,type):
    _concaveHull = ('''WITH dbscan_concave AS
                          (
                         select store_num, ST_ConcaveHull(ST_Union(geom), {}), '{}' as Type
                         from dbscan_clusters
                         where store_num = '{}' and cluster_id::character varying in ({})
                         group by store_num
                         )
                         INSERT INTO dbconcavehull SELECT * FROM dbscan_concave;
                     ''').format(target,type,store,clusters)
    _postgresExecute(_concaveHull)

# COMMAND ----------

# Spatially join the concave hull to census tracts that fall within it
# Uses ST_PointOnSurface which is guaranteed to fall inside the CT
# If CT point falls within hull, it gets picked up in the TA
def _concavetoStore(store):
    _ctjoin = ('''WITH Tract_TA AS (
                    select store_num, tracts.geoid10::bigint, tracts.geom
                    from  dbconcavehull ch
                    LEFT JOIN uscensustracts_102004 AS tracts
                    ON ST_Contains(ch.st_concavehull,ST_PointOnSurface(tracts.geom))
                    WHERE store_num = %r
                )
                INSERT INTO dbfinaltas SELECT * FROM Tract_TA;
                ''') % (store)
    _postgresExecute(_ctjoin)

# COMMAND ----------

# Dissolve TAs
def _dissolveTA(store):
    _dissolvedTA = ('''WITH Dissolved_Store_TA AS
                    (
                    SELECT store_num, ST_Multi(ST_UNION(geom))
                    FROM dbfinaltas
                    WHERE store_num = %r
                    GROUP BY store_num
                        )
                    INSERT INTO dbfinaldissolvedtas SELECT * FROM Dissolved_Store_TA;
                ''') % (store)

# COMMAND ----------

# MAGIC %md
# MAGIC -------
# MAGIC ### Table Creation
# MAGIC -------
# MAGIC Four tables are required for the process. 
# MAGIC 1. **dbscan_clusters** → *Reads static customer information and transforms them into spatial geometries. The final input in the table are results of spatial clustering of census tracts for each store*
# MAGIC 2. **dbconcavehull** → *Stores the enclosing geometry that encompasses the selected clusters*
# MAGIC 3. **dbfinaltas** → *Stores the final trade areas with the census tracts that spatially intersect with the output from the concave hull*
# MAGIC 4. **storeta_error** → *Store IDs for stores that did not meet the trade area requirements and failed to create*

# COMMAND ----------

# Initiate cursor
startTime = datetime.now()
conn = psycopg2.connect(pg_uri_)
cur = conn.cursor()

# COMMAND ----------

# Check if DBScan cluster table exists, if not create the table
dbscan_dne = ('''SELECT to_regclass('dbscan_clusters');''')
_postgresExecute(dbscan_dne)
table_check = cur.fetchall()
if table_check == [(None,)]:
    create_dbscan = ('''CREATE TABLE dbscan_clusters
                        (   store_num character varying COLLATE pg_catalog."default",
                            geoid10 bigint,
                            proportion real,
                            cumul_percent real,
                            geom geometry,
                            cluster_id integer
                        ) WITH (
                            OIDS = FALSE
                        ) TABLESPACE pg_default;
                            ''')
    _postgresExecute(create_dbscan)

# COMMAND ----------

# Check if Concave Hull table exists, if not create the table
concaveHull_dne = ('''SELECT to_regclass('dbconcavehull');''')
_postgresExecute(concaveHull_dne)
table_check = cur.fetchall()
if table_check == [(None,)]:
    create_concaveHull = ('''CREATE TABLE dbconcavehull
                            (store_num character varying COLLATE pg_catalog."default",
                             st_concavehull geometry,
                             Type character varying
                            ) WITH (
                                OIDS = FALSE
                            )
                            TABLESPACE pg_default;
                            ''')
    _postgresExecute(create_concaveHull)

# COMMAND ----------

# Check if Final TAs table exists, if not create the table
Final_TAs_dne = ('''SELECT to_regclass('dbfinaltas');''')
_postgresExecute(Final_TAs_dne)
table_check = cur.fetchall()
if table_check == [(None,)]:
    Final_TAs = ('''CREATE TABLE dbfinaltas
                    (
                        store_num character varying COLLATE pg_catalog."default",
                        geoid10 bigint,
                        geom geometry
                    )
                    WITH (
                        OIDS = FALSE
                    )
                    TABLESPACE pg_default;
                    ''')
    _postgresExecute(Final_TAs)

# COMMAND ----------

# Check if error table exists, if not create the table
error_stores_dne = ('''SELECT to_regclass('storeta_error');''')
_postgresExecute(error_stores_dne)
table_check = cur.fetchall()
if table_check == [(None,)]:
  error_stores = cur.execute('''CREATE TABLE storeta_error
                              (   store_num character varying COLLATE pg_catalog."default"
                              ) WITH (
                                  OIDS = FALSE
                              ) TABLESPACE pg_default; ''')
  _postgresExecute(error_stores)

# COMMAND ----------

# conn = psycopg2.connect(pg_uri_)
# cur = conn.cursor()
# cur.execute('''drop table if exists dbconcavehull_edit''')
# cur.execute('''truncate table dbscan_clusters,dbconcavehull,dbfinaltas,storeta_error ''')
# conn.commit()

# COMMAND ----------

# MAGIC %md
# MAGIC -------
# MAGIC ### Store Type Assignment + Store Selection
# MAGIC -------
# MAGIC The dbscan clustering algorithm takes 2 inputs to define clusters and stores are broadly classified into 4 groups based on their location.
# MAGIC   1. Minimum points
# MAGIC   2. Distance
# MAGIC   
# MAGIC The above inputs are determined by running a spatial join between store locations and their relationship to the census based urban areas extents stored in the **urbanfiltered_102004** table. We do so since the sizes of census tracts are inversely related to population. 
# MAGIC 1. If the store is located in a _significantly large urban area_ then the clustering requires at least 8 census tracts within a 50 meter radius to form a cluster. *Ex: New York, Los Angeles*
# MAGIC 2. If the store is located in a _mid-size urban area_ then the clustering requires at least 7 census tracts within 75 meter radius to form a cluster. *Ex: Washington D.C., San Diego*
# MAGIC 3. If the store is located in a _small urban area_ then the clustering requires at least 5 census tracts within 100 meter radius to form a cluster. *Ex.: Danbury CT, Chico CA*
# MAGIC 3. If the store is _not_ located in an urban area then the clustering requires at least 4 census tracts within 125 meter radius to form a cluster.
# MAGIC 
# MAGIC We then select stores that don't have existing trade areas in the dbfinaltas or storeta_error tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from customer360.store_fact_table where store_number = '08189'

# COMMAND ----------

# Assign cluster distance and minimum size based on store location
# Broadly classified into 4 groups based on proximity and size of urban area 
conn = psycopg2.connect(pg_uri_)
cur = conn.cursor()
stores,min_points,distances = [], [], []
cur.execute("""SELECT
                store_num,
                CASE
                  WHEN (aland10 > 984417620 AND
                    aland10 < 3461116383) THEN 7
                  WHEN (aland10 < 984417620) THEN 5
                  WHEN (aland10 IS NULL) THEN 4
                  ELSE 8
                END AS min_clustersize,
                CASE
                  WHEN (aland10 > 984417620 AND
                    aland10 <= 3461116383) THEN 75
                  WHEN (aland10 <= 984417620) THEN 100
                  WHEN (aland10 IS NULL) THEN 125
                  ELSE 50
                END AS distance_buffer
              FROM (SELECT
                store_num,
                name10,
                stores.geom,
                aland10
              FROM flincstores_102004 stores
              LEFT JOIN urbanfiltered_102004 AS urban
                ON ST_Intersects(stores.geom, urban.geom)
              WHERE store_num in ('03-08189')
              ) AS T
                ORDER BY RANDOM()
  """)
# storelist = storedata.rdd.map(lambda x: x.store).collect()
for row in cur:
  stores.append(row[0])
  min_points.append(row[1])
  distances.append(row[2])
  print(row[0],row[1],row[2])

# COMMAND ----------

# This is the list of stores that we have trade areas for already
# Also includes stores that may have errored out
# Will be used to compare to complete store list
ta_done = []
conn = psycopg2.connect(pg_uri_)
cur = conn.cursor()
cur.execute('''select distinct store_num from dbfinaltas union select distinct store_num from storeta_error''')
for row in cur:
  ta_done.append(row[0])
print(len(ta_done))

# COMMAND ----------

# conn = psycopg2.connect(pg_uri_)
# cur = conn.cursor()
# cur.execute('''select store_num, count(*) from dbfinaltas where store_num in ('03-08351','16-48581','18-14172') group by store_num ''')
# cur.fetchall()

# COMMAND ----------

# conn = psycopg2.connect(pg_uri_)
# cur = conn.cursor()
# cur.execute('''delete from dbscan_clusters where store_num in ('03-08351','16-48581','18-14172','03-08567')''')
# cur.execute('''delete from dbconcavehull where store_num in ('03-08351','16-48581','18-14172','03-08567')''')
# cur.execute('''delete from dbfinaltas where store_num in ('03-08351','16-48581','18-14172','03-08567')''')

# conn.commit()

# COMMAND ----------

'03-08189' in ta_done

# COMMAND ----------

stores

# COMMAND ----------

# List of stores to run in current run
allStores_param = pd.DataFrame(
    {'stores': stores,
     'min_points': min_points,
     'distances': distances
    })

doneStores = pd.DataFrame(
    {'stores': ta_done
    })

# Remove stores that are in both dataframes
merged = allStores_param.merge(doneStores, indicator=True, how='outer')
df = merged[(merged._merge!='both')]

# print(df)

_stores = df['stores'].values.tolist()
_distances = df['distances'].values.tolist()
_min_points = df['min_points'].values.tolist()


print('Total Stores:',len(stores))
print('Stores With Existing TA:',len(ta_done))
print('Stores Left:',len(_stores))

# for store,dist,minpts in zip(_stores,_distances,_min_points):
#       print("Creating Trade Areas For: %r with min_ponts %r, distance %r" % (store, minpts, dist))

# COMMAND ----------

# MAGIC %md
# MAGIC ____________________
# MAGIC ###Trade Area Execution
# MAGIC ____________________
# MAGIC 
# MAGIC The process runs for one store at a time. If generated clusters do not contain at least 55% of the known customers that shop at the store, they are deleted and re-generated with looser distance and minimum point requirements until the proportion requirements are met. The process also determines if a cluster is significant enough to be a standalone cluster or if it is close enough to be included in the main cluster. The gaps between defined clusters are filled by picking up all the additional census tracts that fall within the concave hull enclosing the selected clusters.

# COMMAND ----------

'''delete from dbfinaltas where store_num in ({})'''.format(', '.join(["'{}'".format(s) for s in stores]))

# COMMAND ----------

[(store,dist,minpts) for store,dist,minpts in zip(stores, distances, min_points)]

# COMMAND ----------

conn = psycopg2.connect(pg_uri_)
cur = conn.cursor()
cur.execute('''delete from dbfinaltas where store_num in ({})'''.format(', '.join(["'{}'".format(s) for s in stores])))
cur.execute('''delete from dbscan_clusters where store_num in ({})'''.format(', '.join(["'{}'".format(s) for s in stores])))
cur.execute('''delete from dbconcavehull where store_num in ({})'''.format(', '.join(["'{}'".format(s) for s in stores])))
conn.commit()

for store,dist,minpts in zip(stores, distances, min_points):
    conn.commit()
    try:
        print("Creating Trade Areas For: %r with min_ponts %r, distance %r" % (store, minpts, dist))
        dbscan = _dbscanClustering(store, dist, minpts)

        # If cluster criteria is not met, delete
        while _clusterVerification(store) == False:
            print ('Primary Trade Area Requirement Not Met. Deleting Existing Clusters')
            _deletefromTable(store,'dbscan_clusters')
            minpts = minpts - 1
            dist = dist*2
            print('min_ponts %r, distance %r' % (minpts, dist))
            dbscan = _dbscanClustering(store, dist, minpts)
            _clusterVerification(store)
        else:
            # Check spatial relationship between main cluster(largest proportion of customers) and other clusters
            # If any clusters are disjoint(not connected to main cluster), decide if they should be separate TA polygon
            _indCluster = ('''SELECT * FROM 
                                (SELECT store_num, 
                                        cluster_id, 
                                St_disjoint((SELECT St_union(geom) AS geom 
                                            FROM   dbscan_clusters
                                            WHERE  store_num = %r
                                                   AND cluster_id IS NOT NULL 
                                            GROUP  BY cluster_id 
                                            ORDER  BY Sum(proportion) DESC 
                                            LIMIT  1), St_union(geom)) as disjoint
                                FROM   dbscan_clusters db 
                                WHERE  store_num = %r
                                       AND cluster_id IS NOT NULL
                                GROUP  BY store_num,cluster_id
                                ) AS Y 
                                WHERE disjoint = True
                           ''') % (store,store)
            cur.execute(_indCluster)
            _indCluster = cur.fetchall()

            # If none of the clusters are disjoint → create concave hull to enclose TA
            # Target % for concave hull: 97%
            if _indCluster == []:
                _clusterstoBind = ('''SELECT distinct cluster_id 
                                      FROM dbscan_clusters
                                      WHERE store_num = %r
                                      AND cluster_id is not null''') % (store)
                cur.execute(_clusterstoBind)
                _clusterstoBind = cur.fetchall()
                cl = []
                for i in range(len(_clusterstoBind)):
                    r = (_clusterstoBind[i])[0]
                    cl.append(str(r))
                _cl = "','".join(map(str, cl))
                _cl = "'" + _cl + "'"
                concaveHull(store,0.97,_cl,'Main')

            # If clusters are disjoint, determine if cluster is close enough to be merged
            # Or if it it consists of a significant proportion of sales to be a separate polygon
            else:
                _extendcl = []
                _seppoly = ('''SELECT * FROM (
                                SELECT store_num,
                                   cluster_id,
                                   sum(proportion) as proportion,
                                   st_AREA(ST_UNION(geom))*0.0000003861 as area,
                                   ST_Distance((SELECT St_union(geom) AS geom
                                                FROM   dbscan_clusters
                                                WHERE  store_num = %r
                                                       AND cluster_id IS NOT NULL
                                                GROUP  BY cluster_id
                                                ORDER  BY Sum(proportion) DESC
                                                LIMIT  1), St_union(geom))/1609.34 as st_dist,
                                   St_disjoint((SELECT St_union(geom) AS geom
                                                FROM   dbscan_clusters
                                                WHERE  store_num = %r
                                                       AND cluster_id IS NOT NULL
                                                GROUP  BY cluster_id
                                                ORDER  BY Sum(proportion) DESC
                                                LIMIT  1), St_union(geom)) as disjoint
                                FROM   dbscan_clusters db
                                WHERE   store_num = %r
                                        AND cluster_id IS NOT NULL
                                GROUP  BY store_num,cluster_id
                                    ) AS Y
                            ''') % (store, store, store)
                cur.execute(_seppoly)

                # Concave hull for all clusters that are not disjoint
                # Or within 2-Miles of the main cluster
                p = cur.fetchall()
                for i in range(len(p)):
                    # print (p[i])[2]
                    if list(p[i])[4] <= 2:
                        _extendcl.append((p[i])[1])
                    _newch = _extendcl
                    _newch = "','".join(map(str, _newch))
                    _newch = "'" + _newch + "'"

                     # print (_newch)
                    _deletefromTable(store, 'dbconcavehull')
                    concaveHull(store, 0.55, _newch, 'Main')

                    # If the cluster is at least 2-miles from the main cluster
                    # And contains 0.4%+ of the unique customers
                    # The cluster will be added as a separate polygon
                for i in range(len(p)):
                    if list(p[i])[4] > 2:
                        if list(p[i])[2] >= 0.0004 and list(p[i])[4] >= 2 and list(p[i])[4] <= 10 and list(p[i])[5] is True:
                             # print('This will be a separate polygon')
                            r = "'"+str((p[i])[1])+"'"
                            concaveHull(store, 0.55, r, 'Secondary')
                        else:
                            print('Ignore')

            # Spatially join the concave hull to census tracts that fall within it
            # This will be the primary trade area for the store
            _concavetoStore(store)

            # Get additional Census tracts that make up the top 55% of unique customers
            # By comparing to previous TA
            # Include CT if it falls with 0.5 miles of the existing TA
            _highdensity = ('''SELECT
                                  geoid10
                                FROM (SELECT
                                  cstr.store_num,
                                  cstr.geoid10,
                                  st_length(St_shortestline(ct.geom, ch.st_concavehull)) / 1609.34 AS distance
                                FROM storecustomers_ct_80pct cstr
                                INNER JOIN uscensustracts_102004 ct
                                  ON ct.geoid10::bigint = cstr.geoid10
                                INNER JOIN (SELECT
                                  *
                                FROM dbconcavehull
                                WHERE type = 'Main'
                                AND store_num = %r) ch
                                  ON ch.store_num = cstr.store_num
                                LEFT OUTER JOIN dbfinaltas ta
                                  ON cstr.geoid10 = ta.geoid10
                                WHERE cumul_percent <= 0.55
                                AND ta.geoid10 IS NULL
                                AND cstr.store_num = %r) AS t
                                WHERE t.distance <= 0.2
                            ''') % (store, store)
            # print _highdensity
            _postgresExecute(_highdensity)
            hd = cur.fetchall()
            addHD = []
            for i in range(len(hd)):
                r = (hd[i])[0]
                addHD.append(str(r))
            _addHD = "','".join(map(str, addHD))
            _addHD = "'" + _addHD + "'"

            # If there are additions, replace old concave with new shape
            # New shape consists of high density tracts identified in the last query
            if hd:
                _newconcave = ('''SELECT store_num, 
                                   St_concavehull(St_union(geom), 0.25),'Main'::text as Type 
                            INTO   dbconcavehull_edit 
                            FROM   (SELECT DISTINCT store_num, 
                                                    St_union(ct.geom, ch.st_concavehull) AS geom 
                                    FROM   uscensustracts_102004 ct, 
                                           dbconcavehull ch 
                                    WHERE  ct.geoid10 IN ( {} ) 
                                           AND ch.store_num = '{}' 
                                           AND ch.type = 'Main') AS T 
                            GROUP  BY T.store_num 
            ''').format(_addHD, store)

                _postgresExecute(_newconcave)

                # Update last
                _edit_ch = ('''UPDATE dbconcavehull ch 
                                SET    (st_concavehull) = (SELECT st_concavehull 
                                         FROM   dbconcavehull_edit edit 
                                         WHERE  edit.store_num = %r
                                                AND edit.type = 'Main') 
                                WHERE  ch.store_num = %r
                                       AND ch.type = 'Main'; 
                        ''') % (store,store)

                _postgresExecute(_edit_ch)

                delete_edit = ('''drop table if exists dbconcavehull_edit''')
                _postgresExecute(delete_edit)

                _deletefromTable(store,'dbfinaltas')
                _concavetoStore(store)
            _dissolveTA(store)

    except:
        traceback.print_exc()
        print('Error generating Trade Areas for %s:' % (store))
        error = ('''WITH error_store AS
                    (SELECT %r)
                   INSERT INTO storeta_error
                   SELECT * FROM error_store;''') % (store)
        _postgresExecute(error)
        continue

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC -------
# MAGIC Results
# MAGIC -------
# MAGIC The dbfinaltas table should contain all the outputted trade areas by store. The table is then ported into databricks (without the geometry column since databricks does not support spatial data types) into the work_priyanka.dbfinaltas table.
# MAGIC 
# MAGIC This output is then delivered to the Market Planning Team.

# COMMAND ----------

# Read table to df
conn = psycopg2.connect(pg_uri_)
cur = conn.cursor()
pushdown_query = "(select distinct store_num, geoid10 from dbfinaltas  where geoid10 is not null) emp_alias"
df = spark.read.jdbc(url=pg_uri, table=pushdown_query, properties=connectionProperties)
display(df)
df.cache()
df.createOrReplaceTempView("dbfinaltas")

spark.sql("CREATE TABLE IF NOT EXISTS work_priyanka.dbfinaltas AS Select store_num, geoid10 from dbfinaltas")

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS work_priyanka.dbfinaltas AS Select store_num, geoid10 from dbfinaltas")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from work_priyanka.dbfinaltas where store_num in ('03-08189')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --drop table work_priyanka.dbfinaltas

# COMMAND ----------

# Read table to df
conn = psycopg2.connect(pg_uri_)
cur = conn.cursor()
pushdown_query = "(select distinct store_num, geoid10, cluster_id from dbscan_clusters where geoid10 is not null and store_num = '29-57228') emp_alias"
df = spark.read.jdbc(url=pg_uri, table=pushdown_query, properties=connectionProperties)
display(df)
df.cache()
df.createOrReplaceTempView("dbscan_clusters")

spark.sql("CREATE TABLE IF NOT EXISTS work_priyanka.dbscan_clusters AS Select store_num, geoid10,cluster_id where store_num = '29-57228' from dbscan_clusters")

# COMMAND ----------

# Read table to df
conn = psycopg2.connect(pg_uri_)
cur = conn.cursor()
pushdown_query = "(select * from dbconcavehull where store_num = '29-57228') emp_alias"
df = spark.read.jdbc(url=pg_uri, table=pushdown_query, properties=connectionProperties)
display(df)
df.cache()
df.createOrReplaceTempView("dbscan_clusters")

spark.sql("CREATE TABLE IF NOT EXISTS work_priyanka.dbscan_clusters AS Select store_num, geoid10,cluster_id where store_num = '29-57228' from dbscan_clusters")
