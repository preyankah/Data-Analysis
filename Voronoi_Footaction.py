import psycopg2
from datetime import datetime
import pandas as pd
import traceback
import sys
from datetime import datetime

try:
    conn = psycopg2.connect("dbname='VTA' user='postgres' host='localhost' password='1234'")
    print "Connection Established"
    cur = conn.cursor()

    # Fix date open and close columns
    # cur.execute("""alter table malebannerstores_102004
    #                 alter open_date type date using(open_date::date)
    #             );""")

    # Stores with no change in FY 2017
    Stores_NoChange = ("""  CREATE TABLE FAvoronoi_NoChange AS (
                            WITH voronoi (vor) AS (
                            SELECT ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))
                            FROM  public.malebannerstores_102004
                            WHERE div = 29 and open_date <= '2017-01-29' AND (close_date >= '2017-01-29' or close_date is NULL))
                            SELECT (vor).path, (vor).geom FROM voronoi );
                       """)
    cur.execute(Stores_NoChange)
    conn.commit()

    Stores_NoChange_JoinAttr = (""" CREATE TABLE FootactionVoronoi AS (
                                    SELECT blocks.*, store.str_num as storeID, str_name, city, state, lat, lon, open_date, close_date,
                                    '2017-01-29' as date_changed
                                    FROM malebannerstores_102004 AS store
                                    INNER JOIN FAvoronoi_NoChange AS blocks ON st_within(store.geom, blocks.geom)
                                    WHERE div = 29 and open_date <= '2017-01-29' AND (close_date >= '2017-01-29' or close_date is NULL))
                                """)

    cur.execute(Stores_NoChange_JoinAttr)
    conn.commit()
    drop = ("""DROP TABLE FAvoronoi_NoChange;""")
    cur.execute(drop) #Drop VTA with no attributes attached
    conn.commit()

    # Df of Stores that opened/Closed in FY 2017
    Stores_Open = ("""  SELECT str_num, open_date, close_date
                        FROM public.malebannerstores_102004
                        where div = 29 and ((open_date >= '2017-01-29' AND close_date is NULL) OR close_date <= '2018-02-04')
                        ORDER BY open_date asc;
                   """)

    store_list = []
    date_list = []
    type_list = []
    cur.execute(Stores_Open)
    res = cur.fetchall()
    for r in res:
        store_num = int(r[0])
        open_date = str(r[1])
        close_date = str(r[2])

        if close_date == "None":
            date_list.append(open_date)
            store_list.append(store_num)
            type_list.append("OPEN")
        else:
            date_list.append(close_date)
            store_list.append(store_num)
            type_list.append('CLOSED')

    d = {'Store': store_list, 'Date': date_list, 'Type':type_list}
    df = pd.DataFrame(d)
    df['Date'] = pd.to_datetime(df.Date)
    df = df.sort_values(by='Date')
    df = df.reset_index(drop=True)

    print df

    # for i in range(len(df.index)):
    #     drop = ("""DROP TABLE voronoiFA_change%s;""") % (str(df.Store[i]))
    #     cur.execute(drop)
    #     conn.commit()

    for i in range(len(df.index)):
        print "processing: ", str(df.Store[i])
        _cDate = str(df.Date[i]).replace(" 00:00:00","")

    # Voronoi for open stores
        Stores_open = ("""  CREATE TABLE FAvoronoi_Open%s AS (
                            WITH voronoi (vor) AS (
                            SELECT ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))
                            FROM  public.malebannerstores_102004
                            where div = 29 and open_date <= '%s' AND (close_date > '%s' OR close_date is NULL))
                            SELECT (vor).path, (vor).geom FROM voronoi );
                            """) % (str(df.Store[i]), str(df.Date[i]), str(df.Date[i]))
        print Stores_open
        cur.execute(Stores_open)
        conn.commit()

        Stores_Open_JoinAttr = (""" CREATE TABLE voronoiFA_change%s AS (
                                    SELECT blocks.*, store.str_num as storeID, str_name, city, state, lat, lon, open_date, close_date,
                                    '%s' as date_changed
                                    FROM malebannerstores_102004 AS store
                                    INNER JOIN FAvoronoi_Open%s AS blocks ON st_within(store.geom, blocks.geom)
                                    where div = 29 and open_date <= '%s' AND (close_date > '%s' OR close_date is NULL))
                                    """) % (df.Store[i], _cDate, df.Store[i], str(df.Date[i]), str(df.Date[i]))

        cur.execute(Stores_Open_JoinAttr)
        conn.commit()


        drop = ("""DROP TABLE FAvoronoi_Open%s;""") % (str(df.Store[i]))
        cur.execute(drop)
        conn.commit()
        #Compare first layer to unchanged store layer
        if i == 0:
            selectChanged = ("""SELECT b.storeid
                                from FootactionVoronoi as a, voronoiFA_change%s as b
                                where (ST_Equals(a.geom, b.geom) = true)
                                """) % (str(df.Store[i]))
        # Compare all after to the previously created layer
        else:
            prevStore = i - 1
            selectChanged = ("""SELECT b.storeid
                                from voronoiFA_change%s as a, voronoiFA_change%s as b
                                where (ST_Equals(a.geom, b.geom) = true)
                                """) % (str(df.Store[prevStore]),str(df.Store[i]))

        cur.execute(selectChanged)
        changed = cur.fetchall()
        strList = []
        for st in changed:
            _store = int(st[0])
            strList.append(_store)

        strList = str(strList).strip('[]')
        appendChanged = ("""INSERT INTO public.footactionvoronoi(path, geom, storeid, str_name, city, state, lat, lon, open_date, close_date,date_changed)
                            SELECT * from voronoiFA_change%s
                            where storeid NOT IN (%s)
                        """) % (str(df.Store[i]),str(strList))

        cur.execute(appendChanged)
        conn.commit()

    # Fix date for Final Output shapefile
    dateFix = cur.execute("""alter table footactionvoronoi
                    alter date_changed type date using(date_changed::date)
                """)
    cur.execute(dateFix)
    conn.commit()
    print "Query Complete"
    cur.close()
except:
    traceback.print_exc()
    print "Error/Unable to connect to the database"

