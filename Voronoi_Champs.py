import psycopg2
import pandas as pd
import traceback

try:
    conn = psycopg2.connect("dbname='VTA' user='postgres' host='localhost' password='1234'")
    print "Connection Established"
    cur = conn.cursor()

    # Fix date open and close columns
    # cur.execute("""alter table malebannerstores_102004
    #                 alter open_date type date using(open_date::date)
    #             );""")

    # Stores with no change in FY 2017
    Stores_NoChange = ("""  CREATE TABLE Champsvoronoi_NoChange AS (
                            WITH voronoi (vor) AS (
                            SELECT ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))
                            FROM  public.malebannerstores_102004
                            WHERE div = 18 and open_date <= '2017-01-29' AND (close_date >= '2017-01-29' or close_date is NULL))
                            SELECT (vor).path, (vor).geom FROM voronoi );
                       """)
    cur.execute(Stores_NoChange)
    conn.commit()

    # Join attributes for unchanged stores
    Stores_NoChange_JoinAttr = (""" CREATE TABLE ChampsVoronoi AS (
                                    SELECT blocks.*, store.str_num as storeID, str_name, city, state, lat, lon, open_date, close_date, 
                                    '2017-01-29' as date_changed 
                                    FROM malebannerstores_102004 AS store
                                    INNER JOIN Champsvoronoi_NoChange AS blocks ON st_within(store.geom, blocks.geom)
                                    WHERE div = 18 and open_date <= '2017-01-29' AND (close_date >= '2017-01-29' or close_date is NULL))
                                """)

    cur.execute(Stores_NoChange_JoinAttr)
    conn.commit()
    drop = ("""DROP TABLE Champsvoronoi_NoChange;""")
    cur.execute(drop)  # Drop VTA with no attributes attached
    conn.commit()

    # Df of Stores that opened/Closed in FY 2017
    Stores_Open = ("""  SELECT str_num, open_date, close_date
                        FROM public.malebannerstores_102004
                        where div = 18 and ((open_date >= '2017-01-29' AND close_date is NULL) OR close_date <= '2018-02-04')
                        ORDER BY open_date asc;
                   """)
    #--------------------------------------------------------------------------------------------------------------------#
    # Collect stores with changed trade areas for FY 2017
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

    d = {'Store': store_list, 'Date': date_list, 'Type': type_list}
    df = pd.DataFrame(d)
    df['Date'] = pd.to_datetime(df.Date)
    df = df.sort_values(by='Date')
    df = df.reset_index(drop=True)

    print df
    #--------------------------------------------------------------------------------------------------------------------#
    # Process each change in store (Open/Close)
    for i in range(len(df.index)):
        print "processing: ", str(df.Store[i])
        _cDate = str(df.Date[i]).replace(" 00:00:00", "")

        # Voronoi for changed stores
        # For closed stores, removes the store that closed on that date
        Stores_open = ("""  CREATE TABLE Champsvoronoi_Open%s AS (
                            WITH voronoi (vor) AS (
                            SELECT ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))
                            FROM  public.malebannerstores_102004
                            where div = 18 and open_date <= '%s' AND (close_date > '%s' OR close_date is NULL))
                            SELECT (vor).path, (vor).geom FROM voronoi );
                            """) % (str(df.Store[i]), str(df.Date[i]), str(df.Date[i]))
        print Stores_open
        cur.execute(Stores_open)
        conn.commit()

        # Join attributes to new voronois for changed stores
        Stores_Open_JoinAttr = (""" CREATE TABLE voronoiChamps_change%s AS (
                                    SELECT blocks.*, store.str_num as storeID, str_name, city, state, lat, lon, open_date, close_date,
                                    '%s' as date_changed
                                    FROM malebannerstores_102004 AS store
                                    INNER JOIN Champsvoronoi_Open%s AS blocks ON st_within(store.geom, blocks.geom)
                                    where div = 18 and open_date <= '%s' AND (close_date > '%s' OR close_date is NULL))
                                    """) % (df.Store[i], _cDate, df.Store[i], str(df.Date[i]), str(df.Date[i]))

        cur.execute(Stores_Open_JoinAttr)
        conn.commit()

        # Drop geom table for new voronois without attributes
        drop = ("""DROP TABLE Champsvoronoi_Open%s;""") % (str(df.Store[i]))
        cur.execute(drop)
        conn.commit()


        # Compare first layer to unchanged store layer created in the first step
        if i == 0:
            selectChanged = ("""SELECT b.storeid
                                from ChampsVoronoi as a, voronoiChamps_change%s as b
                                where (ST_Equals(a.geom, b.geom) = true)
                                """) % (str(df.Store[i]))


        # Compare all after to the previously created layer
        # String of unchanged stores, that have the same polygon from the previous layer, take inverse of that for change
        # Since ST_EQUAL = False compares many:many
        else:
            prevStore = i - 1
            selectChanged = ("""SELECT b.storeid
                                from voronoiChamps_change%s as a, voronoiChamps_change%s as b
                                where (ST_Equals(a.geom, b.geom) = true)
                                """) % (str(df.Store[prevStore]), str(df.Store[i]))
        cur.execute(selectChanged)
        changed = cur.fetchall()
        strList = []
        for st in changed:
            _store = int(st[0])
            strList.append(_store)

        # Append changed polygons to original file with date_changed attached
        strList = str(strList).strip('[]')
        appendChanged = ("""INSERT INTO public.Champsvoronoi(path, geom, storeid, str_name, city, state, lat, lon, open_date, close_date, date_changed)
                            SELECT * from voronoiChamps_change%s
                            where storeid NOT IN (%s)
                        """) % (str(df.Store[i]), str(strList))
        cur.execute(appendChanged)
        conn.commit()

    #--------------------------------------------------------------------------------------------------------------------#

    # Fix date_changed type for Final Output shapefile
    dateFix = cur.execute("""ALTER table Champsvoronoi
                             ALTER date_changed type date using(date_changed::date)""")

    conn.commit()
    print "Query Complete"
    cur.close()
except:
    traceback.print_exc()
    print "Error/Unable to connect to the database"

