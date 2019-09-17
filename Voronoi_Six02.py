import psycopg2
import pandas as pd
import traceback

try:
    conn = psycopg2.connect("dbname='VTA' user='postgres' host='localhost' password='1234'")
    print "Connection Established"
    cur = conn.cursor()

    # Fix date open and close columns
    cur.execute("""ALTER TABLE six02_fy2017_102004 ALTER open_date type date 
                    using      (open_date::date), 
                                ALTER close_date type date 
                    using      (close_date::date);
                """)

    # Stores with no change in FY 2017
    Stores_NoChange = (""" CREATE TABLE Six02voronoi_NoChange AS
                          ( WITH voronoi (vor) AS
                             ( SELECT ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))
                              FROM public.six02_fy2017_102004
                              WHERE open_date <= '2017-02-05'
                                AND (close_date >= '2017-02-05'
                                     OR close_date IS NULL)) SELECT (vor).path, (vor).geom
                           FROM voronoi);
                       """)
    cur.execute(Stores_NoChange)
    conn.commit()

    # Join attributes for unchanged stores
    Stores_NoChange_JoinAttr = ("""CREATE TABLE Six02Voronoi AS
                                  ( SELECT blocks.*,
                                           store.str_num AS storeID,
                                           str_name,
                                           city,
                                           state,
                                           lat,
                                           lon,
                                           open_date,
                                           close_date,
                                           '2017-02-05' AS date_changed
                                   FROM six02_fy2017_102004 AS store
                                   INNER JOIN Six02voronoi_NoChange AS blocks ON st_within(store.geom, blocks.geom)
                                   WHERE open_date <= '2017-02-05'
                                     AND (close_date >= '2017-02-05'
                                          OR close_date IS NULL))
                                """)

    cur.execute(Stores_NoChange_JoinAttr)
    conn.commit()
    drop = ("""DROP TABLE Six02voronoi_NoChange;""")
    cur.execute(drop)  # Drop VTA with no attributes attached
    conn.commit()

    # Df of Stores that opened/Closed in FY 2017
    Stores_Open = ("""SELECT str_num,
                           open_date,
                           close_date
                    FROM public.six02_fy2017_102004
                    WHERE ((open_date >= '2017-02-05'
                            AND close_date IS NULL)
                           OR close_date IS NOT NULL)
                    ORDER BY open_date ASC;
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
        Stores_open = ("""  CREATE TABLE Six02voronoi_Open%s AS
                              ( WITH voronoi (vor) AS
                                 ( SELECT ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))
                                  FROM public.six02_fy2017_102004
                                  WHERE open_date <= '%s'
                                    AND (close_date > '%s'
                                         OR close_date IS NULL)) SELECT (vor).path, (vor).geom
                               FROM voronoi);
                            """) % (str(df.Store[i]), str(df.Date[i]), str(df.Date[i]))
        print Stores_open
        cur.execute(Stores_open)
        conn.commit()

        # Join attributes to new voronois for changed stores
        Stores_Open_JoinAttr = (""" CREATE TABLE voronoiSix02_change%s AS
                                      ( SELECT blocks.*,
                                               store.str_num AS storeID,
                                               str_name,
                                               city,
                                               state,
                                               lat,
                                               lon,
                                               open_date,
                                               close_date,
                                               '%s' AS date_changed
                                       FROM six02_fy2017_102004 AS store
                                       INNER JOIN Six02voronoi_Open%s AS blocks ON st_within(store.geom, blocks.geom)
                                       WHERE open_date <= '%s'
                                         AND (close_date > '%s'
                                              OR close_date IS NULL))
                                    """) % (df.Store[i], _cDate, df.Store[i], str(df.Date[i]), str(df.Date[i]))
        cur.execute(Stores_Open_JoinAttr)
        conn.commit()

        # Drop geom table for new voronois without attributes
        drop = ("""DROP TABLE Six02voronoi_Open%s;""") % (str(df.Store[i]))
        cur.execute(drop)
        conn.commit()


        # Compare first layer to unchanged store layer created in the first step
        if i == 0:
            selectChanged = ("""SELECT b.storeid
                                FROM Six02Voronoi AS a,
                                     voronoiSix02_change%s AS b
                                WHERE (ST_Equals(a.geom, b.geom) = TRUE)
                                """) % (str(df.Store[i]))


        # Compare all after to the previously created layer
        # String of unchanged stores, that have the same polygon from the previous layer, take inverse of that for change
        # Since ST_EQUAL = False compares many:many
        else:
            prevStore = i - 1
            selectChanged = ("""SELECT b.storeid
                                FROM voronoiSix02_change%s AS a,
                                     voronoiSix02_change%s AS b
                                WHERE (ST_Equals(a.geom, b.geom) = TRUE)
                                """) % (str(df.Store[prevStore]), str(df.Store[i]))
        cur.execute(selectChanged)
        changed = cur.fetchall()
        strList = []
        for st in changed:
            _store = int(st[0])
            strList.append(_store)

        # Append changed polygons to original file with date_changed attached
        strList = str(strList).strip('[]')
        appendChanged = ("""INSERT INTO public.Six02voronoi(PATH, geom, storeid, str_name, 
                            city, state, lat, lon, open_date, close_date, date_changed)
                            SELECT *
                            FROM voronoiSix02_change%s
                            WHERE storeid NOT IN (%s)
                        """) % (str(df.Store[i]), str(strList))
        cur.execute(appendChanged)
        conn.commit()

    #--------------------------------------------------------------------------------------------------------------------#

    # Fix date_changed type for Final Output shapefile
    dateFix = cur.execute("""ALTER table Six02voronoi
                             ALTER date_changed type date using(date_changed::date)""")

    conn.commit()
    print "Query Complete"
    cur.close()
except:
    traceback.print_exc()
    print "Error/Unable to connect to the database"

