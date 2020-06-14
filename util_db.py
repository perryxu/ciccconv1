import pandas as pd

# def set_primary_key(engine, tablename, keystr = None):
#     if keystr:
#         with engine.connect() as con:
#             con.execute(("ALTER TABLE {} ADD PRIMARY KEY ({})").format(tablename, keystr))
#     else:
#         pass

def df_db(engine, df, tablename, mode ='replace', index = False, keystr ='WINDCODE,DATE'):
    chunksize = 30
    if (mode == 'append'):
        df.to_sql(name = 'temp', con = engine, if_exists = 'replace', index = index, chunksize = chunksize)
        df_col = pd.read_sql('SELECT name FROM PRAGMA_TABLE_INFO("{}")'.format(tablename),engine)
        with engine.begin() as con:
            sql = 'INSERT OR IGNORE INTO {} SELECT {} FROM temp'.format(tablename, ','.join(df_col.name))
            con.execute(sql)
            con.execute('DROP TABLE temp')
    else:
        df.to_sql(name = tablename, con = engine, if_exists = 'replace', index = index, chunksize = chunksize)

def db_all_query(engine, sql, fetch_ind = True):
    cursor = engine.execute(sql)
    if fetch_ind:
        rst = cursor.fetchall()
        return rst
    cursor.close()
    return None
