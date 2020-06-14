import os
import logging
from util_wind import *
from util_db import *
from sqlalchemy import create_engine

def getengine(db_path = 'sqlite:///' + os.getcwd() + '\\CONVDATA.db', echo_param = False):
    return create_engine(db_path, echo = echo_param)

def conv_mktdata_update(hdate, db_path = None):
    """
    设置数据库路径和可转债板块代码
    """
    db_path = 'sqlite:///' + os.getcwd() + '\\CONVDATA.db'
    conv_sec_tick = ';sectorid=a101020600000000'
    fields = 'sectorconstituent'

    # 获得万得可转债sector代码；创建引擎
    df_conv_id = wsetDF(fields, hdate, conv_sec_tick)
    df_conv_id = df_conv_id[df_conv_id['wind_code'].str.contains('.SH') |
                            df_conv_id['wind_code'].str.contains('.SZ')]

    engine = create_engine(db_path, echo=False)

    # 可转债标的股票行业数据及其它静态数据导入数据库
    fields = {'underlyingcode':'CB_UD_CODE',
              'issueamount':'CB_ISSUE_AMT',
              'carrydate':'CB_CARRY_DATE',
              'delist_date':'CB_DELIST_DATE',
              'maturitydate':'CB_MATURITY_DATE'}
    df_conv_static = wssDF(df_conv_id.wind_code.dropna().unique().tolist(), ','.join(fields.keys()), hdate)
    df_conv_static.rename(columns = {k.upper():v for k ,v in fields.items()}, inplace = True)
    # datetime64(timestamp) vs datetime
    df_conv_static = df_conv_static[(df_conv_static.CB_DELIST_DATE >= pd.Timestamp(hdate))]
    logging.warning('=== STATIC_CONV updated: {} ==='.format(hdate.isoformat()))
    df_db(engine, df_conv_static, 'STATIC_CONV', mode='replace')

    # 可转债行情数据导入数据库
    fields = {'curyield':'CB_YIELD',
              'strbpremiumratio':'CB_BOND_PREMIUM_RATE',
              'convvalue':'CB_CONV_PARITY',
              'convpremiumratio':'CB_CONV_PREMIUM_RATE',
              'diluterate':'CB_DELUTION_RATE',
              'clause_conversion2_bondlot':'CB_REMAIN_AMT',
              'close':'CB_CLOSE' ,'low':'CB_LOW' ,'high':'CB_HIGH',
              'amt':'CB_MKTVALUE',
              'turn':'CB_TURNOVER_AMT',
              'amount':'CB_BOND_RATING',
              'latestissurercreditrating':'CB_ISSUER_RATING'}

    tablename = 'TS_CONV'
    table_str = """
    CREATE TABLE IF NOT EXISTS {} (
    DATE DATE NOT NULL,
    WINDCODE TEXT NOT NULL,
	CB_YIELD FLOAT NOT NULL,
	CB_BOND_PREMIUM_RATE FLOAT NOT NULL,
	CB_CONV_PARITY FLOAT NOT NULL,
	CB_CONV_PREMIUM_RATE FLOAT NOT NULL,
	CB_DELUTION_RATE FLOAT NOT NULL,
	CB_REMAIN_AMT FLOAT NOT NULL,
	CB_CLOSE FLOAT NOT NULL,
	CB_LOW FLOAT NOT NULL,
	CB_HIGH FLOAT NOT NULL,
	CB_MKTVALUE FLOAT NOT NULL,
	CB_TURNOVER_AMT FLOAT NOT NULL,
	CB_BOND_RATING TEXT NOT NULL,
	CB_ISSUER_RATING TEXT NOT NULL,
	PRIMARY KEY (DATE, WINDCODE)
    );""".format(tablename)

    df_conv_mkt = wssDF(df_conv_static.WINDCODE.dropna().unique().tolist(), ','.join(fields.keys()), hdate)
    df_conv_mkt.rename(columns = {k.upper():v for k,v in fields.items()}, inplace = True)

    if tablename not in engine.table_names():
        db_all_query(engine, table_str, False)
    logging.warning('=== TS_CONV updated: {} ==='.format(hdate.isoformat()))
    df_db(engine, df_conv_mkt, tablename, mode='append')

    fields = {'industry_gics':'CB_UD_IND_GICS',
              'industry_sw':'CB_UD_IND_SW',
              'industry_citic':'CB_UD_IND_CITIC'}
    df_ud_static = wssDF(df_conv_static.CB_UD_CODE.dropna().unique().tolist(), ','.join(fields.keys()), hdate, ";industryType=1")
    df_ud_static.rename(columns = {k.upper():v for k,v in fields.items()}, inplace = True)
    logging.warning('=== STATIC_UD updated: {} ==='.format(hdate.isoformat()))
    df_db(engine, df_ud_static, 'STATIC_UD', mode='replace')

    # 可转债标的股票行情数据导入数据库
    fields = {'pe_ttm':'CB_UD_PE_TTM',
              'pb_mrq':'CB_UD_PB_MRQ',
              'ps_ttm':'CB_UD_PS_TTM',
              'close':'CB_UD_CLOSE',
              'high':'CB_UD_HIGH',
              'low':'CB_UD_LOW',
              'mkt_freeshares':'CB_UD_CIRC_FREE',
              'mkt_cap':'CB_UD_MKT_CAP'}

    tablename = 'TS_UD'
    table_str = """
        CREATE TABLE IF NOT EXISTS {} (
        DATE DATE NOT NULL,
        WINDCODE TEXT NOT NULL,
    	CB_UD_PE_TTM FLOAT NOT NULL,
    	CB_UD_PB_MRQ FLOAT NOT NULL,
    	CB_UD_PS_TTM FLOAT NOT NULL,
    	CB_UD_CLOSE FLOAT NOT NULL,
    	CB_UD_HIGH FLOAT NOT NULL,
    	CB_UD_LOW FLOAT NOT NULL,
    	CB_UD_CIRC_FREE FLOAT NOT NULL,
    	CB_UD_MKT_CAP FLOAT NOT NULL,
        PRIMARY KEY (DATE, WINDCODE)
        );""".format(tablename)

    df_ud_mkt = wssDF(df_conv_static.CB_UD_CODE.dropna().unique().tolist(), ','.join(fields.keys()), hdate)
    df_ud_mkt.rename(columns = {k.upper():v for k,v in fields.items()}, inplace = True)

    if tablename not in engine.table_names():
        db_all_query(engine, table_str, False)
    logging.warning('=== TS_UD updated: {} ==='.format(hdate.isoformat()))
    df_db(engine, df_ud_mkt, tablename, mode='append')

    return engine

if __name__ == '__main__':
    ls_dates = wtdays('2020-1-1','2020-1-31')
    for date in ls_dates:
        logging.warning('DATA UPDATE: {}'.format(date.date().isoformat()))
        conv_mktdata_update(date.date())