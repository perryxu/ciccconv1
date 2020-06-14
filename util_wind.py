from WindPy import w
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt

def windChecker():
    if not w.isconnected():
        w.start()

def _wsetDF_backup(fields, hdate, suffix = ''):
    windChecker()
    temp = w.wset(fields, "date={hdate}{suffix}".format(hdate=hdate.isoformat(), suffix=suffix))
    if temp.ErrorCode == 0 and temp.Data:
        df_temp = pd.DataFrame()
        for k, v in enumerate(temp.Fields):
            df_temp[v] = temp.Data[k]
        return df_temp
    else:
        raise(Exception('[ERROR] interface error or no data retrieved.'))

def wsetDF(fields, hdate, suffix =''):
    windChecker()
    _,x = w.wset(fields, "date={hdate}{suffix}".format(hdate=hdate.isoformat(), suffix=suffix), usedf=True)
    return x

def _wssDF_backup(codes, fields, hdate, freq = 'D', suffix = ''):
    windChecker()
    temp = w.wss(','.join(set(codes)), fields,
                 "tradeDate={hdate};unit=1;priceAdj=U;cycle={freq}{suffix}".format(hdate=hdate.isoformat(),
                                                                                    freq = freq,
                                                                                    suffix=suffix))
    df_temp = pd.DataFrame()
    df_temp['WINDCODE'] = temp.Codes
    df_temp['DATE'] = hdate.isoformat()
    if temp.ErrorCode == 0 and temp.Data:
        for k,v in enumerate(temp.Fields):
            df_temp[v] = temp.Data[k]
        return df_temp
    else:
        raise(Exception('[ERROR] interface error or no data retrieved.'))

def wssDF(codes, fields, hdate, freq ='D', suffix =''):
    windChecker()
    _, x = w.wss(','.join(set(codes)), fields,
                 "tradeDate={hdate};unit=1;priceAdj=U;cycle={freq}{suffix}".format(hdate=hdate.isoformat(),
                                                                                    freq=freq,
                                                                                    suffix=suffix), usedf=True)
    x.reset_index(inplace=True)
    x.rename(columns = {'index':'WINDCODE'},inplace=True)
    x['DATE'] = hdate.isoformat()
    return x

def _wsdDF(codes, startDate, endDate, fields, freq='D', adj='F'):
    return w.wsd(codes, ','.join(fields), startDate.isoformat(), endDate.isoformat(),
                 "Period={freq};PriceAdj={adj}".format(freq = freq, adj = adj), usedf=True)

def _engine(path):
    return create_engine('sqlite://'+path, echo=False)

def append(df, tablename, engine, type = 'append'):
    if type == 'append':
        pass
    else:
        df.to_sql(tablename, con=engine)

def wtdays(sdate1, sdate2, suffix = ''):
    windChecker()
    return w.tdays(sdate1, sdate2, suffix).Data[0]

if __name__ == '__main__':
    # horizon date (cross panel)
    hdate = dt.date(2020, 6, 9)
    # conv complete list
    suffix_conv_sec_tick = ';sectorid=a101020600000000'
    df_conv_id = wsetDF('sectorconstituent', hdate, suffix_conv_sec_tick)
    df_conv_id = df_conv_id[df_conv_id['wind_code'].str.contains('.SH') | df_conv_id['wind_code'].str.contains('.SZ')]
    # conv time series
    # fields =   "curyield,strbpremiumratio,convvalue,convpremiumratio,diluterate,\
    #             clause_conversion2_bondlot,clause_conversion2_conversionproportion,\
    #             close,low,high,amt,turn,amount,latestissurercreditrating"
    conv_fields = {'curyield':u'转债到期收益率','strbpremiumratio':u'债底溢价率',
                   'convvalue':u'转股价值','convpremiumratio':u'转股溢价率',
                   'diluterate':u'转股稀释率','clause_conversion2_bondlot':u'未转股金额规模',
                   'clause_conversion2_conversionproportion':u'未转股比例',
                   'close':u'收盘价','low':u'最低价','high':u'最高价','amt':u'成交金额',
                   'turn':u'换手率','amount':u'债项评级','latestissurercreditrating':u'发行人评级'}
    df_conv_mkt = wssDF(df_conv_id.wind_code.tolist(), ','.join(conv_fields.keys()), hdate)
    df_conv_mkt.rename(columns = {k.upper():v for k, v in conv_fields.items()}, inplace=True)
    #

