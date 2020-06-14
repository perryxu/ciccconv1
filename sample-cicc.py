import pandas as pd
import datetime
from data_process_conv import getengine
from sklearn.linear_model import LinearRegression

def getTS(sql, i = ['DATE','WINDCODE']):
    obj = pd.read_sql(sql, getengine())
    obj.DATE = pd.to_datetime(obj.DATE)
    obj.set_index(i, inplace=True)
    return obj

# 返回满足当日成交量条件的债券代码
def selByAmt(obj, date, other=None, noCrazy=True):
    # 当日有成交，loc可以是一目计算符
    # t = obj.DB['Amt'].loc[date] > 0
    t = obj['CB_TURNOVER_AMT'].loc[date] > 0
    # 当日有成交但是成交金额不大于流通市值（换手率不超过100%）（避免某些被游资爆炒严重脱离基本面的券）
    # t *= obj.DB['Amt'].loc[date] < (obj.DB['Close'].loc[date] * obj.DB['Outstanding'].loc[date] / 100.0)
    t &= obj['CB_TURNOVER_AMT'].loc[date] < obj['CB_REMAIN_AMT'].loc[date]
    # other是个字典，k是某一列，v是关于这一列取值的范围
    if other:
        for k, v in other.iteritems():
            t &= obj.DB[k].loc[date].between(v[0], v[1])
    # t[t]是只保留原t真值的短序列
    codes = list(t[t].index)
    return codes

# 返回满足在指定区间里有过成交的债券代码
def selByAmtPq(obj, start, end):
    # t = obj.DB['Amt'].loc[start:end].sum() > 0
    t = obj['CB_TURNOVER_AMT'].loc[start:end].groupby('WINDCODE').sum() > 0
    codes = list(t[t].index)
    return codes

# 返回特定债券的日收益率
def getCBReturn(date, codes=None, obj=None):
    # 如果obj为空
    if obj is None:
        obj = getTS('SELECT * FROM TS_CONV ORDER BY DATE, WINDCODE')
        # obj = cb.cb_data()
    # 如果债券代码为空
    if codes is None:
        codes = selByAmt(obj, date)
    # df.index.get_loc返回index的数字计序（loc和iloc之间的转换）
    loc = obj['CB_TURNOVER_AMT'].index.get_loc(date)
    # 指定债券codes的收益率，codes是index
    return 100.0 * (obj['CB_CLOSE'][codes].iloc[loc] / obj['CB_CLOSE'][codes].iloc[loc - 1] - 1.0)

def getUnderlyingCodeTable(codes):
    """
    返回特定债券codes的标的股票代码（不含137和117开头的债券）
    :param codes: 指定债券代码
    :return: 返回codes对应的标的股票代码
    """
    # sql = '''select a.s_info_windcode cbCode,b.s_info_windcode underlyingCode
    # from winddf.ccbondissuance a,winddf.asharedescription b
    # where a.s_info_compcode = b.s_info_compcode and
    # length(a.s_info_windcode) = 9 and
    # substr(a.s_info_windcode,8,2) in ('SZ','SH') and
    # substr(a.s_info_windcode,1,3) not in ('137','117') '''
    sql = """
    SELECT WINDCODE AS CBCODE, CB_UD_CODE AS UNDERLYINGCODE FROM STATIC_CONV
    WHERE LENGTH (CBCODE) = 9 AND SUBSTR(CBCODE, 8, 2) IN ('SZ', 'SH')
    AND SUBSTR(CBCODE, 1, 3) NOT IN ('137','117')
    AND CBCODE IN ("{}")
    """.format('","'.join(codes))
    # con = login(1) # 为我们的万得数据链接对象
    return pd.read_sql(sql, getengine(), index_col='CBCODE')

def cbInd(codes):
    """
    根据underlying的中信行业分类映射到”行业名称“，并返回该行业名称。
    :param codes: 债券的代码
    :return: 标的的行业名称（index是债券的代码）
    """
    dfUd = getUnderlyingCodeTable(codes)
    # sql将中信行业分类mapping到另一张表的行业名，之后采用另一张表的行业名，否则为None。
    # sql = '''select a.s_info_windcode as udCode,
    # b.industriesname indName
    # from
    # winddf.ashareindustriesclasscitics a,
    # winddf.ashareindustriescode b
    # where substr(a.citics_ind_code,1,4) = substr(b.industriescode,1,4) and
    # b.levelnum = '2' and
    # a.cur_sign = '1' and
    # a.s_info_windcode in ({_codes})
    # '''.format(_codes = rsJoin(list(set(dfUd['UNDERLYINGCODE']))))

    # 务必注意用python list format sql的陷阱
    sql = '''
    SELECT WINDCODE AS UDCODE, CB_UD_IND_CITIC AS INDNAME
    FROM STATIC_UD
    WHERE WINDCODE IN ("{}")
    '''.format('","'.join(list(set(dfUd['UNDERLYINGCODE']))))

    # con = login(1) # 为我们的万得数据链接对象
    dfInd = pd.read_sql(sql, getengine(), index_col='UDCODE')
    dfUd['Ind'] = dfUd['UNDERLYINGCODE'].apply(lambda x: dfInd.loc[x, 'INDNAME'] if x in dfInd.index else None)
    return dfUd['Ind']

def factorInd(codes, cbIndMat=None):
    """
    根据债券代码生成行业标记矩阵
    :param codes: 债券代码
    :param cbInd: 债券代码对应的行业名称
    :return: 行业单位阵
    """
    if cbIndMat is None:
        cbIndMat = pd.DataFrame({'ind':cbInd(codes)})
    # cbInd = pd.merge(cbInd, indCls, left_on='ind', right_index=True)
    # 自建单位阵
    dfRet = pd.DataFrame(index=codes, columns=set(cbIndMat['ind']))
    for c in dfRet.columns:
        tempCodes = cbIndMat.loc[cbIndMat['ind'].apply(lambda x:x.encode('gbk')) == c.encode('gbk')].index
        dfRet.loc[tempCodes, c] = 1.0
    return dfRet.fillna(0)

# ***********市值：加权权重***********
def factorSize_cb_outstanding(codes, start, end, obj=None):
    if obj is None:
        obj = getTS('SELECT * FROM TS_CONV ORDER BY DATE, WINDCODE')
    ost_mv = obj['CB_REMAIN_AMT'].loc[start:end, codes]
    return ost_mv

# across columns算分位百分数
def rankCV(df):
    rk = df.rank(axis=1, pct=True)
    return (rk - 0.5).div(rk.std(axis=1), axis='rows')

def t_test(lr, x, y):
    n = len(x) * 1.0
    predY = lr.predict(x)
    e2 = sum((predY - y) ** 2)
    varX = pd.np.var(x) * n
    t = lr.coef_ * pd.np.sqrt(varX) / pd.np.sqrt(e2 / n)
    return t[-1]

def oneFactorReg(start, end, dfFactor, factorName='ToBeTest', dfFctInd=None, obj=None):
    if obj is None:
        obj = getTS('SELECT * FROM TS_CONV ORDER BY DATE, WINDCODE')
    if dfFctInd is None:
        codes = selByAmtPq(obj, start, end)
        # dfFactor为非行业因子，row是日期，col是券号（单因子，如果需要支持多因子，使用dict）
        # 有些债券可能因子数据不全，所以只取行业属性和因子的交集
        codes = list(set(codes).intersection(list(dfFactor.columns)))
        dfFctInd = factorInd(codes)

    # 不包含第一天，因为没有return可以算。
    arrDates = list(obj['CB_TURNOVER_AMT'].loc[start:end].index.get_level_values('DATE').drop_duplicates())[1:]
    lr = LinearRegression(fit_intercept=True)
    dfRet = pd.DataFrame(index=arrDates, columns=['One'] + list(dfFctInd.columns) + [factorName, 't', 'score'])
    dfCBMV = factorSize_cb_outstanding(codes, start, end, obj)

    for date in arrDates:
        print(date)

        # 得到需要的债券代码
        tCodes = selByAmt(obj, date)
        # 得到当天较前一天的债券收益率（是不是有点日期顺序的问题？？？）
        srsReturn = getCBReturn(date, tCodes, obj)

        dfX = pd.DataFrame(index=tCodes)
        # 填入行业单位阵
        dfX[list(dfFctInd.columns)] = dfFctInd
        # 填入单因子
        dfX[factorName] = dfFactor.loc[date]
        dfX.dropna(inplace=True)
        idx = dfX.index
        # WLS：以市值为权重
        arrW = pd.np.sqrt(dfCBMV.loc[date, idx])
        arrW /= arrW.sum()

        lr.fit(dfX.loc[:, :], srsReturn[idx], arrW)

        dfRet.loc[date, list(dfFctInd.columns) + [factorName]] = lr.coef_
        dfRet.loc[date, 'One'] = lr.intercept_
        dfRet.loc[date, 't'] = t_test(lr, dfX.loc[:, :], srsReturn[idx])

        dfRet.loc[date, 'score'] = lr.score(dfX.loc[:, :], srsReturn[idx], arrW)

    print(pd.np.abs(dfRet['t']).mean())
    print(pd.np.abs(dfRet['score']).mean())
    return dfRet

if __name__ == '__main__':
    start = datetime.date(2020,2,3)
    end = datetime.date(2020,6,12)
    dfFactor = getTS('SELECT DATE, WINDCODE, CB_REMAIN_AMT FROM TS_CONV ORDER BY DATE, WINDCODE')
    dfFactor = dfFactor.unstack(1)
    dfFactor.columns = dfFactor.columns.droplevel(0)
    dfRet = oneFactorReg(start, end, rankCV(dfFactor))