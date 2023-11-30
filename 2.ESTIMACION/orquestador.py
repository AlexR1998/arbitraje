import pandas as pd
from datetime import datetime, timedelta
from cointegracion import cointegracion
import MetaTrader5 as mt5
import sqlite3

#==========================================================================================================================
#   VARIABLES DE ENTRADA
#==========================================================================================================================

m_timeframe=mt5.TIMEFRAME_D1
fecha=datetime.now()
fecha=datetime.strptime('31/12/2022','%d/%m/%Y')
fecha_inicial=datetime.strptime('31/12/2022','%d/%m/%Y')
modelos=['engle-granger']
years=10
fuente='bloomberg'                #mt5 o bloomberg

#COSTOS TRANSACCIONALES
comision=0.03
iva=0.19
swap=0.0

db=r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\1.DATA\arbitraje'

#==========================================================================================================================
#   TRANSFORMACIONES INICIALES
#==========================================================================================================================

if fuente=='bloomberg':
    quotes=['BBVACOL CB Equity', 'BCOLO CB Equity',	'BHI CB Equity', 'BMC CB Equity', 'BOGOTA CB Equity', 'BVC CB Equity',	
        'CELSIA CB Equity',	'CEMARGOS CB Equity', 'CNEC CB Equity',	'CONCONC CB Equity', 'CORFICOL CB Equity', 'ECOPETL CB Equity',	
        'ELCONDOR CB Equity',	'ENKA CB Equity', 'ETB CB Equity',	'EXITO CB Equity',	'FABRI CB Equity', 'GEB CB Equity',
        'GRUPOBOL CB Equity', 'GRUPOARG CB Equity',	'AVAL CB Equity', 'GRUPOSUR CB Equity',	'HCOLSEL CB Equity', 'ICOLCAP CB Equity',
        'ISA CB Equity', 'MINEROS CB Equity', 'NUTRESA CB Equity', 'OCCID CB Equity', 'PFAVAL CB Equity', 'PFBBVACO CB Equity', 
        'PFBCOLO CB Equity', 'PFCEMARG CB Equity', 'PFCORCOL CB Equity',	'PFDAVVND CB Equity', 'PFGRUPOA CB Equity',
        'PFGRUPSU CB Equity', 'POPULA CB Equity', 'PROMIG CB Equity', 'TERPEL CB Equity']
    db+='_col'
else:
    quotes=['EURUSD','GBPUSD','AUDUSD','USDCAD','USDCHF','USDJPY','AUDNZD','AUDCAD','AUDCHF','AUDJPY','CHFJPY','EURGBP','EURAUD'
        ,'EURJPY','EURCHF','EURNZD','CADCHF','EURCAD','GBPCHF','GBPJPY','CADJPY','GBPAUD','GBPCAD']


c=cointegracion(0.01,db_path=db,comision=comision,swap=swap,iva_comision=iva)

#==========================================================================================================================
#   ACTUALIZACIÓN CUADROS DE COINTEGRACIÓN
#==========================================================================================================================
if fecha.hour<17:
    fecha=datetime(year=fecha.year,month=fecha.month,day=fecha.day-1,hour=0,minute=0)
else:
    fecha=datetime(year=fecha.year,month=fecha.month,day=fecha.day,hour=0,minute=0)

fecha_i=datetime(year=fecha_inicial.year-10,month=fecha_inicial.month,day=fecha_inicial.day)

if fuente=='mt5':
    data,pairs=c.data_reader_mt5(quotes,fecha_i,fecha,m_timeframe,d_type='CLOSE')
    highs,_=c.data_reader_mt5(quotes,fecha_i,fecha,m_timeframe,d_type='HIGH')
    lows,_=c.data_reader_mt5(quotes,fecha_i,fecha,m_timeframe,d_type='LOW')
elif fuente=='bloomberg':
    data,pairs=c.bloomberg_data_reader(quotes,fecha_i,fecha,d_type='CLOSE')
    highs,_=c.bloomberg_data_reader(quotes,fecha_i,fecha,d_type='HIGH')
    lows,_=c.bloomberg_data_reader(quotes,fecha_i,fecha,d_type='LOW')


alertas_c=[]
cierres_c=[]
resultado_c=[]
for modelo in modelos:
    print(modelo)
    print(f'- GENERANDO CUADRO DE COINTEGRACIÓN {modelo}')
    resultado=c.validador(data,pairs,model=modelo,ingestar=True)

#==========================================================================================================================
#   CREACIÓN DE ALERTAS
#==========================================================================================================================

    print(f'- CREANDO ALERTAS {modelo}')
    #conn=sqlite3.connect(db)
    #ult_alerta=pd.read_sql(f"SELECT MAX(FECHA) AS FECHA FROM HISTORICO_ALERTAS WHERE MODELO='{modelo}'",conn)['FECHA'].values[0]
    #conn.close()
    ult_alerta=None
    if ult_alerta!=None:
        fecha_inicial=datetime.strptime(ult_alerta,'%Y-%m-%d')
        
    alertas=[]
    for fecha in data[data.index>=fecha_inicial].index:
        f_inicial=datetime(year=fecha.year-years,month=fecha.month,day=fecha.day)
        df=data.loc[(data.index>=f_inicial) & (data.index<=fecha)]

        alertas.append(c.genera_alertas(df=df, fecha_final=fecha,model=modelo,desvest=3,rolling_desvest=252,ingestar=True))
        print(fecha)

#==========================================================================================================================
#   ACTUALIZACIÓN DE ALERTAS GENERADAS
#==========================================================================================================================

    print(f'- ACTUALIZANDO ESTADO DE ALERTAS GENERADAS {modelo}')
    cierres=c.follow_alertas(df=data, high=highs, low=lows, fecha_final=fecha, model=modelo, ingestar=True)

    resultado_c.append(resultado)
    alertas_c.append(alertas)
    cierres_c.append(cierres)


# conn=sqlite3.connect(db)
# m_r=pd.read_sql(f"SELECT * FROM PAIRS_FILTER WHERE MODELO='engle-granger'",conn)
# m_c=pd.read_sql(f"SELECT * FROM TEST_SUMMARY WHERE MODELO='engle-granger'",conn)
# conn.close()

# m_r.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\pairs_filter.xlsx')
# m_c.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\test_summary.xlsx')

conn=sqlite3.connect(db)
c=conn.cursor()
c.execute('DROP TABLE TEST_SUMMARY')
conn.commit()
c.execute('DROP TABLE HISTORICO_ALERTAS')
conn.commit()
c.close()




