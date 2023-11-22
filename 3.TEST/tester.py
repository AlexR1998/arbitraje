import pandas as pd
from sys import path
path.append(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\2.ESTIMACION')
from cointegracion import cointegracion
import MetaTrader5 as mt5
from datetime import datetime, timedelta
import numpy as np

#==========================================================================================================================
#   VARIABLES DE ENTRADA
#==========================================================================================================================

evalua=False
solo_largo=False
years=10
timeframe=mt5.TIMEFRAME_D1
fecha_inicial='1/1/2013'
fecha_final='17/11/2023'
modelo='engle-granger'
desvesta=3
rolling_desvesta=252
db=r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\1.DATA\arbitraje'
quotes=['EURUSD','GBPUSD','AUDUSD','USDCAD','USDCHF','USDJPY','AUDNZD','AUDCAD','AUDCHF','AUDJPY','CHFJPY','EURGBP','EURAUD'
        ,'EURJPY','EURCHF','EURNZD','CADCHF','EURCAD','GBPCHF','GBPJPY','CADJPY','GBPAUD','GBPCAD']


#==========================================================================================================================
#   TRANSFORMACIONES INICIALES
#==========================================================================================================================

fecha_inicial=datetime.strptime(fecha_inicial,'%d/%m/%Y')
fecha_final=datetime.strptime(fecha_final,'%d/%m/%Y')
c=cointegracion(0.01,db)
fecha=datetime(year=fecha_inicial.year,month=fecha_inicial.month,day=fecha_inicial.day,hour=0,minute=0)

#==========================================================================================================================
#   PROCESO
#==========================================================================================================================

f_inicial=datetime(year=fecha.year-years,month=fecha.month,day=fecha.day)
data,pairs=c.data_reader_mt5(quotes,f_inicial,fecha_final,timeframe)

val=[]
operaciones=[]
datas=[]
for fecha in data[data.index>=fecha_inicial].index:
    print(fecha)
    #Filtro de fines de semana
    
    try:
        f_inicial=datetime(year=fecha.year-years,month=fecha.month,day=fecha.day)
    except ValueError:
        #Manejo para años bisiestos
        f_inicial=datetime(year=fecha.year-years,month=fecha.month,day=fecha.day-1)
        
    df=data.loc[(data.index>=f_inicial) & (data.index<=fecha)]

    pares_validados=pd.DataFrame()
    if evalua:
        print('Evaluando pares...')
        pares_validados=c.validador(df,pairs,model=modelo)
        val.append(pares_validados)
        print('Operando...')
        operaciones.append(c.genera_alertas(years,timeframe,fecha,modelo,desvesta,rolling_desvesta,pares=pares_validados,df=df,trades_export=True))
    else:
        print('Operando...')
        operaciones.append(c.genera_alertas(years,timeframe,fecha,modelo,desvesta,rolling_desvesta,df=df,trades_export=True))


operaciones=pd.concat(operaciones)
operaciones['FECHA']=operaciones['FECHA'].astype(str)
operaciones['ULTIMA_EVALUACION']=None
print('Validando operaciones...')
trades=c.follow_alertas(timeframe,fecha_final,modelo,alertas=operaciones)



#==========================================================================================================================
#   CONSOLIDA OPERACIONES
#==========================================================================================================================

trades['FECHA_SALIDA']=pd.to_datetime(trades['FECHA_SALIDA'])
trades['FECHA_ENTRADA']=pd.to_datetime(trades['FECHA_ENTRADA'])

trades['HOLDEO_DIAS']=(trades['FECHA_SALIDA']-trades['FECHA_ENTRADA']).dt.days
trades['RETORNO_Y']=((trades['SALIDA_Y']/trades['ENTRADA_Y'])-1)*np.where(trades['DIRECCION_OP']=='CORTO Y',-1,1)
trades['RETORNO_X']=((trades['SALIDA_X']/trades['ENTRADA_X'])-1)*np.where(trades['DIRECCION_OP']=='CORTO Y',1,-1)

if solo_largo:
    trades['RETORNO_Y']=np.where(trades['DIRECCION_OP']=='CORTO Y',0,trades['RETORNO_Y'])
    trades['RETORNO_X']=np.where(trades['DIRECCION_OP']=='CORTO Y',trades['RETORNO_X'],0)

trades['RETORNO_TOTAL']=trades['RETORNO_X']+trades['RETORNO_Y']
trades['RETORNO_TOTAL_EA']=(1+trades['RETORNO_TOTAL'])**(365/trades['HOLDEO_DIAS'])-1
trades['RETORNO_Y_EA']=(1+trades['RETORNO_Y'])**(365/trades['HOLDEO_DIAS'])-1
trades['RETORNO_X_EA']=(1+trades['RETORNO_X'])**(365/trades['HOLDEO_DIAS'])-1

pairs_result=trades.groupby(['X','Y']).agg(RETORNO_TOTAL_EA=('RETORNO_TOTAL_EA','sum'),RETORNO_X_EA=('RETORNO_X_EA','sum'),RETORNO_Y_EA=('RETORNO_Y_EA','sum'),DESVESTA_RET=('RETORNO_TOTAL_EA','std')).reset_index()


operaciones['FECHA']=pd.to_datetime(operaciones['FECHA'])
trades=trades.merge(operaciones[['X','Y','FECHA','SL_Y']],how='left',left_on=['X','Y','FECHA_ENTRADA'],right_on=['X','Y','FECHA'])

trades.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\trades.xlsx',index=False)
operaciones.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\operaciones.xlsx',index=False)
pairs_result.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\pairs_result.xlsx',index=False)

# m_r.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\operaciones_2.xlsx',index=False)
#data.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\data.xlsx',index=True)

# COMENZAR A PROGRAMAR TESTER
    #HACER VALIDACIÓN DE CIERRES CONTRA MAXIMOS/MINIMOS (DEPENDIENDO DIRECCIÓN_OP) PARA TENER EN CUENTA MOVIMIENTOS INTRADÍA
    #INCORPORAR COSTOS DE TRANSACCIÓN
    #MÉTRICAS RESUMEN (RETORNO AGREGADO, DESVESTA, SHARPE, SORTINO)
    #SIMULADOR DE TRADES AGREGADOS ¿?

# COMO GENERALIZAR LAS REGLAS DE TRADING PARA INTERCAMBIAR ENTRE UNAS U OTRAS ¿?
    #ESTO SE LOGRARÁ CUANDO SE TENGA UNA PROPUESTA DE OPERATIVA DIFERENTE (COMPARAR AMBAS Y ABSTRAER)



     
