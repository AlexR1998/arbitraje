import pandas as pd
from sys import path
path.append(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\2.ESTIMACION')
from cointegracion import cointegracion
import MetaTrader5 as mt5
from datetime import datetime, timedelta
import numpy as np
import sqlite3

#==========================================================================================================================
#   VARIABLES DE ENTRADA
#==========================================================================================================================


evalua=False
solo_largo=True
years=10                    #AÑOS DE DATOS TOMADOS PARA CADA ESTIMACIÓN DEL MODELO
timeframe=mt5.TIMEFRAME_D1
fecha_inicial='31/12/2022'    #FECHA DE INICIO DEL TESTING
fecha_final='21/11/2023'    #FECHA FINAL DEL TESTING
modelo='engle-granger'
desvesta=3
rolling_desvesta=252
ret_min=0.05
fuente='bloomberg'                #mt5 o bloomberg
db=r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\1.DATA\arbitraje'

#COSTOS TRANSACCIONALES
comision=.03    #SE COBRA COMISIÓN MÁXIMA (3%) POR COMPRA DE ACCIONES
iva=0.19
swap=.0

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


fecha_inicial=datetime.strptime(fecha_inicial,'%d/%m/%Y')
fecha_final=datetime.strptime(fecha_final,'%d/%m/%Y')
c=cointegracion(0.01,db,comision=comision,iva_comision=iva,swap=swap)
fecha=datetime(year=fecha_inicial.year,month=fecha_inicial.month,day=fecha_inicial.day,hour=0,minute=0)

f_inicial=datetime(year=fecha.year-years,month=fecha.month,day=fecha.day)

if fuente=='mt5':
    data,pairs=c.data_reader_mt5(quotes,f_inicial,fecha_final,timeframe,d_type='CLOSE')
    h,pairs=c.data_reader_mt5(quotes,f_inicial,fecha_final,timeframe,d_type='HIGH')
    l,pairs=c.data_reader_mt5(quotes,f_inicial,fecha_final,timeframe,d_type='LOW')
elif fuente=='bloomberg':
    data,pairs=c.bloomberg_data_reader(quotes,f_inicial,fecha_final,d_type='CLOSE')
    h,_=c.bloomberg_data_reader(quotes,f_inicial,fecha_final,d_type='HIGH')
    l,_=c.bloomberg_data_reader(quotes,f_inicial,fecha_final,d_type='LOW')
#==========================================================================================================================
#   PROCESO
#==========================================================================================================================

val=[]
operaciones=[]
datas=[]
for fecha in data[data.index>=fecha_inicial].index:
    print(fecha)
    try:
        f_inicial=datetime(year=fecha.year-years,month=fecha.month,day=fecha.day)
    except ValueError:
        #Manejo para años bisiestos
        f_inicial=datetime(year=fecha.year-years,month=fecha.month,day=fecha.day-1)
        
    df=data.loc[(data.index>=f_inicial) & (data.index<=fecha)]
    

    pares_validados=pd.DataFrame()
    if evalua:
        print('Evaluando pares...')
        pairs=nueva_lista_tuplas = [tupla for tupla in pairs if any(valor in tupla for valor in df.columns[(df.isna().sum()/df.isna().count())<=0.5])]
        pares_validados=c.validador(df,pairs,model=modelo)
        val.append(pares_validados)
        print('Operando...')
        operaciones.append(c.genera_alertas(df=df,fecha_final=fecha,model=modelo,desvest=desvesta,rolling_desvest=rolling_desvesta,pares=pares_validados,trades_export=True,ret_exigido=ret_min))

    else:
        print('Operando...')
        filtros=f'''X NOT IN ({','.join('"'+df.columns[(df.isna().sum()/df.isna().count())>0.8]+'"')}) AND Y NOT IN ({','.join('"'+df.columns[(df.isna().sum()/df.isna().count())>0.8]+'"')})'''
        operaciones.append(c.genera_alertas(df=df,fecha_final=fecha,model=modelo,desvest=desvesta,rolling_desvest=rolling_desvesta,trades_export=True,filtros=filtros,ret_exigido=ret_min))


operaciones=pd.concat(operaciones)
operaciones['FECHA']=operaciones['FECHA'].astype(str)
operaciones['ULTIMA_EVALUACION']=None
print('Validando operaciones...')
trades=c.follow_alertas(df=data,high=h,low=l,fecha_final=fecha_final,model=modelo,alertas=operaciones)


#==========================================================================================================================
#   CONSOLIDA OPERACIONES
#==========================================================================================================================

#trades=pd.read_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\RESULTADOS\trades.xlsx')

#ACTUALIZACIÓN DE ESTADO DE OPERACIONES
trades['ESTADO_N']='CERRADA'
operaciones=operaciones.merge(trades[['X','Y','FECHA_ENTRADA','MODELO','DIRECCION_OP','ESTADO_N']],how='left',left_on=['X','Y','FECHA','MODELO','DIRECCION_OP'],right_on=['X','Y','FECHA_ENTRADA','MODELO','DIRECCION_OP'])
operaciones['ESTADO']=np.where(operaciones['ESTADO_N'].isna(),operaciones['ESTADO'],operaciones['ESTADO_N'])
operaciones=operaciones.drop(columns=['ESTADO_N','FECHA_ENTRADA'])

#AGRUPAMIENTOS
trades['FECHA_SALIDA']=pd.to_datetime(trades['FECHA_SALIDA'])
trades['FECHA_ENTRADA']=pd.to_datetime(trades['FECHA_ENTRADA'])

trades['HOLDEO_DIAS']=(trades['FECHA_SALIDA']-trades['FECHA_ENTRADA']).dt.days+1

if solo_largo:
    trades['RETORNO_Y']=np.where(trades['DIRECCION_OP']=='CORTO Y',0,trades['RETORNO_Y'])
    trades['RETORNO_X']=np.where(trades['DIRECCION_OP']=='LARGO Y',0,trades['RETORNO_X'])
    trades['RETORNO_Y_NETO']=np.where(trades['DIRECCION_OP']=='CORTO Y',0,trades['RETORNO_Y_NETO'])
    trades['RETORNO_X_NETO']=np.where(trades['DIRECCION_OP']=='LARGO Y',0,trades['RETORNO_X_NETO'])

trades['RETORNO_TOTAL']=trades['RETORNO_X']+trades['RETORNO_Y']
trades['RETORNO_TOTAL_NETO']=trades['RETORNO_X_NETO']+trades['RETORNO_Y_NETO']
trades['RESULTADO']=np.where(trades['RETORNO_TOTAL']>=0,1,0)

pairs_result=trades.groupby(['X','Y']).agg(RETORNO_TOTAL=('RETORNO_TOTAL_NETO','sum'),RETORNO_TOTAL_MEDIO=('RETORNO_TOTAL_NETO','mean'),RETORNO_X_MEDIO=('RETORNO_X_NETO','mean'),RETORNO_Y_MEDIO=('RETORNO_Y_NETO','mean'),N_OPERACIONES=('RESULTADO','count'),N_TP=('RESULTADO','sum'),HOLDEO_MEDIO=('HOLDEO_DIAS','mean')).reset_index()
pairs_result['N_SL']=pairs_result['N_OPERACIONES']-pairs_result['N_TP']
pairs_result['MODELO']=modelo
pairs_result=pairs_result[['X','Y','MODELO','RETORNO_TOTAL','RETORNO_TOTAL_MEDIO','RETORNO_X_MEDIO','RETORNO_Y_MEDIO','N_OPERACIONES','N_TP','N_SL','HOLDEO_MEDIO']]


conn=sqlite3.connect(db)
c=conn.cursor()
try:
    sql = f"DELETE FROM TEST_SUMMARY WHERE MODELO={modelo}"
    c.execute(sql)
    conn.commit()
except:
    print('Tabla inexistente, creando...')
finally:
    pairs_result.to_sql('TEST_SUMMARY', conn, if_exists='append', index = False)
    c.close()
    conn.close()



operaciones.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\RESULTADOS\operaciones.xlsx',index=False)
trades.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\RESULTADOS\trades.xlsx',index=False)
pairs_result.to_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\RESULTADOS\pairs_result.xlsx',index=False)

#COMENTARIOS:
    #NO SE TOTALIZAN RENDIMIENTOS MONETARIOS YA QUE ESTO DEPENDE DE LA GESTIÓN QUE SE REALICE SOBRE CADA UNA DE LAS OPERACIONES

#PENDIENTES:
    #ORGANIZAR CÓDIGO (DEPURAR INEFICIENCIAS, DIVIDIR POR FUNCIONES/MÉTODOS)







