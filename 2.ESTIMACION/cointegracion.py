import numpy as np
import statsmodels.api as sm
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from statsmodels.stats.stattools import durbin_watson
from statsmodels.tsa.api import VAR
from statsmodels.tsa.vector_ar.vecm import coint_johansen
import MetaTrader5 as mt5
from itertools import permutations
import sqlite3
from tqdm import tqdm
from datetime import datetime, timedelta
from sklearn.metrics import r2_score
import math

class cointegracion():
    def __init__(self,p_critico,db_path):
        self.__p_critico=p_critico
        self.__db=db_path
        self.__estad_dw={0.01:0.511,0.05:0.386,0.1:0.322}
        self.__estad_adf={'constante':{0.01:-2.5658,0.05:-1.9393,0.1:-1.6156},'sin_constante':{0.01:-3.9001,0.05:-3.3377,0.1:-3.0462}}
        self.__insumo_path=r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\INSUMOS'

    def __orden_integracion(self,x,p_critico):
        d=-1
        adf=p_critico
        while adf>=p_critico:
            adf=adfuller(x)[1]
            x=np.diff(x.reshape(-1,))
            d+=1
        return d
    

    def __engle_granger(self,x,y,solo_modelo=False):
        if solo_modelo==False:
            d_x=self.__orden_integracion(x,self.__p_critico)
            d_y=self.__orden_integracion(y,self.__p_critico)
            if (d_x!=d_y) and (d_x+d_y<=1): 
                return 

        #==============================================================================
        #   REGRESIÓN
        #==============================================================================
        c='constante'
        x=sm.add_constant(x)
        model=sm.OLS(y,x).fit()
        
        #DETERMINA LA NECESIDAD DE INCORPORAR CONSTANTE EN EL MODELO
        if model.pvalues[0]>=self.__p_critico:
            x=x[:,1].reshape(-1,1)
            model=sm.OLS(y,x).fit()
            c='sin_constante'
        
        if solo_modelo:
            return model,c
        
        #Estadistico F y t para regresión y parámetros: (si no se cumple, se rechaza la relación)
        if not (np.append(model.pvalues, model.f_pvalue)<=self.__p_critico).all():
            return
        
        #==============================================================================
        #   PRUEBA DE ESTACIONARIEDAD
        #==============================================================================
        resid=pd.Series(model.resid)
        
        #PRUEBA DE DICKEY - FULLER AUMENTADA (ADF) (ESTADISTICOS DE ENGLE-GRANGER)
        adf=adfuller(resid)[0]
        #H0: NO ESTACIONARIEDAD
        #ADF >= ESTADISTICO, NO RECHAZO H0
        
        #PRUEBA DURBIN-WATSON (VER COMENTARIOS AL FINAL)
        dw=durbin_watson(resid)
        
        
        #EL MODELO EVALUA LOS SUPUESTOS
        # -LAS SERIES SON INTEGRADAS DEL MISMO ORDEN Y MAYOR A 0
        # -LA REGRESIÓN Y SUS BETAS SON SIGNIFICATIVOS
        # -LA PRUEBA ADF INDICA ESTACIONARIEDAD (NO HAY RAÍZ UNITARIA) BAJO LOS VALORES PROVISTOS POR ENGLE-GRANGER (ENGLE-GRANGER AUMENTADA)
        # -EL ESTADÍSTICO DE DURBIN WATSON ES SIGNIFICATIVO A LOS NIVELES PROPUESTOS (OPCIONAL):
        #       {5% : 0.511, 5% : 0.386, 10% : 0.322}
        #       VALORES PROPUESTOS: DAMODAR GUJARATI TERCERA EDICIÓN (PAG. 711), CON 10.000 SIMULACIONES DE 100 OBSERVACIONES
            
        return adf,self.__estad_adf[c][self.__p_critico],dw,self.__estad_dw[self.__p_critico],c,model

    def __johansen(self,d,solo_modelo=False):
        d.index = pd.DatetimeIndex(d.index).to_period('D')
        max_lag=20
        aic=[]
        for j in range(2,max_lag+1):
            model=VAR(d,freq='D').fit(j)
            if (j==2): aic.append([j,model.aic])
            if (j>2) and (model.aic<aic[-1][1]):
                aic.pop()
                aic.append([j,model.aic])
                
        model=VAR(d,freq='D').fit(aic[0][0])
        #IDENTIFICACIÓN DE COINTEGRACIÓN
        if solo_modelo:
            return model,np.nan

        #det_order:
        #-1 sin constante ni tendencia
        # 0 constante
        # 1 constante y tendencia
        joh_coint=coint_johansen(d,det_order=0, k_ar_diff=aic[0][0])
        
        c_values={0.1:0,0.05:1,0.01:2}
      
        return joh_coint.lr1[0],joh_coint.cvt[0][c_values[self.__p_critico]],joh_coint.lr1[1],joh_coint.cvt[1][c_values[self.__p_critico]],model
            
        
    def bloomberg_data_reader(self,quotes,fecha_inicial,fecha_final,d_type='CLOSE'):
        '''
        Lee los datos de un Excel de bloomberg en una ruta especificada.

        Args:
        - quotes (list): Listado de activos a solicitar-
        - fecha_inicial (datetime): Fecha de inicio de los precios solicitados.
        - fecha_final (datetime): Fecha final de los precios solicitados.
        - d_type (string): Tipo de dato a leer (selecciona la hoja especificada en el insumo) ['CLOSE','HIGH','LOW','OPEN'].
        Returns:
        - DataFrame: Historico de precios de los activos solicitados.
        - List: Lista de la combinatoria de pares presentes en el archivo Excel.
        '''

        df=pd.read_excel(self.__insumo_path+r'\bloomberg.xlsx',sheet_name=d_type)
        df=df.iloc[2:].reset_index(drop=True)
        df=df.dropna(how='all',axis=0)
        df=df.dropna(how='all',axis=1)
        df=df.rename(columns={'Unnamed: 0':'time'})
        df.index=df.time
        df=df.drop(columns='time')
        df=df[quotes]
        df=df.loc[(df.index>=fecha_inicial) & (df.index<=fecha_final)]

        pairs=list(permutations(df.columns[1:],2))
        
        return df, pairs

    def data_reader_mt5(self,quotes,fecha_inicial,fecha_final,timeframe,d_type='CLOSE'):
        '''
        Solicita un par de activos a MT5 y genera un dataframe con los precios entre las fechas y timeframe especificado.

        Args:
        - quotes (list): Listado de activos a solicitar-
        - fecha_inicial (datetime): Fecha de inicio de los precios solicitados.
        - fecha_final (datetime): Fecha final de los precios solicitados.
        - timeframe (mt5.TIMEFRAME): Timeframe de los datos solicitados.
        - d_type (string): Tipo de dato a leer (selecciona la hoja especificada en el insumo) ['CLOSE','HIGH','LOW','OPEN'].
        Returns:
        - DataFrame: Historico de precios de los activos solicitados.
        - List: Lista de la combinatoria de pares presentes en el archivo Excel.
        '''
        d_type=d_type.lower()
        mt5.initialize()
        try:
            df=pd.DataFrame({'time':pd.date_range(fecha_inicial,fecha_final)})
            
            for q in quotes:
                d=pd.DataFrame(mt5.copy_rates_range(q,timeframe, fecha_inicial, fecha_final))[['time',d_type]]
                d['time']=pd.to_datetime(d['time'], unit='s')
                d.columns=['time',q]
                df=df.merge(d,how='left',on='time')
        finally:
            mt5.shutdown()
        df=df.dropna(axis=0,how='any')
        df.index=df.time
        df=df.drop(columns='time')
        pairs=list(permutations(df.columns[1:],2))
        
        return df, pairs

    
    def validador(self, df, pairs, model='engle-granger', ingestar=False):
        '''
        Valida la existencia de cointegración en uno o mas pares de activos

        Args:
        - df (DataFrame): Pandas DataFrame con la serie de precios diarios de los activos.
        - pairs (list): Lista del los pares a validar existencia de cointegracion.
        - model (string, opcional): Modelo a ejecutar (johansen, engle-granger). Default = 'engle-granger'.
        - ingestar (boolean, opcional): Ingestar resultados en base de datos. Default= False.

        Returns:
        - DataFrame: Resumen de resultados del modelo para los pares evaluados.
        '''

        coint=[]
        #Itera entre uno o multiples pares, según sea el input.
        for i in pairs:
            print(i)
            d=df[[i[0],i[1]]]

            #FILTRA LAS SERIES DESDE EL PRIMER VALOR NO NULO PARA AMBAS
            d=d.loc[d.notnull().all(axis=1).loc[lambda x : x==True].idxmin():]
            d=d.astype(float)
            
            if model=='engle-granger':
                x=np.array(d[i[0]]).reshape(-1,1)
                y=np.array(d[i[1]]).reshape(-1,1)
                ret=self.__engle_granger(x, y)
                if (ret is not None) and (ret[0]<ret[1]):
                    coint.append([i[0],i[1],ret[0],ret[1],ret[2],ret[3],ret[4],self.__p_critico,ret[5].rsquared,ret[5].aic,ret[5].f_pvalue,np.sqrt((ret[5].resid**2).sum()/len(ret[5].resid))])
            elif model=='johansen':
                ret=self.__johansen(d)
                if (ret is not None) and (ret[0]>ret[1]):
                    # return ret
                    r2=r2_score(ret[4].fittedvalues[i[1]]+ret[4].resid[i[1]],ret[4].fittedvalues[i[1]])                    
                    coint.append([i[0],i[1],ret[0],ret[1],ret[2],ret[3],np.nan,self.__p_critico,r2,ret[4].aic,0,np.sqrt((ret[4].resid[i[1]]**2).sum()/len(ret[4].resid[i[1]]))])
        
        result=pd.DataFrame(data=coint, columns=['X','Y','ESTAD_1','CRITICO_ESTAD_1','ESTAD_2','CRITICO_ESTAD_2','CONSTANTE','P_CRITICO','R2','AIC','F_PVALUE','RMSE']) 
        
        result['MODELO'] = model
        result['RMSE']=result['RMSE'].astype(float)
        #ESTAD_1 = AUGMENTED DICKEY FULLER
        #ESTAD_2 = DURBIN-WATSON
        if ingestar and len(result)!=0:
            conn=sqlite3.connect(self.__db)
            c=conn.cursor()
            try:
                result['last_ex']=datetime.now()
                sql = f"DELETE FROM ACTIVOS_RELACIONADOS WHERE MODELO='{model}'"
                c.execute(sql)
                conn.commit()
                #result.to_sql('ACTIVOS_RELACIONADOS', conn, if_exists='append', index = False)
            except:
                print('Tabla inexistente, creando...')
            finally:
                result.to_sql('ACTIVOS_RELACIONADOS', conn, if_exists='append', index = False)
                c.close()
                conn.close()
        return result

    def genera_alertas(self, df, years,timeframe,fecha_final,model='engle-granger',desvest=3,rolling_desvest=252,filtros='',ingestar=False,pares=pd.DataFrame(),trades_export=False):
        '''
        Genera alertas de trading solo en la fecha_final ingresada, evaluando los pares marcados como relacionados según el modelo implementado.

        Args:
        - df (DataFrame): DataFrame con los datos de precio a utilizar en los cálculos. 
        - years (int): Modelo a ejecutar (johansen, engle-granger). Default = 'engle-granger'.
        - timeframe (mt5.TIMEFRAME): Timeframe de los datos solicitados.
        - fecha_final (datetime): Fecha final a evaluar existencia de alerta de trading.
        - model (string): Modelo a ejecutar (johansen, engle-granger). Default = 'engle-granger'.
        - desvest (int): Numero de desviaciones estandar a utilizar en la evaluación de la alerta.
        - rolling_desvest (int): Tamaño de la ventana movil para la construcción de la desviación estandar.
        - filtros (str:sql): Filtros a aplicar en la consulta de los pares a valorar.
        - ingestar (boolean, opcional): Ingestar resultados en base de datos. Default= False.
        - pares (DataFrame, opcional): DataFrame X,Y con los pares relacionados y ordenados de este modo. 
        
        - trades_export (boolean, opcional): Decide si exportar o no archivos detalle con los cálculos utilizados en cada trade. 
        Returns:
        - DataFrame: Dataframe con las alertas generadas.
        '''
        
        trades_path=r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\3.TEST\DATA'
        fecha_final=datetime(year=fecha_final.year, month=fecha_final.month, day=fecha_final.day, hour=0, minute=0, second=0)
        #SI SE EJECUTA ANTES DE LAS 5PM (CIERRE INTERNACIONAL) TOMARÁ EL DÍA ANTERIOR
        #if (fecha_final.date()==datetime.now().date()) and (datetime.now().hour<17):
        #    fecha_final=fecha_final-timedelta(days=1)

        filter='' if filtros=='' else 'AND ' + filtros

        if pares.empty:
            conn=sqlite3.connect(self.__db)
            try:
                sql = f"SELECT X,Y FROM ACTIVOS_RELACIONADOS WHERE MODELO='{model}' {filter} "
                pares=pd.read_sql(sql,conn)
            finally:
                conn.close()
        
        quotes=list(dict.fromkeys(list(pares['X'].drop_duplicates())+list(pares['Y'].drop_duplicates())))

# `        if df.empty:
#             fecha_inicial=datetime(year=fecha_final.year-years,month=fecha_final.month,day=fecha_final.day)
#             df,_=self.data_reader_mt5(quotes,fecha_inicial,fecha_final,timeframe)`

        alerta=[]
        for p in tqdm(pares.iterrows()):
            d=df[[p[1]['X'],p[1]['Y']]]
            d=d.loc[d.notnull().all(axis=1).loc[lambda x : x==True].idxmin():]
            d=d.astype(float)
            
            if model=='engle-granger':
                d_x=np.array(d.loc[:,p[1]['X']]).reshape(-1,1)
                d_y=np.array(d.loc[:,p[1]['Y']]).reshape(-1,1)
                mod,c=self.__engle_granger(x=d_x,y=d_y,solo_modelo=True)
                if c=='constante':
                        d_x=sm.add_constant(d_x)
                d['y_est']=mod.predict(d_x)
                d['resid']=mod.resid
                d['stdev_sup']=desvest*d['resid'].rolling(rolling_desvest).std()
                d['stdev_inf']=-d['stdev_sup']
            elif model=='johansen':
                mod,c=self.__johansen(d,solo_modelo=True)
                d['y_est']=mod.fittedvalues[p[1]['Y']]+mod.resid[p[1]['Y']]
                d['resid']=mod.resid[p[1]['Y']]
                d['stdev_sup']=desvest*d['resid'].rolling(rolling_desvest).std()
                d['stdev_inf']=-d['stdev_sup']

            #EVALUA SI HAY TRADE
            if (d.loc[d.index[-2],'resid']>d.loc[d.index[-2],'stdev_sup']) and (d.loc[d.index[-1],'resid']<=d.loc[d.index[-1],'stdev_sup']) and (d.loc[d.index[-1],'resid']>0):
                tp=d.loc[d.index[-1],'y_est']
                entrada_y=d.loc[d.index[-1],p[1]['Y']]
                entrada_x=d.loc[d.index[-1],p[1]['X']]
                sl=entrada_y+d.loc[d.index[-1],'resid']
                alerta.append([p[1]['X'],p[1]['Y'],entrada_x,entrada_y,tp,sl,'CORTO Y'])
                if trades_export:
                    d.to_excel(trades_path+fr"\{p[1]['Y']}-{p[1]['X']} {fecha_final.date()}.xlsx")
            #LARGO Y
            elif (d.loc[d.index[-2],'resid']<d.loc[d.index[-2],'stdev_inf']) and (d.loc[d.index[-1],'resid']>=d.loc[d.index[-1],'stdev_inf']) and (d.loc[d.index[-1],'resid']<0):
                tp=d.loc[d.index[-1],'y_est']
                entrada_y=d.loc[d.index[-1],p[1]['Y']]
                entrada_x=d.loc[d.index[-1],p[1]['X']]
                sl=entrada_y+d.loc[d.index[-1],'resid']
                alerta.append([p[1]['X'],p[1]['Y'],entrada_x,entrada_y,tp,sl,'LARGO Y'])
                if trades_export:
                    d.to_excel(trades_path+fr"\{p[1]['Y']}-{p[1]['X']} {fecha_final.date()}.xlsx")

        alerta=pd.DataFrame(data=alerta,columns=['X','Y','ENTRADA_X','ENTRADA_Y','TP_Y','SL_Y','DIRECCION_OP'])
        alerta['FECHA']=fecha_final.date()
        alerta['MODELO']=model
        alerta['ESTADO']='ABIERTA'
        alerta['ULTIMA_EVALUACION']=np.nan

        if ingestar and len(alerta)!=0:
            conn=sqlite3.connect(self.__db)
            c=conn.cursor()
            try:
                sql = f"DELETE FROM HISTORICO_ALERTAS WHERE MODELO='{model}' AND FECHA='{fecha_final.date()}'"
                c.execute(sql)
                conn.commit()
                #alerta.to_sql('HISTORICO_ALERTAS', conn, if_exists='append', index = False)
            except:
                print('Tabla inexistente, creando...')
            finally:
                alerta.to_sql('HISTORICO_ALERTAS', conn, if_exists='append', index = False)
                c.close()
                conn.close()

        return alerta
    
    
    def follow_alertas(self, timeframe,fecha_final,model='engle-granger',ingestar=False,alertas=pd.DataFrame()):
        '''
        Valida la existencia de cointegración en uno o mas pares de activos

        Args:
        - timeframe (mt5.TIMEFRAME): Timeframe de los datos solicitados.
        - fecha_final (datetime): Fecha final a evaluar existencia de alerta de trading.
        - model (string): Modelo a ejecutar (johansen, engle-granger). Default = 'engle-granger'.
        - desvest (int): Numero de desviaciones estandar a utilizar en la evaluación de la alerta.
        - rolling_desvest (int): Tamaño de la ventana movil para la construcción de la desviación estandar.
        - filtros (str:sql): Filtros a aplicar en la consulta de los pares a valorar.
        - ingestar (boolean, opcional): Ingestar resultados en base de datos. Default= False.
        - operaciones(DataFrame, opcional): DataFrame con el listado de operaciones a evaluar. Debe contener el mismo formato de la tabla HISTORICO_ALERTAS.

        Returns:
        - DataFrame: Dataframe con las alertas generadas.
        '''

        if alertas.empty:
            conn=sqlite3.connect(self.__db)
            try:
                sql = f"SELECT * FROM HISTORICO_ALERTAS WHERE ESTADO='ABIERTA' AND MODELO='{model}'"
                alertas=pd.read_sql(sql,conn)
            finally:
                conn.close()

        def update_alertas(a,fecha_final,model,cierre=True):
            conn=sqlite3.connect(self.__db)
            c=conn.cursor()
            try:
                var="ESTADO='CERRADA'" if cierre else f"ULTIMA_EVALUACION='{fecha_final.date()}'"
                sql = f"UPDATE HISTORICO_ALERTAS SET {var} WHERE X='{a[1]['X']}' AND Y='{a[1]['Y']}' AND FECHA='{a[1]['FECHA']}' AND MODELO='{model}'"
                c.execute(sql)
                conn.commit()
            finally:
                c.close()
                conn.close()

        def guarda_resultados(df):
            data=pd.DataFrame(data=df,columns=['X','Y','RESULTADO','DIRECCION_OP','ENTRADA_X','ENTRADA_Y','SALIDA_X','SALIDA_Y','FECHA_ENTRADA','FECHA_SALIDA','MODELO'])
            if ingestar and len(data)!=0:
                conn=sqlite3.connect(self.__db)
                c=conn.cursor()
                try:
                    sql = f"DELETE FROM HISTORICO_RESULTADOS WHERE X='{data.loc[data.index[0],'X']}' AND Y='{data.loc[data.index[0],'Y']}' AND FECHA_ENTRADA='{data.loc[data.index[0],'FECHA_ENTRADA']}' AND FECHA_SALIDA='{data.loc[data.index[0],'FECHA_SALIDA']}'"
                    c.execute(sql)
                    conn.commit()
                    #data.to_sql('HISTORICO_RESULTADOS', conn, if_exists='append', index = False)
                except:
                    print('Tabla inexistente, creando...')
                finally:
                    data.to_sql('HISTORICO_RESULTADOS', conn, if_exists='append', index = False)
                    c.close()
                    conn.close()

        resultado=[]
        for a in alertas.iterrows():
            cerrado=False
            #fecha_evaluacion=a[1]['ULTIMA_EVALUACION'] if (a[1]['ULTIMA_EVALUACION'] !=None and not math.isnan(a[1]['ULTIMA_EVALUACION'])) else a[1]['FECHA']    
            fecha_evaluacion=a[1]['ULTIMA_EVALUACION'] if a[1]['ULTIMA_EVALUACION'] !=None else a[1]['FECHA']
            fecha_evaluacion=datetime.strptime(fecha_evaluacion,'%Y-%m-%d')
            
            if fecha_evaluacion==fecha_final:
                print('Saltando')
                if ingestar:
                    update_alertas(a,fecha_final,model,cierre=False)
                continue

            df,_=self.data_reader_mt5([a[1]['X'],a[1]['Y']],fecha_evaluacion,fecha_final,timeframe)
            for d in df.iterrows():
                if (a[1]['DIRECCION_OP']=='LARGO Y' and d[1][a[1]['Y']] >= a[1]['TP_Y']) or (a[1]['DIRECCION_OP']=='CORTO Y' and d[1][a[1]['Y']] <= a[1]['TP_Y']):
                    resultado.append([a[1]['X'],a[1]['Y'],'TP',a[1]['DIRECCION_OP'],a[1]['ENTRADA_X'],a[1]['ENTRADA_Y'],d[1][a[1]['X']],a[1]['TP_Y'],a[1]['FECHA'],d[0].date(),model])
                    cerrado=True

                elif (a[1]['DIRECCION_OP']=='LARGO Y' and d[1][a[1]['Y']] <= a[1]['SL_Y']) or (a[1]['DIRECCION_OP']=='CORTO Y' and d[1][a[1]['Y']] >= a[1]['SL_Y']):
                    resultado.append([a[1]['X'],a[1]['Y'],'SL',a[1]['DIRECCION_OP'],a[1]['ENTRADA_X'],a[1]['ENTRADA_Y'],d[1][a[1]['X']],a[1]['SL_Y'],a[1]['FECHA'],d[0].date(),model])
                    cerrado=True
                if cerrado:
                    if ingestar:
                        update_alertas(a,fecha_final,model,cierre=True)
                        guarda_resultados([resultado[len(resultado)-1]])
                    break
            if ingestar:
                update_alertas(a,fecha_final,model,cierre=False)
        resultado=pd.DataFrame(data=resultado,columns=['X','Y','RESULTADO','DIRECCION_OP','ENTRADA_X','ENTRADA_Y','SALIDA_X','SALIDA_Y','FECHA_ENTRADA','FECHA_SALIDA','MODELO'])
        return resultado



