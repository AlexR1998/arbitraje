#EVALUACIÓN DE TRADES/CARACTERISTICAS
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pandas as pd
import sqlite3
import numpy as np
import pickle

db=r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\1.DATA\arbitraje_col'
colcap=pd.read_excel(r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\INSUMOS\Colcap.xlsx')
model_path=r'C:\Users\Alex\Desktop\Investment\Scripts\2.Quant Analysis\2. Modelos\3.Arbitraje\Arbitraje 2.0\3.TEST\clasifier_model'
modelo='engle-granger'
pruebas=False
estimar_modelo=False

conn=sqlite3.connect(db)
rel=pd.read_sql(f"SELECT * FROM ACTIVOS_RELACIONADOS WHERE MODELO='{modelo}'",conn)
pairs_result=pd.read_sql(f"SELECT * FROM TEST_SUMMARY WHERE MODELO='{modelo}'",conn)
conn.close()

rel=pd.merge(left=pairs_result[['X','Y','RETORNO_TOTAL','HOLDEO_MEDIO']],right=rel[['X','Y','ESTAD_1','ESTAD_2','R2','AIC','F_PVALUE','RMSE']],how='left',on=['X','Y'])
rel['RESULTADO']=np.where(rel['RETORNO_TOTAL']>=0,1,0)

rel=rel.merge(colcap,how='left',left_on='X',right_on='TICKER')
rel=rel.rename(columns={'PERCENT_WEIGHT':'WEIGHT_X','VOLUME_AVG_30D':'VOLUME_X'})
rel=rel.merge(colcap,how='left',left_on='Y',right_on='TICKER')
rel=rel.rename(columns={'PERCENT_WEIGHT':'WEIGHT_Y','VOLUME_AVG_30D':'VOLUME_Y'})
rel=rel.fillna(0)

#==========================================================================================================================
#   ANÁLISIS CARACTERÍSTICAS (EXPLORATORIO)
#==========================================================================================================================
x=np.array(rel[['ESTAD_2','R2','AIC','WEIGHT_X','WEIGHT_Y','VOLUME_Y']])
y=np.array(rel['RESULTADO']).reshape(-1,1)

from sklearn.preprocessing import MinMaxScaler
sc = MinMaxScaler() 
x=sc.fit_transform(x)

if pruebas:
    #ANALISIS LASSO
    from sklearn.model_selection import train_test_split
    from sklearn.decomposition import PCA
    from sklearn.neighbors import KNeighborsClassifier

    accuarcy=[]
    for i in range(1,15):
        pca = PCA(n_components=2)
        pca.fit(x)
        X_pca = pca.transform(x)
        X_train, X_test, y_train, y_test = train_test_split(X_pca, y, random_state=0)
        clf = KNeighborsClassifier(n_neighbors=i)
        clf.fit(X_train, y_train)
        accuarcy.append(clf.score(X_test, y_test))
    print(accuarcy)

#==========================================================================================================================
#   MODELO SELECCIONADO
#==========================================================================================================================

k_vecinos=8
n_features=2
pca = PCA(n_components=n_features)
pca.fit(x)
X_pca = pca.transform(x)

if estimar_modelo:
    #ESTIMACIÓN
    X_train, X_test, y_train, y_test = train_test_split(X_pca, y, random_state=0)
    clf = KNeighborsClassifier(n_neighbors=k_vecinos)
    clf.fit(X_train, y_train)
    print("Test set accuracy: {:.2f}".format(clf.score(X_test, y_test)))
    #GUARDAR MODELO
    knnPickle = open(model_path, 'wb') 
    pickle.dump(clf, knnPickle)  
    knnPickle.close()
else:
    clf = pickle.load(open(model_path, 'rb'))
    y_predict_1 = clf.predict(X_pca) 
    #LEER MODELO ANTERIORMENTE ENTRENADO


y_predicted=clf.predict(X_pca)
rel['y_evaluated']=y_predicted

filtro=rel[rel['y_evaluated']==1][['X','Y']]
filtro['MODELO']=modelo


conn=sqlite3.connect(db)
c=conn.cursor()
try:
    sql = f"DELETE FROM PAIRS_FILTER WHERE MODELO='{modelo}´'"
    c.execute(sql)
    conn.commit()
except:
    print('Tabla inexistente, creando...')
finally:
    filtro.to_sql('PAIRS_FILTER', conn, if_exists='append', index = False)
    c.close()
    conn.close()



#PENDIENTES:
    #ORGANIZAR ESTE DESORDEN DE CÓDIGO :(

