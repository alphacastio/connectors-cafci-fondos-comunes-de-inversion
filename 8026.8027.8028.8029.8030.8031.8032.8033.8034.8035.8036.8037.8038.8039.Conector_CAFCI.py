#!/usr/bin/env python
# coding: utf-8

# In[57]:



import io
import pandas as pd
import numpy as np
import datetime as dt_
from datetime import datetime as date_
from scipy import stats
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

clasificaciones = {
                2: "Renta Variable",
                3: "Renta Fija",
                4: "Mercado de Dinero",
                5: "Renta Mixta",
                6: "Pymes", 
                7: "Retorno Total",
                8: "Infraestructura",
                9: "Fondos Cerrados"
                  } 


# In[58]:


def get_data_fondos(filename, asset_class, date):
    try:
        df = pd.read_excel(filename, sheet_name = "Info Diaria")
    except:
        return pd.DataFrame()
    
    df.dropna(axis=0, how='all',inplace=True)
    df.dropna(axis=1, how='all',inplace=True)
    #fields = ["Tipo de Fondos", "Region", "Duration", "Benchmark", "Moneda"]

    #for field in fields:
    #    df[field] = np.nan
    #    df.loc[df["Fondo Común de Inversión"].str.contains( field + ": "), field] = df["Fondo Común de Inversión"].str.replace(field + ": ", "")
    #    df[field] = df[field].ffill()
    df = df[df["Fecha"].notnull()]
    df["asset_class"] = asset_class
    df["Date"] = pd.to_datetime(df["Fecha"], format="%d/%m/%y").dt.strftime("%Y-%m-%d")
    del df["Fecha"]
    return df
    

df_agg = pd.DataFrame()
weekdays = [5,6]
numdays = 30
skipdays = 0
base = date_.today()
date_list = [base - dt_.timedelta(days=skipdays) - dt_.timedelta(days=x) for x in range(numdays)]
for date in date_list:
    if date.weekday() in weekdays:      
        continue
    print(date)
    filename = "https://api.cafci.org.ar/estadisticas/descargar/informacion/diaria/{}/{}"
    
    for clasificacion in clasificaciones:            
        archivo = filename.format(clasificacion, date.strftime("%Y-%m-%d"))
        print(archivo)
        df_agg = df_agg.append(get_data_fondos(archivo, clasificaciones[clasificacion], date.strftime("%Y-%m-%d")) )

df_agg = df_agg.drop_duplicates()


# In[59]:





def upload_assets(df_agg, asset_class, horizonte = None):    
    if horizonte:
        df_temp = df_agg[df_agg["Horizonte"]== horizonte]
        dataset_name = "Financial - Argentina - Fondos Comunes de Inversion - {} - {}".format(asset_class, horizonte)
    else:
        df_temp = df_agg
        dataset_name = "Financial - Argentina - Fondos Comunes de Inversion - {}".format(asset_class)
    
    dataset = alphacast.datasets.read_by_name(dataset_name)
    if not dataset:
        description = "The dataset includes daily information of net worth and price and quantities of shares from Argentina Mutual Funds. Asset Class is {}".format(asset_class)
        dataset = alphacast.datasets.create(dataset_name, 1207, description)
        alphacast.datasets.dataset(dataset["id"]).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["Fondo Común de Inversión"], dateFormat= "%Y-%m-%d")        
    print("uploading dataset {}".format(dataset["id"]))
    alphacast.datasets.dataset(dataset["id"]).upload_data_from_df(df_temp, deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=False)

for asset_class in df_agg["asset_class"].unique():    
    df_temp = df_agg[df_agg["asset_class"]== asset_class]
    if asset_class in ['Renta Variable', 'Renta Fija']:
        for horizonte in df_temp["Horizonte"].unique():
            upload_assets(df_temp, asset_class, horizonte)
    else:
        upload_assets(df_temp, asset_class)

    
    


# In[ ]:




