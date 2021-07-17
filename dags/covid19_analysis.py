import pandas as pd
import numpy as np
from datetime import *
import matplotlib.pyplot as plt
import sqlalchemy
from fbprophet import Prophet
from sklearn.linear_model import LinearRegression
import seaborn as sns

directory = '/usr/local/airflow/dags/img/'

#dags_postgres_1
database_connection = sqlalchemy.create_engine('postgresql://airflow:airflow@0.0.0.0/postgres')

query = """

SELECT  	
	date,
    country,
	SUM(tt_cases) tt_cases,
	SUM(tt_deaths) tt_deaths,
    SUM(people_fully_vaccinated) people_fully_vaccinated
FROM tb_pandemics 
WHERE category = 'covid'
GROUP BY 1,2
ORDER BY 1,2;

"""


df_covid = pd.read_sql(con=database_connection,sql=query)

df_covid.columns

df_covid = df_covid[['date','country','tt_cases','tt_deaths','people_fully_vaccinated']]
df_covid.rename(columns={"people_fully_vaccinated":"tt_vaccine"},inplace=True)

df_covid['tt_cases'] = np.where(df_covid['tt_cases'].isnull(),0,df_covid['tt_cases'])
df_covid['tt_deaths'] = np.where(df_covid['tt_deaths'].isnull(),0,df_covid['tt_deaths'])
df_covid['tt_vaccine'] = np.where(df_covid['tt_vaccine'].isnull(),0,df_covid['tt_vaccine'])


df_covid_cases = pd.DataFrame(df_covid.groupby(['country'])['tt_cases'].sum())

# get top 3 countrys by cases
df_covid_cases_top5_contador = pd.DataFrame(df_covid_cases.sort_values(['tt_cases'],ascending=False).head(5))
df_covid_cases_top5_contador['country'] = df_covid_cases_top5_contador.index
df_covid_cases_top5_contador['contador'] = 1
df_covid_cases_top5_contador['contador'] = df_covid_cases_top5_contador['contador'].cumsum()

# using prophet to cases and deaths predicts
for i in range(1,6):
    if i == 1:
        df_covid_cases_cont = pd.DataFrame()
        df_covid_cases = pd.DataFrame(df_covid_cases_top5_contador.loc[df_covid_cases_top5_contador['contador'] == i])
        df_covid_cases_cont = df_covid_cases_cont.append(df_covid.loc[df_covid['country'].isin(df_covid_cases['country'])])
    else:
        df_covid_cases = pd.DataFrame(df_covid_cases_top5_contador.loc[df_covid_cases_top5_contador['contador'] == i])
        df_covid_cases_cont = df_covid_cases_cont.append(df_covid.loc[df_covid['country'].isin(df_covid_cases['country'])])
        top_country = []
        top_country = df_covid_cases_cont.country.unique()
    
    # starting predict cases
    indicator = 'cases'
    print("Starting predict data top"+str(i)+" "+indicator+": "+str(df_covid_cases.country.unique()))
    df_prophet_cases = df_covid_cases_cont.reset_index()
    df_prophet_cases = df_prophet_cases.loc[df_prophet_cases['country'].isin(df_covid_cases['country'])]
    df_prophet_cases = df_prophet_cases[["date","tt_cases"]]
    df_prophet_cases.rename(columns={"date":"ds","tt_cases":"y"},inplace=True)
    days = 60
    m = Prophet(interval_width=0.95)
    m.fit(df_prophet_cases)
    df_prophet_cases_futuro = m.make_future_dataframe(periods=days)
    previsao_cases = m.predict(df_prophet_cases_futuro)
    previsao_cases[["ds","yhat","yhat_lower","yhat_upper"]]
    
    m.plot(previsao_cases,xlabel="Data",ylabel="Total Casos")
    plt.title("Predict Cases: "+str(df_covid_cases.country.unique()))
    plt.savefig(directory+'img_predict_'+indicator+'_'+str(df_covid_cases.country.unique()))
    plt.gcf().autofmt_xdate()
    plt.show;
    
    # starting predict Deaths
    indicator = 'deaths'
    print("Starting predict data top"+str(i)+" "+indicator+": "+str(df_covid_cases.country.unique()))
    df_prophet_deaths = df_covid_cases_cont
    df_prophet_deaths = df_prophet_deaths.loc[df_prophet_deaths['country'].isin(df_covid_cases['country'])]
    df_prophet_deaths = df_prophet_deaths[["date","tt_deaths"]]
    df_prophet_deaths.rename(columns={"date":"ds","tt_deaths":"y"},inplace=True)
    m = Prophet(interval_width=0.95)
    m.fit(df_prophet_deaths)
    df_prophet_deaths_futuro = m.make_future_dataframe(periods=days)
    previsao_deaths = m.predict(df_prophet_deaths_futuro)
    previsao_deaths[["ds","yhat","yhat_lower","yhat_upper"]]
    
    m.plot(previsao_deaths,xlabel="Data",ylabel="Total Deaths")
    plt.title("Predict Deaths: "+str(df_covid_cases.country.unique()))
    plt.savefig(directory+'img_predict_'+indicator+'_'+str(df_covid_cases.country.unique()))
    plt.gcf().autofmt_xdate()
    plt.show;
    
    # starting predict Deaths
    indicator = 'vaccine'
    print("Starting predict data top"+str(i)+" "+indicator+": "+str(df_covid_cases.country.unique()))
    df_prophet_vaccine = df_covid_cases_cont
    df_prophet_vaccine = df_prophet_vaccine.loc[df_prophet_vaccine['country'].isin(df_covid_cases['country'])]
    df_prophet_vaccine = df_prophet_vaccine[["date","tt_vaccine"]]
    df_prophet_vaccine.rename(columns={"date":"ds","tt_vaccine":"y"},inplace=True)
    m = Prophet(interval_width=0.95)
    m.fit(df_prophet_vaccine)
    df_prophet_vaccine_futuro = m.make_future_dataframe(periods=days)
    previsao_vaccine = m.predict(df_prophet_vaccine_futuro)
    previsao_vaccine[["ds","yhat","yhat_lower","yhat_upper"]]
    
    m.plot(previsao_vaccine,xlabel="Data",ylabel="Total Vaccine")
    plt.title("Predict Vaccine: "+str(df_covid_cases.country.unique()))
    plt.savefig(directory+'img_predict_'+indicator+'_'+str(df_covid_cases.country.unique()))
    plt.gcf().autofmt_xdate()
    plt.show;
    

# Regressão linear
# Deaths cases
X = df_prophet_deaths['y'].values
y = df_prophet_cases['y'].values

X = X.reshape(-1,1)

from sklearn.linear_model import LinearRegression
regressor = LinearRegression()
regressor.fit(X, y)

import matplotlib.pyplot as plt
plt.scatter(X, y)
plt.plot(X, regressor.predict(X), color = 'red')
plt.title ("Regressão linear simples")
plt.xlabel("Deaths")
plt.ylabel("Cases")



# Testando um modelo de machine learning

previsores = df_covid.iloc[:, 1:4].values
classe = df_covid.iloc[:, 4].values


from sklearn.preprocessing import LabelEncoder, OneHotEncoder
labelencoder_previsores = LabelEncoder()

#labels = labelencoder_previsores.fit_transform(previsores[:,1])
previsores[:,0] = labelencoder_previsores.fit_transform(previsores[:,0])
previsores[:,1] = labelencoder_previsores.fit_transform(previsores[:,1])
previsores[:,2] = labelencoder_previsores.fit_transform(previsores[:,2])

labelencoder_classe = LabelEncoder()
classe = labelencoder_classe.fit_transform(classe)

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
previsores = scaler.fit_transform(previsores)

from sklearn.model_selection import train_test_split
previsores_treinamento, previsores_teste, classe_treinamento, classe_teste = train_test_split(previsores, classe, test_size=0.20, random_state=0)


#####################################################
from sklearn.neighbors import KNeighborsClassifier
classificador = KNeighborsClassifier(n_neighbors=20,metric='minkowski',p=2)

classificador.fit(previsores_treinamento,classe_treinamento)
previsoes = classificador.predict(previsores_teste)

from sklearn.metrics import confusion_matrix, accuracy_score
precisao = accuracy_score(classe_teste,previsoes)
matriz = confusion_matrix(classe_teste,previsoes)   

import collections
collections.Counter(classe_teste)

print(precisao)
