# BOT ANALYTICS

## Aplicação para extração e carga de dados da covid e tweets relacionados.
A documentação está dividida de acordo com os arquivos e a lógica da aplicação:

### DockerFile
Arquivo de imagem docker puckel/airflow, onde realizamos a instalação de algumas bibliotecas em que vamos utilizar na aplicação Python.

### docker-compose.yml
Os containers estão divididos em 3 imagens:

1 - tcc/bot-analytics:1.0: Imagem puckel/airflow, onde realizamos algumas modificações no DockerFile antes de subirmos a imagem. 
No repositório dags/ executar o comando: # $docker image build -t tcc/bot-analytics:1.0 . 

2 - postgres:9.6: Imagem postgres, onde vamos carregar os dados no servidor local.

3 - dpage/pgadmin4: Imagem pgadmin, onde temos um console para gerenciar e analisar os dados e performance do servidor.

### dag_load_data.py
Arquivo python, que utilzamos para criar e gerenciar o job no airflow.
Separei o job em duas tasks, sendo:
task_1: Script python para extração, transformação e carga dos dados da covid/vacinação, ebola, h1n1 e sars. 
taks_2: Script python para extração, transformação e carga dos tweets relacionados a covid.

### get_data_pandemics
Arquivo Python para coleta dos dados de covid/vacinação, ebola, h1n1 e sars
Esse script foi divido atráves dos seguintes dataframes:
df_covid

