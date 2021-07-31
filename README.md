# BOT ANALYTICS

## Aplicação para extração e carga de dados da covid e tweets.
A documentação está dividida de acordo com os arquivos e a lógica da aplicação:

### DockerFile
Arquivo de imagem docker puckel/airflow, onde realizamos a instalação de algumas bibliotecas em que vamos utilizar para executar os scripts python.

### requeriments.txt
Bibliotecas utilizadas nos scripts python de cargas e análises de dados.

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
- df_covid: Dataset da covid, contendo dados a partir do início dos casos até o D-1, sendo o D-1 da data da execução do job. Foi realizado algumas tratativas, como um acumulado dos dias por paises, remoção de outiliers e dados negativos substituindo-os pelo mesmo dia da semana passada. E um outro dataset, contendo os dados de vacinações, onde mergeamos essas duas bases pelo ID do pais em um único dataframe.
- df_ebola: Dataset do ebola coletado no Kaggle. Link: https://www.kaggle.com/kingburrito666/ebola-cases. 
- df_h1n1: Dataset do h1n1 coletado do Kaggle. Link: https://www.kaggle.com/de5d5fe61fcaa6ad7a66/pandemic-2009-h1n1-swine-flu-influenza-a-dataset
- df_sars: Dataset do sars coletado do Kaggle. Link: https://www.kaggle.com/imdevskp/sars-outbreak-2003-complete-dataset 
- df_final: É a junção de todos os datasets coletados. Foi criado uma coluna categoria em cada dataset, para categorizarmos os dados.

Após carregarmos o dataframe df_final, inserimos os dados em dois ambientes: 
- bigquery: É necessário criar uma conexão no google bigquery, e inserir o arquivo JSON com as conexões dentro do diretório dags/credentials. Lembre-se de alterar a variável "pk_json_input", passando o name do seu arquivo JSON. Em meu site, eu disponibilzei um step-by-step para criar essa conexão, e conseguir inserir dados via pandas: http://diegorabelo.com/pandas-bigquery-insert-data-in-bigquery/

- postgresql: Servidor local, que foi configurado no arquivo: docker-compose.yml

### get_data_twitter
Arquivo Python para coleta de tweets via twint.
Optei por essa lib, por ela consumir dados do twitter de forma anônima, sendo o ideal para o propósito deste trabalho.
A data da coleta é D-1 a partir do momento de execução, com amostras de 1000 registros. A lingua inglesa "en" foi a escolhida, para utilizarmos a biblioteca em python TextBlob, para o processamento dos textos e análise de sentimentos que funciona muito bem nesse idioma.

Assim como o passo anterior, vamos inserir nos dois ambientes:

- bigquery: É necessário criar uma conexão no google bigquery, e inserir o arquivo JSON com as conexões dentro do diretório dags/credentials. Lembre-se de alterar a variável "pk_json_input", passando o name do seu arquivo JSON. Em meu site, eu disponibilzei um step-by-step para criar essa conexão, e conseguir inserir dados via pandas: http://diegorabelo.com/pandas-bigquery-insert-data-in-bigquery/

- postgresql: Servidor local, que foi configurado no arquivo: docker-compose.yml


### covid19_analysis
Arquivo python para análise exploratória dos dados que foram estruturados e inseridos em nosso servidor Postgresql local

Nessa análise, vamos coletar os dados sommente da covid, através da query sql:

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

Vamos utilizar a biblioteca Prophet para prevermos a quantidade de casos, mortes e vacina para os próximos 90 dias, dos 5 países com a maior quantidade de casos. Esses dados serão plotados em gráficos dentro de um laço de repetição (for) e exportado as imagens no diretório 

Após prevermos, realizaremos as seguintes análises, mas agora utilizando a biblioteca sklearn do python:
- Regressão Linear para validarmos a tendencia do aumento de mortes em relação ao aumento de casos
- Regressão Linear para validarmos a tendencia do aumento de vacinas em relação ao aumento de casos 

