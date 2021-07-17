FROM puckel/docker-airflow

USER root

COPY requeriments.txt .

RUN pip install -r requeriments.txt
RUN pip install --upgrade pip
RUN pip install google-auth
RUN pip install pandas-gbq
RUN pip install openpyxl
RUN pip install xlrd
RUN pip install matplotlib
RUN pip install pandas-datareader
RUN pip install textblob
RUN pip install pycountry
RUN apt-get -y update
RUN apt-get -y install git
RUN pip3 install --user --upgrade git+https://github.com/twintproject/twint.git@origin/master#egg=twint