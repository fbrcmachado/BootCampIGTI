##IMPORTANDO INICIALMENTE AS LIBS QUE SERÃO UTILIZADAS:

import pandas as pd
import zipfile
import requests
from io import BytesIO
import os
import gc
import sqlalchemy


##SETANDO ONDE SERÁ O LOCAL EM QUE OS DADOS SERÃO DESCARREGADOS E, CASO NÃO EXISTA, CRIA O DIRETÓRIO
directory = 'C:/Users/antonio.machado/Desktop/Enem'
os.makedirs(directory, exist_ok=True)

##SETA A URL DOS ARQUIVOS E FAZ O SEU DOWNLOAD
url = "http://download.inep.gov.br/microdados/microdados_enem_2019.zip"

filebytes = BytesIO(
    requests.get(url).content
)

##EXTRAI OS ARQUIVOS
myzip = zipfile.ZipFile(filebytes)
myzip.extractall(directory)

##LIMPA A MEMÓRIA E LÊ O ARQUIVO COM OS DADOS
print("gc: ", gc.collect())
df_enem = pd.read_csv('c:/Users/antonio.machado/Desktop/Enem/DADOS/MICRODADOS_ENEM_2019.csv', sep=';', encoding='latin-1')

##FILTRAMOS OS ALUNOS COM RESIDÊNCIA EM MG, COMO QUER A QUESTÃO
df_enem = df_enem.loc[
        df_enem.SG_UF_RESIDENCIA =='MG'
        ]

##SALVAMOS OS DADOS EM UM ARQUIVO .CSV
df_enem.to_csv('C:/Users/antonio.machado/Desktop/Enem/DADOS/DF_MG_2019.csv', sep=';', index=False, encoding='latin-1')

##IMPORTANDO DADOS PARA O MYSQL
database_username = 'root'
database_password = 'xxxxxxx'
database_ip       = 'localhost'
database_name     = 'database_name'

engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username,
                                                                                  database_password,
                                                                                  database_ip,
                                                                                  database_name))
    

##INSERE DATAFRAME NO MYSQL
df_enem.to_sql(name = 'enem_mg_2019', con = engine, index = False, chunksize=1000, if_exists = 'append')

