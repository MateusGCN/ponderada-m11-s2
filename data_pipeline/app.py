from flask import Flask
import requests
import pandas as pd
import os
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert

app = Flask(__name__)

def setup_environment():
    try:
        create_bucket_if_not_exists("raw-data")
        execute_sql_script('sql/create_table.sql')
    except Exception as e:
        print(f"Erro ao configurar o ambiente: {e}")

def process_and_store_data():
    for i in range(5):
        try:
            response = requests.get("https://randomuser.me/api/")
            response.raise_for_status()  
        except requests.RequestException as e:
            print(f"Erro na requisição HTTP: {e}")
            continue

        try:
            filename = process_data(response)
            upload_file("raw-data", filename)
            download_file("raw-data", filename, f"downloaded_{filename}")
            
            
            os.remove(filename)
        except Exception as e:
            print(f"Erro ao manipular arquivos: {e}")
            continue

        try:
            df_parquet = pd.read_parquet(f"downloaded_{filename}")
            df_prepared = prepare_dataframe_for_insert(df_parquet)
            client = get_client()
            insert_dataframe(client, 'working_data', df_prepared)
            
         
            os.remove(f"downloaded_{filename}")
        except Exception as e:
            print(f"Erro ao processar ou inserir dados: {e}")

if __name__ == '__main__':
    setup_environment()
    process_and_store_data()
    print("Dados recebidos, armazenados e processados com sucesso")
