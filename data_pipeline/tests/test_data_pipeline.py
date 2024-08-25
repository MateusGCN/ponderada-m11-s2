import pytest
from unittest.mock import patch
from os import path
import os
from data_pipeline.minio_client import upload_file, download_file
from data_pipeline.clickhouse_client import get_client, insert_dataframe

# Supondo que a função a ser testada seja definida em algum lugar do seu pacote
from app import process_and_store_data

def test_http_request_failure(mocker):
    mocker.patch('requests.get', side_effect=Exception("Falha na requisição"))
    with pytest.raises(Exception):
        process_and_store_data()

def test_file_deletion_after_upload(mocker):
    # Mock das funções e simulação do processamento
    mocker.patch('data_pipeline.minio_client.upload_file')
    mocker.patch('data_pipeline.minio_client.download_file')
    mocker.patch('data_pipeline.data_processing.process_data', return_value="testfile.txt")
    mocker.patch('os.remove')

    process_and_store_data()

    # Verificar se os.remove foi chamado com o arquivo correto
    os.remove.assert_called_once_with("testfile.txt")

def test_file_deletion_after_processing(mocker):
    mocker.patch('data_pipeline.minio_client.upload_file')
    mocker.patch('data_pipeline.minio_client.download_file')
    mocker.patch('pandas.read_parquet')
    mocker.patch('data_pipeline.data_processing.process_data', return_value="testfile.txt")
    mocker.patch('data_pipeline.data_processing.prepare_dataframe_for_insert')
    mocker.patch('data_pipeline.clickhouse_client.get_client')
    mocker.patch('data_pipeline.clickhouse_client.insert_dataframe')
    mocker.patch('os.remove')

    process_and_store_data()

    # Verificar se os.remove foi chamado com o arquivo baixado após o processamento
    os.remove.assert_any_call("downloaded_testfile.txt")

# Adicione outros testes conforme necessário
