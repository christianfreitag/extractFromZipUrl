import requests
import zipfile
import io
import os
import shutil
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

########################################################################################################
URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_eleitorado/perfil_eleitorado_ATUAL.zip"
TEMP_DIR_FILE = "temp_extrated_files"
TEMP_ZIP_FILE = "temp_downloaded_file.zip"
FILE_NAME = URL.split("/")[-1].replace(".zip", ".csv")
########################################################################################################

def download_and_extract_zip(url: str):

    csv_path = ""
    with requests.Session() as session:
        response = session.get(url, stream=True)
        total_size_in_bytes = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 Kibibyte
        print("Iniciando download do arquivo zip...")

        if response.status_code == 200:
            with open(f"{TEMP_ZIP_FILE}", "wb") as temp_file:
                downloaded_size = 0
                for data in response.iter_content(block_size):
                    downloaded_size += len(data)
                    temp_file.write(data)
                    done = int(50 * downloaded_size / total_size_in_bytes)
                    print(f"\r[{'█' * done}{'.' * (50 - done)}] {downloaded_size * 100 / total_size_in_bytes:.2f}%", end="")
                print("\nDownload completo.")
            

            with zipfile.ZipFile(f"{TEMP_ZIP_FILE}", 'r') as zip_ref:
                zip_ref.extractall(f"{TEMP_DIR_FILE}")
                for file_name in zip_ref.namelist():
                    if file_name.endswith('.csv'):
                        csv_path = f"{TEMP_DIR_FILE}/{file_name}"
                        
            os.remove(f"{TEMP_ZIP_FILE}")
        else:
            print("Falha no download do arquivo.")
            return None
    return csv_path

def process_file(csv_path: str):
    if(csv_path == ""):
        print("Caminho do arquivo CSV não informado.")
        return

    df = pd.read_csv(csv_path, sep=';', encoding='ISO-8859-1')
    print(df.head())

    df_agrupado = df.groupby('SG_UF').agg({'QT_ELEITORES_PERFIL': 'sum'})
    print(df_agrupado.head(30))


csv_path = f"{TEMP_DIR_FILE}/{FILE_NAME }"
if(not os.path.exists(f"{TEMP_DIR_FILE}")):
    csv_path = download_and_extract_zip(URL)
    print(f"Arquivo CSV extraído: {csv_path}")
    process_file(csv_path)
else:
    print(f"Arquivo CSV extraído: {csv_path}")
    process_file(csv_path)

    #shutil.rmtree("temp_eleitorado_atual_extracted_files")
