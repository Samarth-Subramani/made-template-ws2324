import pandas as pd
from zipfile import ZipFile
import requests
from io import BytesIO
import sqlite3

def download_and_process_nasa_dataset():
    # Function to download and preprocess NASA GISS dataset
    nasa_giss_url = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv"
    nasa_data = pd.read_csv(nasa_giss_url, skiprows=1)
    nasa_data.to_sql("global_temperature", sqlite3.connect("../data/global_temperature.sqlite"), index=False, if_exists="replace")
    print("NASA dataset is created and stored in /data directory")

def download_and_process_fao_dataset():
    # Function to download and preprocess FAO dataset
    fao_zip_url = "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_All_Data.zip"
    response = requests.get(fao_zip_url)
    with ZipFile(BytesIO(response.content)) as zip_file:
        fao_data = pd.read_csv(zip_file.extract('Production_Crops_Livestock_E_All_Data.csv'), encoding='latin1', low_memory=False)
    fao_data.to_sql("crop_yield", sqlite3.connect("../data/crop_yield.sqlite"), index=False, if_exists="replace")
    print("FAO dataset is created and stored in /data directory")

if __name__ == "__main__":
    download_and_process_nasa_dataset()
    download_and_process_fao_dataset()
