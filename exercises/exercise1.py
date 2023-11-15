import pandas as pd
from sqlalchemy import create_engine, Integer, String, Text, Float

# Data source URL
data_source_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"

# SQLite database settings
database_url = "sqlite:///airports.sqlite"
table_name = "airports"

def fetch_data(url):
    # Fetch data from the given URL and return a Pandas DataFrame.
    return pd.read_csv(url, delimiter=';', on_bad_lines='skip')

def write_to_sql(data_frame, database_url, table_name):
  # Write Pandas DataFrame to SQLite database.
    engine = create_engine(database_url)
    data_frame.to_sql(table_name, engine, index=False, if_exists="replace", dtype={
        'column_1': Integer(),
        'column_2': String(),
        'column_3': String(),
        'column_4': String(),
        'column_5': String(),  
        'column_6': String(),  
        'column_7': Float(),
        'column_8': Float(),
        'column_9': Integer(),
        'column_10': Float(),
        'column_11': String(),  
        'column_12': String(),  
        'geo_punkt': String()   
    })

def main():
    # Fetch data from the source
    airports_data = fetch_data(data_source_url)

    # Write data to SQLite database
    write_to_sql(airports_data, database_url, table_name)

    print("Data pipeline completed successfully.")

if _name_ == "_main_":
    main()
