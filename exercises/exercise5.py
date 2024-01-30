import urllib.request
import zipfile
import pandas as pd
import sqlite3
import os

# Step 1: Download GTFS Data
url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
gtfs_zip_file = "GTFS.zip"
urllib.request.urlretrieve(url, gtfs_zip_file)

# Step 2: Unzip and Read Data
with zipfile.ZipFile(gtfs_zip_file, 'r') as zip_ref:
    zip_ref.extractall("GTFS_data")
stops_file = os.path.join("GTFS_data", "stops.txt")

# Step 3: Data Processing
stops_df = pd.read_csv(stops_file)
stops_df = stops_df[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]
# Check Shape and Types
print("Shape:", stops_df.shape)  # Check the number of rows and columns
print("Types:", stops_df.dtypes)  # Check data types of each column

# Filter based on specified criteria
filtered_stops_df = stops_df[
    (stops_df["zone_id"] == '2001') &
    (stops_df["stop_lat"].between(-90, 90)) &
    (stops_df["stop_lon"].between(-90, 90))
]

# Check Quality
print("Quality: No. of Empty Values")
print(filtered_stops_df.isnull().sum())  # Check for empty values

# Convert columns to appropriate data types
filtered_stops_df["stop_id"] = filtered_stops_df["stop_id"].astype(int)
filtered_stops_df["zone_id"] = filtered_stops_df["zone_id"].astype(int)

# Step 4: Write to SQLite Database
conn = sqlite3.connect('gtfs.sqlite')
filtered_stops_df.to_sql('stops', conn, if_exists='replace', index=False, dtype={
    'stop_id': 'INTEGER',
    'stop_name': 'TEXT',
    'stop_lat': 'FLOAT',
    'stop_lon': 'FLOAT',
    'zone_id': 'INTEGER'
})
conn.close()

