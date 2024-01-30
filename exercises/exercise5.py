import urllib.request
import zipfile
import pandas as pd
import sqlite3

# Step 1: Download the GTFS data
url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
zip_file_path, _ = urllib.request.urlretrieve(url, "GTFS.zip")

# Step 2: Extract the contents of the ZIP file
with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
    zip_ref.extractall("GTFS_data")

# Step 3: Read and filter stops.txt
stops_df = pd.read_csv("GTFS_data/stops.txt", dtype={"stop_id": "TEXT", "stop_name": "TEXT", "stop_lat": "FLOAT", "stop_lon": "FLOAT", "zone_id": "TEXT"})

filtered_stops_df = stops_df[(stops_df["zone_id"] == "2001") & 
                             (stops_df["stop_lat"].between(-90, 90, inclusive=True)) & 
                             (stops_df["stop_lon"].between(-90, 90, inclusive=True))]

# Step 4: Validate data (no additional validation required based on the provided constraints)

# Step 5: Write data into SQLite database
conn = sqlite3.connect("gtfs.sqlite")
filtered_stops_df.to_sql("stops", conn, index=False, if_exists="replace")

# Close the connection
conn.close()
