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
stops_df = pd.read_csv("GTFS_data/stops.txt", encoding="utf-8", dtype={"stop_id": str, "stop_name": str, "stop_lat": float, "stop_lon": float, "zone_id": str})

# Check Shape and Types
print("Shape:", stops_df.shape)  # Check the number of rows and columns
print("Types:", stops_df.dtypes)  # Check data types of each column

# Filter based on specified criteria
filtered_stops_df = stops_df[
    (stops_df["zone_id"] == "2001") &
    (stops_df["stop_lat"].between(-90, 90, inclusive='both')) &
    (stops_df["stop_lon"].between(-90, 90, inclusive='both'))
].copy()

# Check Quality
print("Quality: No. of Empty Values")
print(filtered_stops_df.isnull().sum())  # Check for empty values

# Write data into SQLite database
conn = sqlite3.connect("gtfs.sqlite")
filtered_stops_df.to_sql("stops", conn, index=False, if_exists="replace")

# Close the connection
conn.close()
