#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Import necessary Libraries
import os
import requests


# In[ ]:


# Base URL for NYC TLC Yellow Taxi trip data
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{}.parquet"

# Year and months to download (focusing on the first 3 months of the year)
year = 2025
months = ["01", "02", "03"]  # Jan, Feb, Mar


# In[ ]:


# Local folder structure
folder_path = "data"
os.makedirs(folder_path, exist_ok=True)

def download_file(url, save_path):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(save_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            print(f"SUCCESS: Downloaded {save_path}")
        else:
            print(f"ERROR: Failed to download {url} (Status code: {response.status_code})")
    except Exception as e:
        print(f"EXCEPTION: Error downloading {url} -> {e}")


# In[ ]:


# Download files for Jan–Mar 2025
for month in months:
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    url = base_url.format(year, month)
    save_path = os.path.join(folder_path, file_name)
    download_file(url, save_path)

print(f"All files saved in: {folder_path}")

