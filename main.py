from src.get_weather import get_weather_data
from src.transitfeed_scraper import *
from src.base_logger import *
import pandas as pd
import glob
import os

logger.info("Start")
# weather_df = get_weather_data()
# print(weather_df)

# links = download_GTFS(company="nashville-mta")

# logger.debug(os.getcwd())
# basefolder = "./data/nashville-mta"
# listfiles = glob.glob(f"{basefolder}/*.zip")

# raw_gtfs_files, all_gtfs_files = bottom_to_top(listfiles)

# Run download_APC_bucket.sh
df = pd.read_parquet("./data/complete_APC")
print(df)
