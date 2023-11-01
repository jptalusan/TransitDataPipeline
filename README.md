# Transit Company

## WeGo

### APC Data
Use gcloud to get the data from bucket:
* For data up to one month back:

```
gsutil cp gs://apc-historical-datasets/APC_Wego/complete-with-remark-repartitioned-nodupes.parquet .
```
* or run `src/download_APC_bucket.sh`

* This is repartitioned by `year`, `month`, `day` and `route_id`.
* Not also make sure to get overload_id == 0 and then drop trips that are not complete (meaning it featured a disruption in the middle)

### GTFS Data
Scrapes data from transitfeed.com. You can use [this repo](https://github.com/jptalusan/mta_carta_pipeline/tree/master/data) or just follow these commands. `bottom_to_top` or `top_to_bottom` just filters the GTFS to identify gaps and overlaps so that only the updated GTFS is used for merging.
```
links = download_GTFS(company="nashville-mta")

logger.debug(os.getcwd())
basefolder = "./data/nashville-mta"
listfiles = glob.glob(f"{basefolder}/*.zip")

raw_gtfs_files, all_gtfs_files = bottom_to_top(listfiles)
```

### Weather Data
* Retrieve historical weather data from the Weather.com API for a specified location.
* Todo: Add save dir to get_weather_data and check for duplicates (or allow overwrite)
```
from src.get_weather import get_weather_data

weather_df = get_weather_data()
print(weather_df)
```

### Census Data
* Please add where to get census data from and how to read it.
* If possible show how to extract from the census website.

### Inrix Data (Traffic)
* Add location on how to get it.
* Should be in DS3 or Ammar's folder.
* Discuss the features of this. Also include the map file:`/data/disk2/MapData2023/maprelease-geojson/USA_Tennessee_geojson.zip`