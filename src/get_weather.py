from datetime import datetime
import pandas as pd
import requests
import pytz
from src.base_logger import *
from pathlib import Path


def get_weather_data(
    weather_station="RJAA",
    country_code="JP",
    startDate="20190201",
    endDate="20190301",
    number=2,
    timezone="US/Central",
    save_dir=None,
):
    """
    Retrieve historical weather data from the Weather.com API for a specified location.

    Args:
        weather_station (str, optional): The weather station code. Defaults to "RJAA".
        country_code (str, optional): The country code. Defaults to "JP".
        startDate (str, optional): The start date for data retrieval in "YYYYMMDD" format. Defaults to "20190201".
        endDate (str, optional): The end date for data retrieval in "YYYYMMDD" format. Defaults to "20190301".
        number (int, optional): The number of observations to retrieve. Defaults to 2.
        timezone (str, optional): The desired timezone for timestamps in the data. Defaults to "US/Central".

    Returns:
        pandas.DataFrame: A DataFrame containing historical weather data with the following columns:
            - "Time": Timestamp in the specified timezone.
            - "tempf": Temperature in Fahrenheit.
            - "dewPt": Dew Point in percentage.
            - "rh": Humidity.
            - "wdir_cardinal": Wind direction.
            - "wspd": Wind speed in mph.
            - "gust": Wind gust (if available) in mph, 0.0 if not available.
            - "pressure": Pressure in inches.
            - "precip": Precipitation in inches, "0.0" if not available.
            - "wx_phrase": Weather conditions.

    Note:
        The function retrieves historical weather data for the specified location and time range using the Weather.com API.
        The data is returned as a pandas DataFrame with the columns described above.

    Example:
        # Retrieve weather data for Tokyo, Japan for February 2019
        weather_data = get_weather_data(weather_station="RJTT", country_code="JP", startDate="20190201", endDate="20190228")
    """
    logger.debug("TEST")
    apiKey = "e1f10a1e78da46f5b10a1e78da96f525"
    tz = pytz.timezone(timezone)

    # startDate = dateparser.parse(startDate)
    # endDate = dateparser.parse(endDate)
    ends = pd.date_range(start=startDate, end=endDate, freq="1M")

    starts = pd.date_range(start=startDate, end=endDate, freq="1M")
    starts = [s.replace(day=1) for s in starts]
    s_e = zip(starts, ends)

    weather_dfs = []
    for s, e in s_e:
        s = s.strftime("%Y%m%d")
        e = e.strftime("%Y%m%d")

        endpoint = f"https://api.weather.com/v1/location/{weather_station}:{number}:{country_code}/observations/historical.json?apiKey={apiKey}&units=e&startDate={s}&endDate={e}"
        response = requests.get(endpoint).json()["observations"]
        location = f"{response[0]['obs_name']}".replace("/", "_")
        weather_data = sorted(response, key=lambda k: k["valid_time_gmt"])

        header = [
            "Time",
            "Temperature °F",
            "Dew Point %",
            "Humidity",
            "Wind",
            "Wind Speed mph",
            "Wind Gust",
            "Pressure in",
            "Percip. in",
            "Conditions",
        ]

        table = []
        for item in weather_data:
            row = [
                datetime.fromtimestamp(item["valid_time_gmt"], tz),
                item["temp"],
                f'{item["dewPt"]}',
                f'{item["rh"]}',
                item["wdir_cardinal"],
                item["wspd"],
                f'{item["gust"] if item["gust"] else 0}',
                f'{item["pressure"]}',
                f'{item["precip_total"] if item["precip_total"] else "0.0"}',
                item["wx_phrase"],
            ]
            table.append(row)

        columns = ["Time", "tempf", "dewPt", "rh", "wdir_cardinal", "wspd", "gust", "pressure", "precip", "wx_phrase"]
        weather_df = pd.DataFrame(table, columns=columns)
        weather_dfs.append(weather_df)

    weather_dfs = pd.concat(weather_dfs)

    if save_dir:
        Path(f"{save_dir}").mkdir(parents=True, exist_ok=True)
        weather_dfs.to_csv(
            f"{save_dir}/{location}_{weather_station}_{country_code}_{startDate}_{endDate}.csv", index=False
        )
    return weather_dfs
