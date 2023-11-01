import requests
import dateparser
from tqdm import tqdm
import os
from bs4 import BeautifulSoup
from pathlib import Path
from src.base_logger import *
import pandas as pd
import glob
import copy
import warnings

# Ignore dateparser warnings regarding pytz
warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)
import gtfs_kit as gk

main_url = "https://transitfeeds.com"


def bottom_to_top(listfiles):
    """
    Process a list of GTFS (General Transit Feed Specification) files to find overlapping date ranges and gaps.

    Args:
        listfiles (list): A list of file paths to GTFS feed files.

    Returns:
        tuple: A tuple containing two pandas DataFrames:
            1. raw_gtfs_files: Contains information about the original GTFS files with columns:
               - "gtfs_file": File name of the GTFS feed.
               - "gtfs_file_date": Date parsed from the file name.
               - "begin_date": Start date of the feed.
               - "end_date": End date of the feed.
               - "real_begin_date": The maximum date between "gtfs_file_date" and "begin_date".
               - "real_end_date": The computed end date.
               - "gap": Time gap between feeds.
            2. all_gtfs_files: Contains information about the filtered and sorted GTFS files with columns:
               - "gtfs_file": File name of the GTFS feed.
               - "begin_date": Start date of the feed.
               - "end_date": End date of the feed.
               - "real_begin_date": The maximum date between "gtfs_file_date" and "begin_date".
               - "real_end_date": The computed end date.
               - "gap": Time gap between feeds.

    Description:
        The function processes a list of GTFS feed files and extracts information such as file names, start dates,
        end dates, and more. It then uses a "bottom to top" algorithm to sort and filter the feed files based on
        their date ranges, ensuring that overlapping date ranges and gaps are appropriately handled.

        The function returns two DataFrames: "raw_gtfs_files" contains the original data before processing, while
        "all_gtfs_files" contains the final sorted and filtered data.

    Example:
        # Process a list of GTFS files and get the processed data
        raw_data, processed_data = bottom_to_top(["gtfs_feed_20220101.zip", "gtfs_feed_20220115.zip"])
    """
    all_gtfs_files = []
    for l in tqdm(listfiles):
        feed = gk.read_feed(l, dist_units="mi")
        try:
            k = feed.describe()
            end_date = k[(k.indicator == "end_date")]["value"].iloc[0]
            end_date = dateparser.parse(end_date, date_formats=["%Y%m%d"])
            begin_date = k[(k.indicator == "start_date")]["value"].iloc[0]
            begin_date = dateparser.parse(begin_date, date_formats=["%Y%m%d"])
            filename = os.path.basename(l)
            name = os.path.splitext(filename)[0]
            gtfs_file_date = dateparser.parse(name, date_formats=["%Y%m%d"])
            df = pd.DataFrame(
                [[filename, gtfs_file_date, begin_date, end_date]],
                columns=["gtfs_file", "gtfs_file_date", "begin_date", "end_date"],
            )
            all_gtfs_files.append(df)
        except Exception as e:
            print("Failed:", e)
            pass
    all_gtfs_files = pd.concat(all_gtfs_files, ignore_index=True)

    # Bottom to top algorithm
    bottom_to_top = True
    all_gtfs_files = all_gtfs_files.sort_values(["gtfs_file_date", "begin_date", "end_date"], ascending=bottom_to_top)

    all_gtfs_files = all_gtfs_files.reset_index()
    all_gtfs_files = all_gtfs_files.drop("index", axis=1)
    all_gtfs_files["real_begin_date"] = all_gtfs_files[["gtfs_file_date", "begin_date"]].max(axis=1)

    # change this in the top to bottom
    all_gtfs_files["real_end_date"] = all_gtfs_files.real_begin_date.shift(-1)
    all_gtfs_files["real_end_date"] = all_gtfs_files["real_end_date"] - pd.Timedelta(days=1)
    all_gtfs_files["real_end_date"] = all_gtfs_files["real_end_date"].fillna(all_gtfs_files.end_date)

    raw_gtfs_files = copy.deepcopy(all_gtfs_files)
    # Begin dates must occur before the real end dates
    all_gtfs_files = all_gtfs_files[all_gtfs_files["begin_date"] < all_gtfs_files["real_end_date"]]

    all_gtfs_files = all_gtfs_files.drop_duplicates(subset=["begin_date", "end_date", "real_end_date"], keep="last")
    all_gtfs_files["gap"] = all_gtfs_files["begin_date"].shift(-1) - all_gtfs_files["end_date"]
    raw_gtfs_files["gap"] = raw_gtfs_files["begin_date"].shift(-1) - raw_gtfs_files["end_date"]

    all_gtfs_files = all_gtfs_files.drop(["gtfs_file_date"], axis=1)
    return raw_gtfs_files, all_gtfs_files


def top_to_bottom(listfiles):
    all_gtfs_files = []
    for l in tqdm(listfiles):
        feed = gk.read_feed(l, dist_units="mi")
        try:
            k = feed.describe()
            end_date = k[(k.indicator == "end_date")]["value"].iloc[0]
            end_date = dateparser.parse(end_date, date_formats=["%Y%m%d"])
            begin_date = k[(k.indicator == "start_date")]["value"].iloc[0]
            begin_date = dateparser.parse(begin_date, date_formats=["%Y%m%d"])
            filename = os.path.basename(l)
            name = os.path.splitext(filename)[0]
            gtfs_file_date = dateparser.parse(name, date_formats=["%Y%m%d"])
            df = pd.DataFrame(
                [[filename, gtfs_file_date, begin_date, end_date]],
                columns=["gtfs_file", "gtfs_file_date", "begin_date", "end_date"],
            )
            all_gtfs_files.append(df)
        except Exception as e:
            print("Failed:", e)
            pass
    all_gtfs_files = pd.concat(all_gtfs_files, ignore_index=True)

    # Top to Bottom algorithm
    bottom_to_top = False
    all_gtfs_files = all_gtfs_files.sort_values(["gtfs_file_date"], ascending=bottom_to_top)

    all_gtfs_files = all_gtfs_files.reset_index()
    all_gtfs_files = all_gtfs_files.drop("index", axis=1)

    raw_gtfs_files = copy.deepcopy(all_gtfs_files)
    all_gtfs_files = all_gtfs_files.drop_duplicates(subset=["begin_date", "end_date"], keep="first")
    for_removal = []
    for k, v in all_gtfs_files.iterrows():
        _bd = v["begin_date"]
        _ed = v["end_date"]
        _gd = v["gtfs_file_date"]
        idxs = all_gtfs_files[
            (all_gtfs_files["begin_date"] >= _bd)
            & (all_gtfs_files["end_date"] <= _ed)
            & (all_gtfs_files["gtfs_file_date"] < _gd)
        ].index
        for_removal.extend(idxs)

    bad_df = all_gtfs_files.index.isin(for_removal)
    all_gtfs_files = all_gtfs_files[~bad_df]
    all_gtfs_files = all_gtfs_files.reset_index(drop=True)
    all_gtfs_files["real_end_date"] = all_gtfs_files.begin_date.shift(1) - pd.Timedelta("1d")
    all_gtfs_files["real_end_date"] = all_gtfs_files["real_end_date"].fillna(all_gtfs_files.end_date)

    all_gtfs_files["gap"] = all_gtfs_files["real_end_date"] - all_gtfs_files["end_date"]

    raw_gtfs_files["real_end_date"] = raw_gtfs_files.begin_date.shift(1) - pd.Timedelta("1d")
    raw_gtfs_files["real_end_date"] = raw_gtfs_files["real_end_date"].fillna(raw_gtfs_files.end_date)
    raw_gtfs_files["gap"] = raw_gtfs_files["real_end_date"] - raw_gtfs_files["end_date"]

    all_gtfs_files = all_gtfs_files.drop(["gtfs_file_date"], axis=1)

    return raw_gtfs_files, all_gtfs_files


def get_download_links(page_link, start_date, end_date):
    """
    Extract download links for GTFS (General Transit Feed Specification) files from a webpage.

    Args:
        page_link (str): The URL of the webpage to scrape for download links.
        start_date (str, optional): The start date for filtering downloads. Format: "YYYY-MM-DD".
        end_date (str, optional): The end date for filtering downloads. Format: "YYYY-MM-DD".

    Returns:
        tuple: A tuple containing two lists:
            1. pages: URLs of additional pages with download links.
            2. download_links: List of tuples with date and download link pairs.

    Description:
        This function scrapes a webpage specified by 'page_link' to extract download links for GTFS files.
        It optionally filters the links based on the provided 'start_date' and 'end_date'. It returns a tuple
        with two lists: 'pages' containing links to additional pages and 'download_links' containing tuples of
        date and download link pairs.

    Example:
        # Extract download links from a webpage and filter by date range
        page_link = "https://example.com/gtfs-downloads"
        start_date = "2023-10-01"
        end_date = "2023-10-31"
        pages, download_links = get_download_links(page_link, start_date, end_date)
    """
    if start_date:
        start_date = dateparser.parse(start_date, date_formats=["%Y-%m-%d"])
    if end_date:
        end_date = dateparser.parse(end_date, date_formats=["%Y-%m-%d"])
    r = requests.get(page_link)
    soup = BeautifulSoup(r.content, "html.parser")

    s = soup.find("div", class_="row")
    content = s.find_all("a")
    # logger.debug(f"{content}")
    pages = []

    _download_links = []

    for c in content:
        if not c.get("class"):
            if "p=" in c.get("href"):
                pages.append(main_url + c.get("href"))
                # logger.debug(c.get("href"))
            else:
                dt = dateparser.parse(c.text)
                if dt:
                    if start_date:
                        if dt < start_date:
                            continue
                    if end_date:
                        if dt > end_date:
                            continue
                    dt = dt.strftime("%Y-%m-%d")
                    logger.debug(f"{dt}")
                    _download_links.append((dt, main_url + c.get("href")))

    return pages, _download_links


def download_GTFS(company="nashville-mta", start_date="2023-10-01", end_date=None, save_dir="./data", overwrite=True):
    """
    Download GTFS files for a specific transit company within a specified date range.

    Args:
        company (str): The name of the transit company. Supported: "nashville-mta".
        start_date (str, optional): The start date for filtering downloads. Format: "YYYY-MM-DD". Defaults to "2023-10-01".
        end_date (str, optional): The end date for filtering downloads. Format: "YYYY-MM-DD". Defaults to None.
        save_dir (str, optional): The directory to save downloaded files. Defaults to "./data".
        overwrite (bool, optional): Whether to overwrite existing files with the same name. Defaults to True.

    Returns:
        bool: True if the download process is successful.

    Description:
        This function downloads GTFS files for a specific transit company within the specified date range.
        It first retrieves the download links using 'get_download_links' and then downloads the files
        to the specified 'save_dir'. It allows for overwriting existing files and returns True if the process is successful.

    Example:
        # Download GTFS files for the Nashville MTA transit company within a date range
        download_GTFS(company="nashville-mta", start_date="2023-10-01", end_date="2023-10-31", save_dir="./gtfs_data")
    """
    if company == "nashville-mta":
        main_link = f"{main_url}/p/nashville-mta/220"
    else:
        raise "Company is unknown."

    pages, download_links = get_download_links(main_link, start_date, end_date)
    for p in pages:
        download_links.extend(get_download_links(p, start_date, end_date)[1])

    download_links = list(set(download_links))

    [logger.debug(dl) for dl in download_links]
    area_name = main_link.split("/")[-2]
    Path(f"{save_dir}/{area_name}").mkdir(parents=True, exist_ok=True)

    for name, dl in download_links:
        # for name, dl in download_links:
        # Prevent confusion with duplicate files
        dl_name = dl.split("/")[-1]
        check = dateparser.parse(dl_name, date_formats=["%Y%m%d"])
        if check:
            dl_name = check.strftime("%Y%m%d")
        if dl_name == "latest":
            dl_name = name.replace("-", "")
        if "transitfeeds.com" in dl_name:
            continue

        savepath = f"{save_dir}/{area_name}/{dl_name}.zip"

        if not overwrite:
            i = 1
            while os.path.exists(savepath):
                dl_name = f"{dl_name}-{i}"
                savepath = f"{save_dir}/{area_name}/{dl_name}.zip"

        with open(savepath, "wb") as out_file:
            content = requests.get(dl + "/download", stream=True).content
            out_file.write(content)

    return True
