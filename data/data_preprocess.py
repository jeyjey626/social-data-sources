#!/usr/bin/env python3

import os
import pandas as pd

dataframes = []

# first, we downloaded all the archive files from https://s3.amazonaws.com/tripdata/index.html?fbclid=IwAR15trP4MTHZSkcCqpxBBYtCJLGnAEycwRhWPrYct6QuMSstojYqAamT_D8

for file in os.listdir("."):
    if file.endswith(".zip"):
        with zipfile.ZipFile(file, 'r') as zip_ref:
            zip_ref.extractall(".")

for file in os.listdir("."):
    if file.endswith(".csv"):
        print(file)
        df = pd.read_csv(file)

        # making the column names consistent (some files use upper case)
        if "starttime" not in df.columns:
            df = df[["Start Time", "Start Station Latitude", "Start Station Longitude",
            "End Station Latitude", "End Station Longitude"]]

            df.columns = ["starttime", "start station latitude", "start station longitude",
            "end station latitude", "end station longitude"]
        else:
            df = df[["starttime", "start station latitude", "start station longitude",
            "end station latitude", "end station longitude"]]
        
        # converting strings to datetimes
        df["starttime"] = pd.to_datetime(df["starttime"], infer_datetime_format=True)

        # group by year and month
        df["year"] = df["starttime"].dt.year
        df["month"] = df["starttime"].dt.month
        df["day"] = df["starttime"].dt.day
        df["hour"] = df["starttime"].dt.hour
        df_groupped = df.groupby(["start station latitude", "start station longitude",
            "end station latitude", "end station longitude", "start station name", "end station name",
            "start station id", "end station id", "year", "month"]).size().reset_index(name="trip count")
        print(df_groupped.head())

        dataframes.append(df_groupped)

# concatenate and save all the dataframes
full_dataframe = pd.concat(dataframes, ignore_index=True)
full_dataframe.to_csv("citybike-aggregated-by-hour.csv")
