import datetime
import io
import os
import zipfile
from datetime import date

import pandas as pd
import requests


class NSEData:
    def __init__(
        self,
        days=30,
        starting_date="today",
        input_path="stock_datasets",
        output_path="symbols_data",
    ):
        """
        days - number of days prior to starting_date
        starting_date
        input_path - folder which will be created to download the CSV files into
        output_path - folder which will be created to store the outputs which are the symbol csv files
        """
        self.days = days
        self.starting_date = starting_date
        self.input_path = input_path
        self.output_path = output_path
        self.df = pd.DataFrame()

    def _is_weekday(self, day):
        if day.weekday() < 5:
            return True
        return False

    def _construct_url(self, date):
        day = "%02d" % date.day  # convert to double digit - ex: 3 -> 03
        month = date.strftime("%B").upper()[:3]
        return "https://archives.nseindia.com/content/historical/EQUITIES/{2}/{1}/cm{0}{1}{2}bhav.csv.zip".format(
            day, month, date.year
        )

    def _get_last_weekdays(self):
        """days defaults to 30 and starting_date defaults to current day, otherwise specify date as DD-MM-YYYY"""
        if self.starting_date == "today":
            self.starting_date = date.today()
        else:
            try:
                self.starting_date = datetime.datetime.strptime(
                    self.starting_date, "%d-%m-%Y"
                ).date()
            except:
                print(
                    "Error: Could not parse date given, please pass the date in the following format: DD-MM-YYYY"
                )
                raise

        current_date = self.starting_date
        count = 0
        while count < self.days:
            if self._is_weekday(current_date):
                count += 1
                yield current_date
            current_date -= datetime.timedelta(days=1)

    def get_data(self, days=30, path="stock_datasets", starting_date="today"):
        # make the input directory
        if not os.path.exists(path):
            os.makedirs(path)

        # send the requests
        for date in self._get_last_weekdays():
            url = self._construct_url(date)
            try:
                r = requests.get(url, timeout=2)
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(path=path)
            except:
                print("Could not fetch from {}".format(url))

        print("Downloaded files into {}".format(path))

    def _merge(self):
        files = os.listdir(self.input_path)
        dfs = []

        print("Reading files into dataframe..")
        for file in files:
            try:
                dfs.append(pd.read_csv(os.path.join(self.input_path, file)))
            except:
                print(
                    "Something went wrong with reading {}",
                    os.path.join(self.input_path, file),
                )
                raise

        df = pd.concat(dfs)
        df["date"] = pd.to_datetime(df["TIMESTAMP"], format="%d-%b-%Y")
        print("Done reading files into dataframe..")

        self.df = df[
            "SYMBOL SERIES OPEN HIGH LOW CLOSE LAST PREVCLOSE TOTTRDQTY TIMESTAMP date".split(
                " "
            )
        ]

    def write_to_symbol_csv(self):
        # Merge all csv into a single dataframe
        self._merge()
        # get all unique symbols
        symbols = self.df.SYMBOL.unique()

        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

        print("Starting to group into individual SYMBOL csv files")
        for symbol in symbols:
            current_df = self.df[self.df.SYMBOL == symbol].sort_values(by=["date"])
            current_df.drop(["date"], axis=1, inplace=True)
            current_df.to_csv(
                os.path.join(self.output_path, symbol) + ".csv", index=False
            )

        print("Done grouping into individual SYMBOL csv files")
        print("Created {} files".format(len(os.listdir(self.output_path))))


if __name__ == "__main__":
    nse = NSEData(starting_date="09-03-2020")

    # get the data
    nse.get_data()

    # extract individual symbol data into separate files
    nse.write_to_symbol_csv()

    print("Success!")
