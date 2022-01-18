import logging
from typing import Generator, List, Optional
import datetime
from pathlib import Path
from tempfile import TemporaryFile
from zipfile import ZipFile
import csv
import httpx
from bs4 import BeautifulSoup
from .settings import Settings


class SecFiles:
    """Class Object to help parse and download the Sec files"""

    BASE_URL = "https://www.sec.gov"
    FILES_URL = f"{BASE_URL}/dera/data/financial-statement-data-sets.html"

    def __init__(self, start_date: str = '1900-01-01') -> None:
        self.start_date = datetime.date.fromisoformat(start_date)
        self.config = Settings()
        self.is_downloaded = False
        self.download_files()
        self.files = {"sub": list(), "tag": list(), "num": list(), "pre": list()}
        self.parse_files()

    @property
    def sub(self):
        return self.gen_fin_files("sub", self.start_date)

    @property
    def tag(self):
        return self.gen_fin_files("tag", self.start_date)

    @property
    def num(self):
        return self.gen_fin_files("num", self.start_date)

    @property
    def pre(self):
        return self.gen_fin_files("pre", self.start_date)

    def parse_files(self):
        """Sets the files attribute to a list of tuples containing the folder name
        and dict with row generator for each .txt file in each directory"""
        file_path = self.config.dir_path
        if not file_path.exists():
            raise NotADirectoryError(f"{file_path} is not a directory")
        logging.info(f"Parsing Financial Files in {file_path}")
        [
            [
                self.files[file_path.stem].append((date, file_path))
                for file_path in fin_dir.iterdir()
                if file_path.suffix == ".txt"
            ]
            for date, fin_dir in self.main_dir()
        ]

    def main_dir(self) -> list:
        """Returns a sorted list of tuples with (date, file path)"""
        file_path = self.config.dir_path
        return sorted(
            [(self.yyyyQq_to_date(f.name), f) for f in file_path.iterdir()],
            key=lambda x: x[0],
        )

    @staticmethod
    def yyyyQq_to_date(yr_qtr_str: str) -> datetime.date:
        """Converts a str in the form {year}Q{qtr} (ie 2010Q2) to the end of the quarter 2010-03-31"""
        yr, qtr = yr_qtr_str.split("Q")
        mon = 1 + int(qtr) * 3 if int(qtr) < 4 else 1
        yr = int(yr) if mon > 1 else int(yr) + 1
        return datetime.date(yr, mon, 1) - datetime.timedelta(days=1)

    def gen_fin_files(self, file_type: str, start_date: datetime.date) -> Generator:
        """Generator that yeilds the dicts from gen_dict for each file that is greater than the start_date"""
        for date, file_path in self.files[file_type]:
            if date > start_date:
                logging.info(f"Reading file from quarter ending {date.isoformat()}")
                yield from self.gen_dict(file_path)
            else:
                logging.info(f"Skipping quarter ending {date.isoformat()}")

    @staticmethod
    def gen_dict(file_path: Path) -> Generator:
        """Generator that returns a dict for each row in the file"""
        with open(file_path) as f:
            reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            head = reader.__next__()
            for row in reader:
                yield {k: v for k, v in zip(head, row)}

    def download_files(self):
        """Downloads all the financial statment files from the SEC website"""
        download_path = self.config.dir_path
        download_path.mkdir(exist_ok=True)

        logging.info("Scraping the urls from the SEC webpage")
        url_list = self.get_urls()

        with httpx.Client(base_url=self.BASE_URL) as client:
            for url in url_list:

                year = url.get("year")
                qtr = url.get("qtr")
                extract_dir = download_path / f"{year}{qtr}"

                if extract_dir.exists():
                    logging.info(
                        f"Financial Statement Files for {year} {qtr} seem to already be at {extract_dir}"
                    )
                else:
                    logging.info(
                        f"Downloading Financial Statement Files for {year} {qtr}"
                    )
                    self.download(client, url.get("url"), extract_dir)

        self.is_downloaded = True

    @staticmethod
    def download(client: httpx.Client, url: str, save_to: Path):
        logging.info(f"Downloading {url}")
        with client.stream("GET", url) as resp, TemporaryFile() as temp_file:
            resp.raise_for_status()

            for chunk in resp.iter_bytes():
                temp_file.write(chunk)

            logging.info(f"Unziping Archive in {save_to}")

            with ZipFile(temp_file) as archive:
                archive.extractall(save_to)

    @staticmethod
    def parse_row(row: list) -> dict:
        """Parses the url for each year and quarter in the table"""
        year, qtr = row[0].text.upper().strip().split(": ")[-1].split()
        url = row[0].find("a").attrs["href"]
        return dict(year=year, qtr=qtr, url=url)

    def get_urls(self) -> List[dict]:
        """Gets the list of URLs for the financial files for each year and quarter on the webpage"""
        r = httpx.get(self.FILES_URL)
        r.raise_for_status()
        page = BeautifulSoup(r.text, "html.parser")
        tbody = page.find("table").find("tbody")
        rows = [row.find_all("td") for row in tbody.find_all("tr")]
        return [self.parse_row(row) for row in rows]
