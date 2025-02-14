import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime


class Soup:
    """
    Holds static method for interacting with internet
    """

    @staticmethod
    def get_bls_soup() -> BeautifulSoup:
        """
        Gets html soup from bls website
        :return: html soup
        """
        url = "https://download.bls.gov/pub/time.series/pr/"
        headers = {"User-Agent": "MyDataSyncScript/1.0 (contact: example@example.com)"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, "lxml")
        return soup

    @staticmethod
    def get_file_url(file_names) -> dict:
        """
        Creates dict with file names as keys and urls as values
        :param file_names:
        :return:
        """
        base_url = "https://download.bls.gov/pub/time.series/pr/"
        full_urls = {}
        for file_name in file_names:
            full_urls[file_name] = base_url + file_name

        return full_urls

    @staticmethod
    def get_bls_file_names(soup) -> list:
        """
        Gets file names from soup and output a list of the file names
        :param soup:
        :return: list of file names
        """
        file_names = []
        # Iterate over all <a> tags in the HTML
        for link in soup.find_all("a"):
            href = link.get("href")
            # Filter out the parent directory link and any non-file entries
            if href and href != "../" and "Parent Directory" not in link.text:
                # Extract the base file name by splitting the href string on '/'
                base_name = href.split("/")[-1]
                file_names.append(base_name)

        return file_names

    @staticmethod
    def get_bls_date_time(soup) -> list:
        """
        Gets the dates and times of the uploaded files from the bls site
        :param soup:
        :return: list of dates and times
        """
        pre_tag = soup.find("pre")
        if not pre_tag:
            raise Exception("Could not find the <pre> block in the HTML content.")

        # Use a newline as separator to ensure each <br> is converted into a newline.
        text = pre_tag.get_text(separator="\n")
        lines = text.splitlines()

        # Define a regex pattern to capture the date and time.
        # Example line: "2/6/2025  8:30 AM          102 pr.class"
        pattern = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s+[AP]M)")

        date_time_list = []
        for line in lines:
            match = pattern.search(line)
            if match:
                date, time_str = match.groups()
                date_time_list.append((date, time_str))

        return date_time_list

    @staticmethod
    def convert_to_datetime(date_time_list) -> list:
        """
        Converts the seperate dates and times into datetime objects
        :param date_time_list:
        :return: list of datetime objects
        """

        datetime_list = []
        for date_str, time_str in date_time_list:
            dt_str = f"{date_str} {time_str}"
            dt = datetime.strptime(dt_str, "%m/%d/%Y %I:%M %p")
            datetime_list.append(dt)

        return datetime_list

    @staticmethod
    def download_file(url):
        """
        Downloads file from internet using url
        :param url:
        :return:
        """
        headers = {"User-Agent": "MyDataSyncScript/1.0 example@example.com"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.content
