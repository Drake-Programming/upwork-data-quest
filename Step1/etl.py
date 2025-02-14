from util import UTIL
from Soup import Soup
from bls_source import BLS
from s3_target import S3Target
from typing import Tuple


class ETL:
    """
    main interface to extract, transform and load bls data
    """

    def __init__(self, bls_source: BLS, s3_target: S3Target):
        """
        Constructor for BLS ETL pipeline
        :param bls_source:
        :param s3_target:
        """
        self.bls_source = bls_source
        self.s3_target = s3_target
        self.meta_key = "meta_file.csv"

    def extract(self) -> Tuple[list, list, bool]:
        """
        scraps bls site, checks meta file in S3 bucket, adds file names to remove or add
        :return: files_removal, files_added, bool
        """
        bls_meta_df = self.bls_source.meta_df
        s3_meta_df = self.s3_target.download_meta_file()
        files_removal, files_added = UTIL.compare_dataframes(bls_meta_df, s3_meta_df)

        if not files_removal and not files_added:
            return files_removal, files_added, True

        else:
            return files_removal, files_added, False

    def transform(
        self, files_removal, files_added, transformed=False
    ) -> Tuple[list, bool]:
        """
        Removes files from S3 bucket and makes urls list from file list adds
        :param files_removal:
        :param files_added:
        :param transformed:
        :return: file_urls, bool
        """
        if transformed:
            return files_added, True

        for file_name in files_removal:
            self.s3_target.remove_file(file_name)

        file_urls = Soup.get_file_url(files_added)
        return file_urls, False

    def load(self, files_url, loaded=False) -> None:
        """
        Downloads files from bls site and loads them into S3 bucket, replaces meta files in bucket with fresh data
        :param files_url:
        :param loaded:
        :return:
        """
        if loaded:
            return None

        for name, url in files_url.items():
            print(f"Downloading: {name} from {url}")
            content = Soup.download_file(url)
            print(f"Downloaded {len(content)} bytes")
            self.s3_target.upload_file(content, name)

        self.s3_target.upload_file(self.bls_source.get_meta_file(), self.meta_key)

    def run(self):
        """
        Runs each stage of the ETL pipeline
        :return: None
        """
        files_removal, files_added, transformed = self.extract()
        file_urls, loaded = self.transform(files_removal, files_added, transformed)
        self.load(file_urls, loaded)
        return None
