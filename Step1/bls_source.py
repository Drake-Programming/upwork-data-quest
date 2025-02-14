from io import StringIO
from Soup import Soup
import pandas as pd


class BLS:
    """
    Scraps bls site and stores info
    """

    def __init__(self):
        """
        Constructor for BLS Object
        """
        soup = Soup.get_bls_soup()
        dates = Soup.get_bls_date_time(soup)
        self.file_names = Soup.get_bls_file_names(soup)
        self.formatted_dates = Soup.convert_to_datetime(dates)
        self.meta_df = pd.DataFrame(
            {"DateTime": self.formatted_dates, "FileNames": self.file_names}
        )

    def get_meta_file(self):
        """
        Converts pandas dataframe to csv, used to create metafile
        :return:
        """
        out_buffer = StringIO()
        self.meta_df.to_csv(out_buffer, index=False)
        return out_buffer.getvalue()
