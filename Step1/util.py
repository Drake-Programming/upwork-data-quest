import pandas as pd
from typing import Tuple


class UTIL:
    """
    Stores utility functions
    """

    @staticmethod
    def compare_dataframes(
        source_df: pd.DataFrame, target_df: pd.DataFrame
    ) -> Tuple[list, list]:
        """
        Compares source meta file and target bucket meta file and will add file names to lists to remove or add
        :param source_df:
        :param target_df:
        :return: lists of file names
        """
        # Convert the two frames into dictionaries for easy lookup:
        # file_name -> date_time
        source_dict = dict(zip(source_df["FileNames"], source_df["DateTime"]))
        target_dict = dict(zip(target_df["FileNames"], target_df["DateTime"]))

        remove_list = []
        download_list = []

        # 1. Check REMOVED
        for file_name in target_dict:
            if file_name not in source_dict:
                print(f"{file_name} - Site File Removed")
                remove_list.append(file_name)

        # 2. Check ADDED or UPDATED
        for file_name, source_dt in source_dict.items():
            if file_name not in target_dict:
                # Not present in s3_frame at all -> ADDED
                print(f"{file_name} - Site File Added")
                download_list.append(file_name)
            else:
                # Present in both, but compare DateTime
                target_dt = target_dict[file_name]
                target_dt_converted = pd.to_datetime(target_dt)
                if source_dt != target_dt_converted:
                    print(type(source_dt))
                    print(type(target_dt))

                    # Different timestamps -> UPDATED
                    print(f"{file_name} - Site File Updated")
                    download_list.append(file_name)

        return remove_list, download_list
