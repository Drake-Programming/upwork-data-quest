from io import StringIO
import boto3
from botocore.exceptions import ClientError
import pandas as pd


class S3Target:
    """
    Makes an object that stores bucket info and method to interact with bucket
    """

    def __init__(self, target_bucket: str, access_key: str, secret_key: str):
        """
        Constructor for S3Target
        :param target_bucket:
        :param access_key:
        :param secret_key:
        """
        self._s3 = boto3.resource(
            "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )
        self.bucket = self._s3.Bucket(target_bucket)
        self.meta_key = "meta_file.csv"

    def upload_file(self, content: str, key: str) -> bool:
        """
        Uploads a file to S3 Bucket
        :param content:
        :param key:
        :return: Bool
        """
        self.bucket.put_object(Body=content, Key=key)
        return True

    def remove_file(self, key: str) -> bool:
        """
        Removes a file from S3 Bucket
        :param key:
        :return:
        """
        self.bucket.Object(key).delete()
        return True

    def download_meta_file(self) -> pd.DataFrame:
        """
        Downloads meta file from S3 Bucket and converts it to dataframe
        :return:
        """
        try:
            s3_meta_csv = (
                self.bucket.Object(self.meta_key)
                .get()
                .get("Body")
                .read()
                .decode("utf-8")
            )
            s3_meta_df = pd.read_csv(StringIO(s3_meta_csv))
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NoSuchKey":
                print("S3 Meta File Not Found")
                s3_meta_df = pd.DataFrame(columns=["FileNames", "DateTime"])
        return s3_meta_df

    def download_file(self, key: str):
        """
        Downloads a file from S3 Bucket
        :param key:
        :return:
        """
        try:
            s3_file = self.bucket.Object(key).get().get("Body").read().decode("utf-8")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NoSuchKey":
                print("S3 File Not Found")
                return False
        return s3_file
