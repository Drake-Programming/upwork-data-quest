from api_source import APISource
from s3_target import S3Target


class ETL:
    """
    main interface to extract, and load api data
    """

    def __init__(self, api_source: APISource, s3_target: S3Target):
        self.json_key = "usa_population.json"
        self.api_source = api_source
        self.s3_target = s3_target

    def extract(self) -> str:
        """
        Gets json string from api url
        :return: string json
        """
        api_json = self.api_source.get_json()
        return api_json

    def load(self, json: str):
        """
        Loads json string from api url into S3 Target Bucket
        :param json:
        :return:
        """
        self.s3_target.upload_file(json, self.json_key)

    def run(self):
        """
        Runs each stage of the ETL pipeline
        :return: None
        """
        response = self.extract()
        self.load(response)
        return None
