import requests
import json


class APISource:
    """
    Connects to web api
    """

    def __init__(self, url: str):
        """
        Constructor for APISource
        :param url:
        """
        self.url = url

    def get_json(self) -> str:
        """
        Gets response from web url
        :return: json string
        """
        response = requests.get(self.url)
        json_string = self.response_to_json(response)

        return json_string

    def response_to_json(self, response) -> str:
        """
        Converts response from web url to json
        :param response:
        :return:
        """
        json_string = json.dumps(response.json())

        return json_string
