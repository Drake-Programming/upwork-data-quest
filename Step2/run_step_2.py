from etl import ETL
from api_source import APISource
from s3_target import S3Target


def main() -> None:
    """
    Runs the ETL job
    :return: None
    """
    api_source = APISource(
        "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    )
    s3_target = S3Target("", "", "")
    etl = ETL(api_source, s3_target)
    etl.run()
    return None


if __name__ == "__main__":
    main()
