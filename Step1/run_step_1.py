from etl import ETL
from bls_source import BLS
from s3_target import S3Target


def main() -> None:
    """
    Runs the ETL job
    :return: None
    """
    bls_source = BLS()
    # Put in your aws bucket name,  account access, and secret key
    s3_target = S3Target("", "", "")
    etl = ETL(bls_source, s3_target)
    etl.run()
    return None


if __name__ == "__main__":
    main()
