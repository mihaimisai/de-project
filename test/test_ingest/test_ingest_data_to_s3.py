from moto import mock_aws
import boto3
import pytest
from src.ingest.function.utils.ingest_data_to_s3 import (
    fetch_data,
    convert_to_csv,
    ingest_data_to_s3,
)
import datetime
import logging
from unittest.mock import patch, MagicMock
import pandas as pd
from testfixtures import LogCapture


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-2")

        yield client


@pytest.fixture
def db(postgresql):
    connection = postgresql
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE counterparty
        (id serial PRIMARY KEY,
        name varchar,
        last_updated timestamp);"""
    )
    cursor.execute(
        """
        INSERT INTO counterparty
        (name, last_updated)
        VALUES
        ('test1', '2024-02-13'),
        ('test2', '2025-01-14')"""
    )
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture
def mock_logger():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    return logger


class TestIngestDataToS3:

    def test_fetch_data_no_time_stamp(self, db, mock_logger):

        table_name = "counterparty"

        expected_data = [
            (1, "test1", datetime.datetime(2024, 2, 13, 0, 0)),
            (2, "test2", datetime.datetime(2025, 1, 14, 0, 0)),
        ]
        expected_df = pd.DataFrame(
            expected_data, columns=["id", "name", "last_updated"]
        )

        result_df = fetch_data(db, table_name, None, mock_logger)

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_fetch_data_with_time_stamp(self, db, mock_logger):

        table_name = "counterparty"

        expected_data = [(2, "test2", datetime.datetime(2025, 1, 14, 0, 0))]
        expected_df = pd.DataFrame(
            expected_data, columns=["id", "name", "last_updated"]
        )

        result_df = fetch_data(db, table_name, "2025-01-01", mock_logger)

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_fetch_data_throws_error(self, db):
        table_name = "counterparty"
        with LogCapture(level=logging.INFO) as logstream:
            logstream.clear()
            try:
                fetch_data(db, table_name, "nonsense")
            except Exception:
                print("Exception in test func")
        for log in logstream:
            assert log == ("test_logger", "ERROR", "Error fetching data: ")

    def test_convert_to_csv(self):
        data = {"staff_id": [1, 2, 3], "first_name": ["Mihai", "Shea", "Anna"]}
        df = pd.DataFrame(data)

        result = convert_to_csv(df)
        result_str = result.decode("utf-8")

        expected_csv = "staff_id,first_name\n1,Mihai\n2,Shea\n3,Anna\n"

        assert result_str == expected_csv

    @patch(
        "src.ingest.function.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch(
        "src.ingest.function.utils.connect_to_db.Connection",
        return_value=MagicMock(),
    )
    def test_ingest_data_to_s3_time_stamp_called(self, s3_client, mock_logger):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"
        table_name = "counterparty"

        with patch(
            "src.ingest.function.utils.ingest_data_to_s3.timestamp_data_retrival"  # noqa: E501
        ) as mock_timestamp:

            ingest_data_to_s3(
                s3_client, mock_logger, table_name, s3_ingestion, s3_timestamp
            )
            mock_timestamp.assert_called_once

    @patch(
        "src.ingest.function.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch(
        "src.ingest.function.utils.connect_to_db.Connection",
        return_value=MagicMock(),
    )
    def test_ingest_data_to_s3_fetch_data_called(self, mock_logger, s3_client):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"
        table_name = "counterparty"

        with patch(
            "src.ingest.function.utils.ingest_data_to_s3.fetch_data"
        ) as mock_fetch_data:

            ingest_data_to_s3(
                s3_client, mock_logger, table_name, s3_ingestion, s3_timestamp
            )

            mock_fetch_data.assert_called_once()

    @patch(
        "src.ingest.function.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch(
        "src.ingest.function.utils.connect_to_db.Connection",
        return_value=MagicMock(),
    )
    def test_ingest_data_to_s3_convert_called(self, mock_logger, s3_client):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"
        table_name = "counterparty"

        with patch(
            "src.ingest.function.utils.ingest_data_to_s3.convert_to_csv"
        ) as mock_convert:

            ingest_data_to_s3(
                s3_client, mock_logger, table_name, s3_ingestion, s3_timestamp
            )

            mock_convert.assert_called_once()

    @patch(
        "src.ingest.function.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch(
        "src.ingest.function.utils.connect_to_db.Connection",
        return_value=MagicMock(),
    )
    def test_ingest_data_to_s3_data_upload_called(
        self, mock_logger, s3_client
    ):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"

        with patch(
            "src.ingest.function.utils.ingest_data_to_s3.s3_data_upload"
        ) as mock_upload:

            table_name = "counterparty"

            ingest_data_to_s3(
                s3_client, mock_logger, table_name, s3_ingestion, s3_timestamp
            )

            mock_upload.assert_called_once()

    @patch(
        "src.ingest.function.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch(
        "src.ingest.function.utils.connect_to_db.Connection",
        return_value=MagicMock(),
    )
    def test_ingest_data_to_s3_upload_time_stamp_called(
        self, mock_logger, s3_client
    ):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"

        with patch(
            "src.ingest.function.utils.ingest_data_to_s3.upload_time_stamp"
        ) as mock_upload:

            table_name = "counterparty"

            ingest_data_to_s3(
                s3_client, mock_logger, table_name, s3_ingestion, s3_timestamp
            )

            mock_upload.assert_called_once()

    @patch(
        "src.ingest.function.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch(
        "src.ingest.function.utils.connect_to_db.Connection",
        return_value=MagicMock(),
    )
    def test_ingest_data_to_s3_success_log(self, conn, mock_logger):
        table_name = "counterparty"
        with LogCapture(level=logging.INFO) as logstream:
            logstream.clear()
            try:
                fetch_data(conn, table_name, "2024-01-01", mock_logger)
            except Exception:
                print("Exception in test func")
        for log in logstream:
            assert log == (
                "test_logger",
                "INFO",
                f"Successfully fetched data from table: {table_name}",
            )

    def test_ingest_data_to_s3_check_exception_raised(self, mock_logger):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"
        table_name = "counterparty"

        with LogCapture(level=logging.INFO) as logstream:
            logstream.clear()
            try:
                ingest_data_to_s3(
                    s3_client,
                    mock_logger,
                    table_name,
                    s3_ingestion,
                    s3_timestamp,
                )
            except Exception:
                print("Exception in test func")

        assert logstream[0] == (
            "test_logger",
            "ERROR",
            "Connection failed: One or more PostgreSQL credentials are missing.",  # noqa 501
        )
        assert logstream[1] == (
            "test_logger",
            "ERROR",
            """Error connecting to PostgreSQL or executing query for table 'counterparty': One or more PostgreSQL credentials are missing.""",  # noqa 501
        )
