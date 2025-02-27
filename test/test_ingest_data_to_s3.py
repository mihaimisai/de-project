from moto import mock_aws
import boto3
import pytest
from src.utils.ingest_data_to_s3 import fetch_data, convert_to_csv, ingest_data_to_s3
import datetime
import logging
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import os


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
        CREATE TABLE test_table 
        (id serial PRIMARY KEY, 
        name varchar,
        last_updated timestamp);"""
    )
    cursor.execute(
        """
        INSERT INTO test_table
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
    logger = Mock()
    return logger


class TestIngestDataToS3:

    def test_fetch_data_no_time_stamp(self, db):

        table_name = "test_table"

        expected_data = [
            (1, "test1", datetime.datetime(2024, 2, 13, 0, 0)),
            (2, "test2", datetime.datetime(2025, 1, 14, 0, 0)),
        ]
        expected_df = pd.DataFrame(
            expected_data, columns=["id", "name", "last_updated"]
        )

        result_df = fetch_data(db, table_name, None, mock_logger)

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_fetch_data_with_time_stamp(self, db):

        table_name = "test_table"

        expected_data = [(2, "test2", datetime.datetime(2025, 1, 14, 0, 0))]
        expected_df = pd.DataFrame(
            expected_data, columns=["id", "name", "last_updated"]
        )

        result_df = fetch_data(db, table_name, "2025-01-01", mock_logger)

        pd.testing.assert_frame_equal(result_df, expected_df)

    # def test_fetch_data_throws_error(self, db, logger):

    #     table_name = 'test_table'

    #     result = fetch_data(db, table_name, 'nonsense')

    #     assert result == []

    #     logger.error.assert_called_with("Expected error message")

    def test_convert_to_csv(self):
        data = {"staff_id": [1, 2, 3], "first_name": ["Mihai", "Shea", "Anna"]}
        df = pd.DataFrame(data)

        result = convert_to_csv(df)
        result_str = result.decode("utf-8")

        expected_csv = "staff_id,first_name\n1,Mihai\n2,Shea\n3,Anna\n"

        assert result_str == expected_csv

    @patch(
        "src.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch("src.utils.connect_to_db.Connection", return_value=MagicMock())
    def test_ingest_data_to_s3_time_stamp_called(self, s3_client, mock_logger):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"
        table_name = "test_table"

        with patch(
            "src.utils.ingest_data_to_s3.timestamp_data_retrival"
        ) as mock_timestamp:
            mock_timestamp.return_value = "2024-01-01"

            ingest_data_to_s3(
                s3_client, mock_logger, table_name, s3_ingestion, s3_timestamp
            )
            mock_timestamp.assert_called_once


    @patch(
        "src.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch("src.utils.connect_to_db.Connection", return_value=MagicMock())
    def test_ingest_data_to_s3_fetch_data_called(self, mock_logger, s3_client):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"
        table_name = "test_table"

        with patch("src.utils.ingest_data_to_s3.fetch_data") as mock_fetch_data:
            mock_fetch_data.return_value = pd.DataFrame(
                [
                (1, "test1", datetime.datetime(2024, 2, 13, 0, 0)),
                (2, "test2", datetime.datetime(2025, 1, 14, 0, 0)),
            ],
                columns=["id", "name", "last_updated"],
            )

            ingest_data_to_s3(
                s3_client, mock_logger, table_name, s3_ingestion, s3_timestamp
            )
            
            mock_fetch_data.assert_called_once()

    @patch(
        "src.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch("src.utils.connect_to_db.Connection", return_value=MagicMock())
    def test_ingest_data_to_s3_convert_called(self, mock_logger, s3_client):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"
        
        with patch("src.utils.ingest_data_to_s3.convert_to_csv") as mock_convert:
            
            table_name = 'test_table'
            
            mock_convert.return_value = "staff_id,first_name\n1,Mihai\n2,Shea\n3,Anna\n"
            
            ingest_data_to_s3(
                s3_client, mock_logger, table_name, s3_ingestion, s3_timestamp
            )
            
            mock_convert.assert_called_once()
            
    @patch(
        "src.utils.connect_to_db.pg_access",
        return_value=("localhost", 1234, "test_db", "user", "password"),
    )
    @patch("src.utils.connect_to_db.Connection", return_value=MagicMock())
    def test_ingest_data_to_s3_data_upload_called(self, mock_logger, s3_client):
        s3_ingestion = "ingestion"
        s3_timestamp = "timestamp"
        
        with patch("src.utils.s3_data_upload.s3_data_upload") as mock_upload:
            
            table_name = 'test_table'
            
            # mock_upload.return_value = "staff_id,first_name\n1,Mihai\n2,Shea\n3,Anna\n"
            
            ingest_data_to_s3(
                s3_client, mock_logger, table_name, s3_ingestion, s3_timestamp
            )
            
            mock_upload.assert_called_once()
    

    # test upload is called
    # test upload time is called
    # test succes fetched data
    # test any error is logged
