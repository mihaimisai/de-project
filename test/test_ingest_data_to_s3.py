from moto import mock_aws
import boto3
import pytest
from src.utils.ingest_data_to_s3 import fetch_data, convert_to_csv
import datetime
import logging
from unittest.mock import Mock
import pandas as pd

@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client('s3', region_name='eu-west-2')
        client.create_bucket(Bucket='ingestion')
        client.create_bucket(Bucket='timestamp')
    return client
        
@pytest.fixture
def db(postgresql):
    connection = postgresql
    cursor = connection.cursor() 
    cursor.execute("""
        CREATE TABLE test_table 
        (id serial PRIMARY KEY, 
        name varchar,
        last_updated timestamp);""") 
    cursor.execute("""
        INSERT INTO test_table
        (name, last_updated)
        VALUES
        ('test1', '2024-02-13'),
        ('test2', '2025-01-14')""")
    connection.commit()
    yield connection
    connection.close()
    
@pytest.fixture
def logger():
    logger = Mock()
    return logger


class TestIngestDataToS3:

    def test_fetch_data_no_time_stamp(self, db):
        
        table_name = 'test_table'
        
        expected = [
            (1, "test1", datetime.datetime(2024, 2, 13, 0, 0)),
            (2, "test2", datetime.datetime(2025, 1, 14, 0, 0))
        ]
        
        result = fetch_data(db, table_name, None, logger)
        
        assert result == expected

    def test_fetch_data_with_time_stamp(self, db):
        
        table_name = 'test_table'
        
        expected = [
            (2, "test2", datetime.datetime(2025, 1, 14, 0, 0))
        ]
        
        result = fetch_data(db, table_name, '2025-01-01', logger)

        assert result == expected

    def test_fetch_data_throws_error(self, db):
        
        table_name = 'test_table'
        
        result = fetch_data(db, table_name, 'nonsense', logger)
        
        assert result == []
        
        logger.error.assert_called_with("Expected error message")
        
    def test_convert_to_csv(self):
 
        data = {
            'staff_id': [1, 2, 3],
            'first_name': ['Mihai', 'Shea', 'Anna']
        }
        
        df = pd.DataFrame(data)

        result = convert_to_csv(df)

        result_str = result.decode("utf-8")
        
        expected_csv = "staff_id,first_name\n1,Mihai\n2,Shea\n3,Anna\n"

        assert result_str == expected_csv
        
        