import pandas as pd
import numpy as np
import pytest
from  unittest.mock import MagicMock
from src.load.function.utils.insert_dataframe_in_db import insert_dataframe_in_db

@pytest.fixture
def dummy_dataframe():
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 22, 28, 35]
    }
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def mock_conn():
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = MagicMock()
    return mock_conn

@pytest.fixture
def mock_logger():
    return MagicMock()

def test_insert_dataframe_success(mock_conn, mock_logger, dummy_dataframe):
    table_name = "test_table"
    df = dummy_dataframe
    insert_dataframe_in_db(mock_conn, table_name, df, mock_logger)
    assert mock_conn.cursor.return_value.execute.call_count == len(df)
    mock_conn.commit.assert_called_once()
    mock_logger.info.assert_called_once_with(f"Successfully inserted {len(df)} rows into {table_name}")

def test_insert_dataframe_failure(mock_conn, mock_logger, dummy_dataframe):
    table_name = "test_table"
    df = dummy_dataframe
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.execute.side_effect = Exception("Database error")
    
    with pytest.raises(Exception, match="Database error"):
        insert_dataframe_in_db(mock_conn, table_name, df, mock_logger)

    mock_conn.rollback.assert_called_once()
    mock_logger.error.assert_called_once_with(f"General Error inserting data into {table_name}: Database error")
