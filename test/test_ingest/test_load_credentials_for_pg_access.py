from src.ingest.function.utils.load_credentials_for_pg_access import (
    pg_access,
)
from unittest.mock import patch
import os
import pytest


class TestLoadCredentialsForPGAccess:
    def test_load_crededntials_success(self):
        env_vars = {
            "DB_HOST": "localhost",
            "DB_PORT": "1234",
            "DB": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_pass",
        }
        with patch.dict(os.environ, env_vars):
            result = pg_access()

        expected = ["localhost", "1234", "test_db", "test_user", "test_pass"]
        assert result == expected

    def test_load_crededntials_one_missing(self):
        env_vars = {
            "DB_HOST": "localhost",
            "DB_PORT": "1234",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_pass",
        }
        with pytest.raises(ValueError):
            with patch.dict(os.environ, env_vars):
                pg_access()

    def test_load_crededntials_all_missing(self):
        env_vars = {}
        with pytest.raises(ValueError):
            with patch.dict(os.environ, env_vars):
                pg_access()
