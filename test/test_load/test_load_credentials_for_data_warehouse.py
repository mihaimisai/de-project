from src.load.function.utils.load_credentials_for_data_warehouse import (
    dw_access,
)  # noqa
from unittest.mock import patch
import os
import pytest


class TestLoadCredentialsForDataWarehouse:
    def test_load_crededntials_success(self):
        env_vars = {
            "DB_HOST_DW": "localhost",
            "DB_PORT_DW": "1234",
            "DB_DW": "test_db",
            "DB_USER_DW": "test_user",
            "DB_PASSWORD_DW": "test_pass",
        }
        with patch.dict(os.environ, env_vars):
            result = dw_access()

        expected = ["localhost", "1234", "test_db", "test_user", "test_pass"]
        assert result == expected

    def test_load_crededntials_one_missing(self):
        env_vars = {
            "DB_HOST_DW": "localhost",
            "DB_PORT_DW": "1234",
            "DB_USER_DW": "test_user",
            "DB_PASSWORD_DW": "test_pass",
        }
        with pytest.raises(ValueError):
            with patch.dict(os.environ, env_vars):
                dw_access()

    def test_load_crededntials_all_missing(self):
        env_vars = {}
        with pytest.raises(ValueError):
            with patch.dict(os.environ, env_vars):
                dw_access()
