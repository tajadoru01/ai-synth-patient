import io
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

for mod_name in ("psycopg2", "psycopg2.extras", "dotenv"):
    sys.modules.setdefault(mod_name, MagicMock())
sys.modules["dotenv"].load_dotenv = lambda: None

sys.path.insert(0, "airflow/scripts")
import load_raw_data as loader 

def _write_csv(tmp_path: Path, filename: str, content: str) -> Path:
    p = tmp_path / filename
    p.write_text(content)
    return p

class TestLoadCsvToRaw:
    def _make_conn(self, existing_ids=None):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [(i,) for i in (existing_ids or [])]
        return conn, cur

    def test_inserts_all_rows_when_table_has_no_pk(self, tmp_path):
        """conditions / observations / medications have no PK dedup logic."""
        csv_path = _write_csv(tmp_path, "conditions.csv",
                              "CODE,DESCRIPTION\n001,Asthma\n002,Hypertension\n")
        conn, cur = self._make_conn()
        loader.load_csv_to_raw(csv_path, "raw.conditions", conn)
        cur.copy_expert.assert_called_once()

    def test_deduplicates_patients_by_id(self, tmp_path):
        """Rows whose id already exists in the DB must be dropped."""
        csv_path = _write_csv(tmp_path, "patients.csv",
                              "id,FIRST,LAST\naaa,Alice,Smith\nbbb,Bob,Jones\n")
        conn, cur = self._make_conn(existing_ids=["aaa"])
        loader.load_csv_to_raw(csv_path, "raw.patients", conn)
        cur.copy_expert.assert_called_once()
        buf_arg = cur.copy_expert.call_args[0][1]
        buf_arg.seek(0)
        content = buf_arg.read()
        assert "bbb" in content
        assert "aaa" not in content

    def test_returns_zero_when_all_rows_already_loaded(self, tmp_path):
        csv_path = _write_csv(tmp_path, "patients.csv",
                              "id,FIRST,LAST\naaa,Alice,Smith\n")
        conn, cur = self._make_conn(existing_ids=["aaa"])
        rows = loader.load_csv_to_raw(csv_path, "raw.patients", conn)
        cur.copy_expert.assert_not_called()
        assert rows == 0

    def test_column_names_are_lowercased(self, tmp_path):
        csv_path = _write_csv(tmp_path, "conditions.csv",
                              "CODE,DESCRIPTION\n001,Asthma\n")
        conn, cur = self._make_conn()
        loader.load_csv_to_raw(csv_path, "raw.conditions", conn)
        sql_arg = cur.copy_expert.call_args[0][0]
        assert "code" in sql_arg
        assert "description" in sql_arg
        assert "CODE" not in sql_arg

    def test_commits_after_successful_load(self, tmp_path):
        csv_path = _write_csv(tmp_path, "conditions.csv",
                              "CODE,DESCRIPTION\n001,Asthma\n")
        conn, _ = self._make_conn()
        loader.load_csv_to_raw(csv_path, "raw.conditions", conn)
        conn.commit.assert_called_once()

class TestRunLoad:
    def test_skips_missing_files_without_raising(self, tmp_path):
        """run_load should warn about missing files but not crash."""
        with patch.object(loader, "get_conn") as mock_get_conn, \
             patch.dict("os.environ", {"DATA_DIR": str(tmp_path)}):
            mock_conn = mock_get_conn.return_value
            result = loader.run_load()
        assert result == 0
        mock_conn.close.assert_called_once()

    def test_returns_total_rows_inserted(self, tmp_path):
        _write_csv(tmp_path, "conditions.csv", "CODE,DESCRIPTION\n001,Asthma\n002,COPD\n")

        with patch.object(loader, "get_conn") as mock_get_conn, \
             patch.object(loader, "load_csv_to_raw", return_value=2) as mock_load, \
             patch.dict("os.environ", {"DATA_DIR": str(tmp_path)}):

            total = loader.run_load()

        assert mock_load.call_count >= 1
        assert total >= 2

    def test_closes_connection_even_on_error(self, tmp_path):
        with patch.object(loader, "get_conn") as mock_get_conn, \
             patch.object(loader, "load_csv_to_raw", side_effect=RuntimeError("oops")), \
             patch.dict("os.environ", {"DATA_DIR": str(tmp_path)}):

            _write_csv(tmp_path, "conditions.csv", "CODE\n001\n")
            mock_conn = mock_get_conn.return_value

            with pytest.raises(RuntimeError):
                loader.run_load()

        mock_conn.close.assert_called_once()