import json
import sys
import types
from unittest.mock import MagicMock, patch, call

import pytest

for mod_name in ("psycopg2", "psycopg2.extras", "openai", "dotenv"):
    sys.modules.setdefault(mod_name, MagicMock())

sys.modules["dotenv"].load_dotenv = lambda: None
sys.modules["psycopg2"].extras = sys.modules["psycopg2.extras"]
sys.path.insert(0, "airflow/scripts")

import ai_triage_agent as agent  # noqa: E402

@pytest.fixture()
def sample_patient():
    return {
        "patient_id": "pt-001",
        "full_name": "Jane Doe",
        "age": 72,
        "gender": "F",
        "active_conditions": "Hypertension; Diabetes mellitus type 2; Heart failure",
        "recent_encounter_type": "inpatient",
        "days_since_last_encounter": 14,
        "current_medications": "Metformin 500 MG; Lisinopril 10 MG",
        "latest_systolic_bp": 165,
        "latest_diastolic_bp": 95,
        "latest_bmi": 32.1,
        "latest_glucose": 210,
    }

@pytest.fixture()
def minimal_patient():
    """Patient with no optional vitals/meds — exercises None-handling branches."""
    return {
        "patient_id": "pt-002",
        "full_name": None,
        "age": None,
        "gender": None,
        "active_conditions": None,
        "recent_encounter_type": None,
        "days_since_last_encounter": None,
        "current_medications": None,
        "latest_systolic_bp": None,
        "latest_diastolic_bp": None,
        "latest_bmi": None,
        "latest_glucose": None,
    }

def _make_openai_response(payload: dict, model: str = "gpt-4o-mini",
                          prompt_tokens: int = 50, completion_tokens: int = 30):
    """Build a minimal mock that mirrors openai.ChatCompletion structure."""
    resp = MagicMock()
    resp.choices[0].message.content = json.dumps(payload)
    resp.model = model
    resp.usage.prompt_tokens = prompt_tokens
    resp.usage.completion_tokens = completion_tokens
    return resp

class TestBuildUserPrompt:
    def test_includes_patient_name(self, sample_patient):
        prompt = agent.build_user_prompt(sample_patient)
        assert "Jane Doe" in prompt

    def test_includes_vitals(self, sample_patient):
        prompt = agent.build_user_prompt(sample_patient)
        assert "165" in prompt   # systolic BP
        assert "32.1" in prompt  # BMI

    def test_none_medications_replaced(self, minimal_patient):
        prompt = agent.build_user_prompt(minimal_patient)
        assert "None documented" in prompt

    def test_none_conditions_replaced(self, minimal_patient):
        prompt = agent.build_user_prompt(minimal_patient)
        # conditions field also falls back
        assert prompt.count("None documented") >= 1

    def test_missing_keys_do_not_raise(self):
        """build_user_prompt must not KeyError on a sparse dict."""
        prompt = agent.build_user_prompt({"patient_id": "x"})
        assert isinstance(prompt, str)

    def test_prompt_asks_for_triage_assessment(self, sample_patient):
        prompt = agent.build_user_prompt(sample_patient)
        assert "triage" in prompt.lower()

class TestAlreadyProcessedToday:
    def _mock_conn(self, has_row: bool):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (1,) if has_row else None
        return conn

    def test_returns_true_when_row_exists(self):
        conn = self._mock_conn(has_row=True)
        assert agent.already_processed_today(conn, "pt-001") is True

    def test_returns_false_when_no_row(self):
        conn = self._mock_conn(has_row=False)
        assert agent.already_processed_today(conn, "pt-001") is False

    def test_queries_correct_patient_id(self):
        conn = self._mock_conn(has_row=False)
        agent.already_processed_today(conn, "pt-XYZ")
        cursor = conn.cursor.return_value.__enter__.return_value
        args = cursor.execute.call_args[0]
        assert "pt-XYZ" in args[1]

class TestWriteSummary:
    def test_inserts_correct_values(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value

        result = {
            "risk_score": 4,
            "risk_label": "VERY HIGH",
            "summary": "Patient has complex multi-morbidity.",
        }
        agent.write_summary(conn, "pt-001", result, "gpt-4o-mini", 60, 40)

        execute_call = cursor.execute.call_args
        params = execute_call[0][1]
        assert params[0] == "pt-001"
        assert params[1] == 4
        assert params[2] == "VERY HIGH"
        assert params[3] == "Patient has complex multi-morbidity."
        assert params[4] == "gpt-4o-mini"
        assert params[5] == 60
        assert params[6] == 40

    def test_commits_transaction(self):
        conn = MagicMock()
        result = {"risk_score": 1, "risk_label": "LOW", "summary": "Stable."}
        agent.write_summary(conn, "pt-001", result, "gpt-4o-mini", 10, 5)
        conn.commit.assert_called_once()

class TestRunTriageAgent:
    def _run(self, patients, openai_response, already_done=False):
        """Helper: patch DB, env vars, and OpenAI, then call run_triage_agent."""
        env_vars = {"OPENAI_API_KEY": "sk-test-fake"}
        with patch.dict("os.environ", env_vars), \
             patch.object(agent, "get_conn") as mock_get_conn, \
             patch.object(agent, "fetch_high_risk_patients", return_value=patients), \
             patch.object(agent, "already_processed_today", return_value=already_done), \
             patch.object(agent, "write_summary") as mock_write, \
             patch("ai_triage_agent.OpenAI") as mock_openai_cls:

            mock_client = MagicMock()
            mock_openai_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = openai_response

            result = agent.run_triage_agent()
            return result, mock_write, mock_client

    def test_processes_new_patient(self, sample_patient):
        llm_resp = _make_openai_response(
            {"risk_score": 4, "risk_label": "VERY HIGH", "summary": "Complex case."})
        summary, mock_write, _ = self._run([sample_patient], llm_resp)
        mock_write.assert_called_once()
        assert "Processed=1" in summary

    def test_skips_already_processed_patient(self, sample_patient):
        llm_resp = _make_openai_response(
            {"risk_score": 1, "risk_label": "LOW", "summary": "Stable."})
        summary, mock_write, _ = self._run([sample_patient], llm_resp, already_done=True)
        mock_write.assert_not_called()
        assert "Skipped=1" in summary

    def test_handles_json_decode_error(self, sample_patient):
        """If LLM returns invalid JSON, the patient is counted as an error."""
        bad_resp = MagicMock()
        bad_resp.choices[0].message.content = "not valid json {{{"
        bad_resp.model = "gpt-4o-mini"
        bad_resp.usage.prompt_tokens = 10
        bad_resp.usage.completion_tokens = 5

        summary, mock_write, _ = self._run([sample_patient], bad_resp)
        mock_write.assert_not_called()
        assert "Errors=1" in summary

    def test_handles_unexpected_exception(self, sample_patient):
        """If write_summary raises, the agent logs an error and continues."""
        llm_resp = _make_openai_response(
            {"risk_score": 2, "risk_label": "MODERATE", "summary": "Monitor."})

        with patch.dict("os.environ", {"OPENAI_API_KEY": "sk-test-fake"}), \
             patch.object(agent, "get_conn"), \
             patch.object(agent, "fetch_high_risk_patients", return_value=[sample_patient]), \
             patch.object(agent, "already_processed_today", return_value=False), \
             patch.object(agent, "write_summary", side_effect=RuntimeError("DB down")), \
             patch("ai_triage_agent.OpenAI") as mock_openai_cls:

            mock_client = MagicMock()
            mock_openai_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = llm_resp

            result = agent.run_triage_agent()
        assert "Errors=1" in result

    def test_closes_connection_on_success(self, sample_patient):
        llm_resp = _make_openai_response(
            {"risk_score": 1, "risk_label": "LOW", "summary": "Stable."})

        with patch.dict("os.environ", {"OPENAI_API_KEY": "sk-test-fake"}), \
             patch.object(agent, "get_conn") as mock_get_conn, \
             patch.object(agent, "fetch_high_risk_patients", return_value=[sample_patient]), \
             patch.object(agent, "already_processed_today", return_value=False), \
             patch.object(agent, "write_summary"), \
             patch("ai_triage_agent.OpenAI") as mock_openai_cls:

            mock_client = MagicMock()
            mock_openai_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = llm_resp
            mock_conn = mock_get_conn.return_value
            agent.run_triage_agent()

        mock_conn.close.assert_called_once()

    def test_empty_patient_list(self):
        """No patients → no LLM calls, summary shows zeroes."""
        llm_resp = _make_openai_response({"risk_score": 1, "risk_label": "LOW", "summary": "."})
        summary, mock_write, mock_client = self._run([], llm_resp)
        mock_client.chat.completions.create.assert_not_called()
        assert "Processed=0" in summary