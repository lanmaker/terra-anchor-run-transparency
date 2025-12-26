import json
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from src.config import RAW_DIR

SQL_JOBS = {
    "anchor_deposits_hourly.csv": "src/sql/anchor_deposits.sql",
    "anchor_redeems_hourly.csv": "src/sql/anchor_redeems.sql",
    "wallet_activity.csv": "src/sql/wallet_activity.sql",
}

DEFAULT_API_URL = "https://api-v2.flipsidecrypto.xyz/json-rpc"
DEFAULT_TTL = 60
DEFAULT_MAX_AGE = 60
DEFAULT_POLL = 10
DEFAULT_PAGE_SIZE = 50000
DEFAULT_CREATE_METHOD = "createQueryRun"
DEFAULT_STATUS_METHOD = "getQueryRun"
DEFAULT_RESULTS_METHOD = "getQueryRunResults"


class FlipsideClient:
    def __init__(self) -> None:
        load_dotenv()
        self.api_key = os.environ.get("FLIPSIDE_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing FLIPSIDE_API_KEY in environment or .env")
        self.api_url = os.environ.get("FLIPSIDE_API_URL", DEFAULT_API_URL)
        self.ttl_minutes = int(os.environ.get("FLIPSIDE_TTL_MINUTES", DEFAULT_TTL))
        self.max_age_minutes = int(os.environ.get("FLIPSIDE_MAX_AGE_MINUTES", DEFAULT_MAX_AGE))
        self.poll_seconds = int(os.environ.get("FLIPSIDE_POLL_SECONDS", DEFAULT_POLL))
        self.page_size = int(os.environ.get("FLIPSIDE_PAGE_SIZE", DEFAULT_PAGE_SIZE))
        self.create_method = os.environ.get("FLIPSIDE_CREATE_METHOD", DEFAULT_CREATE_METHOD)
        self.status_method = os.environ.get("FLIPSIDE_STATUS_METHOD", DEFAULT_STATUS_METHOD)
        self.results_method = os.environ.get("FLIPSIDE_RESULTS_METHOD", DEFAULT_RESULTS_METHOD)

    def _call(self, method: str, params: list[dict]) -> dict:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        headers = {
            "x-api-key": self.api_key,
            "content-type": "application/json",
        }
        resp = requests.post(self.api_url, headers=headers, data=json.dumps(payload), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"Flipside API error: {data['error']}")
        return data

    def submit_query(self, sql: str) -> str:
        params = [
            {
                "sql": sql,
                "ttlMinutes": self.ttl_minutes,
                "maxAgeMinutes": self.max_age_minutes,
            }
        ]
        data = self._call(self.create_method, params)
        result = data.get("result", {})
        query_run_id = result.get("queryRunId") or result.get("query_run_id")
        if not query_run_id:
            raise RuntimeError(f"No queryRunId in response: {data}")
        return query_run_id

    def wait_for_completion(self, query_run_id: str, timeout_minutes: int = 60) -> None:
        deadline = time.time() + timeout_minutes * 60
        while True:
            data = self._call(self.status_method, [{"queryRunId": query_run_id}])
            result = data.get("result", {})
            state = result.get("state") or result.get("status") or result.get("queryRunState")
            if state in {"QUERY_STATE_SUCCESS", "SUCCESS", "FINISHED"}:
                return
            if state in {"QUERY_STATE_FAILED", "FAILED", "ERROR"}:
                raise RuntimeError(f"Query failed: {result}")
            if time.time() > deadline:
                raise TimeoutError("Query timed out")
            time.sleep(self.poll_seconds)

    def fetch_results(self, query_run_id: str) -> pd.DataFrame:
        page = 1
        all_rows: list = []
        columns: list[str] | None = None

        while True:
            params = [
                {
                    "queryRunId": query_run_id,
                    "page": {"number": page, "size": self.page_size},
                }
            ]
            data = self._call(self.results_method, params)
            result = data.get("result", {})
            rows = result.get("rows") or result.get("records") or result.get("data") or []
            if columns is None:
                columns = result.get("columnNames") or result.get("columns")
            if not rows:
                break

            all_rows.extend(rows)
            if len(rows) < self.page_size:
                break
            page += 1

        if not all_rows:
            return pd.DataFrame()

        if isinstance(all_rows[0], dict):
            return pd.DataFrame(all_rows)
        if columns:
            return pd.DataFrame(all_rows, columns=columns)
        return pd.DataFrame(all_rows)


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    client = FlipsideClient()

    for output_name, sql_path in SQL_JOBS.items():
        sql_file = Path(sql_path)
        sql = sql_file.read_text()

        print(f"Submitting {sql_file}...")
        query_run_id = client.submit_query(sql)
        client.wait_for_completion(query_run_id)
        df = client.fetch_results(query_run_id)

        out_path = RAW_DIR / output_name
        df.to_csv(out_path, index=False)
        print(f"Saved {out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
