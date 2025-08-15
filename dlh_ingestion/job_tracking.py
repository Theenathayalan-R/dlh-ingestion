import datetime
from dataclasses import dataclass
from typing import Any


@dataclass
class JobTracker:
    spark: Any
    batch_id: str
    run_id: str
    spark_app_id: str
    catalog_name: str
    database: str
    job_status_tbl: str
    job_id: str
    table_name: str
    run_group: str
    logger: Any

    def __post_init__(self):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.batch_time = now
        self.start_time = now
        self.end_time = now

    def insert_initial_status(self):
        try:
            data = [{
                "run_id": self.run_id,
                "batch_id": self.batch_id,
                "spark_app_id": self.spark_app_id,
                "run_group": self.run_group,
                "dlh_layer": "staging",
                "job_id": self.job_id,
                "table_name": self.table_name,
                "schema": "",
                "column_count": 0,
                "status": "STARTED",
                "start_time": self.start_time,
                "end_time": None,
                "records_processed": 0,
                "records_rejected": 0,
                "error_message": "",
                "load_date": "",
                "comments": ""
            }]
            return self.spark.createDataFrame(data)
        except Exception as e:  # pragma: no cover - mirrors legacy
            self.logger.log(f"Error creating initial job status DataFrame: {str(e)}", "ERROR")
            raise

    def update_status(self, status, column_count, records_processed=None, error_message=None, load_date=None):
        try:
            end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if status in ["COMPLETED", "FAILED"] else None
            records_processed_str = records_processed if records_processed is not None else 0
            error_message_str = error_message.replace("'", "") if error_message else ""
            load_date_str = f"{load_date}" if load_date is not None else ""

            data = [{
                "run_id": self.run_id,
                "batch_id": self.batch_id,
                "spark_app_id": self.spark_app_id,
                "run_group": self.run_group,
                "dlh_layer": "staging",
                "job_id": self.job_id,
                "table_name": self.table_name,
                "schema": "",
                "column_count": column_count,
                "status": status,
                "start_time": self.start_time,
                "end_time": end_time,
                "records_processed": records_processed_str,
                "records_rejected": 0,
                "error_message": error_message_str or "",
                "load_date": load_date_str or "",
                "comments": ""
            }]
            return self.spark.createDataFrame(data)
        except Exception as e:  # pragma: no cover
            self.logger.log(f"Error creating job status update DataFrame: {str(e)}", "ERROR")
            raise
