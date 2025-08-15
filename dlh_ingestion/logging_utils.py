import sys
import logging
import datetime
from dataclasses import dataclass
from typing import List


@dataclass
class LogEntry:
    ts: str
    level: str
    message: str

    def format(self) -> str:
        return f"{self.ts} - {self.level} - {self.message}"


class CustomLogger:
    """Refactored logger (backwards compatible interface).

    Keeps in-memory log_stream for later persistence.
    """

    def __init__(self, spark, s3_bucket: str, s3_prefix: str, app_name: str):
        self.spark = spark
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.app_name = app_name
        self.log_stream: List[str] = []
        self.today = datetime.datetime.now().strftime("%Y%m%d")

        self.logger = logging.getLogger(app_name)
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def log(self, message: str, level: str = "INFO"):
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        log_level = level_map.get(level.upper(), logging.INFO)
        self.logger.log(log_level, message)
        self.log_stream.append(
            f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {level.upper()} - {message}"
        )

    def get_log_content(self) -> str:
        return "\n".join(self.log_stream)

    def save_to_s3(self, log_reference: str):  # mimic legacy behavior
        try:
            s3_path = f"{self.s3_bucket}{log_reference}{self.today}"
            self.log(f"s3 path for log loading... {s3_path}", "INFO")
            log_rdd = self.spark.sparkContext.parallelize([self.get_log_content()])
            log_df = self.spark.createDataFrame(log_rdd, "string")
            (
                log_df.coalesce(1)
                .write.format("text")
                .mode("append")
                .option("header", "false")
                .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                .option("compression", "none")
                .save(s3_path)
            )
            self.log("Logs successfully saved to s3 path", "INFO")
            return True
        except Exception as e:  # pragma: no cover - defensive
            self.log(f"Failed to save logs to S3 bucket: {str(e)}", "ERROR")
            return False

    def __enter__(self):  # context manager support
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover simple flush
        try:
            # Attempt to flush logs to default reference path if available
            self.save_to_s3(self.s3_prefix)
        except Exception:
            pass
        # Do not suppress exceptions
        return False
