import re
import string
from typing import Any


def sanitize_column_name(col_name: str, logger: Any):
    try:
        for char in string.punctuation:
            if char != "_":
                col_name = col_name.replace(char, " ")
        col_name = re.sub(r"\s+", " ", col_name).strip()
        col_name = re.sub(r"[^A-Za-z0-9_]", "_", col_name)
        col_name = col_name.replace(" ", "_")
        col_name = re.sub(r"_+", "_", col_name).strip("_")
        return col_name
    except Exception as e:  # pragma: no cover
        logger.log(f"Error sanitizing column name: {str(e)}", "ERROR")
        raise
