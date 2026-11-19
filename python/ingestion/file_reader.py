
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd


def read_csv(file_path):
    """Read a CSV file into a DataFrame."""
    return pd.read_csv(file_path, dtype=str)


def read_excel(file_path):
    """Read an Excel file into a DataFrame."""
    return pd.read_excel(file_path, dtype=str)


def read_json(file_path):
    """Read nested JSON redemption transactions."""
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def read_xml(file_path):
    """Read XML member profiles into a DataFrame."""
    tree = ET.parse(file_path)
    root = tree.getroot()

    records = []

    for member in root.findall(".//member"):
        record = {
            child.tag: child.text
            for child in member
        }
        records.append(record)

    return pd.DataFrame(records)


def read_file(file_path, file_format=None):
    """
    Read a file based on its format.
    Returns a DataFrame for tabular formats and a dictionary/list for JSON.
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    file_format = (file_format or path.suffix.lstrip(".")).upper()

    readers = {
        "CSV": read_csv,
        "XLSX": read_excel,
        "JSON": read_json,
        "XML": read_xml
    }

    if file_format not in readers:
        raise ValueError(f"Unsupported file format: {file_format}")

    return readers[file_format](path)
