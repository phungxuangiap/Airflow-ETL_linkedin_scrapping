"""
Data validation utilities
"""
from typing import Any, Dict, List, Optional
import pyarrow as pa
from src.utils.logger import get_logger

logger = get_logger(__name__)


def validate_schema(data: pa.Table, expected_columns: List[str]) -> bool:
    """
    Validate that data contains expected columns

    Args:
        data: PyArrow table
        expected_columns: List of expected column names

    Returns:
        True if valid

    Raises:
        ValueError if validation fails
    """
    actual_columns = set(data.column_names)
    expected_columns_set = set(expected_columns)

    missing_columns = expected_columns_set - actual_columns
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    logger.info(f"Schema validation passed for {len(expected_columns)} columns")
    return True


def validate_not_empty(data: pa.Table) -> bool:
    """
    Validate that data is not empty

    Args:
        data: PyArrow table

    Returns:
        True if not empty

    Raises:
        ValueError if empty
    """
    if data.num_rows == 0:
        raise ValueError("Data is empty")

    logger.info(f"Data contains {data.num_rows} rows")
    return True


def validate_no_nulls(data: pa.Table, columns: List[str]) -> bool:
    """
    Validate that specified columns have no null values

    Args:
        data: PyArrow table
        columns: List of column names to check

    Returns:
        True if no nulls

    Raises:
        ValueError if nulls found
    """
    for col in columns:
        if col not in data.column_names:
            raise ValueError(f"Column {col} not found in data")

        null_count = pa.compute.sum(pa.compute.is_null(data[col])).as_py()
        if null_count > 0:
            raise ValueError(f"Column {col} contains {null_count} null values")

    logger.info(f"No null validation passed for columns: {columns}")
    return True


def validate_data_types(data: pa.Table, expected_types: Dict[str, pa.DataType]) -> bool:
    """
    Validate data types of columns

    Args:
        data: PyArrow table
        expected_types: Dict mapping column names to expected PyArrow types

    Returns:
        True if types match

    Raises:
        ValueError if types don't match
    """
    for col_name, expected_type in expected_types.items():
        if col_name not in data.column_names:
            raise ValueError(f"Column {col_name} not found")

        actual_type = data.schema.field(col_name).type
        if actual_type != expected_type:
            raise ValueError(
                f"Column {col_name} has type {actual_type}, expected {expected_type}"
            )

    logger.info(f"Data type validation passed for {len(expected_types)} columns")
    return True


def validate_unique(data: pa.Table, column: str) -> bool:
    """
    Validate that a column has unique values

    Args:
        data: PyArrow table
        column: Column name to check

    Returns:
        True if unique

    Raises:
        ValueError if duplicates found
    """
    if column not in data.column_names:
        raise ValueError(f"Column {column} not found")

    unique_count = len(pa.compute.unique(data[column]))
    total_count = data.num_rows

    if unique_count != total_count:
        duplicates = total_count - unique_count
        raise ValueError(f"Column {column} has {duplicates} duplicate values")

    logger.info(f"Uniqueness validation passed for column: {column}")
    return True
