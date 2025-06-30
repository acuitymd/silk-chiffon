"""
silk_chiffon - Fast Arrow format conversions

This package provides high-performance conversions between Apache Arrow IPC,
Parquet, and DuckDB formats.
"""

from typing import List, Optional, Tuple, Union, Literal, Dict
from silk_chiffon._silk_chiffon import arrow_to_arrow, arrow_to_parquet, arrow_to_duckdb

__version__ = "0.1.0"
__all__ = ["arrow_to_arrow", "arrow_to_parquet", "arrow_to_duckdb"]

# Re-export the functions with better docstrings
_original_arrow_to_parquet = arrow_to_parquet
_original_arrow_to_duckdb = arrow_to_duckdb
_original_arrow_to_arrow = arrow_to_arrow


def arrow_to_parquet(
    input_path: str,
    output_path: str,
    *,
    sort_by: Optional[List[Union[str, Tuple[str, Literal["asc", "desc"]]]]] = None,
    compression: Literal["zstd", "snappy", "gzip", "lz4", "none"] = "none",
    write_sorted_metadata: bool = False,
    bloom_filter_all: Optional[Union[bool, float, Dict[str, float]]] = None,
    bloom_filter_columns: Optional[List[Union[str, Dict[str, float]]]] = None,
    max_row_group_size: int = 1_048_576,
    statistics: Literal["none", "chunk", "page"] = "page",
    record_batch_size: int = 122_880,
    enable_dictionary: bool = True,
    writer_version: Literal["v1", "v2"] = "v2",
) -> None:
    """
    Convert Arrow IPC format to Parquet format.

    Args:
        input_path: Path to input Arrow IPC file (stream or file format)
        output_path: Path to output Parquet file
        sort_by: Sort columns - can be column names (ascending by default) or tuples of (column, direction).
                 Examples: ["id"], [("name", "desc")], ["id", ("timestamp", "asc")]
        compression: Compression algorithm to use
        write_sorted_metadata: Whether to write sort metadata (requires sort_by)
        bloom_filter_all: Enable bloom filters for all columns.
                         - True: Use default FPP (0.01)
                         - float: Use specified FPP
                         - dict: {"fpp": 0.001}
        bloom_filter_columns: Enable bloom filters for specific columns.
                             - List of column names (default FPP)
                             - List of dicts: [{"column": "id", "fpp": 0.001}]
        max_row_group_size: Maximum number of rows per row group
        statistics: Level of statistics to compute
        record_batch_size: Size of Arrow record batches
        enable_dictionary: Whether to use dictionary encoding
        writer_version: Parquet writer version

    Raises:
        IOError: If input/output files cannot be accessed
        ValueError: If parameters are invalid
        RuntimeError: If conversion fails

    Examples:
        >>> # Simple conversion
        >>> arrow_to_parquet("input.arrow", "output.parquet")

        >>> # With compression and sorting
        >>> arrow_to_parquet(
        ...     "input.arrow",
        ...     "output.parquet",
        ...     sort_by=["timestamp", ("user_id", "desc")],
        ...     compression="zstd"
        ... )

        >>> # With bloom filters
        >>> arrow_to_parquet(
        ...     "input.arrow",
        ...     "output.parquet",
        ...     bloom_filter_all=0.001,  # Custom FPP for all columns
        ...     compression="snappy"
        ... )
    """
    return _original_arrow_to_parquet(
        input_path,
        output_path,
        sort_by=sort_by,
        compression=compression,
        write_sorted_metadata=write_sorted_metadata,
        bloom_filter_all=bloom_filter_all,
        bloom_filter_columns=bloom_filter_columns,
        max_row_group_size=max_row_group_size,
        statistics=statistics,
        record_batch_size=record_batch_size,
        enable_dictionary=enable_dictionary,
        writer_version=writer_version,
    )


def arrow_to_duckdb(
    input_path: str,
    output_path: str,
    table_name: str,
    *,
    sort_by: Optional[List[Union[str, Tuple[str, Literal["asc", "desc"]]]]] = None,
    truncate: bool = False,
    drop_table: bool = False,
) -> None:
    """
    Convert Arrow IPC format to DuckDB database.

    Args:
        input_path: Path to input Arrow IPC file
        output_path: Path to DuckDB database file
        table_name: Name of the table to create in DuckDB
        sort_by: Sort columns before writing (see arrow_to_parquet for format)
        truncate: Remove entire database file before writing
        drop_table: Drop the table if it exists (error if exists and this is False)

    Raises:
        IOError: If input/output files cannot be accessed
        ValueError: If parameters are invalid
        RuntimeError: If conversion fails or table already exists

    Examples:
        >>> # Create new table in new database
        >>> arrow_to_duckdb("data.arrow", "analytics.db", "sales_data")

        >>> # Add table to existing database
        >>> arrow_to_duckdb("users.arrow", "analytics.db", "users")

        >>> # Replace existing table
        >>> arrow_to_duckdb(
        ...     "updated_data.arrow",
        ...     "analytics.db",
        ...     "sales_data",
        ...     drop_table=True
        ... )
    """
    return _original_arrow_to_duckdb(
        input_path,
        output_path,
        table_name,
        sort_by=sort_by,
        truncate=truncate,
        drop_table=drop_table,
    )


def arrow_to_arrow(
    input_path: str,
    output_path: str,
    *,
    sort_by: Optional[List[Union[str, Tuple[str, Literal["asc", "desc"]]]]] = None,
    compression: Literal["zstd", "lz4", "none"] = "none",
    record_batch_size: int = 122_880,
) -> None:
    """
    Convert between Arrow IPC stream and file formats.

    Args:
        input_path: Path to input Arrow IPC file
        output_path: Path to output Arrow IPC file
        sort_by: Sort columns before writing (see arrow_to_parquet for format)
        compression: Compression algorithm to use
        record_batch_size: Size of Arrow record batches

    Raises:
        IOError: If input/output files cannot be accessed
        ValueError: If parameters are invalid
        RuntimeError: If conversion fails

    Examples:
        >>> # Convert stream to file format
        >>> arrow_to_arrow("stream.arrows", "file.arrow")

        >>> # Sort and compress
        >>> arrow_to_arrow(
        ...     "input.arrow",
        ...     "output.arrow",
        ...     sort_by=["timestamp"],
        ...     compression="lz4"
        ... )
    """
    return _original_arrow_to_arrow(
        input_path,
        output_path,
        sort_by=sort_by,
        compression=compression,
        record_batch_size=record_batch_size,
    )
