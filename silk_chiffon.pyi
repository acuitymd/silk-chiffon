"""Type stubs for silk_chiffon Python bindings."""

from typing import Dict, List, Literal, Optional, Tuple, Union

SortDirection = Literal["ascending", "descending"]
SortColumn = Union[str, Tuple[str, SortDirection]]

def arrow_to_arrow(
    input_path: str,
    output_path: str,
    *,
    sort_by: Optional[List[SortColumn]] = None,
    compression: str = "none",
    record_batch_size: int = 122_880,
) -> None:
    """Convert Arrow IPC stream or file format to Arrow IPC file format with optional sorting and compression."""
    ...

def arrow_to_parquet(
    input_path: str,
    output_path: str,
    *,
    sort_by: Optional[List[SortColumn]] = None,
    compression: str = "none",
    write_sorted_metadata: bool = False,
    bloom_filter_all: Optional[Union[bool, float, Dict[str, float]]] = None,
    bloom_filter_columns: Optional[List[Union[str, Dict[str, float]]]] = None,
    max_row_group_size: int = 1_048_576,
    statistics: str = "page",
    record_batch_size: int = 122_880,
    enable_dictionary: bool = True,
    writer_version: str = "v2",
) -> None:
    """Convert Arrow IPC stream or file format to Parquet file format with advanced options."""
    ...

def arrow_to_duckdb(
    input_path: str,
    output_path: str,
    table_name: str,
    *,
    sort_by: Optional[List[SortColumn]] = None,
    truncate: bool = False,
    drop_table: bool = False,
) -> None:
    """Convert Arrow IPC stream or file format to DuckDB database."""
    ...

def split_to_arrow(
    input_path: str,
    output_template: str,
    split_column: str,
    *,
    sort_by: Optional[List[SortColumn]] = None,
    compression: str = "none",
    create_dirs: bool = True,
    overwrite: bool = False,
    record_batch_size: int = 122_880,
) -> None:
    """
    Split Arrow IPC file into multiple Arrow files based on column values.

    Args:
        input_path: Path to input Arrow IPC file
        output_template: Output file template with {value} placeholder
        split_column: Column name to split by
        sort_by: Optional list of columns to sort by (name or (name, direction))
        compression: Compression type (none/zstd/lz4)
        create_dirs: Create output directories if they don't exist
        overwrite: Overwrite existing output files
        record_batch_size: Number of rows per batch
    """
    ...

def split_to_parquet(
    input_path: str,
    output_template: str,
    split_column: str,
    *,
    sort_by: Optional[List[SortColumn]] = None,
    compression: str = "none",
    create_dirs: bool = True,
    overwrite: bool = False,
    record_batch_size: int = 122_880,
    write_sorted_metadata: bool = False,
    bloom_filter_all: Optional[Union[bool, float, Dict[str, float]]] = None,
    bloom_filter_columns: Optional[List[Union[str, Dict[str, float]]]] = None,
    max_row_group_size: int = 1_048_576,
    statistics: str = "page",
    enable_dictionary: bool = True,
    writer_version: str = "v2",
) -> None:
    """
    Split Arrow IPC file into multiple Parquet files based on column values.

    Args:
        input_path: Path to input Arrow IPC file
        output_template: Output file template with {value} placeholder
        split_column: Column name to split by
        sort_by: Optional list of columns to sort by (name or (name, direction))
        compression: Compression type (none/zstd/snappy/gzip/lz4)
        create_dirs: Create output directories if they don't exist
        overwrite: Overwrite existing output files
        record_batch_size: Number of rows per batch
        write_sorted_metadata: Embed sorted metadata in Parquet files
        bloom_filter_all: Enable bloom filters for all columns
        bloom_filter_columns: Enable bloom filters for specific columns
        max_row_group_size: Maximum rows per row group
        statistics: Statistics level (none/chunk/page)
        enable_dictionary: Enable dictionary encoding
        writer_version: Parquet writer version (v1/v2)
    """
    ...
