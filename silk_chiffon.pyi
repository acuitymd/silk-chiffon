"""Type stubs for silk_chiffon Python bindings."""

from typing import Dict, List, Literal, Optional, Tuple, Union

SortDirection = Literal["asc", "desc"]
SortColumn = Union[str, Tuple[str, SortDirection]]

def arrow_to_arrow(
    input_path: str,
    output_path: str,
    *,
    query: Optional[str] = None,
    sort_by: Optional[List[SortColumn]] = None,
    compression: str = "none",
    record_batch_size: int = 122_880,
    output_ipc_format: str = "file",
) -> None:
    """
    Convert Arrow IPC stream or file format to Arrow IPC stream or file format with optional SQL query, sorting, and compression.

    Args:
        input_path: Path to input Arrow IPC file
        output_path: Path to output Arrow IPC file
        query: Optional SQL query to apply
        sort_by: Optional list of columns to sort by (name or (name, direction))
        compression: Compression type (none/zstd/lz4)
        record_batch_size: Number of rows per batch
        output_ipc_format: Output IPC format (file/stream)
    """
    ...

def arrow_to_parquet(
    input_path: str,
    output_path: str,
    *,
    query: Optional[str] = None,
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
    """
    Convert Arrow IPC stream or file format to Parquet file format with optional SQL query and advanced options.

    Args:
        input_path: Path to input Arrow IPC file
        output_path: Path to output Parquet file
        query: Optional SQL query to apply before conversion
        sort_by: Optional list of columns to sort by (name or (name, direction))
        compression: Compression type (none/zstd/snappy/gzip/lz4)
        write_sorted_metadata: Embed sorted metadata in Parquet file
        bloom_filter_all: Enable bloom filters for all columns
        bloom_filter_columns: Enable bloom filters for specific columns
        max_row_group_size: Maximum rows per row group
        statistics: Statistics level (none/chunk/page)
        record_batch_size: Number of rows per batch
        enable_dictionary: Enable dictionary encoding
        writer_version: Parquet writer version (v1/v2)
    """
    ...

def arrow_to_duckdb(
    input_path: str,
    output_path: str,
    table_name: str,
    *,
    query: Optional[str] = None,
    sort_by: Optional[List[SortColumn]] = None,
    truncate: bool = False,
    drop_table: bool = False,
) -> None:
    """
    Convert Arrow IPC stream or file format to DuckDB database with optional SQL query.

    Args:
        input_path: Path to input Arrow IPC file
        output_path: Path to output DuckDB database file
        table_name: Name of the table to create in DuckDB
        query: Optional SQL query to apply before conversion
        sort_by: Optional list of columns to sort by (name or (name, direction))
        truncate: Truncate the table before inserting data
        drop_table: Drop and recreate the table before inserting data
    """
    ...

def split_to_arrow(
    input_path: str,
    output_template: str,
    split_column: str,
    *,
    query: Optional[str] = None,
    sort_by: Optional[List[SortColumn]] = None,
    compression: str = "none",
    create_dirs: bool = True,
    overwrite: bool = False,
    record_batch_size: int = 122_880,
    list_outputs: str = "none",
    output_ipc_format: str = "file",
    exclude_columns: List[str] = [],
) -> Dict[str, str]:
    """
    Split Arrow IPC file into multiple Arrow files based on column values.

    Args:
        input_path: Path to input Arrow IPC file
        output_template: Output file template with {value} placeholder
        split_column: Column name to split by
        query: Optional SQL query to apply before splitting
        sort_by: Optional list of columns to sort by (name or (name, direction))
        compression: Compression type (none/zstd/lz4)
        create_dirs: Create output directories if they don't exist
        overwrite: Overwrite existing output files
        record_batch_size: Number of rows per batch
        list_outputs: Output listing format (none/text/json)
        output_ipc_format: Output IPC format (file/stream)

    Returns:
        Dictionary mapping split values to output file paths
    """
    ...

def split_to_parquet(
    input_path: str,
    output_template: str,
    split_column: str,
    *,
    query: Optional[str] = None,
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
    list_outputs: str = "none",
    exclude_columns: List[str] = [],
) -> Dict[str, str]:
    """
    Split Arrow IPC file into multiple Parquet files based on column values.

    Args:
        input_path: Path to input Arrow IPC file
        output_template: Output file template with {value} placeholder
        split_column: Column name to split by
        query: Optional SQL query to apply before splitting
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
        list_outputs: Output listing format (none/text/json)

    Returns:
        Dictionary mapping split values to output file paths
    """
    ...

def merge_to_arrow(
    input_paths: List[str],
    output_path: str,
    *,
    query: Optional[str] = None,
    sort_by: Optional[List[SortColumn]] = None,
    compression: str = "none",
    record_batch_size: int = 122_880,
    output_ipc_format: str = "file",
) -> None:
    """
    Merge multiple Arrow IPC files into a single Arrow IPC file.

    Args:
        input_paths: List of input Arrow IPC file paths (supports glob patterns)
        output_path: Path to output Arrow IPC file
        query: Optional SQL query to apply to the merged data
        sort_by: Optional list of columns to sort by (name or (name, direction))
        compression: Compression type (none/zstd/lz4)
        record_batch_size: Number of rows per batch
        output_ipc_format: Output IPC format (file/stream)
    """
    ...

def merge_to_parquet(
    input_paths: List[str],
    output_path: str,
    *,
    query: Optional[str] = None,
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
    """
    Merge multiple Arrow IPC files into a single Parquet file.

    Args:
        input_paths: List of input Arrow IPC file paths (supports glob patterns)
        output_path: Path to output Parquet file
        query: Optional SQL query to apply to the merged data
        sort_by: Optional list of columns to sort by (name or (name, direction))
        compression: Compression type (none/zstd/snappy/gzip/lz4)
        write_sorted_metadata: Embed sorted metadata in Parquet file
        bloom_filter_all: Enable bloom filters for all columns
        bloom_filter_columns: Enable bloom filters for specific columns
        max_row_group_size: Maximum rows per row group
        statistics: Statistics level (none/chunk/page)
        record_batch_size: Number of rows per batch
        enable_dictionary: Enable dictionary encoding
        writer_version: Parquet writer version (v1/v2)
    """
    ...

def merge_to_duckdb(
    input_paths: List[str],
    output_path: str,
    table_name: str,
    *,
    query: Optional[str] = None,
    sort_by: Optional[List[SortColumn]] = None,
    truncate: bool = False,
    drop_table: bool = False,
    record_batch_size: int = 122_880,
) -> None:
    """
    Merge multiple Arrow IPC files into a single DuckDB database.

    Args:
        input_paths: List of input Arrow IPC file paths (supports glob patterns)
        output_path: Path to output DuckDB database file
        table_name: Name of the table to create in DuckDB
        query: Optional SQL query to apply to the merged data
        sort_by: Optional list of columns to sort by (name or (name, direction))
        truncate: Truncate the database file before writing (removes entire file)
        drop_table: Drop and recreate the table before inserting data
        record_batch_size: Number of rows per batch
    """
    ...
