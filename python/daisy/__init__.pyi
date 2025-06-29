from typing import List, Optional, Tuple, Union, Literal, Dict, TypeAlias

__version__: str
__all__: List[str]

SortDirection: TypeAlias = Literal["asc", "desc"]
SortColumn: TypeAlias = Union[str, Tuple[str, SortDirection]]
ParquetCompression: TypeAlias = Literal["zstd", "snappy", "gzip", "lz4", "none"]
ArrowCompression: TypeAlias = Literal["zstd", "lz4", "none"]
Statistics: TypeAlias = Literal["none", "chunk", "page"]
WriterVersion: TypeAlias = Literal["v1", "v2"]

def arrow_to_parquet(
    input_path: str,
    output_path: str,
    *,
    sort_by: Optional[List[SortColumn]] = None,
    compression: ParquetCompression = "none",
    write_sorted_metadata: bool = False,
    bloom_filter_all: Optional[Union[bool, float, Dict[str, float]]] = None,
    bloom_filter_columns: Optional[List[Union[str, Dict[str, float]]]] = None,
    max_row_group_size: int = 1_048_576,
    statistics: Statistics = "page",
    record_batch_size: int = 122_880,
    enable_dictionary: bool = True,
    writer_version: WriterVersion = "v2",
) -> None: ...
def arrow_to_duckdb(
    input_path: str,
    output_path: str,
    table_name: str,
    *,
    sort_by: Optional[List[SortColumn]] = None,
    truncate: bool = False,
    drop_table: bool = False,
) -> None: ...
def arrow_to_arrow(
    input_path: str,
    output_path: str,
    *,
    sort_by: Optional[List[SortColumn]] = None,
    compression: ArrowCompression = "none",
    record_batch_size: int = 122_880,
) -> None: ...
