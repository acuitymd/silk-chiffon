import os
import tempfile
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import silk_chiffon


def create_test_arrow_file(path: str, num_rows: int = 1000):
    """Create a test Arrow file with sample data."""
    data = {
        "id": list(range(num_rows)),
        "name": [f"name_{i}" for i in range(num_rows)],
        "value": [float(i) * 1.5 for i in range(num_rows)],
        "category": [f"cat_{i % 5}" for i in range(num_rows)],
    }
    table = pa.table(data)

    with pa.OSFile(path, "wb") as sink:
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)


class TestArrowToParquet:
    """Test arrow_to_parquet function."""

    def test_basic_conversion(self, tmp_path):
        """Test basic Arrow to Parquet conversion."""
        arrow_file = tmp_path / "test.arrow"
        parquet_file = tmp_path / "test.parquet"

        create_test_arrow_file(str(arrow_file))
        silk_chiffon.arrow_to_parquet(str(arrow_file), str(parquet_file))

        assert parquet_file.exists()

        table = pq.read_table(str(parquet_file))
        assert len(table) == 1000
        assert set(table.column_names) == {"id", "name", "value", "category"}

    def test_with_compression(self, tmp_path):
        """Test conversion with different compression algorithms."""
        arrow_file = tmp_path / "test.arrow"
        create_test_arrow_file(str(arrow_file))

        compressions = ["snappy", "gzip", "zstd", "lz4"]
        for comp in compressions:
            parquet_file = tmp_path / f"test_{comp}.parquet"
            silk_chiffon.arrow_to_parquet(str(arrow_file), str(parquet_file), compression=comp)
            assert parquet_file.exists()

            pf = pq.ParquetFile(str(parquet_file))
            actual_comp = pf.metadata.row_group(0).column(0).compression.lower()
            if comp == "lz4":
                assert actual_comp in ["lz4", "lz4_raw"]
            else:
                assert actual_comp == comp.lower()

    def test_with_sorting(self, tmp_path):
        """Test conversion with sorting."""
        arrow_file = tmp_path / "test.arrow"
        parquet_file = tmp_path / "test_sorted.parquet"

        create_test_arrow_file(str(arrow_file))
        silk_chiffon.arrow_to_parquet(
            str(arrow_file), str(parquet_file), sort_by=["category", ("value", "desc")]
        )

        assert parquet_file.exists()

        table = pq.read_table(str(parquet_file))
        categories = table["category"].to_pylist()
        values = table["value"].to_pylist()

        last_cat = None
        for i, cat in enumerate(categories):
            if last_cat and cat != last_cat:
                assert cat >= last_cat
            elif last_cat == cat and i > 0:
                assert values[i] <= values[i - 1]
            last_cat = cat

    def test_with_bloom_filters(self, tmp_path):
        """Test conversion with bloom filters."""
        arrow_file = tmp_path / "test.arrow"

        create_test_arrow_file(str(arrow_file))

        parquet_file1 = tmp_path / "test_bloom_all.parquet"
        silk_chiffon.arrow_to_parquet(
            str(arrow_file), str(parquet_file1), bloom_filter_all=0.05
        )
        assert parquet_file1.exists()

        parquet_file2 = tmp_path / "test_bloom_columns.parquet"
        silk_chiffon.arrow_to_parquet(
            str(arrow_file), str(parquet_file2), bloom_filter_columns=["id", "category"]
        )
        assert parquet_file2.exists()

        pf1 = pq.ParquetFile(str(parquet_file1))
        pf2 = pq.ParquetFile(str(parquet_file2))
        assert pf1.metadata.num_rows == 1000
        assert pf2.metadata.num_rows == 1000

    def test_statistics_options(self, tmp_path):
        """Test different statistics options."""
        arrow_file = tmp_path / "test.arrow"
        create_test_arrow_file(str(arrow_file))

        stats_options = ["none", "chunk", "page"]
        for stats in stats_options:
            parquet_file = tmp_path / f"test_stats_{stats}.parquet"
            silk_chiffon.arrow_to_parquet(str(arrow_file), str(parquet_file), statistics=stats)
            assert parquet_file.exists()

    def test_writer_versions(self, tmp_path):
        """Test different writer versions."""
        arrow_file = tmp_path / "test.arrow"
        create_test_arrow_file(str(arrow_file))

        versions = ["v1", "v2"]
        for version in versions:
            parquet_file = tmp_path / f"test_{version}.parquet"
            silk_chiffon.arrow_to_parquet(
                str(arrow_file), str(parquet_file), writer_version=version
            )
            assert parquet_file.exists()

            pf = pq.ParquetFile(str(parquet_file))
            assert pf.metadata.num_rows == 1000

    def test_row_group_size(self, tmp_path):
        """Test custom row group size."""
        arrow_file = tmp_path / "test.arrow"
        parquet_file = tmp_path / "test_small_rg.parquet"

        create_test_arrow_file(str(arrow_file), num_rows=2000)
        silk_chiffon.arrow_to_parquet(
            str(arrow_file), str(parquet_file), max_row_group_size=500
        )

        assert parquet_file.exists()

        pf = pq.ParquetFile(str(parquet_file))
        assert pf.metadata.num_row_groups == 4
        assert pf.metadata.num_rows == 2000


class TestArrowToDuckDB:
    """Test arrow_to_duckdb function."""

    def test_basic_conversion(self, tmp_path):
        """Test basic Arrow to DuckDB conversion."""
        arrow_file = tmp_path / "test.arrow"
        db_file = tmp_path / "test.db"

        create_test_arrow_file(str(arrow_file))
        silk_chiffon.arrow_to_duckdb(str(arrow_file), str(db_file), "test_table")

        assert db_file.exists()

        conn = duckdb.connect(str(db_file))
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == 1000

        columns = conn.execute("DESCRIBE test_table").fetchall()
        column_names = [col[0] for col in columns]
        assert set(column_names) == {"id", "name", "value", "category"}
        conn.close()

    def test_with_sorting(self, tmp_path):
        """Test conversion with sorting."""
        arrow_file = tmp_path / "test.arrow"
        db_file = tmp_path / "test.db"

        create_test_arrow_file(str(arrow_file))
        silk_chiffon.arrow_to_duckdb(
            str(arrow_file),
            str(db_file),
            "sorted_table",
            sort_by=["category", ("id", "desc")],
        )

        assert db_file.exists()

        conn = duckdb.connect(str(db_file))
        result = conn.execute("SELECT category, id FROM sorted_table").fetchall()

        last_category = None
        last_id_in_category = None

        for category, id_val in result:
            if last_category is None:
                last_category = category
                last_id_in_category = id_val
            elif category != last_category:
                assert category >= last_category
                last_category = category
                last_id_in_category = id_val
            else:
                assert id_val <= last_id_in_category
                last_id_in_category = id_val

        assert len(result) == 1000
        conn.close()

    def test_drop_table(self, tmp_path):
        """Test drop_table option."""
        arrow_file = tmp_path / "test.arrow"
        db_file = tmp_path / "test.db"

        create_test_arrow_file(str(arrow_file))

        silk_chiffon.arrow_to_duckdb(str(arrow_file), str(db_file), "test_table")

        with pytest.raises(Exception):
            silk_chiffon.arrow_to_duckdb(str(arrow_file), str(db_file), "test_table")

        silk_chiffon.arrow_to_duckdb(
            str(arrow_file), str(db_file), "test_table", drop_table=True
        )

        conn = duckdb.connect(str(db_file))
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == 1000
        conn.close()

    def test_truncate_file(self, tmp_path):
        """Test truncate option."""
        arrow_file = tmp_path / "test.arrow"
        db_file = tmp_path / "test.db"

        create_test_arrow_file(str(arrow_file))

        conn = duckdb.connect(str(db_file))
        conn.execute("CREATE TABLE other_table (id INT)")
        conn.execute("INSERT INTO other_table VALUES (1), (2), (3)")
        conn.close()

        silk_chiffon.arrow_to_duckdb(str(arrow_file), str(db_file), "test_table")

        conn = duckdb.connect(str(db_file))
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "other_table" in table_names
        assert "test_table" in table_names
        conn.close()

        silk_chiffon.arrow_to_duckdb(str(arrow_file), str(db_file), "new_table", truncate=True)

        conn = duckdb.connect(str(db_file))
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "other_table" not in table_names
        assert "new_table" in table_names
        assert "test_table" not in table_names
        conn.close()

    def test_append_to_existing_db(self, tmp_path):
        """Test appending multiple tables to same database."""
        arrow_file1 = tmp_path / "test1.arrow"
        arrow_file2 = tmp_path / "test2.arrow"
        db_file = tmp_path / "test.db"

        create_test_arrow_file(str(arrow_file1), num_rows=500)
        create_test_arrow_file(str(arrow_file2), num_rows=300)

        silk_chiffon.arrow_to_duckdb(str(arrow_file1), str(db_file), "table1")

        silk_chiffon.arrow_to_duckdb(str(arrow_file2), str(db_file), "table2")

        conn = duckdb.connect(str(db_file))

        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "table1" in table_names
        assert "table2" in table_names

        result1 = conn.execute("SELECT COUNT(*) FROM table1").fetchone()
        assert result1[0] == 500

        result2 = conn.execute("SELECT COUNT(*) FROM table2").fetchone()
        assert result2[0] == 300

        conn.close()


class TestArrowToArrow:
    """Test arrow_to_arrow function."""

    def test_file_to_stream(self, tmp_path):
        """Test converting between Arrow formats."""
        input_file = tmp_path / "test.arrow"
        output_file = tmp_path / "test_output.arrow"

        create_test_arrow_file(str(input_file))
        silk_chiffon.arrow_to_arrow(str(input_file), str(output_file))

        assert output_file.exists()

        try:
            with pa.OSFile(str(output_file), "rb") as source:
                reader = pa.ipc.open_file(source)
                table = reader.read_all()
                assert len(table) == 1000
                assert set(table.column_names) == {"id", "name", "value", "category"}
        except:
            with pa.OSFile(str(output_file), "rb") as source:
                reader = pa.ipc.open_stream(source)
                table = reader.read_all()
                assert len(table) == 1000
                assert set(table.column_names) == {"id", "name", "value", "category"}

    def test_stream_to_file(self, tmp_path):
        """Test converting between formats with stream input."""
        stream_file = tmp_path / "test.arrows"
        data = {"id": list(range(100)), "value": [float(i) for i in range(100)]}
        table = pa.table(data)

        with pa.OSFile(str(stream_file), "wb") as sink:
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)

        output_file = tmp_path / "test_output.arrow"
        silk_chiffon.arrow_to_arrow(str(stream_file), str(output_file))

        assert output_file.exists()

        try:
            with pa.OSFile(str(output_file), "rb") as source:
                reader = pa.ipc.open_file(source)
                table = reader.read_all()
                assert len(table) == 100
                assert set(table.column_names) == {"id", "value"}
        except:
            with pa.OSFile(str(output_file), "rb") as source:
                reader = pa.ipc.open_stream(source)
                table = reader.read_all()
                assert len(table) == 100
                assert set(table.column_names) == {"id", "value"}

    def test_with_compression(self, tmp_path):
        """Test conversion with different compression algorithms."""
        input_file = tmp_path / "test.arrow"
        create_test_arrow_file(str(input_file))

        compressions = ["lz4", "zstd"]
        for comp in compressions:
            output_file = tmp_path / f"test_{comp}.arrow"
            silk_chiffon.arrow_to_arrow(str(input_file), str(output_file), compression=comp)
            assert output_file.exists()

            with pa.OSFile(str(output_file), "rb") as source:
                reader = pa.ipc.open_file(source)
                table = reader.read_all()
                assert len(table) == 1000

    def test_batch_size(self, tmp_path):
        """Test custom batch size."""
        input_file = tmp_path / "test.arrow"
        output_file = tmp_path / "test_batched.arrow"

        create_test_arrow_file(str(input_file), num_rows=1000)
        silk_chiffon.arrow_to_arrow(str(input_file), str(output_file), record_batch_size=250)

        assert output_file.exists()

        with pa.OSFile(str(output_file), "rb") as source:
            reader = pa.ipc.open_file(source)
            assert reader.num_record_batches == 4

            total_rows = sum(
                reader.get_batch(i).num_rows for i in range(reader.num_record_batches)
            )
            assert total_rows == 1000


class TestErrorHandling:
    """Test error handling in Python bindings."""

    def test_invalid_input_file(self):
        """Test handling of non-existent input file."""
        with pytest.raises(Exception):
            silk_chiffon.arrow_to_parquet("/non/existent/file.arrow", "output.parquet")

    def test_invalid_compression(self, tmp_path):
        """Test handling of invalid compression type."""
        arrow_file = tmp_path / "test.arrow"
        create_test_arrow_file(str(arrow_file))

        with pytest.raises(Exception):
            silk_chiffon.arrow_to_parquet(
                str(arrow_file),
                str(tmp_path / "output.parquet"),
                compression="invalid_compression",
            )

    def test_invalid_sort_direction(self, tmp_path):
        """Test handling of invalid sort direction."""
        arrow_file = tmp_path / "test.arrow"
        create_test_arrow_file(str(arrow_file))

        with pytest.raises(Exception):
            silk_chiffon.arrow_to_parquet(
                str(arrow_file),
                str(tmp_path / "output.parquet"),
                sort_by=[("id", "invalid_direction")],
            )

    def test_invalid_statistics_option(self, tmp_path):
        """Test handling of invalid statistics option."""
        arrow_file = tmp_path / "test.arrow"
        create_test_arrow_file(str(arrow_file))

        with pytest.raises(Exception):
            silk_chiffon.arrow_to_parquet(
                str(arrow_file),
                str(tmp_path / "output.parquet"),
                statistics="invalid_stats",
            )
