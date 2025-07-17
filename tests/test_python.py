from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import silk_chiffon


def make_test_file(path: str, rows: int = 1000):
    t: pa.Table = pa.table(  # type: ignore
        {
            "id": list(range(rows)),
            "name": [f"name_{i}" for i in range(rows)],
            "value": [float(i) * 1.5 for i in range(rows)],
            "category": [f"cat_{i % 5}" for i in range(rows)],
        }
    )

    with pa.OSFile(path, "wb") as sink:
        with pa.ipc.new_file(sink, t.schema) as writer:
            writer.write_table(t)


def make_test_file_nulls(path: str):
    t: pa.Table = pa.table(  # type: ignore
        {
            "id": list(range(100)),
            "name": [f"name_{i}" for i in range(100)],
            "value": [float(i) * 1.5 for i in range(100)],
            "category": [f"cat_{i % 3}" if i % 10 != 0 else None for i in range(100)],
        }
    )

    with pa.OSFile(path, "wb") as sink:
        with pa.ipc.new_file(sink, t.schema) as writer:
            writer.write_table(t)


class TestArrowToParquet:
    def test_conversion(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "test.parquet"

        make_test_file(str(src))
        silk_chiffon.arrow_to_parquet(str(src), str(dst))

        assert dst.exists()
        t: pa.Table = pq.read_table(str(dst))  # type: ignore
        assert len(t) == 1000
        assert set(t.column_names) == {"id", "name", "value", "category"}

    def test_zstd_compression(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "test.parquet"

        make_test_file(str(src))
        silk_chiffon.arrow_to_parquet(str(src), str(dst), compression="zstd")

        pf = pq.ParquetFile(str(dst))
        assert pf.metadata.row_group(0).column(0).compression.lower() == "zstd"  # type: ignore

    def test_sorting(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "test.parquet"

        make_test_file(str(src))
        silk_chiffon.arrow_to_parquet(
            str(src), str(dst), sort_by=["category", ("value", "desc")]
        )

        t: pa.Table = pq.read_table(str(dst))  # type: ignore
        cats = t["category"].to_pylist()
        vals = t["value"].to_pylist()

        last_cat = None
        for i, cat in enumerate(cats):
            if last_cat and cat != last_cat:
                assert cat >= last_cat
            elif last_cat == cat and i > 0:
                current_val = vals[i]
                prev_val = vals[i - 1]
                assert (
                    current_val is not None
                    and prev_val is not None
                    and current_val <= prev_val
                )
            last_cat = cat

    def test_bloom_filter(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "test.parquet"

        make_test_file(str(src))
        silk_chiffon.arrow_to_parquet(
            str(src), str(dst), bloom_filter_columns=["id", "category"]
        )

        assert dst.exists()
        pf = pq.ParquetFile(str(dst))
        assert pf.metadata.num_rows == 1000

    def test_small_row_groups(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "test.parquet"

        make_test_file(str(src), rows=2000)
        silk_chiffon.arrow_to_parquet(str(src), str(dst), max_row_group_size=500)

        pf = pq.ParquetFile(str(dst))
        assert pf.metadata.num_row_groups == 4


class TestArrowToDuckDB:
    def test_basic(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        db = tmp_path / "test.db"

        make_test_file(str(src))
        silk_chiffon.arrow_to_duckdb(str(src), str(db), "test_table")

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db))  # type: ignore
        count = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]  # type: ignore
        assert count == 1000

        cols = {col[0] for col in conn.execute("DESCRIBE test_table").fetchall()}
        assert cols == {"id", "name", "value", "category"}
        conn.close()

    def test_sorted(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        db = tmp_path / "test.db"

        make_test_file(str(src))
        silk_chiffon.arrow_to_duckdb(
            str(src), str(db), "sorted", sort_by=["category", ("id", "desc")]
        )

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db))  # type: ignore
        rows = conn.execute("SELECT category, id FROM sorted").fetchall()

        prev_cat = None
        prev_id = None

        for cat, id_val in rows:
            if prev_cat is None:
                prev_cat = cat
                prev_id = id_val
            elif cat != prev_cat:
                assert cat >= prev_cat
                prev_cat = cat
                prev_id = id_val
            else:
                assert id_val <= prev_id
                prev_id = id_val

        conn.close()

    def test_drop_existing(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        db = tmp_path / "test.db"

        make_test_file(str(src))

        silk_chiffon.arrow_to_duckdb(str(src), str(db), "mytable")

        with pytest.raises(Exception):
            silk_chiffon.arrow_to_duckdb(str(src), str(db), "mytable")

        silk_chiffon.arrow_to_duckdb(str(src), str(db), "mytable", drop_table=True)

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db))  # type: ignore
        count = conn.execute("SELECT COUNT(*) FROM mytable").fetchone()[0]  # type: ignore
        assert count == 1000
        conn.close()


class TestArrowToArrow:
    def test_file_conversion(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "output.arrow"

        make_test_file(str(src))
        silk_chiffon.arrow_to_arrow(str(src), str(dst))

        assert dst.exists()

        try:
            with pa.OSFile(str(dst), "rb") as f:
                reader = pa.ipc.open_file(f)
                t = reader.read_all()
        except Exception:
            with pa.OSFile(str(dst), "rb") as f:
                reader = pa.ipc.open_stream(f)
                t = reader.read_all()

        assert len(t) == 1000
        assert set(t.column_names) == {"id", "name", "value", "category"}

    def test_lz4_compression(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "compressed.arrow"

        make_test_file(str(src))
        silk_chiffon.arrow_to_arrow(str(src), str(dst), compression="lz4")

        with pa.OSFile(str(dst), "rb") as f:
            reader = pa.ipc.open_file(f)
            t = reader.read_all()
            assert len(t) == 1000

    def test_custom_batch_size(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "batched.arrow"

        make_test_file(str(src))
        silk_chiffon.arrow_to_arrow(str(src), str(dst), record_batch_size=250)

        with pa.OSFile(str(dst), "rb") as f:
            reader = pa.ipc.open_file(f)
            assert reader.num_record_batches == 4
            total = sum(
                reader.get_batch(i).num_rows for i in range(reader.num_record_batches)
            )
            assert total == 1000


class TestErrors:
    def test_missing_file(self):
        with pytest.raises(Exception):
            silk_chiffon.arrow_to_parquet("/does/not/exist.arrow", "out.parquet")

    def test_bad_compression(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        make_test_file(str(src))

        with pytest.raises(Exception):
            silk_chiffon.arrow_to_parquet(str(src), "out.parquet", compression="bad")

    def test_bad_sort_direction(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        make_test_file(str(src))

        with pytest.raises(Exception):
            silk_chiffon.arrow_to_parquet(
                str(src),
                "out.parquet",
                sort_by=[("id", "wrong")],  # type: ignore
            )


class TestSplitToArrow:
    def test_split_by_category(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "split_{value}.arrow")

        make_test_file(str(src))
        silk_chiffon.split_to_arrow(str(src), template, "category")

        for i in range(5):
            f = tmp_path / f"split_cat_{i}.arrow"
            assert f.exists()

            with pa.OSFile(str(f), "rb") as source:
                reader = pa.ipc.open_file(source)
                t = reader.read_all()
                assert len(t) == 200
                cats = t["category"].to_pylist()
                assert all(c == f"cat_{i}" for c in cats)

    def test_split_with_nulls(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "split_{value}.arrow")

        make_test_file_nulls(str(src))
        silk_chiffon.split_to_arrow(str(src), template, "category")

        expected = ["cat_0", "cat_1", "cat_2", "__NULL__"]
        for cat in expected:
            f = tmp_path / f"split_{cat}.arrow"
            assert f.exists()

    def test_split_sorted(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "sorted_{value}.arrow")

        make_test_file(str(src))
        silk_chiffon.split_to_arrow(
            str(src), template, "category", sort_by=[("value", "desc")]
        )

        for i in range(5):
            f = tmp_path / f"sorted_cat_{i}.arrow"
            with pa.OSFile(str(f), "rb") as source:
                reader = pa.ipc.open_file(source)
                t = reader.read_all()
                vals = t["value"].to_pylist()
                assert all(vals[j] >= vals[j + 1] for j in range(len(vals) - 1))  # type: ignore

    def test_create_dirs(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        out_dir = tmp_path / "out" / "nested"
        template = str(out_dir / "split_{value}.arrow")

        make_test_file(str(src), rows=100)
        assert not out_dir.exists()

        silk_chiffon.split_to_arrow(str(src), template, "category", create_dirs=True)

        assert out_dir.exists()
        assert len(list(out_dir.glob("*.arrow"))) == 5

    def test_overwrite_protection(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "split_{value}.arrow")

        make_test_file(str(src), rows=100)

        silk_chiffon.split_to_arrow(str(src), template, "category")

        with pytest.raises(Exception):
            silk_chiffon.split_to_arrow(str(src), template, "category")

        silk_chiffon.split_to_arrow(str(src), template, "category", overwrite=True)


class TestSplitToParquet:
    def test_basic_split(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "split_{value}.parquet")

        make_test_file(str(src))
        silk_chiffon.split_to_parquet(str(src), template, "category")

        for i in range(5):
            f = tmp_path / f"split_cat_{i}.parquet"
            assert f.exists()

            t: pa.Table = pq.read_table(str(f))  # type: ignore
            assert len(t) == 200
            cats = t["category"].to_pylist()
            assert all(c == f"cat_{i}" for c in cats)

    def test_snappy_compression(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "split_{value}.parquet")

        make_test_file(str(src), rows=500)
        silk_chiffon.split_to_parquet(
            str(src), template, "category", compression="snappy"
        )

        f = tmp_path / "split_cat_0.parquet"
        pf = pq.ParquetFile(str(f))
        assert pf.metadata.row_group(0).column(0).compression.lower() == "snappy"  # type: ignore

    def test_with_bloom_filter(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "bloom_{value}.parquet")

        make_test_file(str(src), rows=500)
        silk_chiffon.split_to_parquet(
            str(src), template, "category", bloom_filter_columns=["id"]
        )

        for i in range(5):
            f = tmp_path / f"bloom_cat_{i}.parquet"
            assert f.exists()

    def test_sorted_with_metadata(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "sorted_{value}.parquet")

        make_test_file(str(src))
        silk_chiffon.split_to_parquet(
            str(src),
            template,
            "category",
            sort_by=["id", ("value", "desc")],
            write_sorted_metadata=True,
        )

        for i in range(5):
            f = tmp_path / f"sorted_cat_{i}.parquet"
            assert f.exists()

            t: pa.Table = pq.read_table(str(f))  # type: ignore
            ids = t["id"].to_pylist()
            expected = [j for j in range(1000) if j % 5 == i]
            assert ids == expected

    def test_small_row_groups(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "rg_{value}.parquet")

        make_test_file(str(src))
        silk_chiffon.split_to_parquet(
            str(src), template, "category", max_row_group_size=50
        )

        f = tmp_path / "rg_cat_0.parquet"
        pf = pq.ParquetFile(str(f))
        assert pf.metadata.num_row_groups == 4  # 200 rows / 50


class TestSplitErrors:
    def test_missing_column(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "split_{value}.arrow")

        make_test_file(str(src))

        with pytest.raises(Exception):
            silk_chiffon.split_to_arrow(str(src), template, "missing_col")

    def test_bad_template(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        template = str(tmp_path / "split_no_placeholder.arrow")

        make_test_file(str(src))

        with pytest.raises(Exception):
            silk_chiffon.split_to_arrow(str(src), template, "category")


class TestMergeToArrow:
    def test_basic_merge(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "merged.arrow"

        make_test_file(str(src1), rows=100)
        make_test_file(str(src2), rows=100)

        silk_chiffon.merge_to_arrow([str(src1), str(src2)], str(dst))

        assert dst.exists()

        with pa.OSFile(str(dst), "rb") as f:
            reader = pa.ipc.open_file(f)
            t = reader.read_all()
            assert len(t) == 200
            assert set(t.column_names) == {"id", "name", "value", "category"}

    def test_merge_with_sorting(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "sorted.arrow"

        t1: pa.Table = pa.table(  # type: ignore
            {
                "id": [5, 6, 7, 8],
                "name": ["E", "F", "G", "H"],
                "value": [5.0, 6.0, 7.0, 8.0],
                "category": ["cat_0", "cat_1", "cat_2", "cat_3"],
            }
        )

        t2: pa.Table = pa.table(  # type: ignore
            {
                "id": [1, 2, 3, 4],
                "name": ["A", "B", "C", "D"],
                "value": [1.0, 2.0, 3.0, 4.0],
                "category": ["cat_0", "cat_1", "cat_2", "cat_3"],
            }
        )

        with pa.OSFile(str(src1), "wb") as sink:
            with pa.ipc.new_file(sink, t1.schema) as writer:
                writer.write_table(t1)

        with pa.OSFile(str(src2), "wb") as sink:
            with pa.ipc.new_file(sink, t2.schema) as writer:
                writer.write_table(t2)

        silk_chiffon.merge_to_arrow(
            [str(src1), str(src2)], str(dst), sort_by=[("id", "asc")]
        )

        with pa.OSFile(str(dst), "rb") as f:
            reader = pa.ipc.open_file(f)
            t = reader.read_all()
            ids = t["id"].to_pylist()
            assert ids == [1, 2, 3, 4, 5, 6, 7, 8]

    def test_merge_with_query(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "filtered.arrow"

        t1: pa.Table = pa.table(  # type: ignore
            {
                "id": list(range(0, 50)),
                "name": [f"name_{i}" for i in range(0, 50)],
                "value": [float(i) * 1.5 for i in range(0, 50)],
                "category": [f"cat_{i % 5}" for i in range(0, 50)],
            }
        )

        t2: pa.Table = pa.table(  # type: ignore
            {
                "id": list(range(50, 100)),
                "name": [f"name_{i}" for i in range(50, 100)],
                "value": [float(i) * 1.5 for i in range(50, 100)],
                "category": [f"cat_{i % 5}" for i in range(50, 100)],
            }
        )

        with pa.OSFile(str(src1), "wb") as sink:
            with pa.ipc.new_file(sink, t1.schema) as writer:
                writer.write_table(t1)

        with pa.OSFile(str(src2), "wb") as sink:
            with pa.ipc.new_file(sink, t2.schema) as writer:
                writer.write_table(t2)

        silk_chiffon.merge_to_arrow(
            [str(src1), str(src2)], str(dst), query="SELECT * FROM data WHERE id < 25"
        )

        with pa.OSFile(str(dst), "rb") as f:
            reader = pa.ipc.open_file(f)
            t = reader.read_all()
            assert t.num_rows == 25
            assert max(t["id"].to_pylist()) < 25  # type: ignore

    def test_merge_with_glob_pattern(self, tmp_path: Path):
        dst = tmp_path / "merged_glob.arrow"

        for i in range(3):
            src = tmp_path / f"data_{i}.arrow"
            make_test_file(str(src), rows=33)

        glob_pattern = str(tmp_path / "data_*.arrow")
        silk_chiffon.merge_to_arrow([glob_pattern], str(dst))

        with pa.OSFile(str(dst), "rb") as f:
            reader = pa.ipc.open_file(f)
            t = reader.read_all()
            assert len(t) == 99  # 3 files x 33 rows

    def test_merge_with_compression(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "compressed.arrow"

        make_test_file(str(src1), rows=1000)
        make_test_file(str(src2), rows=1000)

        silk_chiffon.merge_to_arrow(
            [str(src1), str(src2)], str(dst), compression="zstd"
        )

        assert dst.exists()
        with pa.OSFile(str(dst), "rb") as f:
            reader = pa.ipc.open_file(f)
            t = reader.read_all()
            assert len(t) == 2000


class TestMergeToParquet:
    def test_basic_merge(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "merged.parquet"

        make_test_file(str(src1), rows=100)
        make_test_file(str(src2), rows=100)

        silk_chiffon.merge_to_parquet([str(src1), str(src2)], str(dst))

        assert dst.exists()
        t: pa.Table = pq.read_table(str(dst))  # type: ignore
        assert len(t) == 200
        assert set(t.column_names) == {"id", "name", "value", "category"}

    def test_merge_with_compression(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "compressed.parquet"

        make_test_file(str(src1), rows=500)
        make_test_file(str(src2), rows=500)

        silk_chiffon.merge_to_parquet(
            [str(src1), str(src2)], str(dst), compression="zstd"
        )

        pf = pq.ParquetFile(str(dst))
        assert pf.metadata.row_group(0).column(0).compression.lower() == "zstd"  # type: ignore
        assert pf.metadata.num_rows == 1000

    def test_merge_with_bloom_filters(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "bloom.parquet"

        make_test_file(str(src1), rows=100)
        make_test_file(str(src2), rows=100)

        silk_chiffon.merge_to_parquet(
            [str(src1), str(src2)], str(dst), bloom_filter_columns=["id", "category"]
        )

        assert dst.exists()
        pf = pq.ParquetFile(str(dst))
        assert pf.metadata.num_rows == 200

    def test_merge_sorted_with_metadata(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "sorted.parquet"

        make_test_file(str(src1), rows=50)
        make_test_file(str(src2), rows=50)

        silk_chiffon.merge_to_parquet(
            [str(src1), str(src2)],
            str(dst),
            sort_by=[("category", "asc"), ("id", "desc")],
            write_sorted_metadata=True,
        )

        t: pa.Table = pq.read_table(str(dst))  # type: ignore
        assert len(t) == 100

        cats = t["category"].to_pylist()
        ids = t["id"].to_pylist()

        last_cat = None
        for i, cat in enumerate(cats):
            if last_cat and cat != last_cat:
                assert cat >= last_cat
            elif last_cat == cat and i > 0:
                assert ids[i] <= ids[i - 1]  # type: ignore
            last_cat = cat

    def test_merge_with_row_groups(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "row_groups.parquet"

        make_test_file(str(src1), rows=1000)
        make_test_file(str(src2), rows=1000)

        silk_chiffon.merge_to_parquet(
            [str(src1), str(src2)], str(dst), max_row_group_size=500
        )

        pf = pq.ParquetFile(str(dst))
        assert pf.metadata.num_row_groups == 4  # 2000 rows / 500
        assert pf.metadata.num_rows == 2000


class TestMergeToDuckDB:
    def test_basic_merge(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        db = tmp_path / "merged.db"

        make_test_file(str(src1), rows=100)
        make_test_file(str(src2), rows=100)

        silk_chiffon.merge_to_duckdb([str(src1), str(src2)], str(db), "merged_table")

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db))  # type: ignore
        count = conn.execute("SELECT COUNT(*) FROM merged_table").fetchone()[0]  # type: ignore
        assert count == 200

        cols = {col[0] for col in conn.execute("DESCRIBE merged_table").fetchall()}
        assert cols == {"id", "name", "value", "category"}
        conn.close()

    def test_merge_with_sorting(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        db = tmp_path / "sorted.db"

        make_test_file(str(src1), rows=50)
        make_test_file(str(src2), rows=50)

        silk_chiffon.merge_to_duckdb(
            [str(src1), str(src2)], str(db), "sorted_table", sort_by=[("value", "desc")]
        )

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db))  # type: ignore
        values = [
            row[0] for row in conn.execute("SELECT value FROM sorted_table").fetchall()
        ]

        for i in range(1, len(values)):
            assert values[i] <= values[i - 1]

        conn.close()

    def test_merge_with_drop_table(self, tmp_path: Path):
        src = tmp_path / "input.arrow"
        db = tmp_path / "test.db"

        make_test_file(str(src), rows=50)

        silk_chiffon.merge_to_duckdb([str(src)], str(db), "test_table")

        with pytest.raises(Exception):
            silk_chiffon.merge_to_duckdb([str(src)], str(db), "test_table")

        silk_chiffon.merge_to_duckdb([str(src)], str(db), "test_table", drop_table=True)

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db))  # type: ignore
        count = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]  # type: ignore
        assert count == 50
        conn.close()

    def test_merge_with_query(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        db = tmp_path / "filtered.db"

        t1: pa.Table = pa.table(  # type: ignore
            {
                "id": list(range(0, 100)),
                "name": [f"name_{i}" for i in range(0, 100)],
                "value": [float(i) * 1.5 for i in range(0, 100)],
                "category": [f"cat_{i % 5}" for i in range(0, 100)],
            }
        )

        t2: pa.Table = pa.table(  # type: ignore
            {
                "id": list(range(100, 200)),
                "name": [f"name_{i}" for i in range(100, 200)],
                "value": [float(i) * 1.5 for i in range(100, 200)],
                "category": [f"cat_{i % 5}" for i in range(100, 200)],
            }
        )

        with pa.OSFile(str(src1), "wb") as sink:
            with pa.ipc.new_file(sink, t1.schema) as writer:
                writer.write_table(t1)

        with pa.OSFile(str(src2), "wb") as sink:
            with pa.ipc.new_file(sink, t2.schema) as writer:
                writer.write_table(t2)

        silk_chiffon.merge_to_duckdb(
            [str(src1), str(src2)],
            str(db),
            "filtered_table",
            query="SELECT * FROM data WHERE id >= 50 AND id < 150",
        )

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db))  # type: ignore
        count = conn.execute("SELECT COUNT(*) FROM filtered_table").fetchone()[0]  # type: ignore
        assert count == 100

        min_id = conn.execute("SELECT MIN(id) FROM filtered_table").fetchone()[0]  # type: ignore
        max_id = conn.execute("SELECT MAX(id) FROM filtered_table").fetchone()[0]  # type: ignore
        assert min_id == 50
        assert max_id == 149
        conn.close()

    def test_merge_with_truncate(self, tmp_path: Path):
        src = tmp_path / "input.arrow"
        db = tmp_path / "test.db"

        make_test_file(str(src), rows=50)

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db))  # type: ignore
        conn.execute("CREATE TABLE other_table (x INT)")  # type: ignore
        conn.execute("INSERT INTO other_table VALUES (42)")  # type: ignore
        conn.close()

        silk_chiffon.merge_to_duckdb([str(src)], str(db), "new_table", truncate=True)

        conn = duckdb.connect(str(db))  # type: ignore

        count = conn.execute("SELECT COUNT(*) FROM new_table").fetchone()[0]  # type: ignore
        assert count == 50

        with pytest.raises(Exception):
            conn.execute("SELECT * FROM other_table")

        conn.close()


class TestMergeErrors:
    def test_empty_input_list(self, tmp_path: Path):
        dst = tmp_path / "output.arrow"

        with pytest.raises(Exception):
            silk_chiffon.merge_to_arrow([], str(dst))

    def test_nonexistent_files(self, tmp_path: Path):
        dst = tmp_path / "output.arrow"

        with pytest.raises(Exception):
            silk_chiffon.merge_to_arrow(
                ["/nonexistent/file1.arrow", "/nonexistent/file2.arrow"], str(dst)
            )

    def test_schema_mismatch(self, tmp_path: Path):
        src1 = tmp_path / "input1.arrow"
        src2 = tmp_path / "input2.arrow"
        dst = tmp_path / "output.arrow"

        make_test_file(str(src1), rows=10)

        t2: pa.Table = pa.table(  # type: ignore
            {"id": [1, 2, 3], "different_column": ["A", "B", "C"]}
        )

        with pa.OSFile(str(src2), "wb") as sink:
            with pa.ipc.new_file(sink, t2.schema) as writer:
                writer.write_table(t2)

        with pytest.raises(Exception):
            silk_chiffon.merge_to_arrow([str(src1), str(src2)], str(dst))
