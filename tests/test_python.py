import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pathlib import Path
import silk_chiffon


def make_test_file(path: str, rows: int = 1000):
    t: pa.Table = pa.table( # type: ignore
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
    t: pa.Table = pa.table( # type: ignore
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
        t: pa.Table = pq.read_table(str(dst)) # type: ignore
        assert len(t) == 1000
        assert set(t.column_names) == {"id", "name", "value", "category"}

    def test_zstd_compression(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "test.parquet"

        make_test_file(str(src))
        silk_chiffon.arrow_to_parquet(str(src), str(dst), compression="zstd")

        pf = pq.ParquetFile(str(dst))
        assert pf.metadata.row_group(0).column(0).compression.lower() == "zstd" # type: ignore

    def test_sorting(self, tmp_path: Path):
        src = tmp_path / "test.arrow"
        dst = tmp_path / "test.parquet"

        make_test_file(str(src))
        silk_chiffon.arrow_to_parquet(
            str(src), str(dst), sort_by=["category", ("value", "desc")]
        )

        t: pa.Table = pq.read_table(str(dst)) # type: ignore
        cats = t["category"].to_pylist()
        vals = t["value"].to_pylist()

        last_cat = None
        for i, cat in enumerate(cats):
            if last_cat and cat != last_cat:
                assert cat >= last_cat
            elif last_cat == cat and i > 0:
                current_val = vals[i]
                prev_val = vals[i - 1]
                assert current_val is not None and prev_val is not None and current_val <= prev_val
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

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db)) # type: ignore
        count = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0] # type: ignore
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

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db)) # type: ignore
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

        conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db)) # type: ignore
        count = conn.execute("SELECT COUNT(*) FROM mytable").fetchone()[0] # type: ignore
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
                str(src), "out.parquet", sort_by=[("id", "wrong")] # type: ignore
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
                assert all(vals[j] >= vals[j + 1] for j in range(len(vals) - 1)) # type: ignore

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

            t: pa.Table = pq.read_table(str(f)) # type: ignore
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
        assert pf.metadata.row_group(0).column(0).compression.lower() == "snappy" # type: ignore

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

            t: pa.Table = pq.read_table(str(f)) # type: ignore
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
