#!/usr/bin/env bash

set -ex

[ -f people.arrow ] && rm people.arrow
[ -f people.parquet ] && rm people.parquet
[ -f people.duckdb ] && rm people.duckdb

duckdb -f duckdb_cli_create_files.sql
datafusion-cli --quiet --file datafusion_cli_create_files.sql
