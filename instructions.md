# Row-Group Predicate Pushdown for PyIceberg Parquet Reads

## Problem

PyIceberg does **not** perform row-group-level predicate pushdown when reading Parquet files. Even when data is sorted and row groups have non-overlapping min/max statistics, PyIceberg reads ALL row groups within a file and filters in memory. DuckDB's Iceberg extension, reading the same data with the same filter, achieves 2.5x+ speedup by skipping non-matching row groups.

### Benchmark Evidence (30K CNPJs, 1 year, 12 monthly partitions, row-group-limit=50K → ~14 row groups per file)

| Query                | Unsorted (s) | Sorted (s) | Speedup |
|----------------------|--------------|------------|---------|
| pyiceberg_single_cnpj | 13.13s      | 13.79s     | 0.95x   |
| duckdb_single_cnpj    | 14.91s      | 5.86s      | **2.55x** |
| duckdb_range_cnpj     | 14.09s      | 5.04s      | **2.80x** |

The sorted table has row groups with non-overlapping CNPJ ranges (e.g., RG0: 00000000–00002173, RG1: 00002173–00004347, ...). DuckDB skips non-matching row groups. PyIceberg reads them all.

## Root Cause

The issue is in `pyiceberg/io/pyarrow.py`, function `_task_to_record_batches` (line 1604).

### Current code (lines 1618–1658):

```python
arrow_format = _get_file_format(task.file.file_format, pre_buffer=True, buffer_size=(ONE_MEGABYTE * 8))
with io.new_input(task.file.file_path).open() as fin:
    fragment = arrow_format.make_fragment(fin)
    # ... schema handling ...

    fragment_scanner = ds.Scanner.from_fragment(
        fragment=fragment,
        schema=physical_schema,
        filter=pyarrow_filter if not positional_deletes else None,
        columns=[col.name for col in file_project_schema.columns],
    )

    batches = fragment_scanner.to_batches()
    for batch in batches:
        # ... processes ALL batches, filters in memory at line 1674-1682 ...
```

### Why it doesn't prune row groups:

1. **Fragment created from file-like object**: `arrow_format.make_fragment(fin)` creates a `ParquetFileFragment` from a streaming file-like object, not a filesystem path. When created this way, PyArrow may not be able to efficiently access Parquet footer metadata for row-group statistics.

2. **Scanner doesn't skip row groups**: `ds.Scanner.from_fragment(fragment, filter=...)` is supposed to push down the filter, but empirically it reads all row groups. The batches returned correspond 1:1 to row groups — every batch is returned, then filtered in memory at lines 1674–1682 (the "Temporary fix until PyArrow 21" block).

3. **Double filtering with no pruning**: The filter is passed to the Scanner (line 1653) AND re-applied manually on each batch (line 1677). Even if the Scanner did row-group pruning, the second pass re-filters everything. But in practice, neither path skips row groups.

## Proposed Fix

PyArrow's `ParquetFileFragment` provides two methods for row-group-level pruning:

- **`fragment.split_by_row_group(filter=pyarrow_filter)`** — Yields sub-fragments for only the row groups whose metadata satisfies the filter. Row groups whose statistics contradict the filter are excluded entirely.
- **`fragment.subset(filter=pyarrow_filter)`** — Returns a new fragment containing only the matching row groups.

### Approach: Use `split_by_row_group` before scanning

In `_task_to_record_batches`, after creating the fragment and constructing `pyarrow_filter`, use `split_by_row_group` to get only the row-group fragments that could contain matching data, then scan each sub-fragment:

```python
# Instead of scanning the full fragment:
fragment_scanner = ds.Scanner.from_fragment(
    fragment=fragment,
    schema=physical_schema,
    filter=pyarrow_filter if not positional_deletes else None,
    columns=[col.name for col in file_project_schema.columns],
)
batches = fragment_scanner.to_batches()

# Use row-group pruning:
if pyarrow_filter is not None and not positional_deletes:
    # split_by_row_group uses Parquet row group statistics to skip
    # row groups whose min/max ranges don't match the filter
    row_group_fragments = fragment.split_by_row_group(filter=pyarrow_filter)
else:
    row_group_fragments = [fragment]

for rg_fragment in row_group_fragments:
    rg_scanner = ds.Scanner.from_fragment(
        fragment=rg_fragment,
        schema=physical_schema,
        filter=pyarrow_filter if not positional_deletes else None,
        columns=[col.name for col in file_project_schema.columns],
    )
    batches = rg_scanner.to_batches()
    # ... rest of batch processing ...
```

### Important considerations:

1. **Positional deletes**: When `positional_deletes` is not None, the filter is already disabled at the Scanner level (line 1653). Row-group pruning should also be skipped in this case since positional delete indices are file-global and depend on knowing the exact row offsets.

2. **File-like object compatibility**: Verified that `split_by_row_group()` works when the fragment is created from a file-like object (`make_fragment(fin)`). Tested with Azure ADLS (`AzureBlobFile`) — it is seekable, PyArrow reads the Parquet footer correctly, and `split_by_row_group(filter=...)` prunes row groups based on column statistics. S3 and GCS file objects are also seekable, so this should work across all major cloud storage backends. For `FsspecInputFile` (the default IO for cloud storage), `open()` returns a seekable file. For `PyArrowFile`, `open(seekable=True)` returns a `pyarrow.NativeFile` via `open_input_file()`, which is also seekable.

3. **Fallback**: If the Parquet file has no column statistics (e.g., written by engines that don't produce them), `split_by_row_group` returns all row groups — verified this degrades gracefully.

4. **The "Temporary fix until PyArrow 21" block** (lines 1674–1682): This re-applies the filter on every batch in Python. After row-group pruning, this is still needed for correctness (row-group stats are ranges, not exact filters), but it will process far fewer rows.

## Files to modify

- `pyiceberg/io/pyarrow.py` — `_task_to_record_batches` function (line 1604)

## How to test

1. Create an Iceberg table with a partition spec and `write.parquet.row-group-limit` set low (e.g., 500)
2. Write data pre-sorted by a non-partition column (e.g., cnpj) so row groups have non-overlapping min/max ranges
3. Scan with `row_filter=EqualTo('cnpj', '<value>')` and measure:
   - Number of record batches yielded (should be ~1 instead of all row groups)
   - Wall-clock time (should be proportionally faster)
4. Compare against unsorted data (same row count) to confirm no regression

## Related upstream issues

- [apache/iceberg-python#30](https://github.com/apache/iceberg-python/issues/30) — Expose PyIceberg table as PyArrow Dataset (open, broader scope)
- [apache/iceberg-python#1295](https://github.com/apache/iceberg-python/issues/1295) — `In` filter grabs irrelevant row groups (closed as stale, never fixed)
- [apache/iceberg#6567](https://github.com/apache/iceberg/issues/6567) — File-level filtering on non-partition columns (closed, fixed — but only file-level, not row-group-level)
- [apache/arrow#1426](https://github.com/apache/arrow/issues/1426) — PyArrow support for row group filters
