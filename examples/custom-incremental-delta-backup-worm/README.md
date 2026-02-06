# Incremental Delta Table Backup WORM enabled

A lean sample script to incrementally backup a delta-table ensuring consistent
backups of individual delta-table versions while ensuring WORM (Write once, Read many)
compatibility. This is meant for backups residing in immutable storage, e.g. Azure Blob
Storage with immutable policies enabled.


## Why?

There are usually two approaches to back up a delta-table: Deep Clone & replication using 3rd party /
(cloud-provider) native tooling, e.g. azcopy or others operating on the file-level.
Advantage of Deep Cloning is that it is able to work incrementally and provides ACID compliant consistency
based on the delta-log. Drawback in the scenario of WORM is, that it potentially needs to modify existing files
in the storage backend which would break backups into such storage backends.
Tooling that operates on the file-level brings more flexibility for WORM compatibility, but in case a table
is modified during a backup, the backup would be inconsistent.

This approach outlined in the sample script would combine the two approaches:
- WORM compatibility due to append-only file-based backup
- incremental and consistent backup by leveraging the delta-log 


## What it does

- Compares source vs. backup `_delta_log` versions to enable incremental backups
- For missing versions, lists data files via [delta-rs](https://delta-io.github.io/delta-rs/)
- Copies version json, checkpoint parquet, and data parquet files
- Skips files that already exists in the backup (e.g. when referenced by multiple versions)
- Does not write `_last_checkpoint` in the backup (ensure WORM compatibility)


## How to use

1. Create a delta-table to backup
2. Modify source and backup paths in the script as you wish
3. Run the script
4. Compare the source delta-table with the backup delta-table with
```python
source_df.subtract(backup_df).show()
```


## Notes

- This sample is currently working with local files. This may be updated to support other
storage backends (e.g. Azure Blob Storage)
- This sample currently copies file-by-file from source to backup location. This could be
parallelized for improved efficiency and performance when backing up large tables. 
- Keep source `VACUUM` retention long enough to cover backup lag.
- Never run `OPTIMIZE`, `VACUUM`, or any mutating Delta operations on the backup.
- `_last_checkpoint` is usually updated in place. To stay WORM compatible we skip this file.
This has no impact on readability of the delta-table backup. `_last_checkpoint` is only used by
some readers for better performance.

