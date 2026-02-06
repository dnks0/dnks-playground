import re
import shutil
from pathlib import Path
from deltalake import DeltaTable


source_path = Path("./.sample")
backup_path = Path("./.backup")


def version_diff(source: DeltaTable, backup: DeltaTable | None = None) -> list[int]:
    source_versions = set([transaction.get("version") for transaction in source.history()])
    backup_versions = set([transaction.get("version") for transaction in backup.history()]) if backup else set()
    return sorted(source_versions - backup_versions)


def files_for_version(path: Path, version: int,) -> list[str]:
    return DeltaTable(table_uri=path, version=version).file_uris()


def backup() -> None:
    source_log_path = source_path.joinpath("_delta_log")
    backup_log_path = backup_path.joinpath("_delta_log")

    sdt = DeltaTable(table_uri=source_path,)
    bdt = DeltaTable(table_uri=backup_path,) if backup_log_path.exists() else None

    versions_to_backup = version_diff(source=sdt, backup=bdt,)

    backup_log_path.mkdir(parents=True, exist_ok=True)

    for version in versions_to_backup:

        version_str = f"{version:020d}"

        version_file = source_log_path.joinpath(f"{version_str}.json")

        checkpoint_files = [
            p for p in source_log_path.iterdir()
            if p.is_file() and re.match(rf".*{version_str}\.checkpoint(\.\d+)?\.parquet$", str(p))
        ]

        files = files_for_version(path=source_path, version=version,)

        # copy version file
        print(f"copying {version_file} to {backup_log_path.joinpath(Path(version_file).name)}")
        shutil.copy(version_file, backup_log_path.joinpath(Path(version_file).name))

        # copy checkpoint files
        for file in checkpoint_files:
            target = backup_log_path.joinpath(Path(file).name)

            print(f"copying {file} to {target}")
            if target.exists():
                continue
            shutil.copy(file, target)

        # copy data files
        for file in files:
            target = backup_path.joinpath(Path(file).name)

            print(f"copying {file} to {target}")
            if target.exists():
                continue
            shutil.copy(file, target)


if __name__ == "__main__":
    backup()

