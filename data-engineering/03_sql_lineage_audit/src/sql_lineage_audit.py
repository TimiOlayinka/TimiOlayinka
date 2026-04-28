"""Extract simple table lineage edges from SQL files."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path


IDENTIFIER = r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)(?:\.(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)){0,2}'
TARGET_PATTERNS = [
    re.compile(rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+({IDENTIFIER})", re.IGNORECASE),
    re.compile(rf"\bCREATE\s+TEMP(?:ORARY)?\s+TABLE\s+({IDENTIFIER})", re.IGNORECASE),
    re.compile(rf"\bINSERT\s+INTO\s+({IDENTIFIER})", re.IGNORECASE),
]
SOURCE_PATTERN = re.compile(rf"\b(?:FROM|JOIN)\s+({IDENTIFIER})", re.IGNORECASE)
CTE_PATTERN = re.compile(rf"(?:\bWITH|,)\s+({IDENTIFIER})\s+AS\s*\(", re.IGNORECASE)
BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)
LINE_COMMENT_PATTERN = re.compile(r"--.*?$", re.MULTILINE)


@dataclass(frozen=True, order=True)
class LineageEdge:
    source_table: str
    target_table: str
    sql_file: str


def strip_comments(sql: str) -> str:
    without_block_comments = BLOCK_COMMENT_PATTERN.sub(" ", sql)
    return LINE_COMMENT_PATTERN.sub(" ", without_block_comments)


def normalise_identifier(identifier: str) -> str:
    parts = [part.strip().strip('"').lower() for part in identifier.split(".")]
    return ".".join(part for part in parts if part)


def extract_cte_names(sql: str) -> set[str]:
    return {normalise_identifier(match.group(1)).split(".")[-1] for match in CTE_PATTERN.finditer(sql)}


def extract_targets(sql: str) -> set[str]:
    targets: set[str] = set()
    for pattern in TARGET_PATTERNS:
        targets.update(normalise_identifier(match.group(1)) for match in pattern.finditer(sql))
    return targets


def extract_sources(sql: str) -> set[str]:
    cte_names = extract_cte_names(sql)
    sources = set()
    for match in SOURCE_PATTERN.finditer(sql):
        source = normalise_identifier(match.group(1))
        if source.split(".")[-1] not in cte_names:
            sources.add(source)
    return sources


def extract_edges_from_sql(sql: str, sql_file: str) -> list[LineageEdge]:
    clean_sql = strip_comments(sql)
    targets = extract_targets(clean_sql)
    sources = extract_sources(clean_sql)
    return sorted(
        LineageEdge(source_table=source, target_table=target, sql_file=sql_file)
        for target in targets
        for source in sources
        if source != target
    )


def scan_sql_root(root: str | Path) -> list[LineageEdge]:
    root_path = Path(root)
    edges: list[LineageEdge] = []
    for sql_path in sorted(root_path.rglob("*.sql")):
        relative_path = sql_path.relative_to(root_path).as_posix()
        edges.extend(extract_edges_from_sql(sql_path.read_text(encoding="utf-8"), relative_path))
    return sorted(set(edges))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract table lineage edges from a folder of SQL files.")
    parser.add_argument("--root", required=True, help="Root folder to scan recursively for .sql files.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(json.dumps([asdict(edge) for edge in scan_sql_root(args.root)], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
