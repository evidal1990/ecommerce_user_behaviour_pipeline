from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
from typing import Mapping, Sequence

import polars as pl
from psycopg2.extras import Json, execute_values

from database.supabase import SupabaseConnectionManager

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DEFAULT_GOLD_TABLES: dict[str, str] = {
    "kpis_descriptive": "kpis",
    "kpis_behavioral": "kpis",
    "kpis_operational": "kpis",
    "kpis_strategical": "kpis",
}

# Colunas do ``ON CONFLICT (...)`` têm de coincidir *exatamente* com um PRIMARY KEY
# ou UNIQUE *não parcial* na tabela (ordem irrelevante).
KPI_CONFLICT_COLUMNS: tuple[str, ...] = (
    "kpi_name",
    "dimension_name",
    "dimension_value",
    "reference_date",
    "dimensions",
)

# Usa isto apenas se o teu UNIQUE/PK na base **não** incluir ``dimensions``
# (atenção: KPIs como churn/NPS precisam de ``dimensions`` na chave).
KPI_CONFLICT_COLUMNS_WITHOUT_DIMENSIONS: tuple[str, ...] = (
    "kpi_name",
    "dimension_name",
    "dimension_value",
    "reference_date",
)


def _validate_identifier(name: str) -> str:
    if not _IDENT.fullmatch(name):
        raise ValueError(f"Identificador SQL inválido: {name!r}")
    return name


def _cell_for_postgres(col: str, value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if col == "dimensions":
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return Json({})
            return Json(json.loads(s))
        if isinstance(value, dict):
            return Json(value)
        raise TypeError(f"Coluna dimensions: tipo não suportado ({type(value)})")
    return value


def _dataframe_tuples(df: pl.DataFrame) -> list[tuple]:
    columns = df.columns
    rows: list[tuple] = []
    for row in df.iter_rows(named=False):
        rows.append(
            tuple(_cell_for_postgres(col, row[i]) for i, col in enumerate(columns))
        )
    return rows


def _build_insert_sql(
    table_sql: str,
    columns: list[str],
    *,
    conflict_columns: Sequence[str] | None,
    conflict_constraint: str | None = None,
) -> str:
    cols_sql = ", ".join(columns)
    base = f"INSERT INTO {table_sql} ({cols_sql}) VALUES %s"
    if conflict_constraint:
        constr = _validate_identifier(conflict_constraint)
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns)
        return f"{base} ON CONFLICT ON CONSTRAINT {constr} DO UPDATE SET {set_clause}"
    if not conflict_columns:
        return base
    conflict = [_validate_identifier(c) for c in conflict_columns]
    missing = set(conflict) - set(columns)
    if missing:
        raise ValueError(
            f"Colunas de conflito não estão no CSV: {sorted(missing)}. "
            f"Colunas atuais: {columns}"
        )
    conflict_sql = ", ".join(conflict)
    key_set = set(conflict)
    update_cols = [c for c in columns if c not in key_set]
    if not update_cols:
        return f"{base} ON CONFLICT ({conflict_sql}) DO NOTHING"
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    return (
        f"{base} ON CONFLICT ({conflict_sql}) DO UPDATE SET {set_clause}"
    )


class LoadGold:
    """
    Carrega ficheiros CSV de ``data/gold`` para tabelas PostgreSQL (Supabase).

    Pré-requisitos:
      - Variáveis de ambiente da base configuradas (ver ``SupabaseConnectionManager``).
      - Tabelas já criadas no Supabase com colunas alinhadas aos cabeçalhos dos CSV.
      - Modo por defeito na pipeline: ``truncate_first=True`` e ``upsert=False`` (limpa
        cada tabela de destino uma vez e reinsere; não exige UNIQUE para ``ON CONFLICT``).
      - Com ``upsert=True``, é necessário UNIQUE/PK alinhado a ``KPI_CONFLICT_COLUMNS``
        ou o nome da constraint via ``conflict_constraint`` / env
        ``GOLD_KPIS_ON_CONFLICT_CONSTRAINT``.

    Exemplo de UNIQUE se quiseres upsert incremental sem truncar::

        ALTER TABLE kpis ADD CONSTRAINT kpis_business_key
          UNIQUE (kpi_name, dimension_name, dimension_value, reference_date, dimensions);

    Exemplo::

        from dotenv import load_dotenv
        load_dotenv(\".env.production\")

        db = SupabaseConnectionManager()
        try:
            LoadGold(db).load_all()
        finally:
            db.close_all()
    """

    def __init__(
        self,
        db: SupabaseConnectionManager,
        gold_dir: str | Path | None = None,
        table_by_stem: Mapping[str, str] | None = None,
        *,
        conflict_constraint: str | None = None,
    ) -> None:
        self._db = db
        repo_root = Path(__file__).resolve().parents[2]
        self._gold_dir = (
            Path(gold_dir) if gold_dir is not None else repo_root / "data" / "gold"
        )
        self._table_by_stem = (
            dict(table_by_stem)
            if table_by_stem is not None
            else dict(DEFAULT_GOLD_TABLES)
        )
        env_constr = (os.environ.get("GOLD_KPIS_ON_CONFLICT_CONSTRAINT") or "").strip()
        self._conflict_constraint = conflict_constraint or (env_constr or None)

    def load_csv(
        self,
        csv_path: str | Path,
        table_name: str,
        *,
        batch_rows: int = 100,
        truncate_first: bool = False,
        upsert: bool = False,
        conflict_columns: Sequence[str] | None = None,
        conflict_constraint: str | None = None,
    ) -> int:
        """
        Lê um CSV e grava em ``table_name``.

        Com ``upsert=True`` (opcional; exige UNIQUE/PK compatível na tabela):

        - Se ``conflict_constraint`` (ou ``LoadGold(..., conflict_constraint=...)`` /
          env ``GOLD_KPIS_ON_CONFLICT_CONSTRAINT``) estiver definido, usa
          ``ON CONFLICT ON CONSTRAINT <nome>``.
        - Caso contrário usa ``ON CONFLICT (colunas)`` com ``conflict_columns``
          (por defeito :data:`KPI_CONFLICT_COLUMNS`).

        Por defeito ``upsert=False``: ``INSERT`` simples (na pipeline usa
        ``load_all(..., truncate_first=True)`` para substituir o snapshot completo).

        Coluna ``dimensions`` é enviada como ``jsonb`` via ``Json``.
        """
        path = Path(csv_path)
        if not path.is_file():
            raise FileNotFoundError(path)

        table_sql = _validate_identifier(table_name)
        constr = (
            conflict_constraint
            if conflict_constraint is not None
            else self._conflict_constraint
        )
        conflict: Sequence[str] | None
        if not upsert:
            constr = None
            conflict = None
        elif constr:
            conflict = None
        else:
            conflict = (
                tuple(conflict_columns)
                if conflict_columns is not None
                else KPI_CONFLICT_COLUMNS
            )

        inserted = 0
        reader = pl.read_csv_batched(
            path,
            batch_size=batch_rows,
            try_parse_dates=True,
        )

        with self._db.get_cursor() as cur:
            if truncate_first:
                cur.execute(f"TRUNCATE TABLE {_validate_identifier(table_sql)}")

            while True:
                chunks = reader.next_batches(1)
                if not chunks:
                    break
                chunk = chunks[0]
                if chunk.is_empty():
                    continue
                columns = [_validate_identifier(c) for c in chunk.columns]
                tuples = _dataframe_tuples(chunk)
                insert_stmt = _build_insert_sql(
                    table_sql,
                    columns,
                    conflict_columns=conflict,
                    conflict_constraint=constr,
                )
                execute_values(
                    cur, insert_stmt, tuples, page_size=min(10_000, len(tuples))
                )
                inserted += len(tuples)

        return inserted

    def load_all(
        self,
        *,
        truncate_first: bool = False,
        upsert: bool = False,
        conflict_columns: Sequence[str] | None = None,
        conflict_constraint: str | None = None,
    ) -> dict[str, int]:
        """
        Carrega cada ``*.csv`` em ``gold_dir`` que tenha entrada em ``table_by_stem``.

        Se ``truncate_first=True``, faz ``TRUNCATE`` **uma vez** por tabela de destino
        antes de inserir todos os CSV (evita apagar dados já carregados de outro ficheiro
        quando vários stems apontam para a mesma tabela, ex.: ``kpis``).

        Por defeito ``upsert=False`` (só requer UNIQUE na base se usares ``upsert=True``).
        """
        counts: dict[str, int] = {}

        if truncate_first:
            targets = sorted(
                {_validate_identifier(t) for t in self._table_by_stem.values()}
            )
            if targets:
                with self._db.get_cursor() as cur:
                    for t in targets:
                        cur.execute(f"TRUNCATE TABLE {t}")

        for stem, table in sorted(self._table_by_stem.items()):
            path = self._gold_dir / f"{stem}.csv"
            if not path.is_file():
                continue
            counts[stem] = self.load_csv(
                path,
                table,
                truncate_first=False,
                upsert=upsert,
                conflict_columns=conflict_columns,
                conflict_constraint=conflict_constraint,
            )
        return counts


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(".env.production")

    manager = SupabaseConnectionManager()
    try:
        result = LoadGold(manager).load_all(truncate_first=True, upsert=False)
        for name, n in result.items():
            print(f"{name}: {n} linhas")
    finally:
        manager.close_all()
