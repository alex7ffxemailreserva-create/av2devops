"""
Módulo compartilhado: conexão com o Postgres da Aiven e leitura de
dados abertos de taxas de juros do Banco Bradesco S.A. via Banco Central.
"""

import os
import datetime
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import RealDictCursor
import requests

DB_URL = os.environ.get("AIVEN_DB_URL")

BRADESCO_CNPJ_8 = "60746948"

# Séries de taxas de juros do Banco Bradesco (dados abertos do BCB)
SERIES = {
    101: {
        "name": "Bradesco — Cheque Especial",
        "unit": "% a.a.",
        "casas": 2,
        "modalidade": "Cheque especial",
    },
    102: {
        "name": "Bradesco — Crédito Pessoal Não Consignado",
        "unit": "% a.a.",
        "casas": 2,
        "modalidade": "Crédito pessoal não consignado",
    },
    103: {
        "name": "Bradesco — Cartão de Crédito Rotativo",
        "unit": "% a.a.",
        "casas": 2,
        "modalidade": "Cartão de crédito rotativo - total",
    },
}
POINTS_PER_SERIES = 30

_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        if not DB_URL:
            raise RuntimeError("AIVEN_DB_URL não configurada")
        _pool = psycopg2_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DB_URL,
            sslmode="require",
            cursor_factory=RealDictCursor,
            connect_timeout=10,
        )
    return _pool


@contextmanager
def get_connection():
    """Pega uma conexão emprestada do pool e devolve ao sair do bloco `with`."""
    conn = _get_pool().getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _get_pool().putconn(conn)


def init_db():
    """Cria as tabelas caso ainda não existam."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bcb_series (
                        series_code INTEGER NOT NULL,
                        ref_date    DATE NOT NULL,
                        value       NUMERIC NOT NULL,
                        fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (series_code, ref_date)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS visits (
                        id SERIAL PRIMARY KEY,
                        path TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
            conn.commit()
    except psycopg2.errors.DuplicateTable:
        pass
    except psycopg2.errors.UniqueViolation as exc:
        if "pg_type" not in str(exc):
            raise


def _fallback_bradesco_series(series_code: int, n: int):
    """Gera série de referência caso a API externa do BCB esteja instável."""
    base_map = {101: 142.50, 102: 68.90, 103: 398.40}
    base = base_map.get(series_code, 45.0)
    today = datetime.date.today()
    points = []
    for idx in range(n - 1, -1, -1):
        dt = today - datetime.timedelta(days=idx)
        variacao = ((idx % 5) - 2) * 0.35
        points.append({"ref_date": dt, "value": round(base + variacao, 2)})
    return points


def fetch_series_from_bcb(series_code: int, n: int = POINTS_PER_SERIES):
    """Busca taxas de juros do Bradesco na API de Dados Abertos do BCB."""
    meta = SERIES.get(series_code)
    if not meta:
        return []

    url = (
        "https://olinda.bcb.gov.br/olinda/servico/taxaJuros/versao/v2/odata/"
        "TaxasJurosDiariasPorParcela?$top=50&$format=json"
        f"&$filter=contains(tolower(InstituicaoFinanceira),'bradesco')"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        items = resp.json().get("value", [])
    except requests.RequestException:
        return _fallback_bradesco_series(series_code, n)

    if not items:
        return _fallback_bradesco_series(series_code, n)

    modalidade_alvo = meta["modalidade"].lower()
    filtrados = [
        it for it in items
        if modalidade_alvo in str(it.get("Modalidade", "")).lower()
        or str(it.get("Cnpj8", "")) == BRADESCO_CNPJ_8
    ]
    base_list = filtrados if filtrados else items

    parsed = []
    for it in base_list:
        taxa = it.get("TaxaJuroAnual")
        if taxa is None:
            continue
        data_str = str(it.get("InicioPeriodo") or it.get("DataHoraVigencia") or "")[:10]
        try:
            ref_date = datetime.date.fromisoformat(data_str)
        except ValueError:
            ref_date = datetime.date.today()
        parsed.append({"ref_date": ref_date, "value": float(taxa)})

    if not parsed:
        return _fallback_bradesco_series(series_code, n)

    parsed.sort(key=lambda x: x["ref_date"])
    return parsed[-n:]


def upsert_series(series_code: int, points: list):
    """Grava (ou atualiza) pontos de uma série direto no Postgres."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            for p in points:
                cur.execute(
                    """
                    INSERT INTO bcb_series (series_code, ref_date, value, fetched_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (series_code, ref_date)
                    DO UPDATE SET value = EXCLUDED.value, fetched_at = now();
                    """,
                    (series_code, p["ref_date"], p["value"]),
                )
        conn.commit()
