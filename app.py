# app.py — Habisolute | Agendamentos de Concretagens (Cloud-ready + Win11 UI)
# - Streamlit + PostgreSQL (Supabase) via Secrets (DB_URL) ou SQLite local
# - Login + Usuários (Admin)
# - Auditoria: criado_por / alterado_por + histórico (antes/depois)
# - CNPJ: busca automática (Razão Social / Fantasia / Endereço) via cnpj.ws
# - Status com cores (Agendado azul, Cancelado vermelho, Aguardando amarelo)
#
# Reqs (requirements.txt):
#   streamlit
#   pandas
#   openpyxl
#   requests
#   sqlalchemy
#   psycopg2-binary

import os
import re
import json
import base64
import hashlib
import secrets
import urllib.parse
import socket

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import requests
import streamlit as st


# =============================================================================
# Helpers
# =============================================================================


def _local_tz():
    """Timezone used by the app (default: America/Sao_Paulo)."""
    tzname = os.environ.get("APP_TZ") or os.environ.get("TZ") or "America/Sao_Paulo"
    try:
        return ZoneInfo(tzname)
    except Exception:
        # Fallback to naive local time if ZoneInfo fails
        return None

def _local_now():
    tz = _local_tz()
    return datetime.now(tz) if tz else datetime.now()

def only_digits(s: str) -> str:
    """Return only digits from string (safe for None)."""
    return re.sub(r"\D+", "", str(s or ""))


def fmt_br(value, decimals=2, strip_zeros=True):
    """Formata número para pt-BR (1.234,56) e remove zeros finais."""
    if value is None:
        return ""
    try:
        v = float(value)
    except Exception:
        return str(value)
    s = f"{v:,.{decimals}f}"
    # US -> pt-BR
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    if strip_zeros and "," in s:
        s = s.rstrip("0").rstrip(",")
    return s

def format_numbers_in_df(df):
    """Deixa Volume/FCK/Slump sem aquele monte de zeros nas tabelas."""
    if df is None:
        return df
    try:
        out = df.copy()
    except Exception:
        return df
    if "volume_m3" in out.columns:
        out["volume_m3"] = out["volume_m3"].apply(lambda x: fmt_br(x, 2, True))
    if "fck_mpa" in out.columns:
        out["fck_mpa"] = out["fck_mpa"].apply(lambda x: fmt_br(x, 0, True))
    if "slump_mm" in out.columns:
        out["slump_mm"] = out["slump_mm"].apply(lambda x: fmt_br(x, 0, True))
    return out

def style_status_df(df, status_col="status"):
    """Return a Styler that colors the status column (if present)."""
    if df is None:
        return df
    try:
        import pandas as _pd  # noqa
    except Exception:
        return df
    if getattr(df, "empty", False) or status_col not in getattr(df, "columns", []):
        return df

    def _style(v):
        v = (str(v or "")).strip().lower()
        if v == "agendado":
            return "background-color: rgba(59, 130, 246, .18); color: #1d4ed8; font-weight: 700;"
        if v == "aguardando":
            return "background-color: rgba(234, 179, 8, .22); color: #a16207; font-weight: 700;"
        if v == "confirmado":
            return "background-color: rgba(34, 197, 94, .18); color: #15803d; font-weight: 700;"
        if v == "execução" or v == "execucao":
            return "background-color: rgba(16, 185, 129, .18); color: #047857; font-weight: 700;"
        if v == "concluído" or v == "concluido":
            return "background-color: rgba(148, 163, 184, .22); color: #334155; font-weight: 700;"
        if v == "cancelado":
            return "background-color: rgba(239, 68, 68, .18); color: #b91c1c; font-weight: 700;"
        return ""

    try:
        return df.style.applymap(_style, subset=[status_col])
    except Exception:
        return df

def parse_number(s, default=None):
    """Parse first number from text (accepts comma as decimal). Returns float or default."""
    try:
        if s is None:
            return default
        txt = str(s).strip()
        if txt == "":
            return default
        m = re.search(r"[-+]?\d+(?:[\.,]\d+)?", txt)
        if not m:
            return default
        num = m.group(0).replace(",", ".")
        return float(num)
    except Exception:
        return default


def calc_hora_fim(hora_inicio: str, duracao_min: Optional[int]) -> str:
    """Calcula hora fim (HH:MM) a partir da hora inicial e duração em minutos.
    Retorna string vazia se a entrada for inválida.
    """
    try:
        if not hora_inicio:
            return ""
        parts = str(hora_inicio).strip().split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        mins = int(duracao_min or 0)
        total = (h * 60 + m + mins) % (24 * 60)
        return f"{total // 60:02d}:{total % 60:02d}"
    except Exception:
        return ""


from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Text, ForeignKey, Boolean,
    select, insert, update, text, and_, or_,
    delete,
)

# =============================================================================
# SCHEMA (SQLAlchemy Core)
# =============================================================================
# Obs: Mantemos tipos simples (String/Float/Integer/Boolean) para compatibilidade
# entre SQLite (local) e Postgres/Supabase (nuvem).
metadata = MetaData()

users = Table(
    "users", metadata,
    Column("id", Integer, primary_key=True),
    Column("username", String(80), unique=True, nullable=False),
    Column("name", String(120), nullable=True),
    Column("role", String(40), nullable=True),
    Column("pass_salt", String(64), nullable=False),
    Column("pass_hash", String(128), nullable=False),
    Column("is_active", Boolean, nullable=False, server_default=text("true")),
    Column("created_at", String(40), nullable=True),
    Column("last_login_at", String(40), nullable=True),
)

obras = Table(
    "obras", metadata,
    Column("id", Integer, primary_key=True),
    Column("nome", String(200), nullable=False),
    Column("cliente", String(200), nullable=True),
    Column("cidade", String(120), nullable=True),
    Column("endereco", String(240), nullable=True),
    Column("responsavel", String(120), nullable=True),
    Column("telefone", String(50), nullable=True),
    Column("cnpj", String(40), nullable=True),
    Column("razao_social", String(220), nullable=True),
    Column("nome_fantasia", String(220), nullable=True),
    Column("criado_em", String(40), nullable=True),
    Column("atualizado_em", String(40), nullable=True),
    Column("criado_por", String(120), nullable=True),
    Column("alterado_por", String(120), nullable=True),
)

concretagens = Table(
    "concretagens", metadata,
    Column("id", Integer, primary_key=True),
    Column("obra_id", Integer, ForeignKey("obras.id", ondelete="SET NULL"), nullable=True),

    Column("obra", String(200), nullable=True),
    Column("cliente", String(200), nullable=True),
    Column("cidade", String(120), nullable=True),

    Column("data", String(20), nullable=False),           # YYYY-MM-DD
    Column("hora_inicio", String(10), nullable=True),     # HH:MM
    Column("duracao_min", Integer, nullable=True),

    Column("volume_m3", Float, nullable=True),
    Column("fck_mpa", Float, nullable=True),
    Column("slump_mm", Float, nullable=True),
    Column("slump_txt", Text, nullable=True),

    Column("usina", String(200), nullable=True),
    Column("bomba", String(200), nullable=True),
    Column("equipe", String(200), nullable=True),

    Column("status", String(40), nullable=True),

    Column("observacoes", Text, nullable=True),

    Column("criado_em", String(40), nullable=True),
    Column("criado_por", String(120), nullable=True),
    Column("atualizado_em", String(40), nullable=True),
    Column("alterado_por", String(120), nullable=True),
)

historico = Table(
    "historico", metadata,
    Column("id", Integer, primary_key=True),
    Column("acao", String(80), nullable=False),
    Column("entidade", String(80), nullable=False),
    Column("entidade_id", Integer, nullable=True),
    Column("detalhes", Text, nullable=True),
    Column("usuario", String(120), nullable=True),
    Column("criado_em", String(40), nullable=True),
)
from sqlalchemy.engine import Engine

TZ_LABEL = "America/Sao_Paulo"

# ============================
# Windows 11-ish styling
# ============================
WIN11_CSS = """/* ============================
   Habisolute — Tema (Laranja + Dark Sidebar)
   ============================ */
:root{
  --hab-orange:#ff5a00;
  --hab-orange-2:#f97316;
  --hab-dark:#0b1220;
  --hab-slate:#0f172a;
  --hab-bg:#f8fafc;
  --hab-card:#ffffff;
  --hab-border:rgba(15, 23, 42, .10);
  --hab-text:#0f172a;
  --hab-muted:#64748b;
}



/* Títulos um pouco mais abaixo */
.block-container h1{ margin-top: .25rem !important; }
/* Fundo geral */
.stApp{
  background: linear-gradient(180deg, #fff7ed 0%, #ffffff 40%, #f8fafc 100%) !important;
}

/* Centralização / largura */
 .block-container{
  padding-top: 3.6rem !important;
  padding-bottom: 2.2rem !important;
  max-width: 1200px !important;
}
/* Título das páginas – descer um pouco */
h1, h2, h3{ margin-top: .35rem !important; }


/* Cards “macios” */
div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stMetric"]) {
  background: var(--hab-card);
  border: 1px solid var(--hab-border);
  border-radius: 16px;
  padding: 14px 14px 6px 14px;
  box-shadow: 0 6px 22px rgba(2, 6, 23, .06);
}

/* Sidebar dark */
section[data-testid="stSidebar"] > div{
  background: linear-gradient(180deg, var(--hab-dark) 0%, var(--hab-slate) 100%) !important;
  border-right: 1px solid rgba(255,255,255,.08);
}
section[data-testid="stSidebar"] *{
  color: rgba(255,255,255,.90) !important;
}
section[data-testid="stSidebar"] a{
  color: rgba(255,255,255,.90) !important;
}

/* Radio do menu */
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label{
  background: rgba(255,255,255,.06);
  border: 1px solid rgba(255,255,255,.10);
  border-radius: 14px;
  padding: .55rem .75rem;
  margin-bottom: .35rem;
}
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label:hover{
  border-color: rgba(255,90,0,.85);
  box-shadow: 0 0 0 3px rgba(255,90,0,.18);
}
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label:has(input:checked){
  background: rgba(255,90,0,.20);
  border-color: rgba(255,90,0,.95);
}

/* Inputs – mais visíveis */
div[data-baseweb="input"] > div,
div[data-baseweb="textarea"] > div,
div[data-baseweb="select"] > div{
  background: #ffffff !important;
  border-radius: 12px !important;
  border: 1px solid var(--hab-border) !important;
  box-shadow: 0 1px 0 rgba(2, 6, 23, .02);
}
div[data-baseweb="input"] input,
div[data-baseweb="textarea"] textarea{
  color: var(--hab-text) !important;
}
label, .stMarkdown, .stTextInput label, .stNumberInput label, .stSelectbox label{
  color: var(--hab-text) !important;
}

/* Inputs na sidebar (ficam escuros) */
section[data-testid="stSidebar"] div[data-baseweb="input"] > div,
section[data-testid="stSidebar"] div[data-baseweb="textarea"] > div,
section[data-testid="stSidebar"] div[data-baseweb="select"] > div{
  background: rgba(255,255,255,.08) !important;
  border-color: rgba(255,255,255,.14) !important;
}
section[data-testid="stSidebar"] div[data-baseweb="input"] input,
section[data-testid="stSidebar"] div[data-baseweb="textarea"] textarea{
  color: rgba(255,255,255,.92) !important;
}

/* Focus (borda laranja) */
div[data-baseweb="input"] > div:focus-within,
div[data-baseweb="textarea"] > div:focus-within,
div[data-baseweb="select"] > div:focus-within{
  border-color: rgba(255,90,0,.90) !important;
  box-shadow: 0 0 0 3px rgba(255,90,0,.22) !important;
}

/* Botões */
.stButton>button{
  border-radius: 14px !important;
  font-weight: 800 !important;
  border: 1px solid rgba(255,90,0,.30) !important;
}
.stButton>button[kind="primary"],
.stButton>button[data-testid="baseButton-primary"]{
  background: var(--hab-orange) !important;
  color: white !important;
}
.stButton>button[kind="primary"]:hover,
.stButton>button[data-testid="baseButton-primary"]:hover{
  background: var(--hab-orange-2) !important;
  box-shadow: 0 8px 20px rgba(255,90,0,.22) !important;
}

/* Dataframes */
div[data-testid="stDataFrame"]{
  border-radius: 16px !important;
  overflow: hidden !important;
  border: 1px solid var(--hab-border) !important;
  box-shadow: 0 10px 24px rgba(2, 6, 23, .06) !important;
}"""

# ============================
# Status + cores
# ============================
STATUS = ["Agendado", "Aguardando", "Confirmado", "Execucao", "Concluido", "Cancelado"]

def status_chip(status: str) -> str:
    s = (status or "").strip()
    cls = "gray"
    if s == "Agendado":
        cls = "blue"
    elif s == "Cancelado":
        cls = "red"
    elif s == "Aguardando":
        cls = "yellow"
    elif s in ("Confirmado", "Execucao"):
        cls = "green"
    elif s == "Concluido":
        cls = "gray"
    return f'<span class="hab-chip {cls}">{s}</span>'

def style_status_df(df: pd.DataFrame) -> "pd.io.formats.style.Styler":
    """Aplica cores por status + formatação numérica BR (sem 'monte de zeros')."""
    color_map = {
        "Agendado": ("#1d4ed8", "#e8efff"),
        "Cancelado": ("#b91c1c", "#ffecec"),
        "Aguardando": ("#92400e", "#fff7ed"),
        "Confirmado": ("#166534", "#eaffea"),
        "Execucao": ("#166534", "#eaffea"),
        "Concluido": ("#374151", "#f1f5f9"),
    }

    def _fmt_num(v, nd=2):
        if v is None:
            return ""
        try:
            fv = float(v)
        except Exception:
            return str(v)
        import math
        if math.isnan(fv):
            return ""
        s = f"{fv:.{nd}f}".replace(".", ",")
        # remove zeros à direita (30,00 -> 30; 100,00 -> 100; 25,50 mantém)
        if "," in s:
            s = s.rstrip("0").rstrip(",")
        return s

    def _apply(col: pd.Series):
        styles = []
        for v in col.astype(str).tolist():
            fg, bg = color_map.get(v, ("#374151", "#f1f5f9"))
            styles.append(f"background-color: {bg}; color: {fg}; font-weight: 700;")
        return styles

    sty = df.style

    # Formatação de números mais "limpa"
    fmt: dict = {}
    if "volume_m3" in df.columns:
        fmt["volume_m3"] = lambda v: _fmt_num(v, 2)
    if "fck_mpa" in df.columns:
        fmt["fck_mpa"] = lambda v: _fmt_num(v, 2)
    if "slump_mm" in df.columns:
        fmt["slump_mm"] = lambda v: _fmt_num(v, 0)
    if "duracao_min" in df.columns:
        fmt["duracao_min"] = lambda v: _fmt_num(v, 0)

    if fmt:
        sty = sty.format(fmt, na_rep="")

    if "status" in df.columns:
        sty = sty.apply(_apply, subset=["status"])

    # Cabeçalho com cara Habisolute
    sty = sty.set_table_styles([
        {"selector": "th", "props": [("background-color", "#ff5a00"), ("color", "white"), ("font-weight", "800")]},
        {"selector": "td", "props": [("border-color", "rgba(15,23,42,.08)")]},
    ])

    return sty


def today_local() -> date:
    return _local_now().date()

def now_iso() -> str:
    return _local_now().strftime("%Y-%m-%d %H:%M:%S")

def to_dt(d: str, h: str) -> datetime:
    return datetime.strptime(f"{d} {h}", "%Y-%m-%d %H:%M")

def ensure_date(x) -> date:
    """Coerce inputs (date/datetime/str/Timestamp) to a `datetime.date`.
    Accepts ISO strings (YYYY-MM-DD) and common BR format (DD/MM/YYYY).
    """
    if x is None:
        return date.today()
    # pandas Timestamp
    try:
        import pandas as _pd
        if isinstance(x, _pd.Timestamp):
            x = x.to_pydatetime()
    except Exception:
        pass
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return date.today()
        # ISO
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            pass
        # BR
        try:
            return datetime.strptime(s[:10], "%d/%m/%Y").date()
        except Exception:
            pass
    # last resort
    try:
        return date.fromisoformat(str(x)[:10])
    except Exception:
        return date.today()

def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return max(a_start, b_start) < min(a_end, b_end)

# ============================
# DB (SQLite local OR Postgres via DB_URL in secrets)
# ============================

def _ensure_sslmode_require(db_url: str) -> str:
    """Supabase Postgres requires SSL. If sslmode is not present, append sslmode=require."""
    if not db_url:
        return db_url
    u = db_url.strip()
    if u.startswith("postgresql://") or u.startswith("postgres://"):
        if "sslmode=" in u:
            return u
        joiner = "&" if "?" in u else "?"
        return u + f"{joiner}sslmode=require"
    return u

def _safe_db_host(db_url: str) -> str:
    """Return a redacted host string for debugging without leaking credentials."""
    try:
        pr = urllib.parse.urlparse(db_url)
        host = pr.hostname or ""
        port = pr.port or ""
        return f"{host}:{port}" if port else host
    except Exception:
        return ""


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    """Cria e cacheia o Engine SQLAlchemy (SQLite local ou Postgres via secrets/env).

    Melhorias incluídas:
    - fallback para variáveis de ambiente (DB_URL/DATABASE_URL)
    - pool_pre_ping/pool_recycle para conexões mais estáveis
    - opcional: força resolução IPv4 (útil em alguns Windows/DNS) quando psycopg2 estiver disponível
    """
    # 1) Fonte da URL do banco
    db_url = None
    try:
        db_url = (
            st.secrets.get("DB_URL")
            or st.secrets.get("db_url")
            or st.secrets.get("DATABASE_URL")
            or st.secrets.get("database_url")
        )
    except Exception:
        db_url = None

    if not db_url:
        db_url = os.environ.get("DB_URL") or os.environ.get("DATABASE_URL")
    # For Supabase/Postgres on cloud, SSL is usually required.
    if db_url and db_url.startswith('postgres') and 'sslmode=' not in db_url:
        sep = '&' if '?' in db_url else '?'
        db_url = db_url + f"{sep}sslmode=require"

    # Supabase pooler strings sometimes omit the '+psycopg2' driver; SQLAlchemy handles it.

    # 2) Se tiver URL explícita, usa-a
    if db_url:
        db_url = db_url.strip()
        if db_url.startswith("postgres"):
            db_url = _ensure_sslmode_require(db_url)

            # --- opcional: forçar IPv4 via creator (reduz falhas DNS/IPv6 em alguns ambientes) ---
            force_ipv4 = os.environ.get("HABI_FORCE_IPV4", "1").strip().lower() not in ("0", "false", "no")
            if force_ipv4:
                try:
                    import psycopg2  # type: ignore
                    u = urllib.parse.urlparse(db_url)
                    host = u.hostname or ""
                    port = int(u.port or 5432)

                    ipv4 = None
                    try:
                        # 1) try direct IPv4 lookup
                        for res in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM):
                            ipv4 = res[4][0]
                            break
                    except Exception:
                        # 2) fallback: lookup any family and pick the first IPv4
                        try:
                            for res in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
                                if res and res[0] == socket.AF_INET:
                                    ipv4 = res[4][0]
                                    break
                        except Exception:
                            pass

                    if ipv4:
                        user = urllib.parse.unquote(u.username or "")
                        pwd = urllib.parse.unquote(u.password or "")
                        dbname = (u.path or "").lstrip("/")
                        q = dict(urllib.parse.parse_qsl(u.query))
                        sslmode = q.get("sslmode", "require")

                        def _creator():
                            return psycopg2.connect(
                                dbname=dbname,
                                user=user,
                                password=pwd,
                                host=ipv4,
                                connect_timeout=int(os.environ.get('DB_CONNECT_TIMEOUT','10')),
                                port=port,
                                sslmode=sslmode,
                            )

                        return create_engine(
                            db_url,
                            future=True,
                            pool_pre_ping=True,
                            pool_recycle=3600,
                            creator=_creator,
                        )
                except Exception:
                    pass

            return create_engine(db_url, future=True, pool_pre_ping=True, pool_recycle=3600)

        # SQLite ou outro driver
        if db_url.startswith("sqlite"):
            return create_engine(db_url, future=True, connect_args={"check_same_thread": False})
        return create_engine(db_url, future=True, pool_pre_ping=True)

    # 3) Padrão local (SQLite)
    return create_engine("sqlite:///agendamentos.db", future=True, connect_args={"check_same_thread": False})
def migrate_schema(eng):
    """Aplica migrações simples (ADD COLUMN) quando o banco já existe com schema antigo.
    Não remove nem altera colunas existentes.
    """
    try:
        from sqlalchemy import inspect
    except Exception:
        return

    dialect = getattr(eng.dialect, "name", "")
    insp = inspect(eng)

    def existing_cols(table_name: str) -> set:
        try:
            return {c["name"] for c in insp.get_columns(table_name)}
        except Exception:
            return set()

    # Tipos por dialeto (suficiente para Postgres e SQLite)
    def t_varchar(n=255):
        return f"VARCHAR({n})" if dialect != "sqlite" else "TEXT"

    def t_text():
        return "TEXT"

    def t_float():
        return "DOUBLE PRECISION" if dialect != "sqlite" else "REAL"

    def t_int():
        return "INTEGER"

    def t_ts():
        return "TIMESTAMP" if dialect != "sqlite" else "TEXT"

    desired = {
        "obras": {
            "nome": t_varchar(255),
            "cliente": t_varchar(255),
            "cnpj": t_varchar(40),
            "endereco": t_varchar(255),
            "cidade": t_varchar(120),
            "uf": t_varchar(8),
            "responsavel": t_varchar(120),
            "telefone": t_varchar(40),
            "email": t_varchar(120),
            "observacoes": t_text(),
            "criado_em": t_ts(),
            "atualizado_em": t_ts(),
            "criado_por": t_varchar(80),
            "alterado_por": t_varchar(80),
        },
        "concretagens": {
            "obra_id": t_int(),
            "obra": t_varchar(255),
            "cliente": t_varchar(255),
            "cidade": t_varchar(120),
            "data": t_varchar(20),
            "hora_inicio": t_varchar(10),
            "hora_fim": t_varchar(10),
            "volume": t_float(),
            "fornecedor": t_varchar(255),
            "fck": t_float(),
            "slump": t_float(),
            "tipo_servico": t_varchar(120),
            "observacoes": t_text(),
            "criado_em": t_ts(),
            "atualizado_em": t_ts(),
            "criado_por": t_varchar(80),
            "alterado_por": t_varchar(80),
        },
        "users": {
            "username": t_varchar(80),
            "pass_hash": t_varchar(255),
            "role": t_varchar(30),
            "is_active": "BOOLEAN" if dialect != "sqlite" else "INTEGER",
            "created_at": t_ts(),
        },
    }

    with eng.begin() as con:
        for tbl, cols in desired.items():
            have = existing_cols(tbl)
            if not have:
                continue
            for col, sqltype in cols.items():
                if col in have:
                    continue
                stmt = f'ALTER TABLE "{tbl}" ADD COLUMN "{col}" {sqltype}'
                try:
                    con.execute(text(stmt))
                except Exception:
                    pass

def migrate_schema(eng):
    """Best-effort schema migrations (keeps app working on existing DBs)."""
    try:
        with eng.begin() as conn:
            if eng.dialect.name == "sqlite":
                # SQLite: no IF NOT EXISTS for ADD COLUMN in older versions
                rows = conn.execute(text("PRAGMA table_info(concretagens)")).fetchall()
                cols = {r[1] for r in rows}
                if "slump_txt" not in cols:
                    conn.execute(text("ALTER TABLE concretagens ADD COLUMN slump_txt TEXT"))
            else:
                # Postgres
                conn.execute(text("ALTER TABLE concretagens ADD COLUMN IF NOT EXISTS slump_txt TEXT"))
    except Exception:
        # If migration fails, app still runs; worst case: we just won’t store slump_txt.
        pass

def init_db():
    eng = get_engine()
    # Testa conexão antes de tentar criar tabelas (evita crash "mudo" no Cloud)
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        st.error("❌ Não consegui conectar no banco Postgres (Supabase).")
        st.caption(f"Detalhe técnico: {type(e).__name__}: {str(e)[:300]}")
        host = ""
        try:
            raw = None
            try:
                raw = st.secrets.get("DB_URL", None)
            except Exception:
                raw = None
            if raw:
                host = _safe_db_host(_ensure_sslmode_require(str(raw).strip()))
        except Exception:
            host = ""
        if host:
            st.caption(f"Host do DB (sem senha): {host}")
        st.markdown("""
**Como corrigir (2 minutos):**
1) No Streamlit Cloud → **Manage app → Settings → Secrets**
2) Garanta que seu TOML tenha a linha:
```toml
DB_URL="postgresql://usuario:SENHA@HOST:PORT/DB?sslmode=require"
```
3) Se sua senha tem caracteres especiais (ex: `@ # : /`), use a string do botão **Connect** do Supabase (Pooler/Supavisor), ou faça URL-encode desses caracteres.
4) Clique em **Save** e depois **Reboot** no app.

Se preferir, use o **Pooler (Supavisor)** no Supabase → Connect (recomendado para apps em nuvem).\n\n**Dica prática (resolve 90% dos casos no Streamlit Cloud):**\n- No Supabase → *Connect* → copie a string **Transaction pooler** (ou Session pooler)\n- Cole no DB_URL do Secrets (ela costuma usar host do tipo `aws-...pooler.supabase.com` e porta `6543`)\n
""")
        st.stop()

    metadata.create_all(eng)
    migrate_schema(eng)
    ensure_default_admin()

def df_from_rows(rows, cols) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=cols)

def fetch_df(stmt) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        res = conn.execute(stmt)
        rows = res.fetchall()
        cols = res.keys()
    return df_from_rows(rows, cols)

def exec_stmt(stmt) -> int:
    eng = get_engine()
    with eng.begin() as conn:
        res = conn.execute(stmt)
        try:
            pk = res.inserted_primary_key
            return int(pk[0]) if pk and pk[0] is not None else 0
        except Exception:
            return 0

def fetch_one(stmt) -> Optional[Dict[str, Any]]:
    df = fetch_df(stmt)
    if df.empty:
        return None
    return df.iloc[0].to_dict()

# ============================
# Password hashing (PBKDF2)
# ============================
def _pbkdf2_hash(password: str, salt_b64: str) -> str:
    salt = base64.b64decode(salt_b64.encode("utf-8"))
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return base64.b64encode(dk).decode("utf-8")

def make_password(password: str) -> Tuple[str, str]:
    salt = secrets.token_bytes(16)
    salt_b64 = base64.b64encode(salt).decode("utf-8")
    ph = _pbkdf2_hash(password, salt_b64)
    return salt_b64, ph

def verify_password(password: str, salt_b64: str, ph_b64: str) -> bool:
    return _pbkdf2_hash(password, salt_b64) == ph_b64

# ============================
# Users / Auth
# ============================
def ensure_default_admin():
    df = fetch_df(select(users.c.id).limit(1))
    if df.empty:
        salt, ph = make_password("admin123")
        exec_stmt(insert(users).values(
            username="admin", name="Administrador", role="admin",
            pass_salt=salt, pass_hash=ph, is_active=True,
            created_at=now_iso(), last_login_at=None
        ))

def get_user(username: str) -> Optional[Dict[str, Any]]:
    return fetch_one(select(users).where(users.c.username == username))

def list_users() -> pd.DataFrame:
    return fetch_df(select(
        users.c.id, users.c.username, users.c.name, users.c.role,
        users.c.is_active, users.c.created_at, users.c.last_login_at
    ).order_by(users.c.id.desc()))

def create_user(username: str, name: str, role: str, password: str):
    salt, ph = make_password(password)
    exec_stmt(insert(users).values(
        username=username, name=name, role=role,
        pass_salt=salt, pass_hash=ph,
        is_active=True, created_at=now_iso(), last_login_at=None
    ))

def set_user_active(user_id: int, active: bool):
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(update(users).where(users.c.id == int(user_id)).values(is_active=bool(active)))

def reset_user_password(user_id: int, new_password: str):
    salt, ph = make_password(new_password)
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(update(users).where(users.c.id == int(user_id)).values(pass_salt=salt, pass_hash=ph))

def update_last_login(username: str):
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(update(users).where(users.c.username == username).values(last_login_at=now_iso()))

def current_user() -> str:
    return st.session_state.get("user", {}).get("username", "desconhecido")

def current_role() -> str:
    return st.session_state.get("user", {}).get("role", "user")

def login_box():
    st.sidebar.markdown("### 🔐 Login")
    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user:
        st.sidebar.success(f"Logado: {st.session_state.user['username']}")
        st.sidebar.caption(f"Perfil: {st.session_state.user['role']}")
        if st.sidebar.button("Sair", use_container_width=True):
            st.session_state.user = None
            st.rerun()
        return

    u = st.sidebar.text_input("Usuário", key="login_u")
    p = st.sidebar.text_input("Senha", type="password", key="login_p")

    if st.sidebar.button("Entrar", use_container_width=True, type="primary"):
        user = get_user(u.strip())
        if not user or not bool(user.get("is_active", False)):
            st.sidebar.error("Usuário inválido ou inativo.")
            return
        if not verify_password(p, user["pass_salt"], user["pass_hash"]):
            st.sidebar.error("Senha inválida.")
            return
        st.session_state.user = {"id": user["id"], "username": user["username"], "role": user["role"], "name": user.get("name") or ""}
        update_last_login(user["username"])
        st.rerun()

def require_login():
    if not st.session_state.get("user"):
        st.stop()

# ============================
# CNPJ lookup (BrasilAPI + fallbacks)
# ============================

@st.cache_data(ttl=24*3600, show_spinner=False)
def fetch_cnpj_data(cnpj: str):
    """Busca dados públicos de CNPJ com fallback.

    Retorna: (ok: bool, msg: str, payload: dict|None)
    payload contém chaves:
      - cnpj, razao_social, nome_fantasia, endereco, cidade, uf, cep, cliente_sugerido
    """
    cnpj_digits = only_digits(cnpj or "")
    if len(cnpj_digits) != 14:
        return False, "CNPJ inválido (precisa ter 14 dígitos).", None

    if requests is None:
        return False, "Biblioteca 'requests' não está disponível no ambiente.", None

    headers = {
        "User-Agent": "Mozilla/5.0 (Streamlit; +https://streamlit.io)",
        "Accept": "application/json, text/plain, */*",
    }

    def _mk_payload(parsed: dict) -> dict:
        # garante todas as chaves esperadas
        return {
            "cnpj": parsed.get("cnpj") or cnpj_digits,
            "razao_social": parsed.get("razao_social") or "",
            "nome_fantasia": parsed.get("nome_fantasia") or "",
            "endereco": parsed.get("endereco") or "",
            "cidade": parsed.get("cidade") or "",
            "uf": parsed.get("uf") or "",
            "cep": only_digits(parsed.get("cep") or "")[:8],
            "cliente_sugerido": parsed.get("cliente_sugerido") or "",
        }

    def _parse_brasilapi(j: dict) -> dict:
        # BrasilAPI (cnpj/v1)
        legal_name = (j.get("razao_social") or "").strip()
        trade_name = (j.get("nome_fantasia") or "").strip()
        logradouro = (j.get("logradouro") or "").strip()
        numero = (j.get("numero") or "").strip()
        complemento = (j.get("complemento") or "").strip()
        bairro = (j.get("bairro") or "").strip()
        municipio = (j.get("municipio") or "").strip()
        uf = (j.get("uf") or "").strip()
        cep = (j.get("cep") or "").strip()
        parts = [p for p in [logradouro, numero, complemento, bairro] if p]
        endereco = ", ".join(parts)
        cliente = trade_name or legal_name
        return {
            "cnpj": cnpj_digits,
            "razao_social": legal_name,
            "nome_fantasia": trade_name,
            "endereco": endereco,
            "cidade": municipio,
            "uf": uf,
            "cep": cep,
            "cliente_sugerido": cliente,
        }

    def _parse_cnpjws(j: dict) -> dict:
        # publica.cnpj.ws
        # Estrutura pode variar; usamos campos defensivos
        estab = j.get("estabelecimento") or {}
        legal_name = (j.get("razao_social") or j.get("nome") or "").strip()
        trade_name = (estab.get("nome_fantasia") or j.get("nome_fantasia") or "").strip()
        logradouro = (estab.get("logradouro") or "").strip()
        numero = (estab.get("numero") or "").strip()
        complemento = (estab.get("complemento") or "").strip()
        bairro = (estab.get("bairro") or "").strip()
        municipio = (estab.get("cidade", {}).get("nome") if isinstance(estab.get("cidade"), dict) else estab.get("cidade")) or ""
        municipio = str(municipio).strip()
        uf = (estab.get("estado", {}).get("sigla") if isinstance(estab.get("estado"), dict) else estab.get("estado")) or ""
        uf = str(uf).strip()
        cep = (estab.get("cep") or "").strip()
        parts = [p for p in [logradouro, numero, complemento, bairro] if p]
        endereco = ", ".join(parts)
        cliente = trade_name or legal_name
        return {
            "cnpj": cnpj_digits,
            "razao_social": legal_name,
            "nome_fantasia": trade_name,
            "endereco": endereco,
            "cidade": municipio,
            "uf": uf,
            "cep": cep,
            "cliente_sugerido": cliente,
        }

    def _parse_receitaws(j: dict) -> dict:
        # receitaws.com.br
        legal_name = (j.get("nome") or j.get("razao_social") or "").strip()
        trade_name = (j.get("fantasia") or j.get("nome_fantasia") or "").strip()
        logradouro = (j.get("logradouro") or "").strip()
        numero = (j.get("numero") or "").strip()
        complemento = (j.get("complemento") or "").strip()
        bairro = (j.get("bairro") or "").strip()
        municipio = (j.get("municipio") or "").strip()
        uf = (j.get("uf") or "").strip()
        cep = (j.get("cep") or "").strip()
        parts = [p for p in [logradouro, numero, complemento, bairro] if p]
        endereco = ", ".join(parts)
        cliente = trade_name or legal_name
        return {
            "cnpj": cnpj_digits,
            "razao_social": legal_name,
            "nome_fantasia": trade_name,
            "endereco": endereco,
            "cidade": municipio,
            "uf": uf,
            "cep": cep,
            "cliente_sugerido": cliente,
        }

    providers = [
        ("BrasilAPI", f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_digits}", _parse_brasilapi),
        ("CNPJ.ws", f"https://publica.cnpj.ws/cnpj/{cnpj_digits}", _parse_cnpjws),
        ("ReceitaWS", f"https://www.receitaws.com.br/v1/cnpj/{cnpj_digits}", _parse_receitaws),
    ]

    last_err = None
    for name, url, parser in providers:
        try:
            r = requests.get(url, headers=headers, timeout=12)
            # alguns provedores respondem com html/text em erros
            ct = (r.headers.get("content-type") or "").lower()
            if r.status_code == 200 and ("json" in ct or r.text.strip().startswith("{")):
                j = r.json()
                # ReceitaWS retorna {"status":"ERROR", "message":...}
                if isinstance(j, dict) and str(j.get("status", "")).upper() == "ERROR":
                    last_err = f"{name}: {j.get('message') or 'erro'}"
                    continue
                parsed = parser(j if isinstance(j, dict) else {})
                return True, "OK", _mk_payload(parsed)

            # 404 = não achou nesse provedor (tenta o próximo)
            if r.status_code in (404, 429, 500, 502, 503, 504):
                last_err = f"{name}: HTTP {r.status_code}"
                continue

            last_err = f"{name}: HTTP {r.status_code}"
        except Exception as e:
            last_err = f"{name}: {type(e).__name__}: {e}"

    return False, f"Não foi possível consultar o CNPJ. ({last_err or 'sem detalhes'})", None

def calc_trucks(volume_m3: float, capacidade_m3: float = 8.0) -> int:
    if volume_m3 <= 0:
        return 0
    import math
    return int(math.ceil(volume_m3 / capacidade_m3))

def default_duration_min(volume_m3: float) -> int:
    trucks = calc_trucks(volume_m3, 8.0)
    return int(60 + trucks * 12)

def get_obras_df() -> pd.DataFrame:
    return fetch_df(select(
        obras.c.id, obras.c.nome, obras.c.cliente, obras.c.cidade,
        obras.c.endereco, obras.c.responsavel, obras.c.telefone,
        obras.c.cnpj, obras.c.razao_social, obras.c.nome_fantasia,
        obras.c.criado_em
    ).order_by(obras.c.id.desc()))

def get_concretagens_df(range_start, range_end) -> pd.DataFrame:
    range_start = ensure_date(range_start)
    range_end = ensure_date(range_end)
    ds = range_start.isoformat()
    de = range_end.isoformat()

    eng = get_engine()
    sql = text("""
        SELECT
            c.id,
            c.obra_id,
            o.nome        AS obra,
            o.cliente     AS cliente,
            o.cidade      AS cidade,
            o.responsavel AS responsavel,
            o.telefone    AS telefone,
            c.data,
            c.hora_inicio,
            c.duracao_min,
            c.volume_m3,
            c.usina,
            c.fck_mpa,
            c.slump_mm,
            c.bomba,
            c.equipe,
            c.status,
            c.observacoes,
            c.criado_por as criado_por,
            c.alterado_por as alterado_por,
            c.atualizado_em as atualizado_em,
            c.criado_em as created_at
        FROM concretagens c
        LEFT JOIN obras o ON o.id = c.obra_id
        WHERE c.data >= :ds AND c.data <= :de
        ORDER BY c.data, c.hora_inicio, c.id
    """)
    with eng.connect() as con:
        rows = con.execute(sql, {"ds": ds, "de": de}).mappings().all()

    df = pd.DataFrame(rows)

    # Normalização de nomes de colunas (compatibilidade UI/Admin)
    # Algumas consultas retornam alias em inglês (ex: created_at). Mantemos também o padrão pt-BR.
    if "created_at" in df.columns and "criado_em" not in df.columns:
        df["criado_em"] = df["created_at"]
    if "updated_at" in df.columns and "atualizado_em" not in df.columns:
        df["atualizado_em"] = df["updated_at"]

    if df.empty:
        # garante colunas para não quebrar telas (ex.: dashboard) quando não há registros
        return pd.DataFrame(columns=[
            'id','obra_id','data','hora_inicio','duracao_min','volume_m3','fck_mpa','slump_mm',
            'usina','bomba','equipe','status','observacoes',
            'criado_por','alterado_por','atualizado_em','created_at',
            'obra','cliente','cidade','responsavel','telefone','cnpj'
        ])

    for col in ("duracao_min", "volume_m3", "fck_mpa", "slump_mm"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["hora_fim"] = df.apply(lambda r: calc_hora_fim(r.get("hora_inicio"), r.get("duracao_min")), axis=1)
    return df

def get_next_concretagens_df(days: int = 7) -> pd.DataFrame:
    """Retorna concretagens previstas a partir de hoje (inclusive) pelos próximos `days` dias."""
    try:
        ds = today_local()
    except Exception:
        ds = date.today()
    de = ds + timedelta(days=int(days))
    df = get_concretagens_df(ds, de)

    # ordenação estável para telas (dashboard / agenda)
    sort_cols = [c for c in ("data", "hora_inicio", "obra") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=True, kind="stable")
    return df


def get_concretagem_by_id(cid: int) -> Dict[str, Any]:
    row = fetch_one(select(concretagens).where(concretagens.c.id == int(cid)))
    return row or {}

def delete_concretagem_by_id(cid: int, user: str) -> None:
    """Exclui agendamento (concretagem) e registra no histórico."""
    cid = int(cid)
    before = get_concretagem_by_id(cid)
    if not before:
        return
    # registra histórico antes de remover
    add_history(cid, "DELETE", before, None, user)
    exec_stmt(delete(concretagens).where(concretagens.c.id == cid))


def add_history(concretagem_id: int, action: str, before: Any, after: Any, user: str):
    """Registra auditoria no log genérico `historico`.

    A tabela `historico` deste app é um log simples (acao, entidade, entidade_id, detalhes, usuario, criado_em).
    Aqui salvamos before/after dentro de `detalhes` como JSON.
    """
    detalhes = {"before": before, "after": after}
    exec_stmt(insert(historico).values(
        acao=str(action),
        entidade="concretagens",
        entidade_id=int(concretagem_id),
        detalhes=json.dumps(detalhes, ensure_ascii=False, default=str),
        usuario=str(user or ""),
        criado_em=now_iso()
    ))

def get_history_df(concretagem_id: int) -> pd.DataFrame:
    # Histórias de concretagem ficam registradas no log genérico `historico`
    sql = select(
        historico.c.id,
        historico.c.criado_em,
        historico.c.usuario,
        historico.c.acao,
        historico.c.detalhes,
    ).where(
        (historico.c.entidade == "concretagens") & (historico.c.entidade_id == int(concretagem_id))
    ).order_by(historico.c.id.desc())

    df = fetch_df(sql)

    # parse detalhes (se for JSON)
    if not df.empty and "detalhes" in df.columns:
        def _safe_parse(x):
            try:
                return json.loads(x) if isinstance(x, str) else x
            except Exception:
                return x
        df["detalhes"] = df["detalhes"].apply(_safe_parse)
    return df

def find_conflicts(
    date_iso: str,
    hora_inicio: str,
    duracao_min: int,
    bomba: str = "",
    equipe: str = "",
    ignore_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Detecta conflitos de agenda (sobreposição de horários) para a mesma bomba e/ou equipe no mesmo dia."""
    d = ensure_date(date_iso)

    def _parse_time(t) -> Optional[time]:
        if t is None:
            return None
        if isinstance(t, time):
            return t
        s = str(t).strip()
        if not s:
            return None
        # aceita HH:MM ou HH:MM:SS
        try:
            parts = s.split(":")
            hh = int(parts[0])
            mm = int(parts[1]) if len(parts) > 1 else 0
            ss = int(parts[2]) if len(parts) > 2 else 0
            return time(hh, mm, ss)
        except Exception:
            return None

    t0 = _parse_time(hora_inicio)
    if t0 is None:
        return []

    try:
        dur = int(duracao_min or 0)
    except Exception:
        dur = 0

    new_start = datetime.combine(d, t0)
    new_end = new_start + timedelta(minutes=dur)

    nb = (bomba or "").strip().lower()
    ne = (equipe or "").strip().lower()

    sql = text("""
        SELECT
          c.id, c.obra_id, o.nome AS obra,
          c.data, c.hora_inicio, c.duracao_min,
          c.bomba, c.equipe
        FROM concretagens c
        LEFT JOIN obras o ON o.id = c.obra_id
        WHERE c.data = :d
    """)
    params = {"d": d.isoformat()}
    if ignore_id is not None:
        sql = text(str(sql) + " AND c.id <> :ignore_id")
        params["ignore_id"] = int(ignore_id)

    with get_engine().connect() as con:
        rows = con.execute(sql, params).mappings().all()

    conflicts: List[Dict[str, Any]] = []
    for r in rows:
        # filtra por recurso (bomba/equipe). Se não informar nada, considera qualquer sobreposição do dia.
        reasons = []
        rb = (str(r.get("bomba") or "").strip().lower())
        re = (str(r.get("equipe") or "").strip().lower())
        if nb and rb == nb:
            reasons.append("bomba")
        if ne and re == ne:
            reasons.append("equipe")
        if not (nb or ne):
            reasons = ["agenda"]
        if not reasons:
            continue

        ot = _parse_time(r.get("hora_inicio"))
        if ot is None:
            continue
        od = ensure_date(r.get("data"))
        try:
            odur = int(r.get("duracao_min") or 0)
        except Exception:
            odur = 0
        old_start = datetime.combine(od, ot)
        old_end = old_start + timedelta(minutes=odur)

        if new_start < old_end and old_start < new_end:
            conflicts.append({
                "id": r.get("id"),
                "obra_id": r.get("obra_id"),
                "obra": r.get("obra") or f"Obra #{r.get('obra_id')}",
                "inicio": str(r.get("hora_inicio")),
                "duracao_min": odur,
                "bomba": r.get("bomba"),
                "equipe": r.get("equipe"),
                "reasons": reasons,
            })

    return conflicts

def export_excel(df: pd.DataFrame) -> bytes:
    import io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Agendamentos")
    return output.getvalue()

# ============================
# App start
# ============================
st.set_page_config(page_title="Agendamentos de Concretagens", layout="wide")
st.markdown(f"<style>{WIN11_CSS}</style>", unsafe_allow_html=True)

init_db()

st.markdown(
    f"""
    <div class="hab-card">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;">
        <div>
          <div style="font-size:24px;font-weight:800;">📅 Agendamentos de Concretagens</div>
          <div class="small-muted">Cloud-ready (Supabase/Postgres) • Auditoria • CNPJ automático • Layout Windows 11</div>
        </div>
        <div>
          {status_chip("Agendado")} {status_chip("Aguardando")} {status_chip("Cancelado")}
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True
)

login_box()
require_login()

menu = st.sidebar.radio(
    "Menu",
    ["Dashboard", "Agenda (calendário)", "Novo agendamento", "Agenda (lista)", "Obras", "Histórico", "Admin"],
    index=0
)
if menu == "Dashboard":

    today = today_local()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)

    st.subheader("📌 Próximas concretagens (7 dias)")
    df_next = get_next_concretagens_df(7)
    df_next = format_numbers_in_df(df_next)

    if df_next.empty:
        st.info("Nenhuma concretagem agendada nos próximos 7 dias.")
    else:
        # --- filtros rápidos ---
        all_status = []
        if "status" in df_next.columns:
            all_status = sorted([s for s in df_next["status"].dropna().unique().tolist() if str(s).strip()])

        with st.expander("🔎 Filtros", expanded=False):
            f1, f2 = st.columns([2, 2])
            with f1:
                dash_q = st.text_input(
                    "Buscar (obra / cliente / cidade / usina / equipe)",
                    value="",
                    placeholder="Ex: Tarraf, Ribeirão, Supermix, Equipe 2…",
                    key="dash_q",
                )
            with f2:
                dash_status = st.multiselect(
                    "Status",
                    options=all_status,
                    default=all_status,
                    key="dash_status",
                )

        df_f = df_next.copy()

        if dash_q:
            q = dash_q.strip().lower()
            search_cols = [c for c in ["obra", "cliente", "cidade", "usina", "bomba", "equipe"] if c in df_f.columns]
            if search_cols:
                mask = False
                for c in search_cols:
                    mask = mask | df_f[c].astype(str).str.lower().str.contains(q, na=False)
                df_f = df_f[mask]

        if dash_status and "status" in df_f.columns:
            df_f = df_f[df_f["status"].isin(dash_status)]

        # ordenação padrão
        sort_cols = [c for c in ["data", "hora_inicio"] if c in df_f.columns]
        if sort_cols:
            df_f = df_f.sort_values(sort_cols, ascending=True, na_position="last")

        cols = [
            "data","hora_inicio","obra","cliente","cidade",
            "volume_m3","fck_mpa","slump_mm","usina","bomba","equipe","status","criado_por"
        ]
        cols = [c for c in cols if c in df_f.columns]
        show = df_f[cols].copy()

        st.dataframe(style_status_df(show), use_container_width=True, hide_index=True)

    # ============================
    # Obras (Cadastrar + Editar) + CNPJ
    # ============================
elif menu == "Obras":
    st.subheader("🏗️ Cadastro de Obras")

    mode = st.radio("Modo", ["Cadastrar", "Editar"], horizontal=True)

    df_obras = get_obras_df()

    if mode == "Cadastrar":
        st.markdown("#### ➕ Nova obra")
        st.markdown('<div class="hab-card">', unsafe_allow_html=True)

        cnpj_in = st.text_input("CNPJ (opcional)", value=st.session_state.get("obra_new_cnpj", ""))

        colx1, colx2 = st.columns([1, 1])
        with colx1:
            if st.button("🔎 Buscar dados pelo CNPJ", use_container_width=True, type="primary"):
                ok, msg, payload = fetch_cnpj_data(cnpj_in)
                if not ok:
                    st.error(msg)
                else:
                    st.session_state["obra_new_cnpj"] = payload.get("cnpj", "")
                    st.session_state["obra_new_cliente"] = payload.get("cliente_sugerido", "")
                    st.session_state["obra_new_endereco"] = payload.get("endereco", "")
                    st.session_state["obra_new_cidade"] = payload.get("cidade", "")
                    st.session_state["obra_new_razao"] = payload.get("razao_social", "")
                    st.session_state["obra_new_fantasia"] = payload.get("nome_fantasia", "")
                    st.success("Dados carregados ✅")
                    st.rerun()
        with colx2:
            st.caption("Consulta pública (limite por minuto).")

        with st.form("form_obra_new", clear_on_submit=True):
            nome = st.text_input("Nome da obra *")
            cliente = st.text_input("Cliente (Razão/Nome fantasia)", value=st.session_state.get("obra_new_cliente", ""))
            endereco = st.text_input("Endereço", value=st.session_state.get("obra_new_endereco", ""))
            cidade = st.text_input("Cidade", value=st.session_state.get("obra_new_cidade", ""))
            responsavel = st.text_input("Responsável")
            telefone = st.text_input("Telefone/WhatsApp")

            st.caption("Campos trazidos do CNPJ (se aplicável):")
            razao_social = st.text_input("Razão social", value=st.session_state.get("obra_new_razao", ""))
            nome_fantasia = st.text_input("Nome fantasia", value=st.session_state.get("obra_new_fantasia", ""))
            cnpj_clean = st.text_input("CNPJ (somente números)", value=only_digits(st.session_state.get("obra_new_cnpj", cnpj_in)))

            ok = st.form_submit_button("Salvar obra", use_container_width=True, type="primary")
            if ok:
                if not nome.strip():
                    st.error("Informe o nome da obra.")
                else:
                    new_id = exec_stmt(insert(obras).values(
                        nome=nome.strip(),
                        cliente=cliente.strip(),
                        endereco=endereco.strip(),
                        cidade=cidade.strip(),
                        responsavel=responsavel.strip(),
                        telefone=telefone.strip(),
                        criado_em=_local_now(),
                        cnpj=only_digits(cnpj_clean),
                        razao_social=razao_social.strip(),
                        nome_fantasia=nome_fantasia.strip()
                    ))
                    for k in list(st.session_state.keys()):
                        if k.startswith("obra_new_"):
                            st.session_state.pop(k, None)
                    st.success("Obra cadastrada ✅" + (f" (ID {new_id})" if new_id else ""))
                    try:
                        st.cache_data.clear()
                    except Exception:
                        pass
                    st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

    else:
        st.markdown("#### ✏️ Editar obra")
        if df_obras.empty:
            st.info("Nenhuma obra cadastrada ainda.")
        else:
            labels = df_obras.apply(lambda r: f"#{r['id']} — {r['nome']} ({r.get('cliente','') or 'Sem cliente'})", axis=1).tolist()
            pick = st.selectbox("Selecione a obra", labels)
            obra_id = int(pick.split("—")[0].replace("#", "").strip())

            row = df_obras[df_obras["id"] == obra_id].iloc[0].to_dict()

            st.markdown('<div class="hab-card">', unsafe_allow_html=True)

            cnpj_edit = st.text_input("CNPJ", value=row.get("cnpj") or "", key=f"cnpj_edit_{obra_id}")

            coly1, coly2 = st.columns([1, 1])
            with coly1:
                if st.button("🔎 Atualizar dados pelo CNPJ", use_container_width=True, key=f"btn_cnpj_edit_{obra_id}", type="primary"):
                    ok, msg, payload = fetch_cnpj_data(cnpj_edit)
                    if not ok:
                        st.error(msg)
                    else:
                        st.session_state[f"edit_prefill_{obra_id}"] = payload
                        st.success("Dados do CNPJ carregados ✅")
                        st.rerun()
            with coly2:
                st.caption("Atualiza Cliente/Endereço/Cidade automaticamente.")

            pre = st.session_state.get(f"edit_prefill_{obra_id}", {})
            nome_val = row.get("nome") or ""
            cliente_val = pre.get("cliente_sugerido") or (row.get("cliente") or "")
            endereco_val = pre.get("endereco") or (row.get("endereco") or "")
            cidade_val = pre.get("cidade") or (row.get("cidade") or "")
            razao_val = pre.get("razao_social") or (row.get("razao_social") or "")
            fantasia_val = pre.get("nome_fantasia") or (row.get("nome_fantasia") or "")
            cnpj_val = pre.get("cnpj") or (row.get("cnpj") or "")

            with st.form(f"form_obra_edit_{obra_id}"):
                nome = st.text_input("Nome da obra *", value=nome_val)
                cliente = st.text_input("Cliente (Razão/Nome fantasia)", value=cliente_val)
                endereco = st.text_input("Endereço", value=endereco_val)
                cidade = st.text_input("Cidade", value=cidade_val)
                responsavel = st.text_input("Responsável", value=row.get("responsavel") or "")
                telefone = st.text_input("Telefone/WhatsApp", value=row.get("telefone") or "")
                razao_social = st.text_input("Razão social", value=razao_val)
                nome_fantasia = st.text_input("Nome fantasia", value=fantasia_val)
                cnpj_clean = st.text_input("CNPJ (somente números)", value=only_digits(cnpj_val))

                salvar = st.form_submit_button("Salvar alterações", use_container_width=True, type="primary")
                if salvar:
                    if not nome.strip():
                        st.error("Informe o nome da obra.")
                    else:
                        eng = get_engine()
                        with eng.begin() as conn:
                            conn.execute(update(obras).where(obras.c.id == obra_id).values(
                                nome=nome.strip(),
                                cliente=cliente.strip(),
                                endereco=endereco.strip(),
                                cidade=cidade.strip(),
                                responsavel=responsavel.strip(),
                                telefone=telefone.strip(),
                                cnpj=only_digits(cnpj_clean),
                                razao_social=razao_social.strip(),
                                nome_fantasia=nome_fantasia.strip()
                            ))
                        st.session_state.pop(f"edit_prefill_{obra_id}", None)
                        st.success("Obra atualizada ✅")
                        st.rerun()

            st.markdown('</div>', unsafe_allow_html=True)

    st.divider()
    st.markdown("#### 📚 Obras cadastradas")
    df_obras = get_obras_df()
    if df_obras.empty:
        st.info("Nenhuma obra cadastrada.")
    else:
        show = df_obras[["id","nome","cliente","cidade","cnpj","endereco","responsavel","telefone","criado_em"]].copy()
        st.dataframe(show, use_container_width=True, hide_index=True)

# ============================
# Novo agendamento
# ============================

elif menu == "Agenda (calendário)":
    st.subheader("📅 Agenda (calendário semanal)")

    colw1, colw2, colw3 = st.columns([1.2, 1.0, 1.0])
    with colw1:
        ref_day = st.date_input(
            "Semana de referência",
            value=today,
            help="Selecione qualquer dia da semana que deseja visualizar."
        )
    week_start_cal = ref_day - timedelta(days=ref_day.weekday())
    week_end_cal = week_start_cal + timedelta(days=6)

    with colw2:
        show_done = st.checkbox("Mostrar concluídos/cancelados", value=False)
    with colw3:
        compact = st.checkbox("Modo compacto", value=True)

    obras_df = get_obras_df()
    obra_opts = obras_df["nome"].tolist() if not obras_df.empty else []
    obra_sel = st.multiselect("Filtrar por obras (opcional)", options=obra_opts, default=[])

    default_status = ["Agendado", "Aguardando", "Confirmado", "Execucao"] if not show_done else STATUS
    status_sel = st.multiselect("Status", options=STATUS, default=default_status)

    df_week = get_concretagens_df(week_start_cal.isoformat(), week_end_cal.isoformat())
    if not df_week.empty:
        if obra_sel:
            df_week = df_week[df_week["obra"].isin(obra_sel)].copy()
        if status_sel:
            df_week = df_week[df_week["status"].isin(status_sel)].copy()

    st.caption(
        f"Período: {week_start_cal.strftime('%d/%m/%Y')} a {week_end_cal.strftime('%d/%m/%Y')} ({TZ_LABEL})"
    )

    if df_week.empty:
        st.info("Nenhum agendamento encontrado para os filtros selecionados.")
    else:
        # Pré-calcular conflitos por dia (sobreposição e mesmo recurso bomba/equipe)
        conflicts_ids: set[int] = set()

        def _hhmm(s: str) -> str:
            s = str(s or "")
            return s[:5] if len(s) >= 5 else s

        for d, g in df_week.groupby("data"):
            g2 = g.copy()
            g2 = g2[g2["status"].isin(["Agendado", "Aguardando", "Confirmado", "Execucao"])]
            g2 = g2.sort_values(by=["hora_inicio", "hora_fim"])
            rows = g2.to_dict("records")

            for i in range(len(rows)):
                for j in range(i + 1, len(rows)):
                    a = rows[i]
                    b = rows[j]
                    ai = to_time(a["hora_inicio"])
                    af = to_time(a["hora_fim"])
                    bi = to_time(b["hora_inicio"])
                    bf = to_time(b["hora_fim"])

                    same_resource = False
                    if str(a.get("bomba") or "").strip() and a.get("bomba") == b.get("bomba"):
                        same_resource = True
                    if str(a.get("equipe") or "").strip() and a.get("equipe") == b.get("equipe"):
                        same_resource = True

                    if same_resource and intervals_overlap(ai, af, bi, bf):
                        conflicts_ids.add(int(a["id"]))
                        conflicts_ids.add(int(b["id"]))

        dow = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
        cols = st.columns(7, gap="small")

        for k in range(7):
            day = week_start_cal + timedelta(days=k)
            day_key = day.isoformat()
            day_df = df_week[df_week["data"] == day_key].copy()
            day_df = day_df.sort_values(by=["hora_inicio", "hora_fim"])

            with cols[k]:
                st.markdown(f"#### {dow[k]}")
                st.caption(day.strftime("%d/%m"))
                if day_df.empty:
                    st.caption("—")
                    continue

                total_day = float(day_df["volume_m3"].fillna(0).sum())
                st.caption(f"{len(day_df)} agend. • {total_day:.1f} m³")

                for _, r in day_df.iterrows():
                    rid = int(r["id"])
                    status = str(r["status"])
                    icon = "✅" if status == "Concluido" else ("🟧" if status == "Execucao" else ("❌" if status == "Cancelado" else "🗓️"))
                    warn = " ⚠️" if rid in conflicts_ids else ""
                    title = f"{icon}{warn} {_hhmm(r['hora_inicio'])}–{_hhmm(r['hora_fim'])} • {r['obra']}"
                    st.markdown(f"**{title}**")
                    if compact:
                        st.caption(f"{r.get('volume_m3','')} m³ • {r.get('bomba','')} • {r.get('equipe','')}")
                    else:
                        st.caption(f"Serviço: {r.get('servico','')}")
                        st.caption(f"Volume: {r.get('volume_m3','')} m³")
                        st.caption(f"Bomba/Equipe: {r.get('bomba','')} • {r.get('equipe','')}")
                        st.caption(f"Responsável: {r.get('responsavel','')}")
                        st.caption(f"Status: {status}")

elif menu == "Novo agendamento":
    st.subheader("🧱 Novo agendamento de concretagem")

    df_obras = get_obras_df()
    if df_obras.empty:
        st.warning("Cadastre uma obra primeiro (menu: Obras).")
    else:
        labels = df_obras.apply(lambda r: f"#{r['id']} — {r['nome']} ({r.get('cliente','') or 'Sem cliente'})", axis=1).tolist()
        id_map = {labels[i]: int(df_obras.iloc[i]["id"]) for i in range(len(labels))}

        st.markdown('<div class="hab-card">', unsafe_allow_html=True)
        with st.form("form_conc_new"):
            obra_sel = st.selectbox("Obra *", labels)

            cA, cB = st.columns(2)
            with cA:
                d = st.date_input("Data *", value=today)
            with cB:
                h = st.time_input("Hora início *", value=time(8, 0))

            cC, cD = st.columns(2)
            with cC:
                volume = st.number_input("Volume (m³) *", min_value=0.0, value=30.0, step=1.0)
            with cD:
                dur = st.number_input("Duração prevista (min)", min_value=15, value=default_duration_min(30.0), step=5)

            cE, cF = st.columns(2)
            with cE:
                fck = st.number_input("FCK (MPa)", min_value=0.0, value=25.0, step=1.0)
            with cF:
                slump = st.text_input("Abatimento / Slump", value="100±20 mm")

            usina = st.text_input("Usina / Fornecedor", value="")
            bomba = st.text_input("Bomba (ID/placa/empresa)", value="")
            equipe = st.text_input("Equipe (ex: Equipe 1 / Técnico X)", value="")
            status = st.selectbox("Status", STATUS, index=STATUS.index("Agendado"))
            obs = st.text_area("Observações", value="")

            cap = st.number_input("Capacidade caminhão (m³) p/ estimativa", min_value=4.0, value=8.0, step=0.5)
            st.caption(f"Estimativa: **{calc_trucks(volume, cap)} caminhões** (capacidade {cap} m³).")

            salvar = st.form_submit_button("Salvar agendamento", use_container_width=True, type="primary")

            if salvar:
                data_str = d.strftime("%Y-%m-%d")
                hora_str = h.strftime("%H:%M")
                obra_id = id_map[obra_sel]

                conflicts = find_conflicts(data_str, hora_str, int(dur), bomba, equipe, ignore_id=None)
                if conflicts:
                    st.error("Conflito detectado (mesma bomba/equipe no mesmo horário).")
                    st.dataframe(pd.DataFrame(conflicts), use_container_width=True, hide_index=True)
                    st.stop()

                user = current_user()
                now = now_iso()

                new_id = exec_stmt(insert(concretagens).values(
                    obra_id=obra_id,
                    data=data_str,
                    hora_inicio=hora_str,
                    duracao_min=int(dur),
                    volume_m3=float(volume),
                    fck_mpa=float(fck) if fck else None,
                    slump_mm=parse_number(slump, None),
                    slump_txt=(slump.strip() if slump else None),
                    usina=(usina or "").strip(),
                    bomba=(bomba or "").strip(),
                    equipe=(equipe or "").strip(),
                    status=status,
                    observacoes=(obs or "").strip(),
                    criado_em=now,
                    atualizado_em=now,
                    criado_por=user,
                    alterado_por=user
                ))

                after = get_concretagem_by_id(new_id)
                add_history(new_id, "CREATE", None, after, user)
                st.success(f"Agendamento criado ✅ (ID {new_id})")

        st.markdown('</div>', unsafe_allow_html=True)

# ============================
# Agenda (lista) + editar
# ============================
elif menu == "Agenda (lista)":
    st.subheader("📋 Agenda (lista)")

    colf1, colf2, colf3 = st.columns([1, 1, 2])
    with colf1:
        ini = st.date_input("De", value=week_start, key="ini_list")
    with colf2:
        fim = st.date_input("Até", value=week_end, key="fim_list")
    with colf3:
        busca = st.text_input("Busca (obra/cliente/cidade/usina/bomba/equipe)", value="")

    st.markdown("##### Filtros")
    stt = st.multiselect("Status", STATUS, default=STATUS)

    df = get_concretagens_df(ini, fim)
    if df.empty:
        st.info("Nada no período.")
    else:
        if stt:
            df = df[df["status"].isin(stt)]
        if busca.strip():
            b = busca.strip().lower()
            mask = (
                df["obra"].fillna("").str.lower().str.contains(b) |
                df["cliente"].fillna("").str.lower().str.contains(b) |
                df["cidade"].fillna("").str.lower().str.contains(b) |
                df["usina"].fillna("").str.lower().str.contains(b) |
                df["bomba"].fillna("").str.lower().str.contains(b) |
                df["equipe"].fillna("").str.lower().str.contains(b)
            )
            df = df[mask]

        view_cols = [
            "id","data","hora_inicio","duracao_min","obra","cliente","cidade",
            "volume_m3","fck_mpa","slump_mm","usina","bomba","equipe","status",
            "criado_por","alterado_por","atualizado_em","observacoes"
        ]
        view = df[view_cols].copy()
        st.dataframe(style_status_df(view), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("### ✏️ Editar agendamento")
        ids = df["id"].tolist()
        sel_id = st.selectbox("Selecione pelo ID", ids)

        row = df[df["id"] == sel_id].iloc[0].to_dict()
        st.markdown(
            f"""
            <div class="hab-card">
              <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center;">
                <div><b>ID {sel_id}</b> • {row.get('data')} {row.get('hora_inicio')} • <span class="small-muted">{row.get('obra')}</span></div>
                <div>{status_chip(row.get('status'))}</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        with st.form("edit_form"):
            c1, c2 = st.columns(2)
            with c1:
                new_status = st.selectbox("Status", STATUS, index=STATUS.index(row["status"]))
            with c2:
                new_dur = st.number_input("Duração (min)", min_value=15, value=int(row["duracao_min"]), step=5)

            c3, c4 = st.columns(2)
            with c3:
                new_bomba = st.text_input("Bomba", value=str(row.get("bomba") or ""))
            with c4:
                new_equipe = st.text_input("Equipe", value=str(row.get("equipe") or ""))

            c5, c6 = st.columns(2)
            with c5:
                new_usina = st.text_input("Usina", value=str(row.get("usina") or ""))
            with c6:
                new_slump = st.text_input("Slump", value=str(row.get("slump_mm") or ""))

            c7, c8 = st.columns(2)
            with c7:
                new_volume = st.number_input("Volume (m³)", min_value=0.0, value=float(row.get("volume_m3") or 0.0), step=1.0)
            with c8:
                new_fck = st.number_input("FCK (MPa)", min_value=0.0, value=float(row.get("fck_mpa") or 0.0), step=1.0)

            new_obs = st.text_area("Observações", value=str(row.get("observacoes") or ""))

            salvar = st.form_submit_button("Salvar alterações", use_container_width=True, type="primary")

            if salvar:
                before = get_concretagem_by_id(int(sel_id))
                data_str = str(before["data"])
                hora_str = str(before["hora_inicio"])

                conflicts = find_conflicts(data_str, hora_str, int(new_dur), new_bomba, new_equipe, ignore_id=int(sel_id))
                if conflicts:
                    st.error("Conflito detectado (mesma bomba/equipe no mesmo horário).")
                    st.dataframe(pd.DataFrame(conflicts), use_container_width=True, hide_index=True)
                    st.stop()

                user = current_user()
                now = now_iso()

                eng = get_engine()
                with eng.begin() as conn:
                    conn.execute(update(concretagens).where(concretagens.c.id == int(sel_id)).values(
                        status=new_status,
                        duracao_min=int(new_dur),
                        bomba=(new_bomba or "").strip(),
                        equipe=(new_equipe or "").strip(),
                        usina=(new_usina or "").strip(),
                        slump_mm=parse_number(new_slump, None),
                        slump_txt=(new_slump.strip() if new_slump else None),
                        volume_m3=float(new_volume),
                        fck_mpa=float(new_fck) if new_fck else None,
                        observacoes=(new_obs or "").strip(),
                        atualizado_em=now,
                        alterado_por=user
                    ))

                after = get_concretagem_by_id(int(sel_id))
                add_history(int(sel_id), "UPDATE", before, after, user)

                st.success("Atualizado ✅")
                st.rerun()

# ============================
# Histórico
# ============================
elif menu == "Histórico":
    st.subheader("🧾 Histórico de alterações (auditoria)")

    eng = get_engine()
    with eng.connect() as conn:
        df_recent = pd.read_sql(
            text("""
                SELECT c.id, c.data, c.hora_inicio, o.nome AS obra, c.status
                FROM concretagens c
                JOIN obras o ON o.id=c.obra_id
                ORDER BY c.id DESC
                LIMIT 200
            """),
            conn
        )

    if df_recent.empty:
        st.info("Nenhum agendamento ainda.")
    else:
        pick = st.selectbox(
            "Selecione um agendamento",
            df_recent.apply(lambda r: f"ID {r['id']} — {r['data']} {r['hora_inicio']} — {r['obra']} — {r['status']}", axis=1).tolist()
        )
        sel_id = int(pick.split("—")[0].replace("ID", "").strip())

        hist = get_history_df(sel_id)
        if hist.empty:
            st.caption("Sem histórico ainda.")
        else:
            view = hist.copy()
            view = view.rename(columns={"criado_em": "quando", "usuario": "usuário", "acao": "ação"})
            cols_show = [c for c in ["quando", "usuário", "ação"] if c in view.columns]
            st.dataframe(view[cols_show], use_container_width=True, hide_index=True)

            with st.expander("Ver detalhes (antes/depois)", expanded=False):
                for _, row in hist.iterrows():
                    det = row.get("detalhes", {})
                    before = det.get("before") if isinstance(det, dict) else None
                    after = det.get("after") if isinstance(det, dict) else None

                    st.markdown(f"**#{row.get('id')} — {row.get('acao')} — {row.get('criado_em')} — {row.get('usuario')}**")
                    c1, c2 = st.columns(2)
                    with c1:
                        st.caption("Antes")
                        st.json(before or {})
                    with c2:
                        st.caption("Depois")
                        st.json(after or {})
                    with st.expander("🗑️ Excluir agendamento", expanded=False):
                        st.warning("Atenção: a exclusão é permanente e remove o agendamento da agenda.")
                        confirm = st.text_input("Digite EXCLUIR para confirmar", key=f"del_confirm_{sel_id}")
                        can_del = (confirm or "").strip().upper() == "EXCLUIR"
                        if st.button("Confirmar exclusão", key=f"del_btn_{sel_id}", type="primary", disabled=(not can_del)):
                            delete_concretagem_by_id(int(sel_id), current_user())
                            st.success("✅ Agendamento excluído.")
                            try:
                                st.cache_data.clear()
                            except Exception:
                                pass
                            st.rerun()


                    st.divider()

# ============================
# Admin
# ============================
elif menu == "Admin":
    st.subheader("🛠️ Admin")

    if current_role() != "admin":
        st.error("Acesso restrito ao perfil admin.")
        st.stop()

    tab1, tab2, tab3 = st.tabs(["Usuários", "Alterar minha senha", "Exportar"])

    with tab1:
        st.markdown("### 👥 Usuários")
        dfu = list_users()
        st.dataframe(dfu, use_container_width=True, hide_index=True)

        st.markdown("### ➕ Criar usuário")
        with st.form("create_user_form"):
            c1, c2 = st.columns(2)
            with c1:
                username = st.text_input("Usuário (login) *")
            with c2:
                name = st.text_input("Nome")
            c3, c4 = st.columns(2)
            with c3:
                role = st.selectbox("Perfil", ["user", "admin"], index=0)
            with c4:
                password = st.text_input("Senha *", type="password")
            ok = st.form_submit_button("Criar", use_container_width=True, type="primary")

            if ok:
                if not username.strip() or not password:
                    st.error("Informe usuário e senha.")
                else:
                    try:
                        create_user(username.strip(), name.strip(), role, password)
                        st.success("Usuário criado ✅")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Não foi possível criar: {e}")

        st.markdown("### ⚙️ Ativar/Inativar ou Reset de senha")
        if not dfu.empty:
            user_id = st.selectbox("Selecione o ID do usuário", dfu["id"].tolist())
            row = dfu[dfu["id"] == user_id].iloc[0].to_dict()

            cA, cB = st.columns(2)
            with cA:
                active = st.checkbox("Ativo", value=bool(row["is_active"]))
                if st.button("Salvar ativo/inativo", use_container_width=True):
                    set_user_active(int(user_id), active)
                    st.success("Atualizado ✅")
                    st.rerun()
            with cB:
                newpass = st.text_input("Nova senha (reset)", type="password")
                if st.button("Resetar senha", use_container_width=True):
                    if not newpass:
                        st.error("Informe a nova senha.")
                    else:
                        reset_user_password(int(user_id), newpass)
                        st.success("Senha resetada ✅")

    with tab2:
        st.markdown("### 🔑 Alterar minha senha")
        with st.form("change_my_pass"):
            oldp = st.text_input("Senha atual", type="password")
            newp = st.text_input("Nova senha", type="password")
            newp2 = st.text_input("Confirmar nova senha", type="password")
            ok = st.form_submit_button("Alterar", use_container_width=True, type="primary")
            if ok:
                if newp != newp2:
                    st.error("Confirmação não confere.")
                else:
                    u = get_user(current_user())
                    if not u or not verify_password(oldp, u["pass_salt"], u["pass_hash"]):
                        st.error("Senha atual incorreta.")
                    else:
                        reset_user_password(int(u["id"]), newp)
                        st.success("Senha alterada ✅")

    with tab3:
        st.markdown("### 📦 Exportar agendamentos (Excel)")
        c1, c2 = st.columns(2)
        with c1:
            ini = st.date_input("De", value=week_start, key="ini_export")
        with c2:
            fim = st.date_input("Até", value=week_end, key="fim_export")

        df = get_concretagens_df(ini, fim)
        if df.empty:
            st.info("Nada no período.")
        else:
            rep = df[[
                "data","hora_inicio","duracao_min","obra","cliente","cidade",
                "volume_m3","fck_mpa","slump_mm","usina","bomba","equipe","status",
                "criado_por","alterado_por","criado_em","atualizado_em","observacoes"
            ]].copy()
            st.dataframe(style_status_df(rep), use_container_width=True, hide_index=True)
            xlsx = export_excel(rep)
            st.download_button(
                "⬇️ Baixar Excel",
                data=xlsx,
                file_name=f"agendamentos_{ini.strftime('%Y%m%d')}_{fim.strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

st.sidebar.divider()
st.sidebar.caption("Cores: Agendado (azul) • Aguardando (amarelo) • Cancelado (vermelho) ✅")
