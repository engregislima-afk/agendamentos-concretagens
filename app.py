# app.py — Habisolute | Agendamentos de Concretagens (Cloud-ready + Win11 UI)
# - Streamlit + PostgreSQL (Supabase) via Secrets (DB_URL) ou SQLite local
# - Login + Usuários (Admin)
# - Auditoria: criado_por / alterado_por + histórico (antes/depois)
# - CNPJ: busca automática (Razão Social / Fantasia / Endereço) via BrasilAPI / CNPJ.ws / ReceitaWS
# - Status com cores (Agendado azul, Cancelado vermelho, Aguardando amarelo)
#
# Reqs (requirements.txt):
#   streamlit
#   pandas
#   openpyxl
#   requests
#   sqlalchemy
#   psycopg2-binary
#
# ✅ Patch (2026-01-28):
# - Corrige crash no Postgres durante migrate_schema: usa ALTER TABLE IF EXISTS + try/except (não derruba o app)
# - Corrige bug no "Editar agendamento": new_cps -> new_cps_por_cam
# - Corrige KeyError na "Agenda (lista)" quando colunas não existem: seleciona apenas colunas presentes

import os
import io
import re
import json
import base64
import hashlib
import secrets
import urllib.parse
import socket
import math

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from datetime import datetime, date, time, timedelta
import datetime as dt
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import requests
import streamlit as st

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Text, ForeignKey, Boolean,
    select, insert, update, text,
    delete,
)
from sqlalchemy.engine import Engine


# =============================================================================
# Streamlit helpers
# =============================================================================

def uniq_key(prefix: str = "k") -> str:
    """Gera keys únicas por execução para evitar StreamlitDuplicateElementKey."""
    n = st.session_state.get("_uniq_key_counter", 0) + 1
    st.session_state["_uniq_key_counter"] = n
    return f"{prefix}_{n}"


# =============================================================================
# Time helpers
# =============================================================================

def _local_tz():
    """Timezone used by the app (default: America/Sao_Paulo)."""
    tzname = os.environ.get("APP_TZ") or os.environ.get("TZ") or "America/Sao_Paulo"
    try:
        return ZoneInfo(tzname)
    except Exception:
        return None

def _local_now():
    tz = _local_tz()
    return datetime.now(tz) if tz else datetime.now()

def today_local() -> date:
    return _local_now().date()

def now_iso() -> str:
    return _local_now().strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# Formatting helpers
# =============================================================================

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", str(s or ""))

def fmt_br(value, decimals=2, strip_zeros=True):
    if value is None:
        return ""
    try:
        v = float(value)
    except Exception:
        return str(value)
    s = f"{v:,.{decimals}f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    if strip_zeros and "," in s:
        s = s.rstrip("0").rstrip(",")
    return s

def fmt_compact_num(v, decimals: int = 2) -> str:
    try:
        if v is None:
            return ""
        if isinstance(v, str):
            s = v.strip()
            if s == "":
                return ""
            s = s.replace(".", "").replace(",", ".") if ("," in s and s.count(",") == 1 and s.count(".") > 1) else s.replace(",", ".")
            v = float(s)
        fv = float(v)
        if math.isfinite(fv) is False:
            return ""
        if abs(fv - round(fv)) < 1e-9:
            return str(int(round(fv)))
        return f"{fv:.{decimals}f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v)

def parse_number(s, default=None):
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

def ensure_date(x) -> date:
    """Coerce inputs (date/datetime/str/Timestamp) to a `datetime.date`."""
    if x is None:
        return date.today()
    try:
        if isinstance(x, pd.Timestamp):
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
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            pass
        try:
            return datetime.strptime(s[:10], "%d/%m/%Y").date()
        except Exception:
            pass
    try:
        return date.fromisoformat(str(x)[:10])
    except Exception:
        return date.today()

def to_dt(d: str, h: str) -> datetime:
    return datetime.strptime(f"{d} {h}", "%Y-%m-%d %H:%M")

def to_time(v) -> dt.time:
    if v is None:
        return dt.time(0, 0)
    if isinstance(v, dt.time) and not isinstance(v, dt.datetime):
        return v
    if isinstance(v, dt.datetime):
        return v.time()
    s = str(v).strip()
    if not s:
        return dt.time(0, 0)
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt).time()
        except Exception:
            pass
    ds = re.sub(r"\D+", "", s)
    if len(ds) in (3, 4):
        try:
            h = int(ds[:-2])
            m = int(ds[-2:])
            if 0 <= h <= 23 and 0 <= m <= 59:
                return dt.time(h, m)
        except Exception:
            pass
    return dt.time(0, 0)

def intervals_overlap(ai: dt.time, af: dt.time, bi: dt.time, bf: dt.time) -> bool:
    try:
        a0 = ai.hour * 60 + ai.minute
        a1 = af.hour * 60 + af.minute
        b0 = bi.hour * 60 + bi.minute
        b1 = bf.hour * 60 + bf.minute
    except Exception:
        return False
    if a1 < a0:
        a0, a1 = a1, a0
    if b1 < b0:
        b0, b1 = b1, b0
    return max(a0, b0) < min(a1, b1)


# =============================================================================
# SQLAlchemy schema (Core)
# =============================================================================

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
    Column("colab_qtd", Integer, nullable=True),
    Column("tipo_servico", String(120), nullable=True),

    Column("cap_caminhao_m3", Float, nullable=True),
    Column("cps_por_caminhao", Integer, nullable=True),
    Column("caminhoes_est", Integer, nullable=True),
    Column("formas_est", Integer, nullable=True),

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

config = Table(
    "config",
    metadata,
    Column("chave", String(80), primary_key=True),
    Column("valor", String(250), nullable=True),
    Column("atualizado_em", String(40), nullable=True),
    Column("atualizado_por", String(120), nullable=True),
)

TZ_LABEL = "America/Sao_Paulo"


# =============================================================================
# UI/CSS
# =============================================================================

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
.block-container h1{ margin-top: .25rem !important; }
.stApp{
  background: linear-gradient(180deg, #fff7ed 0%, #ffffff 40%, #f8fafc 100%) !important;
}
.block-container{
  padding-top: 3.6rem !important;
  padding-bottom: 2.2rem !important;
  max-width: 1200px !important;
}
h1, h2, h3{ margin-top: .35rem !important; }
div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stMetric"]) {
  background: var(--hab-card);
  border: 1px solid var(--hab-border);
  border-radius: 16px;
  padding: 14px 14px 6px 14px;
  box-shadow: 0 6px 22px rgba(2, 6, 23, .06);
}
section[data-testid="stSidebar"] > div{
  background: linear-gradient(180deg, var(--hab-dark) 0%, var(--hab-slate) 100%) !important;
  border-right: 1px solid rgba(255,255,255,.08);
}
section[data-testid="stSidebar"] *{ color: rgba(255,255,255,.90) !important; }
section[data-testid="stSidebar"] a{ color: rgba(255,255,255,.90) !important; }
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
div[data-baseweb="input"] > div:focus-within,
div[data-baseweb="textarea"] > div:focus-within,
div[data-baseweb="select"] > div:focus-within{
  border-color: rgba(255,90,0,.90) !important;
  box-shadow: 0 0 0 3px rgba(255,90,0,.22) !important;
}
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
div[data-testid="stDataFrame"]{
  border-radius: 16px !important;
  overflow: hidden !important;
  border: 1px solid var(--hab-border) !important;
  box-shadow: 0 10px 24px rgba(2, 6, 23, .06) !important;
}
.hab-row-card{
  background: var(--hab-card);
  border: 1px solid var(--hab-border);
  border-radius: 16px;
  padding: 14px 16px;
  box-shadow: 0 10px 24px rgba(2, 6, 23, .06);
  margin-bottom: 12px;
}
.hab-row-top{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:10px;
  margin-bottom: 8px;
}
.hab-row-when{ color: var(--hab-text); font-size: 14px; }
.hab-row-main{ margin-bottom: 10px; }
.hab-row-title{ font-weight: 900; color: var(--hab-text); font-size: 16px; line-height: 1.2; }
.hab-row-sub{ color: var(--hab-muted); font-size: 13px; margin-top: 3px; }
.hab-row-grid{
  display:grid;
  grid-template-columns: 1fr 1fr 1fr 2fr;
  gap: 10px 14px;
}
.hab-k{ display:block; font-size: 11px; color: var(--hab-muted); text-transform: uppercase; letter-spacing: .04em; }
.hab-v{ display:block; font-weight: 800; color: var(--hab-text); font-size: 13px; margin-top: 2px; word-break: break-word; }
.hab-row-obs{ margin-top: 10px; padding-top: 10px; border-top: 1px dashed rgba(15, 23, 42, .12); color: var(--hab-text); font-size: 13px; }
.hab-badge{
  padding: 6px 10px;
  border-radius: 999px;
  font-weight: 900;
  font-size: 12px;
  border: 1px solid rgba(15, 23, 42, .12);
}
.hab-badge-blue{ background: rgba(59,130,246,.14); color: #1d4ed8; border-color: rgba(59,130,246,.30); }
.hab-badge-amber{ background: rgba(245,158,11,.18); color: #b45309; border-color: rgba(245,158,11,.32); }
.hab-badge-green{ background: rgba(34,197,94,.16); color: #166534; border-color: rgba(34,197,94,.28); }
.hab-badge-purple{ background: rgba(139,92,246,.16); color: #5b21b6; border-color: rgba(139,92,246,.28); }
.hab-badge-slate{ background: rgba(100,116,139,.14); color: #334155; border-color: rgba(100,116,139,.26); }
.hab-badge-red{ background: rgba(239,68,68,.16); color: #b91c1c; border-color: rgba(239,68,68,.28); }
@media (max-width: 900px){
  .hab-row-grid{ grid-template-columns: 1fr 1fr; }
}
"""

STATUS = ["Agendado", "Aguardando", "Confirmado", "Execucao", "Concluido", "Cancelado"]
SERVICE_TYPES = [
    "Concretagem",
    "Ensaio de Solo",
    "Coleta de Solo",
    "Arrancamento",
    "Coleta de Blocos",
    "Coleta de Prismas",
]


def _norm_status(s: str) -> str:
    s = str(s or "").strip().lower()
    trans = str.maketrans({
        "á":"a","à":"a","â":"a","ã":"a",
        "é":"e","ê":"e",
        "í":"i",
        "ó":"o","ô":"o","õ":"o",
        "ú":"u",
        "ç":"c",
    })
    return s.translate(trans)

def status_class(status: str) -> str:
    s = _norm_status(status)
    if s in ("agendado",):
        return "hab-badge-blue"
    if s in ("aguardando",):
        return "hab-badge-amber"
    if s in ("confirmado",):
        return "hab-badge-green"
    if s in ("execucao",):
        return "hab-badge-purple"
    if s in ("concluido",):
        return "hab-badge-slate"
    if s in ("cancelado",):
        return "hab-badge-red"
    return "hab-badge-slate"

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

def render_concretagens_cards(df: "pd.DataFrame", title: str = ""):
    if df is None or df.empty:
        st.info("Nenhum agendamento encontrado para este período/filtro.")
        return
    if title:
        st.markdown(f"### {title}")

    cols_pref = ["data","hora_inicio","obra","cliente","cidade","tipo_servico","volume_m3","caminhoes_est","formas_est","fck_mpa","slump_mm","usina","bomba","equipe","status","observacoes"]
    for c in cols_pref:
        if c not in df.columns:
            df[c] = ""

    for _, r in df.iterrows():
        data = str(r.get("data","") or "")
        hora = str(r.get("hora_inicio","") or "")
        obra = str(r.get("obra","") or "")
        cliente = str(r.get("cliente","") or "")
        cidade = str(r.get("cidade","") or "")
        vol = fmt_compact_num(r.get("volume_m3",""))
        fck = fmt_compact_num(r.get("fck_mpa",""))
        slump = fmt_compact_num(r.get("slump_mm",""))
        tipo_servico = str(r.get("tipo_servico","") or r.get("servico","") or "").strip() or "Concretagem"
        if tipo_servico and str(tipo_servico).strip() != "Concretagem":
            vol = "-"
            fck = "-"
            slump = "-"
        usina = str(r.get("usina","") or "").strip()
        bomba = str(r.get("bomba","") or "").strip()
        equipe = str(r.get("equipe","") or "").strip()
        status = str(r.get("status","") or "").strip() or "-"
        obs = str(r.get("observacoes","") or "").strip()

        try:
            formas = int(float(r.get("formas_est") or 0))
        except Exception:
            formas = 0
        try:
            cam = int(float(r.get("caminhoes_est") or 0))
        except Exception:
            cam = 0

        badge_cls = status_class(status)

        sub_left = " • ".join([x for x in [cliente, cidade, (tipo_servico if tipo_servico and tipo_servico!="Concretagem" else "")] if x])
        sup = " | ".join([x for x in [("Usina: "+usina) if usina else "", ("Bomba: "+bomba) if bomba else "", ("Equipe: "+equipe) if equipe else ""] if x])

        st.markdown(
            f"""
            <div class="hab-row-card">
              <div class="hab-row-top">
                <div class="hab-row-when">📅 <b>{data}</b> &nbsp;•&nbsp; ⏱️ <b>{hora}</b></div>
                <div class="hab-badge {badge_cls}">{status}</div>
              </div>
              <div class="hab-row-main">
                <div class="hab-row-title">{obra}</div>
                <div class="hab-row-sub">{sub_left}</div>
              </div>
              <div class="hab-row-grid">
                <div><span class="hab-k">Volume</span><span class="hab-v">{vol} m³</span></div>
                <div><span class="hab-k">FCK</span><span class="hab-v">{fck} MPa</span></div>
                <div><span class="hab-k">Slump</span><span class="hab-v">{slump} mm</span></div>
                <div><span class="hab-k">Operação</span><span class="hab-v">{sup if sup else "-"}</span></div>
              </div>
              {f'<div class="hab-row-obs">🧪 Formas (cota): <b>{formas}</b>{(" • Caminhões: <b>"+str(cam)+"</b>") if cam else ""}</div>' if (tipo_servico=="Concretagem" and (formas or cam)) else ''}
              {f'<div class="hab-row-obs">📝 {obs}</div>' if obs else ''}
            </div>
            """,
            unsafe_allow_html=True,
        )


# =============================================================================
# Config (key/value)
# =============================================================================

def get_config_value(key: str, default: Optional[str] = None) -> Optional[str]:
    try:
        eng = get_engine()
        sql = text("SELECT valor FROM config WHERE chave = :k")
        with eng.connect() as con:
            row = con.execute(sql, {'k': key}).mappings().first()
        if row and row.get('valor') is not None:
            return str(row['valor'])
        return default
    except Exception:
        return default

def set_config_value(key: str, value: str, user: str = 'system') -> None:
    eng = get_engine()
    ts = now_iso()
    with eng.begin() as con:
        if con.dialect.name == 'postgresql':
            sql = text(
                """
                INSERT INTO config (chave, valor, atualizado_em, atualizado_por)
                VALUES (:k, :v, :ts, :u)
                ON CONFLICT (chave)
                DO UPDATE SET
                    valor = EXCLUDED.valor,
                    atualizado_em = EXCLUDED.atualizado_em,
                    atualizado_por = EXCLUDED.atualizado_por
                """
            )
        else:
            sql = text(
                """
                INSERT OR REPLACE INTO config (chave, valor, atualizado_em, atualizado_por)
                VALUES (:k, :v, :ts, :u)
                """
            )
        con.execute(sql, {'k': key, 'v': str(value), 'ts': ts, 'u': user})

def _cfg_user_fallback() -> str:
    try:
        u = st.session_state.get("user") or {}
        return str(u.get("username") or "system")
    except Exception:
        return "system"

def config_get_int(key: str, default: int = 0) -> int:
    v = get_config_value(key)
    if (v is None or str(v).strip() == "") and key == "team_capacity":
        v = get_config_value("capacidade_colaboradores")
    try:
        return int(float(str(v).strip().replace(",", ".")))
    except Exception:
        return int(default)

def config_set_int(key: str, value: int, user: Optional[str] = None) -> None:
    u = user or _cfg_user_fallback()
    try:
        iv = int(value)
    except Exception:
        iv = 0
    set_config_value(key, str(iv), user=u)
    if key == "team_capacity":
        set_config_value("capacidade_colaboradores", str(iv), user=u)

def get_team_capacity(default: int = 12) -> int:
    n = config_get_int("team_capacity", default)
    return max(1, int(n) if isinstance(n, int) else default)

_COMMITTED = {"agendado", "aguardando", "confirmado", "execucao"}

def is_committed_status(status: str) -> bool:
    return _norm_status(status) in _COMMITTED

def get_committed_collaborators(date_str: str) -> int:
    try:
        eng = get_engine()
        sql = text(
            """
            SELECT COALESCE(SUM(COALESCE(colab_qtd, 1)), 0) AS total
            FROM concretagens
            WHERE data = :d AND COALESCE(status,'') IN ('Agendado','Aguardando','Confirmado','Execucao','Execução')
            """
        )
        with eng.connect() as con:
            row = con.execute(sql, {'d': date_str}).mappings().first()
        return int(row['total']) if row and row.get('total') is not None else 0
    except Exception:
        return 0


# =============================================================================
# DB Engine
# =============================================================================

def _ensure_sslmode_require(db_url: str) -> str:
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
    try:
        pr = urllib.parse.urlparse(db_url)
        host = pr.hostname or ""
        port = pr.port or ""
        return f"{host}:{port}" if port else host
    except Exception:
        return ""

@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
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

    if db_url and db_url.startswith('postgres') and 'sslmode=' not in db_url:
        sep = '&' if '?' in db_url else '?'
        db_url = db_url + f"{sep}sslmode=require"

    if db_url:
        db_url = db_url.strip()
        if db_url.startswith("postgres"):
            db_url = _ensure_sslmode_require(db_url)

            force_ipv4 = os.environ.get("HABI_FORCE_IPV4", "1").strip().lower() not in ("0", "false", "no")
            if force_ipv4:
                try:
                    import psycopg2  # type: ignore
                    u = urllib.parse.urlparse(db_url)
                    host = u.hostname or ""
                    port = int(u.port or 5432)

                    ipv4 = None
                    try:
                        for res in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM):
                            ipv4 = res[4][0]
                            break
                    except Exception:
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

        if db_url.startswith("sqlite"):
            return create_engine(db_url, future=True, connect_args={"check_same_thread": False})
        return create_engine(db_url, future=True, pool_pre_ping=True)

    return create_engine("sqlite:///agendamentos.db", future=True, connect_args={"check_same_thread": False})


# =============================================================================
# ✅ MIGRATION (patched)
# =============================================================================

def migrate_schema(eng):
    """
    Best-effort schema migration (SQLite/Postgres) to keep old DBs compatible with the current code.
    - Não derruba o app se a tabela não existir
    - Não derruba o app se a coluna já existir
    """
    from sqlalchemy import text

    dialect = eng.dialect.name

    # (table, col, ddl_sqlite, ddl_pg)
    cols = [
        # obras
        ("obras", "ativo", "INTEGER DEFAULT 1", "BOOLEAN DEFAULT TRUE"),
        ("obras", "created_at", "TIMESTAMP", "TIMESTAMP"),
        ("obras", "updated_at", "TIMESTAMP", "TIMESTAMP"),

        # legado: pode não existir
        ("colaboradores", "ativo", "INTEGER DEFAULT 1", "BOOLEAN DEFAULT TRUE"),
        ("colaboradores", "created_at", "TIMESTAMP", "TIMESTAMP"),
        ("colaboradores", "updated_at", "TIMESTAMP", "TIMESTAMP"),

        ("usuarios", "ativo", "INTEGER DEFAULT 1", "BOOLEAN DEFAULT TRUE"),
        ("usuarios", "perfil", "TEXT", "TEXT"),
        ("usuarios", "created_at", "TIMESTAMP", "TIMESTAMP"),
        ("usuarios", "updated_at", "TIMESTAMP", "TIMESTAMP"),

        ("settings", "key", "TEXT", "TEXT"),
        ("settings", "value", "TEXT", "TEXT"),
        ("settings", "updated_at", "TIMESTAMP", "TIMESTAMP"),

        # concretagens
        ("concretagens", "tipo_servico", "TEXT DEFAULT 'Concretagem'", "TEXT DEFAULT 'Concretagem'"),
        ("concretagens", "volume_m3", "REAL", "DOUBLE PRECISION"),
        ("concretagens", "duracao_min", "INTEGER", "INTEGER"),
        ("concretagens", "fck_mpa", "REAL", "DOUBLE PRECISION"),
        ("concretagens", "slump_mm", "INTEGER", "INTEGER"),
        ("concretagens", "slump_txt", "TEXT", "TEXT"),
        ("concretagens", "bomba", "TEXT", "TEXT"),
        ("concretagens", "fornecedor", "TEXT", "TEXT"),

        ("concretagens", "cap_caminhao_m3", "REAL DEFAULT 8.0", "DOUBLE PRECISION DEFAULT 8.0"),
        ("concretagens", "cps_por_caminhao", "INTEGER DEFAULT 6", "INTEGER DEFAULT 6"),
        ("concretagens", "caminhoes_est", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0"),
        ("concretagens", "formas_est", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0"),

        ("concretagens", "created_at", "TIMESTAMP", "TIMESTAMP"),
        ("concretagens", "updated_at", "TIMESTAMP", "TIMESTAMP"),
        ("concretagens", "criado_por", "TEXT", "TEXT"),
        ("concretagens", "atualizado_por", "TEXT", "TEXT"),
    ]

    with eng.begin() as conn:
        if dialect == "sqlite":
            def col_exists(table: str, col: str) -> bool:
                try:
                    res = conn.execute(text(f"PRAGMA table_info({table});")).fetchall()
                    return any(r[1] == col for r in res)
                except Exception:
                    return False

            def add_col(table: str, col: str, ddl: str):
                try:
                    if not col_exists(table, col):
                        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {ddl};"))
                except Exception:
                    pass

            for table, col, ddl_sqlite, _ddl_pg in cols:
                add_col(table, col, ddl_sqlite)

        elif dialect in ("postgresql", "postgres"):
            def add_col_pg(table: str, col: str, ddl: str):
                try:
                    conn.execute(text(
                        f'ALTER TABLE IF EXISTS "{table}" '
                        f'ADD COLUMN IF NOT EXISTS "{col}" {ddl};'
                    ))
                except Exception:
                    pass

            for table, col, _ddl_sqlite, ddl_pg in cols:
                add_col_pg(table, col, ddl_pg)


def init_db():
    eng = get_engine()
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

**Dica prática (resolve 90% no Streamlit Cloud):**
- Supabase → *Connect* → copie a string **Transaction pooler** (ou Session pooler)
- Cole no DB_URL do Secrets (host tipo `...pooler.supabase.com` e porta `6543`)
""")
        st.stop()

    metadata.create_all(eng)
    migrate_schema(eng)
    ensure_default_admin()


# =============================================================================
# DB util
# =============================================================================

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


# =============================================================================
# Auth / Users
# =============================================================================

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


# =============================================================================
# CNPJ lookup
# =============================================================================

@st.cache_data(ttl=24*3600, show_spinner=False)
def fetch_cnpj_data(cnpj: str):
    cnpj_digits = only_digits(cnpj or "")
    if len(cnpj_digits) != 14:
        return False, "CNPJ inválido (precisa ter 14 dígitos).", None

    headers = {
        "User-Agent": "Mozilla/5.0 (Streamlit; +https://streamlit.io)",
        "Accept": "application/json, text/plain, */*",
    }

    def _mk_payload(parsed: dict) -> dict:
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
            ct = (r.headers.get("content-type") or "").lower()
            if r.status_code == 200 and ("json" in ct or r.text.strip().startswith("{")):
                j = r.json()
                if isinstance(j, dict) and str(j.get("status", "")).upper() == "ERROR":
                    last_err = f"{name}: {j.get('message') or 'erro'}"
                    continue
                parsed = parser(j if isinstance(j, dict) else {})
                return True, "OK", _mk_payload(parsed)
            if r.status_code in (404, 429, 500, 502, 503, 504):
                last_err = f"{name}: HTTP {r.status_code}"
                continue
            last_err = f"{name}: HTTP {r.status_code}"
        except Exception as e:
            last_err = f"{name}: {type(e).__name__}: {e}"

    return False, f"Não foi possível consultar o CNPJ. ({last_err or 'sem detalhes'})", None


# =============================================================================
# Calculations
# =============================================================================

def _safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)

def _safe_int(x, default: int = 0) -> int:
    try:
        # aceita string/float e trata NaN
        v = float(x)
        if not math.isfinite(v):
            return int(default)
        return int(v)
    except Exception:
        return int(default)

def calc_trucks(volume_m3: float, capacidade_m3: float = 8.0) -> int:
    """Estimativa de caminhões.

    Robustez: trata None/strings/NaN/inf e evita ValueError em math.ceil(NaN).
    """
    try:
        v = float(volume_m3) if volume_m3 is not None else 0.0
    except Exception:
        v = 0.0
    try:
        c = float(capacidade_m3) if capacidade_m3 is not None else 0.0
    except Exception:
        c = 0.0

    if not math.isfinite(v) or not math.isfinite(c):
        return 0
    if v <= 0 or c <= 0:
        return 0
    return int(math.ceil(v / c))

def calc_cp_qty(caminhoes_est: int, cps_por_caminhao: int) -> int:
    try:
        c = int(caminhoes_est) if caminhoes_est is not None else 0
        p = int(cps_por_caminhao) if cps_por_caminhao is not None else 0
    except Exception:
        return 0
    if c <= 0 or p <= 0:
        return 0
    return c * p

def default_duration_min(volume_m3: float) -> int:
    trucks = calc_trucks(volume_m3, 8.0)
    return int(60 + trucks * 12)


# =============================================================================
# Queries
# =============================================================================

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
            c.tipo_servico,
            c.duracao_min,
            c.volume_m3,
            c.usina,
            c.fck_mpa,
            c.slump_mm,
            c.bomba,
            c.equipe,
            c.colab_qtd,
            c.cap_caminhao_m3,
            c.cps_por_caminhao,
            c.caminhoes_est,
            c.formas_est,
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

    if df.empty:
        return pd.DataFrame(columns=[
            "id","obra_id","obra","cliente","cidade","data","hora_inicio","hora_fim","duracao_min",
            "tipo_servico","volume_m3","fck_mpa","slump_mm","usina","bomba","equipe","colab_qtd","status",
            "cap_caminhao_m3","cps_por_caminhao","caminhoes_est","formas_est",
            "created_at","criado_por","atualizado_em","alterado_por","observacoes"
        ])

    for col in ("duracao_min", "volume_m3", "fck_mpa", "slump_mm", "colab_qtd", "cap_caminhao_m3", "cps_por_caminhao", "caminhoes_est", "formas_est"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["hora_fim"] = df.apply(lambda r: calc_hora_fim(r.get("hora_inicio"), r.get("duracao_min")), axis=1)
    return df

def get_next_concretagens_df(days: int = 7) -> pd.DataFrame:
    ds = today_local()
    de = ds + timedelta(days=int(days))
    df = get_concretagens_df(ds, de)
    sort_cols = [c for c in ("data", "hora_inicio", "obra") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=True, kind="stable")
    return df

def get_concretagem_by_id(cid: int) -> Dict[str, Any]:
    row = fetch_one(select(concretagens).where(concretagens.c.id == int(cid)))
    return row or {}


# =============================================================================
# Audit / history
# =============================================================================

def add_history(concretagem_id: int, action: str, before: Any, after: Any, user: str):
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

    if not df.empty and "detalhes" in df.columns:
        def _safe_parse(x):
            try:
                return json.loads(x) if isinstance(x, str) else x
            except Exception:
                return x
        df["detalhes"] = df["detalhes"].apply(_safe_parse)
    return df

def delete_concretagem_by_id(cid: int, user: str) -> bool:
    """Tenta excluir um agendamento (hard delete).
    Retorna True se excluir de fato; se não for possível (ex.: RLS/permissão),
    tenta ao menos marcar como Cancelado e retorna False.
    """
    cid = int(cid)

    before = get_concretagem_by_id(cid)
    if not before:
        return True

    try:
        add_history(cid, "DELETE", before, None, user)
        exec_stmt(delete(concretagens).where(concretagens.c.id == cid))
    except Exception:
        # fallback: marcar como cancelado para não ficar ativo na agenda
        try:
            cur_obs = (before.get("observacoes") or "").strip()
            note = "Cancelado automaticamente (falha ao excluir)."
            obs2 = (cur_obs + ("\n" if cur_obs else "") + note)[:2000]
            exec_stmt(update(concretagens).where(concretagens.c.id == cid).values(
                status="Cancelado",
                observacoes=obs2,
                updated_at=utcnow(),
            ))
            add_history(cid, "CANCEL_FALLBACK", before, {"status": "Cancelado"}, user)
        except Exception:
            pass
        return False

    # valida se sumiu mesmo
    if get_concretagem_by_id(cid):
        try:
            cur_obs = (before.get("observacoes") or "").strip()
            note = "Cancelado automaticamente (registro permaneceu após tentativa de exclusão)."
            obs2 = (cur_obs + ("\n" if cur_obs else "") + note)[:2000]
            exec_stmt(update(concretagens).where(concretagens.c.id == cid).values(
                status="Cancelado",
                observacoes=obs2,
                updated_at=utcnow(),
            ))
            add_history(cid, "CANCEL_FALLBACK", before, {"status": "Cancelado"}, user)
        except Exception:
            pass
        return False

    return True


def detect_schedule_conflicts(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    required = {"id", "data", "hora_inicio", "duracao_min", "obra", "equipe", "bomba", "status"}
    if any(c not in df.columns for c in required):
        return []

    rows = []
    for _, r in df.iterrows():
        try:
            status = str(r.get("status") or "")
            if status.lower().startswith("cancel"):
                continue
            d = str(r.get("data") or "")
            h = str(r.get("hora_inicio") or "00:00")
            dur = int(r.get("duracao_min") or 0)
            st_dt = to_dt(d, h)
            en_dt = st_dt + dt.timedelta(minutes=max(dur, 0))
            rows.append({
                "id": int(r.get("id")),
                "obra": str(r.get("obra") or ""),
                "data": d,
                "hora": h,
                "inicio": st_dt,
                "fim": en_dt,
                "equipe": str(r.get("equipe") or "").strip(),
                "bomba": str(r.get("bomba") or "").strip(),
            })
        except Exception:
            continue

    def scan(resource_key: str, label: str):
        out = []
        groups = {}
        for x in rows:
            rk = x.get(resource_key, "").strip()
            if not rk:
                continue
            groups.setdefault(rk, []).append(x)
        for rk, items in groups.items():
            items.sort(key=lambda z: z["inicio"])
            for i in range(1, len(items)):
                a = items[i-1]; b = items[i]
                if a["fim"] > b["inicio"]:
                    out.append({"tipo": label, "recurso": rk, "a": a, "b": b})
        return out

    return scan("equipe", "Equipe") + scan("bomba", "Bomba")

def find_conflicts(
    date_iso: str,
    hora_inicio: str,
    duracao_min: int,
    bomba: str = "",
    equipe: str = "",
    ignore_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    d = ensure_date(date_iso)

    def _parse_time(t) -> Optional[time]:
        if t is None:
            return None
        if isinstance(t, time):
            return t
        s = str(t).strip()
        if not s:
            return None
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
        reasons = []
        rb = (str(r.get("bomba") or "").strip().lower())
        re_ = (str(r.get("equipe") or "").strip().lower())
        if nb and rb == nb:
            reasons.append("bomba")
        if ne and re_ == ne:
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


# =============================================================================
# Export helpers
# =============================================================================

def make_excel_bytes(df: pd.DataFrame, sheet_name: str = "Agendamentos") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return bio.getvalue()

def make_pdf_bytes(df: pd.DataFrame, titulo: str = "Agendamentos de Concretagens") -> bytes:
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
    except Exception:
        return b""

    bio = io.BytesIO()
    doc = SimpleDocTemplate(bio, pagesize=landscape(A4), leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"<b>{titulo}</b>", styles["Title"]))
    story.append(Spacer(1, 10))

    cols = [c for c in ["data","hora_inicio","obra","cidade","volume_m3","fck_mpa","slump_mm","usina","bomba","equipe","status"] if c in df.columns]
    data = [cols]
    for _, r in df[cols].iterrows():
        row = []
        for c in cols:
            v = r.get(c)
            if isinstance(v, float):
                row.append(f"{v:.2f}".replace(".", ","))
            else:
                row.append("" if v is None else str(v))
        data.append(row)

    tbl = Table(data, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#0f172a")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,0), 9),
        ("GRID", (0,0), (-1,-1), 0.25, colors.lightgrey),
        ("FONTSIZE", (0,1), (-1,-1), 8),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
    ]))
    story.append(tbl)

    doc.build(story)
    return bio.getvalue()


# =============================================================================
# App start
# =============================================================================

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

today = today_local()
week_start = today - timedelta(days=today.weekday())
week_end = week_start + timedelta(days=6)

menu = st.sidebar.radio(
    "Menu",
    ["Dashboard", "Agenda (calendário)", "Novo agendamento", "Agenda (lista)", "Obras", "Histórico", "Admin"],
    index=0
)

try:
    _cap = get_team_capacity()
    _comm = get_committed_collaborators(today_local().isoformat())
    if _comm >= _cap:
        st.sidebar.markdown(f"### 🚨 Capacidade hoje\n**{_comm} / {_cap} colaboradores**\n\n✅ Alertas: **ATIVO**")
    else:
        st.sidebar.markdown(f"### ⚡ Capacidade hoje\n**{_comm} / {_cap} colaboradores**")
except Exception:
    pass


# =============================================================================
# Pages
# =============================================================================

if menu == "Dashboard":
    st.markdown("## 📅 Agenda por dia")
    sel_date = st.date_input("Data", value=date.today(), format="DD/MM/YYYY", key=uniq_key("dash_date"))
    st.caption("Selecione a data para ver o resumo e os agendamentos desse dia.")
    df_next = get_concretagens_df(sel_date, sel_date)

    if df_next.empty:
        st.info("Sem agendamentos cadastrados para esta data.")
    else:
        with st.expander("🔎 Filtros", expanded=False):
            c1, c2, c3 = st.columns([2,2,2])
            with c1:
                f_status = st.multiselect(
                    "Status",
                    STATUS,
                    default=["Agendado","Aguardando","Confirmado","Execucao"],
                    key=uniq_key("dash_status")
                )
            with c2:
                obras_list = sorted([o for o in df_next["obra"].dropna().unique().tolist() if str(o).strip()])
                f_obras = st.multiselect("Obras", obras_list, default=[], key=uniq_key("dash_obras"))
            with c3:
                cidades = sorted([c for c in df_next.get("cidade", pd.Series([], dtype=str)).dropna().unique().tolist() if str(c).strip()])
                f_cidades = st.multiselect("Cidades", cidades, default=[], key=uniq_key("dash_cidades"))

            c4, c5, c6 = st.columns([2,2,2])
            with c4:
                equipes = sorted([e for e in df_next.get("equipe", pd.Series([], dtype=str)).dropna().unique().tolist() if str(e).strip()])
                f_equipes = st.multiselect("Equipe", equipes, default=[], key=uniq_key("dash_equipes"))
            with c5:
                usinas = sorted([u for u in df_next.get("usina", pd.Series([], dtype=str)).dropna().unique().tolist() if str(u).strip()])
                f_usinas = st.multiselect("Usina/Fornecedor", usinas, default=[], key=uniq_key("dash_usinas"))
            with c6:
                modo = st.radio("Visualização", ["Cards (recomendado)", "Tabela"], horizontal=True, index=0, key=uniq_key("dash_mode"))

        show = df_next.copy()
        if f_status:
            show = show[show["status"].isin(f_status)]
        if f_obras:
            show = show[show["obra"].isin(f_obras)]
        if f_cidades and "cidade" in show.columns:
            show = show[show["cidade"].isin(f_cidades)]
        if f_equipes and "equipe" in show.columns:
            show = show[show["equipe"].isin(f_equipes)]
        if f_usinas and "usina" in show.columns:
            show = show[show["usina"].isin(f_usinas)]

        total = int(len(show))
        total_m3 = float(show["volume_m3"].fillna(0).sum()) if "volume_m3" in show.columns else 0.0
        total_formas = int(show["formas_est"].fillna(0).sum()) if "formas_est" in show.columns else 0
        total_colabs = int(pd.to_numeric(show.get('colab_qtd'), errors='coerce').fillna(0).sum()) if 'colab_qtd' in show.columns else 0

        conflicts = detect_schedule_conflicts(show)
        qtd_conf = len(conflicts)

        k1, k2, k3, k4, k5 = st.columns([1.1,1.1,1.1,1.1,1.1])
        with k1: st.metric("Agendamentos", f"{total}")
        with k2: st.metric("Volume total", f"{fmt_compact_num(total_m3)} m³")
        with k3: st.metric("Formas (dia)", f"{total_formas}")
        with k4: st.metric("Colaboradores (dia)", f"{total_colabs}")
        with k5: st.metric("Conflitos", f"{qtd_conf}")

        if qtd_conf > 0:
            with st.expander("⚠️ Conflitos detectados", expanded=True):
                for c in conflicts[:10]:
                    a = c["a"]; b = c["b"]
                    st.write(f"• **{c['tipo']}** `{c['recurso']}` — ID {a['id']} ({a['data']} {a['hora']}) x ID {b['id']} ({b['data']} {b['hora']})")

        with st.expander("⬇️ Exportar", expanded=False):
            exp = show.copy()
            st.download_button(
                "📄 Baixar CSV",
                data=exp.to_csv(index=False).encode("utf-8"),
                file_name=f"concretagens_7dias_{today_local().isoformat()}.csv",
                mime="text/csv",
                use_container_width=True,
                key=uniq_key("dash_csv"),
            )
            xbytes = make_excel_bytes(exp, sheet_name="7_dias")
            st.download_button(
                "📊 Baixar Excel",
                data=xbytes,
                file_name=f"concretagens_7dias_{today_local().isoformat()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key=uniq_key("dash_xlsx"),
            )
            pbytes = make_pdf_bytes(exp, titulo="Agendamentos — Dia selecionado")
            if pbytes:
                st.download_button(
                    "🧾 Baixar PDF (resumo)",
                    data=pbytes,
                    file_name=f"concretagens_7dias_{today_local().isoformat()}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key=uniq_key("dash_pdf"),
                )

        st.divider()

        show_disp = show.copy()
        if "volume_m3" in show_disp.columns:
            show_disp["volume_m3"] = show_disp["volume_m3"].astype(float).round(2).map(lambda x: str(x).replace(".", ",").rstrip("0").rstrip(",") if pd.notna(x) else "")
        if "fck_mpa" in show_disp.columns:
            show_disp["fck_mpa"] = show_disp["fck_mpa"].astype(float).round(1).map(lambda x: str(x).replace(".", ",").rstrip("0").rstrip(",") if pd.notna(x) else "")
        if "slump_mm" in show_disp.columns:
            show_disp["slump_mm"] = show_disp["slump_mm"].astype(float).round(0).astype("Int64").astype(str).replace("<NA>", "")

        if modo.startswith("Cards"):
            st.caption("📌 Dica: os cards mostram todas as informações **sem precisar arrastar para o lado**.")
            render_concretagens_cards(show_disp, title="")
        else:
            cols = [c for c in ["data","hora_inicio","obra","cliente","cidade","tipo_servico","volume_m3","fck_mpa","slump_mm","caminhoes_est","formas_est","usina","bomba","equipe","status"] if c in show_disp.columns]
            st.dataframe(show_disp[cols], use_container_width=True, hide_index=True)


elif menu == "Obras":
    st.subheader("🏗️ Cadastro de Obras")

    mode = st.radio("Modo", ["Cadastrar", "Editar"], horizontal=True)
    df_obras = get_obras_df()

    if mode == "Cadastrar":
        st.markdown("#### ➕ Nova obra")
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
                        criado_em=now_iso(),
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

    else:
        st.markdown("#### ✏️ Editar obra")
        if df_obras.empty:
            st.info("Nenhuma obra cadastrada ainda.")
        else:
            labels = df_obras.apply(lambda r: f"#{r['id']} — {r['nome']} ({r.get('cliente','') or 'Sem cliente'})", axis=1).tolist()
            pick = st.selectbox("Selecione a obra", labels)
            obra_id = int(pick.split("—")[0].replace("#", "").strip())
            row = df_obras[df_obras["id"] == obra_id].iloc[0].to_dict()

            cnpj_edit = st.text_input("CNPJ", value=row.get("cnpj") or "", key=f"cnpj_edit_{obra_id}")

            if st.button("🔎 Atualizar dados pelo CNPJ", use_container_width=True, key=f"btn_cnpj_edit_{obra_id}", type="primary"):
                ok, msg, payload = fetch_cnpj_data(cnpj_edit)
                if not ok:
                    st.error(msg)
                else:
                    st.session_state[f"edit_prefill_{obra_id}"] = payload
                    st.success("Dados do CNPJ carregados ✅")
                    st.rerun()

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
                                nome_fantasia=nome_fantasia.strip(),
                                atualizado_em=now_iso(),
                                alterado_por=current_user(),
                            ))
                        st.session_state.pop(f"edit_prefill_{obra_id}", None)
                        st.success("Obra atualizada ✅")
                        st.rerun()

    st.divider()
    st.markdown("#### 📚 Obras cadastradas")
    df_obras = get_obras_df()
    if df_obras.empty:
        st.info("Nenhuma obra cadastrada.")
    else:
        st.dataframe(df_obras, use_container_width=True, hide_index=True)


elif menu == "Agenda (calendário)":
    st.subheader("📅 Agenda (calendário semanal)")

    colw1, colw2, colw3 = st.columns([1.2, 1.0, 1.0])
    with colw1:
        ref_day = st.date_input("Semana de referência", value=today, help="Selecione qualquer dia da semana.")
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

    st.caption(f"Período: {week_start_cal.strftime('%d/%m/%Y')} a {week_end_cal.strftime('%d/%m/%Y')} ({TZ_LABEL})")

    if df_week.empty:
        st.info("Nenhum agendamento encontrado para os filtros selecionados.")
    else:
        conflicts_ids: set[int] = set()

        def _hhmm(s: str) -> str:
            s = str(s or "")
            return s[:5] if len(s) >= 5 else s

        for dday, g in df_week.groupby("data"):
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
                        st.caption(f"Serviço: {r.get('tipo_servico','')}")
                        st.caption(f"Volume: {r.get('volume_m3','')} m³")
                        st.caption(f"Bomba/Equipe: {r.get('bomba','')} • {r.get('equipe','')}")
                        st.caption(f"Responsável: {r.get('responsavel','')}")
                        st.caption(f"Status: {status}")


elif menu == "Novo agendamento":
    st.subheader("🗓️ Novo agendamento")

    df_obras = get_obras_df()
    if df_obras.empty:
        st.warning("Cadastre uma obra primeiro (menu: Obras).")
    else:
        labels = df_obras.apply(lambda r: f"#{r['id']} — {r['nome']} ({r.get('cliente','') or 'Sem cliente'})", axis=1).tolist()
        id_map = {labels[i]: int(df_obras.iloc[i]["id"]) for i in range(len(labels))}

        with st.form("form_conc_new"):
            obra_sel = st.selectbox("Obra *", labels)
            tipo_servico = st.selectbox("Tipo de serviço *", SERVICE_TYPES, index=0)

            cA, cB = st.columns(2)
            with cA:
                d = st.date_input("Data *", value=today)
            with cB:
                h = st.time_input("Hora início *", value=time(8, 0))

            if tipo_servico == "Concretagem":
                cC, cD = st.columns(2)
                with cC:
                    volume = st.number_input("Volume (m³) *", min_value=0.0, value=30.0, step=0.5)
                with cD:
                    dur = st.number_input("Duração prevista (min) *", min_value=15, value=default_duration_min(30.0), step=5)

                cE, cF, cG = st.columns(3)
                with cE:
                    fck = st.number_input("FCK (MPa)", min_value=0, value=25, step=1)
                with cF:
                    slump = st.text_input("Slump (cm)", value="")
                with cG:
                    st.checkbox("Bombeado?", value=False, disabled=True)

                st.markdown("**📌 Cota de rupturas (estimativa)**")
                cH, cI = st.columns(2)
                with cH:
                    cap = st.number_input("Capacidade caminhão (m³) *", min_value=0.1, value=8.0, step=0.1)
                with cI:
                    cps_por_cam = st.number_input("Corpos de prova por caminhão *", min_value=1, value=6, step=1)

                caminhaos_est = calc_trucks(volume, cap)
                formas_est = calc_cp_qty(caminhaos_est, cps_por_cam)
                st.caption(f"Estimativa: **{caminhaos_est} caminhão(ões)** → **{formas_est} CPs/forma(s)**")

            else:
                volume = 0.0
                fck = None
                slump = ""
                dur = st.number_input("Duração prevista (min) *", min_value=15, value=60, step=5)
                cap = None
                cps_por_cam = None
                caminhaos_est = 0
                formas_est = 0
                st.caption("Estimativas de caminhões/CPs aplicam-se somente para Concretagem.")

            usina = st.text_input("Usina / Fornecedor", value="")
            bomba = st.text_input("Bomba (ID/placa/empresa)", value="")
            equipe = st.text_input("Equipe (ex: Equipe 1 / Técnico X)", value="")
            colab_qtd = st.number_input("Colaboradores na obra (qtd)", min_value=1, step=1, value=1)
            status = st.selectbox("Status", STATUS, index=STATUS.index("Agendado"))
            obs = st.text_area("Observações", value="")

            _cap_total = get_team_capacity(12)
            _committed = get_committed_collaborators(d.strftime("%Y-%m-%d"))
            _projected = int(_committed) + int(colab_qtd or 1)
            if _projected > int(_cap_total):
                st.warning(f"⚠️ Este agendamento deixa o dia acima da capacidade: {_projected}/{_cap_total} colaboradores comprometidos.")

            salvar = st.form_submit_button("Salvar agendamento", use_container_width=True, type="primary")

            if salvar:
                data_str = d.strftime("%Y-%m-%d")
                hora_str = h.strftime("%H:%M")
                obra_id = id_map[obra_sel]

                conflicts = find_conflicts(data_str, hora_str, int(dur), bomba, equipe, ignore_id=None)
                if conflicts:
                    st.warning("⚠️ Conflito detectado (mesma bomba/equipe no mesmo horário). Você ainda pode salvar mesmo assim.")
                    st.dataframe(pd.DataFrame(conflicts), use_container_width=True, hide_index=True)

                user = current_user()
                now = now_iso()

                new_id = exec_stmt(insert(concretagens).values(
                    obra_id=obra_id,
                    tipo_servico=(tipo_servico or None),
                    data=data_str,
                    hora_inicio=hora_str,
                    duracao_min=int(dur),
                    volume_m3=float(volume),
                    colab_qtd=int(colab_qtd),
                    fck_mpa=float(fck) if fck else None,
                    slump_mm=parse_number(slump, None),
                    slump_txt=(slump.strip() if slump else None),
                    cap_caminhao_m3=(float(cap) if cap else None),
                    cps_por_caminhao=(int(cps_por_cam) if cps_por_cam else None),
                    caminhoes_est=(int(caminhaos_est) if caminhaos_est else None),
                    formas_est=(int(formas_est) if formas_est else None),
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
                try:
                    add_history(new_id, "CREATE", None, after, user)
                except Exception:
                    pass
                st.success(f"Agendamento criado ✅ (ID {new_id})")


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
            "id","data","hora_inicio","hora_fim","duracao_min","tipo_servico",
            "obra","cliente","cidade",
            "volume_m3","caminhoes_est","formas_est","fck_mpa","slump_mm",
            "cap_caminhao_m3","cps_por_caminhao","usina","bomba","equipe","colab_qtd","status",
            "criado_por","alterado_por","atualizado_em","observacoes"
        ]

        # ✅ PATCH: evita KeyError quando alguma coluna não existir
        cols_ok = [c for c in view_cols if c in df.columns]
        view = df[cols_ok].copy()
        st.dataframe(view, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("### ✏️ Editar agendamento")
        ids = df["id"].tolist()
        sel_id = st.selectbox("Selecione pelo ID", ids)

        row = df[df["id"] == sel_id].iloc[0].to_dict()

        with st.form("edit_form"):
            c1, c2 = st.columns(2)
            with c1:
                new_status = st.selectbox("Status", STATUS, index=STATUS.index(row["status"]))
            TIPOS_SERVICO = ["Concretagem", "Ensaio de Solo", "Coleta de Solo", "Arrancamento", "Coleta de Blocos", "Coleta de Prismas"]
            cur_tipo = (row.get("tipo_servico") or "Concretagem")
            try:
                idx_tipo = TIPOS_SERVICO.index(cur_tipo)
            except Exception:
                idx_tipo = 0
            new_tipo_servico = st.selectbox("Tipo de serviço", TIPOS_SERVICO, index=idx_tipo, key=uniq_key("agenda_edit_tipo"))

            with c2:
                new_dur = st.number_input("Duração (min)", min_value=15, value=int(row.get("duracao_min") or 60), step=5)

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
                new_volume = st.number_input("Volume (m³)", min_value=0.0, value=_safe_float(row.get("volume_m3"), 0.0), step=1.0)
            with c8:
                new_fck = st.number_input("FCK (MPa)", min_value=0.0, value=_safe_float(row.get("fck_mpa"), 0.0), step=1.0)

            c9, c10 = st.columns(2)
            with c9:
                new_colab_qtd = st.number_input("Colaboradores na obra (qtd)", min_value=1, step=1, value=_safe_int(row.get("colab_qtd"), 1))
            with c10:
                new_cap = st.number_input("Capacidade caminhão (m³) p/ estimativa", min_value=1.0, max_value=30.0, value=_safe_float(row.get("cap_caminhao_m3"), 8.0), step=0.5)

            c11, c12 = st.columns(2)
            with c11:
                new_cps_por_cam = st.number_input("Corpos de prova por caminhão (séries)", min_value=1, max_value=10, value=int(row.get("cps_por_caminhao") or 6), step=1)
            with c12:
                if new_tipo_servico == "Concretagem":
                    new_caminhoes_est = calc_trucks(new_volume, new_cap)
                    # ✅ PATCH: new_cps -> new_cps_por_cam
                    new_formas_est = calc_cp_qty(new_caminhoes_est, new_cps_por_cam)
                    st.caption(f"Estimativa: **{new_caminhoes_est} caminhão(ões)** → **{new_formas_est} CPs/forma(s)**")
                else:
                    new_caminhoes_est = 0
                    new_formas_est = 0
                    st.caption("Estimativas de caminhões/CPs aplicam-se somente para Concretagem.")

            new_obs = st.text_area("Observações", value=str(row.get("observacoes") or ""))

            salvar = st.form_submit_button("Salvar alterações", use_container_width=True, type="primary")

            if salvar:
                before = get_concretagem_by_id(int(sel_id))
                data_str = str(before.get("data") or "")
                hora_str = str(before.get("hora_inicio") or "")

                conflicts = find_conflicts(data_str, hora_str, int(new_dur), new_bomba, new_equipe, ignore_id=int(sel_id))
                if conflicts:
                    st.warning("⚠️ Conflito detectado (mesma bomba/equipe no mesmo horário). Você ainda pode salvar mesmo assim.")
                    st.dataframe(pd.DataFrame(conflicts), use_container_width=True, hide_index=True)

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
                        tipo_servico=(new_tipo_servico or None),
                        cap_caminhao_m3=float(new_cap) if new_cap else None,
                        # ✅ PATCH: new_cps -> new_cps_por_cam
                        cps_por_caminhao=int(new_cps_por_cam) if new_cps_por_cam else None,
                        caminhoes_est=int(new_caminhoes_est),
                        formas_est=int(new_formas_est),
                        fck_mpa=float(new_fck) if new_fck else None,
                        observacoes=(new_obs or "").strip(),
                        atualizado_em=now,
                        alterado_por=user
                    ))

                after = get_concretagem_by_id(int(sel_id))
                try:
                    add_history(int(sel_id), "UPDATE", before, after, user)
                except Exception:
                    pass

                st.success("Atualizado ✅")
                st.rerun()

        st.markdown("---")
        with st.expander("🗑️ Excluir agendamento", expanded=False):
            st.warning("A exclusão é permanente e remove o agendamento da agenda e do histórico.")
            confirm_del = st.text_input("Digite EXCLUIR para confirmar", value="", key=uniq_key(f"del_confirm_{row['id']}"))
            can_del = (confirm_del.strip().upper() == "EXCLUIR")
            if st.button("Excluir agendamento", key=uniq_key(f"del_btn_{row['id']}"), disabled=not can_del):
                try:
                    ok = delete_concretagem_by_id(int(row["id"]), current_user())
                    if ok:
                        st.success("Agendamento excluído.")
                    else:
                        st.warning("Não foi possível excluir definitivamente. O agendamento foi marcado como Cancelado (quando permitido).")
                    st.rerun()
                except Exception as e:
                    st.error(f"Falha ao excluir: {e}")


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
                    st.divider()


elif menu == "Admin":
    st.subheader("🛠️ Admin")

    if current_role() != "admin":
        st.error("Acesso restrito ao perfil admin.")
        st.stop()

    tab0, tab1, tab2, tab3 = st.tabs(["Capacidade", "Usuários", "Alterar minha senha", "Exportar"])

    with tab0:
        st.subheader("Capacidade diária")
        cap_atual = get_team_capacity(12)
        novo = st.number_input("Colaboradores disponíveis por dia", min_value=1, step=1, value=int(cap_atual))
        if st.button("Salvar capacidade", use_container_width=True):
            config_set_int("team_capacity", int(novo))
            st.success("Capacidade salva.")

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
            rep_cols = [c for c in [
                "data","hora_inicio","duracao_min","obra","cliente","cidade",
                "volume_m3","fck_mpa","slump_mm","usina","bomba","equipe","status",
                "criado_por","alterado_por","created_at","atualizado_em","observacoes"
            ] if c in df.columns]
            rep = df[rep_cols].copy()
            st.dataframe(rep, use_container_width=True, hide_index=True)
            xlsx = make_excel_bytes(rep, sheet_name="Agendamentos")
            st.download_button(
                "⬇️ Baixar Excel",
                data=xlsx,
                file_name=f"agendamentos_{ini.strftime('%Y%m%d')}_{fim.strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

st.sidebar.divider()
st.sidebar.caption("Cores: Agendado (azul) • Aguardando (amarelo) • Cancelado (vermelho) ✅")
