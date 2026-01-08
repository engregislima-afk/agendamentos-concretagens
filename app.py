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

import json
import base64
import hashlib
import secrets
import urllib.parse
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import requests
import streamlit as st

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Text, ForeignKey, Boolean,
    select, insert, update, text
)
from sqlalchemy.engine import Engine

TZ_LABEL = "America/Sao_Paulo"

# ============================
# Windows 11-ish styling
# ============================
WIN11_CSS = """
<style>
:root {
  --bg: #f3f3f3;
  --card: #ffffff;
  --text: #1f1f1f;
  --muted: #5a5a5a;
  --border: #e7e7e7;
  --shadow: 0 6px 22px rgba(0,0,0,.08);
  --radius: 14px;
  --blue: #2563eb;
  --red: #dc2626;
  --yellow: #f59e0b;
  --green: #16a34a;
  --gray: #6b7280;
}

.stApp {
  background: var(--bg);
}

section[data-testid="stSidebar"] {
  background: #ffffff;
  border-right: 1px solid var(--border);
}

h1,h2,h3,h4,h5,h6 { color: var(--text); }
p, li, .stMarkdown { color: var(--text); }

.block-container {
  padding-top: 1.2rem;
  padding-bottom: 2rem;
}

.hab-card {
  background: var(--card);
  border: 1px solid var(--border);
  box-shadow: var(--shadow);
  border-radius: var(--radius);
  padding: 14px 16px;
}

.hab-chip {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 4px 10px;
  border-radius: 999px;
  font-weight: 700;
  font-size: 12px;
  border: 1px solid rgba(0,0,0,.06);
}

.hab-chip.blue { background: rgba(37,99,235,.12); color: var(--blue); }
.hab-chip.red { background: rgba(220,38,38,.12); color: var(--red); }
.hab-chip.yellow { background: rgba(245,158,11,.18); color: #8a5a00; }
.hab-chip.green { background: rgba(22,163,74,.14); color: var(--green); }
.hab-chip.gray { background: rgba(107,114,128,.14); color: var(--gray); }

.small-muted { color: var(--muted); font-size: 12px; }

[data-testid="stMetricValue"] { font-size: 30px; }
[data-testid="stMetricLabel"] { color: var(--muted); }

button[kind="primary"] {
  border-radius: 12px !important;
}

div[data-testid="stDataFrame"] {
  border-radius: var(--radius);
  overflow: hidden;
  border: 1px solid var(--border);
  box-shadow: var(--shadow);
}
</style>
"""

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
    color_map = {
        "Agendado": ("#2563eb", "#e8efff"),
        "Cancelado": ("#dc2626", "#ffecec"),
        "Aguardando": ("#8a5a00", "#fff3d6"),
        "Confirmado": ("#166534", "#eaffea"),
        "Execucao": ("#166534", "#eaffea"),
        "Concluido": ("#374151", "#f1f5f9"),
    }
    def _apply(col: pd.Series):
        styles = []
        for v in col.astype(str).tolist():
            fg, bg = color_map.get(v, ("#374151", "#f1f5f9"))
            styles.append(f"background-color: {bg}; color: {fg}; font-weight: 700;")
        return styles
    return df.style.apply(_apply, subset=["status"])

# ============================
# Time helpers
# ============================
def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def to_dt(d: str, h: str) -> datetime:
    return datetime.strptime(f"{d} {h}", "%Y-%m-%d %H:%M")

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
    db_url = None
    try:
        db_url = st.secrets.get("DB_URL", None)
    except Exception:
        db_url = None

    if db_url and str(db_url).strip():
        url = _ensure_sslmode_require(str(db_url).strip())
        # pool_pre_ping evita conexões "mortas"
        return create_engine(url, pool_pre_ping=True)

    # Local
    return create_engine("sqlite:///concretagens.db", connect_args={"check_same_thread": False})

metadata = MetaData()

obras = Table(
    "obras", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("nome", String(200), nullable=False),
    Column("cliente", String(200)),
    Column("endereco", String(300)),
    Column("cidade", String(120)),
    Column("responsavel", String(120)),
    Column("telefone", String(80)),
    Column("criado_em", String(19), nullable=False),

    Column("cnpj", String(20)),
    Column("razao_social", String(220)),
    Column("nome_fantasia", String(220)),
)

users = Table(
    "users", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String(80), nullable=False, unique=True),
    Column("name", String(120)),
    Column("role", String(20), nullable=False),          # admin | user
    Column("pass_salt", String(200), nullable=False),
    Column("pass_hash", String(200), nullable=False),
    Column("is_active", Boolean, nullable=False, server_default=text("1")),
    Column("created_at", String(19), nullable=False),
    Column("last_login_at", String(19)),
)

concretagens = Table(
    "concretagens", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("obra_id", Integer, ForeignKey("obras.id"), nullable=False),
    Column("data", String(10), nullable=False),          # YYYY-MM-DD
    Column("hora_inicio", String(5), nullable=False),    # HH:MM
    Column("duracao_min", Integer, nullable=False),
    Column("volume_m3", Float, nullable=False),
    Column("fck_mpa", Float),
    Column("slump_mm", String(80)),
    Column("usina", String(200)),
    Column("bomba", String(120)),
    Column("equipe", String(120)),
    Column("status", String(20), nullable=False),
    Column("observacoes", Text),

    Column("criado_em", String(19), nullable=False),
    Column("atualizado_em", String(19), nullable=False),
    Column("criado_por", String(80)),
    Column("alterado_por", String(80)),
)

historico = Table(
    "historico_concretagens", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("concretagem_id", Integer, ForeignKey("concretagens.id"), nullable=False),
    Column("acao", String(20), nullable=False),  # CREATE / UPDATE
    Column("antes_json", Text),
    Column("depois_json", Text),
    Column("feito_por", String(80), nullable=False),
    Column("feito_em", String(19), nullable=False),
)

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
# CNPJ lookup (cnpj.ws)
# ============================
def only_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def fetch_cnpj_data(cnpj: str) -> Tuple[bool, str, Dict[str, Any]]:
    cnpj_digits = only_digits(cnpj)
    if len(cnpj_digits) != 14:
        return False, "CNPJ inválido (precisa ter 14 dígitos).", {}

    url = f"https://cnpj.ws/cnpj/{cnpj_digits}"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            return False, f"Não foi possível consultar CNPJ (status {r.status_code}).", {}
        data = r.json()
    except Exception as e:
        return False, f"Erro ao consultar CNPJ: {e}", {}

    razao = data.get("razao_social") or ""
    estab = data.get("estabelecimento") or {}
    fantasia = estab.get("nome_fantasia") or ""

    logradouro = estab.get("tipo_logradouro") or ""
    nome_log = estab.get("logradouro") or ""
    numero = estab.get("numero") or ""
    bairro = estab.get("bairro") or ""
    cep = estab.get("cep") or ""
    cidade_obj = estab.get("cidade") or {}
    estado_obj = estab.get("estado") or {}

    cidade_nome = cidade_obj.get("nome") or ""
    uf = estado_obj.get("sigla") or ""

    endereco = " ".join([p for p in [logradouro, nome_log] if p]).strip()
    if numero:
        endereco = f"{endereco}, {numero}".strip(", ")
    if bairro:
        endereco = f"{endereco} - {bairro}".strip()
    if cep:
        endereco = f"{endereco} - CEP {cep}".strip()

    cidade_fmt = " - ".join([p for p in [cidade_nome, uf] if p]).strip(" -")

    payload = {
        "cnpj": cnpj_digits,
        "razao_social": razao,
        "nome_fantasia": fantasia,
        "endereco": endereco,
        "cidade": cidade_fmt,
        "cliente_sugerido": fantasia.strip() or razao.strip()
    }
    return True, "OK", payload

# ============================
# Concretagem helpers
# ============================
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

def get_concretagens_df(range_start: date, range_end: date) -> pd.DataFrame:
    ds = range_start.strftime("%Y-%m-%d")
    de = range_end.strftime("%Y-%m-%d")

    eng = get_engine()
    sql = text("""
        SELECT c.id, c.data, c.hora_inicio, c.duracao_min, c.volume_m3, c.fck_mpa, c.slump_mm,
               c.usina, c.bomba, c.equipe, c.status,
               c.criado_por, c.alterado_por, c.criado_em, c.atualizado_em,
               o.nome AS obra, o.cliente, o.cidade, o.id AS obra_id,
               c.observacoes
        FROM concretagens c
        JOIN obras o ON o.id = c.obra_id
        WHERE c.data BETWEEN :ds AND :de
        ORDER BY c.data ASC, c.hora_inicio ASC
    """)
    with eng.connect() as conn:
        df = pd.read_sql(sql, conn, params={"ds": ds, "de": de})
    return df

def get_concretagem_by_id(cid: int) -> Dict[str, Any]:
    row = fetch_one(select(concretagens).where(concretagens.c.id == int(cid)))
    return row or {}

def add_history(concretagem_id: int, action: str, before: Any, after: Any, user: str):
    exec_stmt(insert(historico).values(
        concretagem_id=int(concretagem_id),
        acao=action,
        antes_json=json.dumps(before, ensure_ascii=False, default=str) if before is not None else None,
        depois_json=json.dumps(after, ensure_ascii=False, default=str) if after is not None else None,
        feito_por=user,
        feito_em=now_iso()
    ))

def get_history_df(concretagem_id: int) -> pd.DataFrame:
    return fetch_df(select(
        historico.c.id, historico.c.feito_em, historico.c.feito_por, historico.c.acao,
        historico.c.antes_json, historico.c.depois_json
    ).where(historico.c.concretagem_id == int(concretagem_id)).order_by(historico.c.id.desc()))

def find_conflicts(new_data: str, new_hora: str, new_dur: int, bomba: str, equipe: str, ignore_id: Optional[int] = None) -> List[Dict[str, Any]]:
    active_status = ("Agendado", "Aguardando", "Confirmado", "Execucao")
    eng = get_engine()
    sql = text("""
        SELECT id, data, hora_inicio, duracao_min, bomba, equipe, status
        FROM concretagens
        WHERE data = :d AND status IN :st
    """)
    with eng.connect() as conn:
        df = pd.read_sql(sql, conn, params={"d": new_data, "st": active_status})

    ns = to_dt(new_data, new_hora)
    ne = ns + timedelta(minutes=int(new_dur))

    bomba = (bomba or "").strip().lower()
    equipe = (equipe or "").strip().lower()

    conflicts = []
    for _, r in df.iterrows():
        rid = int(r["id"])
        if ignore_id is not None and rid == int(ignore_id):
            continue

        rs = to_dt(str(r["data"]), str(r["hora_inicio"]))
        re = rs + timedelta(minutes=int(r["duracao_min"]))

        rb = (str(r["bomba"] or "")).strip().lower()
        rq = (str(r["equipe"] or "")).strip().lower()

        same_resource = False
        if bomba and rb:
            same_resource = same_resource or (bomba == rb)
        if equipe and rq:
            same_resource = same_resource or (equipe == rq)

        if same_resource and overlap(ns, ne, rs, re):
            conflicts.append({
                "id": rid,
                "inicio": rs.strftime("%H:%M"),
                "fim": re.strftime("%H:%M"),
                "bomba": r["bomba"],
                "equipe": r["equipe"],
                "status": r["status"],
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
st.markdown(WIN11_CSS, unsafe_allow_html=True)

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
    ["Dashboard", "Novo agendamento", "Agenda (lista)", "Obras", "Histórico", "Admin"],
    index=0
)

today = date.today()
week_start = today - timedelta(days=today.weekday())
week_end = week_start + timedelta(days=6)

# ============================
# Dashboard
# ============================
if menu == "Dashboard":
    dfw = get_concretagens_df(week_start, week_end)

    colA, colB, colC = st.columns(3)
    with colA:
        st.metric("Hoje", today.strftime("%d/%m/%Y"))
    with colB:
        st.metric("Semana", f"{week_start.strftime('%d/%m')} → {week_end.strftime('%d/%m')}")
    with colC:
        st.caption(f"Timezone: {TZ_LABEL}")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Agendado", int((dfw["status"] == "Agendado").sum()) if not dfw.empty else 0)
    c2.metric("Aguardando", int((dfw["status"] == "Aguardando").sum()) if not dfw.empty else 0)
    c3.metric("Confirmado", int((dfw["status"] == "Confirmado").sum()) if not dfw.empty else 0)
    c4.metric("Execução", int((dfw["status"] == "Execucao").sum()) if not dfw.empty else 0)
    c5.metric("Concluído", int((dfw["status"] == "Concluido").sum()) if not dfw.empty else 0)
    c6.metric("Cancelado", int((dfw["status"] == "Cancelado").sum()) if not dfw.empty else 0)

    st.subheader("📌 Próximas concretagens (7 dias)")
    df_next = get_concretagens_df(today, today + timedelta(days=7))
    if df_next.empty:
        st.info("Nenhuma concretagem nos próximos 7 dias.")
    else:
        show = df_next[[
            "data","hora_inicio","obra","cliente","cidade","volume_m3","fck_mpa","slump_mm",
            "usina","bomba","equipe","status","criado_por","alterado_por","atualizado_em"
        ]].copy()
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
                    exec_stmt(insert(obras).values(
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
                    st.success("Obra cadastrada ✅")
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
                    slump_mm=(slump or "").strip(),
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
                        slump_mm=(new_slump or "").strip(),
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
            st.info("Sem histórico.")
        else:
            st.dataframe(hist[["feito_em","feito_por","acao"]], use_container_width=True, hide_index=True)

            with st.expander("Ver detalhes (antes/depois)", expanded=False):
                for _, r in hist.iterrows():
                    st.markdown(f"**{r['feito_em']}** — {r['feito_por']} — `{r['acao']}`")
                    try:
                        a = json.loads(r["antes_json"]) if r.get("antes_json") else None
                    except Exception:
                        a = r.get("antes_json")
                    try:
                        b = json.loads(r["depois_json"]) if r.get("depois_json") else None
                    except Exception:
                        b = r.get("depois_json")
                    st.code(json.dumps({"antes": a, "depois": b}, ensure_ascii=False, indent=2), language="json")
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
