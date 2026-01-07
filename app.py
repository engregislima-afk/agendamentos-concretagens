# app.py — Habisolute | Agendamentos de Concretagens (Escritório + Celular)
# Stack: Streamlit + SQLite + Auditoria (quem criou/alterou) + Histórico (antes/depois)
#
# Como rodar:
#   pip install streamlit pandas openpyxl
#   streamlit run app.py
#
# Primeiro acesso (criado automaticamente se não existir usuário):
#   usuário: admin
#   senha:   admin123
# Depois, altere a senha e crie usuários em "Admin > Usuários".

import sqlite3
import json
import base64
import hashlib
import secrets
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd

# HTTP (consulta CNPJ)
try:
    import requests
except Exception:
    requests = None

import streamlit as st

DB_PATH = "concretagens.db"
TZ_LABEL = "America/Sao_Paulo"

STATUS = ["Agendado", "Confirmado", "Execucao", "Concluido", "Cancelado"]

# ----------------------------
# Util: tempo / json
# ----------------------------
def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def only_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def format_city_uf(city: str, uf: str) -> str:
    city = (city or "").strip()
    uf = (uf or "").strip()
    if city and uf:
        return f"{city} - {uf}"
    return city or uf or ""

@st.cache_data(ttl=60*60, show_spinner=False)
def cnpjws_lookup(cnpj_digits: str):
    """Consulta CNPJ na API pública do CNPJ.ws.
    Retorna dict (JSON) ou None se não encontrado.
    Pode lançar RuntimeError('rate_limit') em excesso de requisições.
    """
    if requests is None:
        raise RuntimeError("requests_not_installed")

    url = f"https://publica.cnpj.ws/cnpj/{cnpj_digits}"
    try:
        r = requests.get(
            url,
            timeout=15,
            headers={"Accept": "application/json", "User-Agent": "Habisolute-Agendamentos/1.0"},
        )
    except Exception as e:
        raise RuntimeError(f"network_error:{e}") from e

    if r.status_code == 200:
        try:
            return r.json()
        except Exception:
            raise RuntimeError("invalid_json")
    if r.status_code == 404:
        return None
    if r.status_code == 429:
        raise RuntimeError("rate_limit")
    raise RuntimeError(f"http_{r.status_code}")

def parse_cnpjws_to_fields(payload: dict) -> dict:
    """Extrai razão social, fantasia e endereço do JSON do CNPJ.ws."""
    razao = (payload.get("razao_social") or "").strip()
    est = payload.get("estabelecimento") or {}
    fantasia = (est.get("nome_fantasia") or "").strip()

    tipo_log = (est.get("tipo_logradouro") or "").strip()
    logradouro = (est.get("logradouro") or "").strip()
    numero = (est.get("numero") or "").strip()
    complemento = (est.get("complemento") or "").strip()
    bairro = (est.get("bairro") or "").strip()
    cep = (est.get("cep") or "").strip()

    cidade = ((est.get("cidade") or {}).get("nome") or "").strip() if isinstance(est.get("cidade"), dict) else ""
    uf = ((est.get("estado") or {}).get("sigla") or "").strip() if isinstance(est.get("estado"), dict) else ""

    rua = " ".join([p for p in [tipo_log, logradouro] if p]).strip()
    end = rua
    if numero:
        end = f"{end}, {numero}" if end else numero
    if complemento:
        end = f"{end} - {complemento}" if end else complemento
    if bairro:
        end = f"{end} - {bairro}" if end else bairro
    if cep:
        end = f"{end} - CEP {cep}" if end else f"CEP {cep}"

    return {
        "razao_social": razao,
        "nome_fantasia": fantasia,
        "endereco": end.strip(),
        "cidade": format_city_uf(cidade, uf),
    }

def to_dt(d: str, h: str) -> datetime:
    return datetime.strptime(f"{d} {h}", "%Y-%m-%d %H:%M")

def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return max(a_start, b_start) < min(a_end, b_end)

def jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)

def jsafe_load(s: Optional[str]) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return s

# ----------------------------
# DB helpers
# ----------------------------
def db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def qdf(sql: str, params: Optional[list] = None) -> pd.DataFrame:
    con = db()
    try:
        return pd.read_sql_query(sql, con, params=params or [])
    finally:
        con.close()

def qexec(sql: str, params: Optional[list] = None) -> int:
    con = db()
    cur = con.cursor()
    try:
        cur.execute(sql, params or [])
        con.commit()
        return cur.lastrowid
    finally:
        con.close()

def ensure_column(table: str, column: str, coltype: str):
    con = db()
    cur = con.cursor()
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
        con.commit()
    except Exception:
        pass
    finally:
        con.close()

def init_db():
    con = db()
    cur = con.cursor()

    # Obras
    cur.execute("""
    CREATE TABLE IF NOT EXISTS obras (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        cliente TEXT,
        endereco TEXT,
        cidade TEXT,
        responsavel TEXT,
        telefone TEXT,
        criado_em TEXT NOT NULL
    )
    """)


    # Migração: garantir campos de CNPJ (sem quebrar instalações antigas)
    ensure_column("obras", "cnpj", "TEXT")
    ensure_column("obras", "razao_social", "TEXT")
    ensure_column("obras", "nome_fantasia", "TEXT")
    # Usuários (login)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        name TEXT,
        role TEXT NOT NULL,              -- admin | user
        pass_salt TEXT NOT NULL,
        pass_hash TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        last_login_at TEXT
    )
    """)

    # Concretagens + auditoria
    cur.execute("""
    CREATE TABLE IF NOT EXISTS concretagens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obra_id INTEGER NOT NULL,
        data TEXT NOT NULL,                -- YYYY-MM-DD
        hora_inicio TEXT NOT NULL,         -- HH:MM
        duracao_min INTEGER NOT NULL,
        volume_m3 REAL NOT NULL,
        fck_mpa REAL,
        slump_mm TEXT,
        usina TEXT,
        bomba TEXT,
        equipe TEXT,
        status TEXT NOT NULL,
        observacoes TEXT,
        criado_em TEXT NOT NULL,
        atualizado_em TEXT NOT NULL,
        criado_por TEXT,
        alterado_por TEXT,
        FOREIGN KEY (obra_id) REFERENCES obras(id)
    )
    """)

    # Histórico de alterações (antes/depois)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS historico_concretagens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concretagem_id INTEGER NOT NULL,
        acao TEXT NOT NULL,               -- CREATE / UPDATE / STATUS
        antes_json TEXT,
        depois_json TEXT,
        feito_por TEXT NOT NULL,
        feito_em TEXT NOT NULL,
        FOREIGN KEY (concretagem_id) REFERENCES concretagens(id)
    )
    """)

    con.commit()
    con.close()

    # Migrações defensivas (se DB já existia)
    ensure_column("concretagens", "criado_por", "TEXT")
    ensure_column("concretagens", "alterado_por", "TEXT")

    # Garante admin padrão
    ensure_default_admin()

# ----------------------------
# Segurança: hash senha (PBKDF2)
# ----------------------------
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

# ----------------------------
# Auth / Users
# ----------------------------
def ensure_default_admin():
    df = qdf("SELECT COUNT(*) AS n FROM users")
    if int(df.iloc[0]["n"]) == 0:
        salt, ph = make_password("admin123")
        qexec("""INSERT INTO users (username, name, role, pass_salt, pass_hash, is_active, created_at)
                 VALUES (?, ?, ?, ?, ?, 1, ?)""",
              ["admin", "Administrador", "admin", salt, ph, now_iso()])

def get_user(username: str) -> Optional[Dict[str, Any]]:
    df = qdf("SELECT * FROM users WHERE username=?", [username])
    if df.empty:
        return None
    return df.iloc[0].to_dict()

def list_users() -> pd.DataFrame:
    return qdf("SELECT id, username, name, role, is_active, created_at, last_login_at FROM users ORDER BY id DESC")

def create_user(username: str, name: str, role: str, password: str):
    salt, ph = make_password(password)
    qexec("""INSERT INTO users (username, name, role, pass_salt, pass_hash, is_active, created_at)
             VALUES (?, ?, ?, ?, ?, 1, ?)""",
          [username, name, role, salt, ph, now_iso()])

def set_user_active(user_id: int, active: bool):
    qexec("UPDATE users SET is_active=? WHERE id=?", [1 if active else 0, int(user_id)])

def reset_user_password(user_id: int, new_password: str):
    salt, ph = make_password(new_password)
    qexec("UPDATE users SET pass_salt=?, pass_hash=? WHERE id=?", [salt, ph, int(user_id)])

def update_last_login(username: str):
    qexec("UPDATE users SET last_login_at=? WHERE username=?", [now_iso(), username])

def current_user() -> str:
    return st.session_state.get("user", {}).get("username", "desconhecido")

def current_role() -> str:
    return st.session_state.get("user", {}).get("role", "user")

def require_login():
    if not st.session_state.get("user"):
        st.stop()

def login_box():
    st.sidebar.markdown("### 🔐 Login")
    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user:
        st.sidebar.success(f"Logado: {st.session_state.user['username']}")
        st.sidebar.caption(f"Perfil: {st.session_state.user['role']}")
        if st.sidebar.button("Sair"):
            st.session_state.user = None
            st.rerun()
        return

    u = st.sidebar.text_input("Usuário", key="login_u")
    p = st.sidebar.text_input("Senha", type="password", key="login_p")
    if st.sidebar.button("Entrar", use_container_width=True):
        user = get_user(u.strip())
        if not user or int(user.get("is_active", 0)) != 1:
            st.sidebar.error("Usuário inválido ou inativo.")
            return
        if not verify_password(p, user["pass_salt"], user["pass_hash"]):
            st.sidebar.error("Senha inválida.")
            return
        st.session_state.user = {"id": user["id"], "username": user["username"], "role": user["role"], "name": user.get("name") or ""}
        update_last_login(user["username"])
        st.rerun()

# ----------------------------
# Regras de concretagem
# ----------------------------
def calc_trucks(volume_m3: float, capacidade_m3: float = 8.0) -> int:
    if volume_m3 <= 0:
        return 0
    import math
    return int(math.ceil(volume_m3 / capacidade_m3))

def default_duration_min(volume_m3: float) -> int:
    # Regra simples: base 60 min + 12 min por caminhão (capacidade 8m³)
    trucks = calc_trucks(volume_m3, 8.0)
    return int(60 + trucks * 12)

def get_obras() -> pd.DataFrame:
    return qdf("SELECT id, nome, cliente, cnpj, razao_social, nome_fantasia, endereco, cidade, responsavel, telefone, criado_em FROM obras ORDER BY id DESC")

def get_concretagens(range_start: date, range_end: date) -> pd.DataFrame:
    return qdf("""
        SELECT c.id, c.data, c.hora_inicio, c.duracao_min, c.volume_m3, c.fck_mpa, c.slump_mm,
               c.usina, c.bomba, c.equipe, c.status,
               c.criado_por, c.alterado_por, c.criado_em, c.atualizado_em,
               o.nome AS obra, o.cliente, o.cidade, o.id AS obra_id,
               c.observacoes
        FROM concretagens c
        JOIN obras o ON o.id = c.obra_id
        WHERE c.data BETWEEN ? AND ?
        ORDER BY c.data ASC, c.hora_inicio ASC
    """, [range_start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")])

def get_concretagem_by_id(cid: int) -> Dict[str, Any]:
    df = qdf("SELECT * FROM concretagens WHERE id=?", [int(cid)])
    if df.empty:
        return {}
    return df.iloc[0].to_dict()

def add_history(concretagem_id: int, action: str, before: Any, after: Any, user: str):
    qexec("""INSERT INTO historico_concretagens
             (concretagem_id, acao, antes_json, depois_json, feito_por, feito_em)
             VALUES (?, ?, ?, ?, ?, ?)""",
          [int(concretagem_id), action, jdump(before) if before is not None else None, jdump(after) if after is not None else None, user, now_iso()])

def get_history(concretagem_id: int) -> pd.DataFrame:
    return qdf("""
        SELECT id, feito_em, feito_por, acao, antes_json, depois_json
        FROM historico_concretagens
        WHERE concretagem_id=?
        ORDER BY id DESC
    """, [int(concretagem_id)])

def find_conflicts(new_data: str, new_hora: str, new_dur: int, bomba: str, equipe: str, ignore_id: Optional[int] = None) -> List[Dict[str, Any]]:
    df = qdf("""
        SELECT id, data, hora_inicio, duracao_min, bomba, equipe, status
        FROM concretagens
        WHERE data = ? AND status IN ('Agendado','Confirmado','Execucao')
    """, [new_data])

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

# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Agendamentos de Concretagens", layout="wide")
init_db()

st.title("📅 Agendamentos de Concretagens")

login_box()
require_login()

# Menu
menu = st.sidebar.radio(
    "Menu",
    ["Dashboard", "Novo agendamento", "Agenda (lista)", "Obras", "Histórico", "Admin"],
    index=0
)

# Range padrão (semana atual)
today = date.today()
week_start = today - timedelta(days=today.weekday())
week_end = week_start + timedelta(days=6)

# ----------------------------
# Dashboard
# ----------------------------
if menu == "Dashboard":
    colA, colB, colC = st.columns(3)
    with colA:
        st.metric("Hoje", today.strftime("%d/%m/%Y"))
    with colB:
        st.metric("Semana", f"{week_start.strftime('%d/%m')} → {week_end.strftime('%d/%m')}")
    with colC:
        st.caption(f"Timezone: {TZ_LABEL}")

    dfw = get_concretagens(week_start, week_end)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Agendado", int((dfw["status"] == "Agendado").sum()) if not dfw.empty else 0)
    c2.metric("Confirmado", int((dfw["status"] == "Confirmado").sum()) if not dfw.empty else 0)
    c3.metric("Execução", int((dfw["status"] == "Execucao").sum()) if not dfw.empty else 0)
    c4.metric("Concluído", int((dfw["status"] == "Concluido").sum()) if not dfw.empty else 0)
    c5.metric("Cancelado", int((dfw["status"] == "Cancelado").sum()) if not dfw.empty else 0)

    st.subheader("📌 Próximas concretagens (7 dias)")
    df_next = get_concretagens(today, today + timedelta(days=7))
    if df_next.empty:
        st.info("Nenhuma concretagem nos próximos 7 dias.")
    else:
        show = df_next[[
            "data","hora_inicio","obra","cliente","cidade","volume_m3","fck_mpa","slump_mm",
            "usina","bomba","equipe","status","criado_por","alterado_por","atualizado_em"
        ]].copy()
        st.dataframe(show, use_container_width=True, hide_index=True)

# ----------------------------
# Obras
# ----------------------------
elif menu == "Obras":
    st.subheader("🏗️ Obras")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown("### ➕ Cadastrar nova obra")

        # --- Autopreencher via CNPJ (fora do form) ---
        st.markdown("#### 🔎 Autopreencher pelo CNPJ")
        cnpj_in = st.text_input("CNPJ (opcional, para autopreencher)", key="obra_cnpj")
        st.caption("Informe com ou sem pontuação. (API pública pode ter limite de consultas.)")

        if st.button("🔎 Buscar dados pelo CNPJ", use_container_width=True):
            cnpj_digits = only_digits(cnpj_in)
            if len(cnpj_digits) != 14:
                st.error("CNPJ inválido. Informe 14 dígitos (com ou sem pontuação).")
            else:
                try:
                    payload = cnpjws_lookup(cnpj_digits)
                    if payload is None:
                        st.warning("CNPJ não encontrado.")
                    else:
                        fields = parse_cnpjws_to_fields(payload)

                        # Preencher campos (antes de renderizar os inputs do form, evitando StreamlitAPIException)
                        st.session_state["obra_cliente"] = (fields.get("nome_fantasia") or fields.get("razao_social") or "").strip()
                        st.session_state["obra_endereco"] = (fields.get("endereco") or "").strip()
                        st.session_state["obra_cidade"] = (fields.get("cidade") or "").strip()

                        # Campos extras (salvos no banco)
                        st.session_state["obra_razao_social"] = (fields.get("razao_social") or "").strip()
                        st.session_state["obra_nome_fantasia"] = (fields.get("nome_fantasia") or "").strip()
                        st.session_state["obra_cnpj"] = cnpj_digits

                        st.success("Dados carregados ✅ (confira e ajuste se precisar)")
                        (st.rerun if hasattr(st, "rerun") else st.experimental_rerun)()
                except RuntimeError as e:
                    msg = str(e)
                    if msg == "requests_not_installed":
                        st.error("Dependência 'requests' não instalada. (No Cloud: inclua requests no requirements.txt).")
                    elif msg == "rate_limit":
                        st.warning("Limite de consultas atingido (API pública). Aguarde ~1 minuto e tente novamente.")
                    elif msg.startswith("network_error"):
                        st.error("Falha de rede ao consultar o CNPJ. Tente novamente.")
                    else:
                        st.error(f"Erro ao consultar CNPJ: {msg}")

        st.divider()

        with st.form("form_obra", clear_on_submit=False):
            cA, cB = st.columns([1, 1])
            with cA:
                nome = st.text_input("Nome da obra *", key="obra_nome")
            with cB:
                cliente = st.text_input("Cliente", key="obra_cliente")

            endereco = st.text_input("Endereço", key="obra_endereco")
            cidade = st.text_input("Cidade", key="obra_cidade")
            responsavel = st.text_input("Responsável", key="obra_responsavel")
            telefone = st.text_input("Telefone/WhatsApp", key="obra_telefone")

            ok = st.form_submit_button("💾 Salvar obra", use_container_width=True)
            if ok:
                if not (nome or "").strip():
                    st.error("Informe o nome da obra.")
                else:
                    cnpj_digits = only_digits(cnpj_in)
                    # Usa os campos extras preenchidos na busca (quando houver)
                    razao = (st.session_state.get("obra_razao_social") or "").strip()
                    fantasia = (st.session_state.get("obra_nome_fantasia") or "").strip()

                    qexec(
                        """INSERT INTO obras (nome, cliente, cnpj, razao_social, nome_fantasia, endereco, cidade, responsavel, telefone, criado_em)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        [
                            (nome or "").strip(),
                            (cliente or "").strip(),
                            cnpj_digits if len(cnpj_digits) == 14 else "",
                            razao,
                            fantasia,
                            (endereco or "").strip(),
                            (cidade or "").strip(),
                            (responsavel or "").strip(),
                            (telefone or "").strip(),
                            now_iso(),
                        ],
                    )
                    st.success("Obra cadastrada ✅")


    with col2:
        st.markdown("### 📚 Obras cadastradas")
        df = get_obras()
        st.dataframe(df, use_container_width=True, hide_index=True)

# ----------------------------
# Novo agendamento
# ----------------------------
elif menu == "Novo agendamento":
    st.subheader("🧱 Novo agendamento de concretagem")

    obras = get_obras()
    if obras.empty:
        st.warning("Cadastre uma obra primeiro (menu: Obras).")
    else:
        obra_label = obras.apply(lambda r: f"#{r['id']} — {r['nome']} ({r.get('cliente','') or 'Sem cliente'})", axis=1).tolist()
        obra_id_map = {obra_label[i]: int(obras.iloc[i]["id"]) for i in range(len(obras))}

        with st.form("form_conc", clear_on_submit=False):
            obra_sel = st.selectbox("Obra *", obra_label)

            # (mobile-friendly) duas colunas no máximo
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
            status = st.selectbox("Status", STATUS, index=0)
            obs = st.text_area("Observações", value="")

            cap = st.number_input("Capacidade caminhão (m³) p/ estimativa", min_value=4.0, value=8.0, step=0.5)
            st.caption(f"Estimativa: **{calc_trucks(volume, cap)} caminhões** (capacidade {cap} m³).")

            salvar = st.form_submit_button("Salvar agendamento", use_container_width=True)

            if salvar:
                data_str = d.strftime("%Y-%m-%d")
                hora_str = h.strftime("%H:%M")
                obra_id = obra_id_map[obra_sel]

                conflicts = find_conflicts(data_str, hora_str, int(dur), bomba, equipe, ignore_id=None)
                if conflicts:
                    st.error("Conflito detectado (mesma bomba/equipe no mesmo horário).")
                    st.dataframe(pd.DataFrame(conflicts), use_container_width=True, hide_index=True)
                    st.stop()

                user = current_user()
                now = now_iso()

                new_id = qexec("""INSERT INTO concretagens
                    (obra_id, data, hora_inicio, duracao_min, volume_m3, fck_mpa, slump_mm, usina, bomba, equipe,
                     status, observacoes, criado_em, atualizado_em, criado_por, alterado_por)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [obra_id, data_str, hora_str, int(dur), float(volume),
                     float(fck) if fck else None, (slump or "").strip(),
                     (usina or "").strip(), (bomba or "").strip(), (equipe or "").strip(),
                     status, (obs or "").strip(),
                     now, now, user, user]
                )

                after = get_concretagem_by_id(new_id)
                add_history(new_id, "CREATE", None, after, user)
                st.success(f"Agendamento criado ✅ (ID {new_id})")

# ----------------------------
# Agenda (lista) + editar
# ----------------------------
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

    df = get_concretagens(ini, fim)
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
        st.dataframe(df[view_cols], use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("### ✏️ Editar agendamento")

        ids = df["id"].tolist()
        sel_id = st.selectbox("Selecione pelo ID", ids)
        row = df[df["id"] == sel_id].iloc[0].to_dict()

        # Form de edição
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

            salvar = st.form_submit_button("Salvar alterações", use_container_width=True)

            if salvar:
                before = get_concretagem_by_id(sel_id)

                data_str = str(before["data"])
                hora_str = str(before["hora_inicio"])

                conflicts = find_conflicts(data_str, hora_str, int(new_dur), new_bomba, new_equipe, ignore_id=int(sel_id))
                if conflicts:
                    st.error("Conflito detectado (mesma bomba/equipe no mesmo horário).")
                    st.dataframe(pd.DataFrame(conflicts), use_container_width=True, hide_index=True)
                    st.stop()

                user = current_user()
                qexec("""UPDATE concretagens
                        SET status=?, duracao_min=?, bomba=?, equipe=?, usina=?, slump_mm=?,
                            volume_m3=?, fck_mpa=?, observacoes=?,
                            atualizado_em=?, alterado_por=?
                        WHERE id=?""",
                      [new_status, int(new_dur),
                       (new_bomba or "").strip(), (new_equipe or "").strip(),
                       (new_usina or "").strip(), (new_slump or "").strip(),
                       float(new_volume), float(new_fck) if new_fck else None,
                       (new_obs or "").strip(),
                       now_iso(), user, int(sel_id)])

                after = get_concretagem_by_id(sel_id)
                add_history(sel_id, "UPDATE", before, after, user)
                st.success("Atualizado ✅")
                st.rerun()

# ----------------------------
# Histórico
# ----------------------------
elif menu == "Histórico":
    st.subheader("🧾 Histórico de alterações (auditoria)")

    # Escolhe um ID existente recente
    df_recent = qdf("""
        SELECT c.id, c.data, c.hora_inicio, o.nome AS obra, c.status
        FROM concretagens c
        JOIN obras o ON o.id=c.obra_id
        ORDER BY c.id DESC
        LIMIT 200
    """)
    if df_recent.empty:
        st.info("Nenhum agendamento ainda.")
    else:
        pick = st.selectbox(
            "Selecione um agendamento",
            df_recent.apply(lambda r: f"ID {r['id']} — {r['data']} {r['hora_inicio']} — {r['obra']} — {r['status']}", axis=1).tolist()
        )
        sel_id = int(pick.split("—")[0].replace("ID", "").strip())

        hist = get_history(sel_id)
        if hist.empty:
            st.info("Sem histórico.")
        else:
            st.dataframe(hist[["feito_em","feito_por","acao"]], use_container_width=True, hide_index=True)

            with st.expander("Ver detalhes (antes/depois)", expanded=False):
                for _, r in hist.iterrows():
                    st.markdown(f"**{r['feito_em']}** — {r['feito_por']} — `{r['acao']}`")
                    a = jsafe_load(r["antes_json"])
                    b = jsafe_load(r["depois_json"])
                    st.code(jdump({"antes": a, "depois": b}), language="json")
                    st.divider()

# ----------------------------
# Admin (usuários + relatórios)
# ----------------------------
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
            ok = st.form_submit_button("Criar", use_container_width=True)

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
                active = st.checkbox("Ativo", value=bool(int(row["is_active"])))
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
            ok = st.form_submit_button("Alterar", use_container_width=True)
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

        df = get_concretagens(ini, fim)
        if df.empty:
            st.info("Nada no período.")
        else:
            rep = df[[
                "data","hora_inicio","duracao_min","obra","cliente","cidade",
                "volume_m3","fck_mpa","slump_mm","usina","bomba","equipe","status",
                "criado_por","alterado_por","criado_em","atualizado_em","observacoes"
            ]].copy()
            st.dataframe(rep, use_container_width=True, hide_index=True)
            xlsx = export_excel(rep)
            st.download_button(
                "⬇️ Baixar Excel",
                data=xlsx,
                file_name=f"agendamentos_{ini.strftime('%Y%m%d')}_{fim.strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

st.sidebar.divider()
st.sidebar.caption("Auditoria ativa: criado_por / alterado_por + histórico antes/depois ✅")
