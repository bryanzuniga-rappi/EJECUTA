import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import io
import snowflake.connector
import json
import time
import base64
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Data Sync Pro", page_icon="❄️", layout="wide")

# =========================================================
# CONFIGURACIÓN DE NOMBRES DE MUNDOS
# =========================================================
NOMBRES_MUNDOS = {
    "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s": "1. Operaciones CH",
    "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok": "2. Golden Dani",
    "1E4L8mssR-C1BXQd67YZnMAuSgALEXVpwQmGN1Nayxv0": "3. SKUs",
    "1KN6xp10n1_4WWlOBFz2AnQrcFjUyuOE5cwxAjg-bGaA": "4. AVL",
    "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I": "5. Históricos",
    "1TBmD3vqOmfNRAgceIvfsxHL3lkO62zSrujVF9ed4LnU": "6. Inventarios",
    "1RQ48gT6PO1tb05TAHdKhL9iIuV4XTmJRTNp8qCmNf_0": "7. Bolsas"
}

# --- DISEÑO ULTRA TOP (GLASSMORPHISM & APPLE STYLE) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&family=JetBrains+Mono:wght@400&display=swap');

    /* Variables de Estilo */
    :root {
        --sf-blue: #29b5e8;
        --glass-bg: rgba(255, 255, 255, 0.03);
        --glass-border: rgba(255, 255, 255, 0.1);
    }

    /* Reset General */
    html, body, [class*="css"] {
        font-family: 'Poppins', sans-serif !important;
        background-color: #050505 !important;
        color: #ffffff;
    }

    /* Contenedor de la Terminal (Estilo Terminal Integrada) */
    .terminal-container {
        background: rgba(0, 0, 0, 0.4);
        backdrop-filter: blur(10px);
        border: 1px solid var(--glass-border);
        border-radius: 12px;
        padding: 15px;
        margin-top: 20px;
        font-family: 'JetBrains Mono', monospace;
        color: #a0a0a0;
        font-size: 0.85rem;
        height: 200px;
        overflow-y: auto;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.8);
    }
    .log-entry { margin-bottom: 4px; border-left: 2px solid var(--sf-blue); padding-left: 10px; }
    .log-success { color: #4ade80; }
    .log-error { color: #f87171; }

    /* Botón Maestro (Grande y Minimalista) */
    div.stButton > button:first-child {
        background: linear-gradient(135deg, #29b5e8 0%, #1a89b3 100%) !important;
        color: white !important;
        border: none !important;
        padding: 20px !important;
        border-radius: 14px !important;
        font-weight: 500 !important;
        font-size: 1rem !important;
        letter-spacing: 0.5px !important;
        box-shadow: 0 10px 20px rgba(41, 181, 232, 0.2) !important;
        transition: all 0.4s cubic-bezier(0.165, 0.84, 0.44, 1) !important;
    }
    div.stButton > button:first-child:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 15px 30px rgba(41, 181, 232, 0.4) !important;
    }

    /* Botones de Tarea (Cuadrados de Cristal) */
    [data-testid="stVerticalBlock"] div.stButton > button {
        background: var(--glass-bg) !important;
        border: 1px solid var(--glass-border) !important;
        color: #e0e0e0 !important;
        border-radius: 12px !important;
        aspect-ratio: 1 / 1 !important;
        width: 100% !important;
        font-size: 0.75rem !important;
        font-weight: 400 !important;
        transition: all 0.3s ease !important;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    [data-testid="stVerticalBlock"] div.stButton > button:hover {
        background: rgba(41, 181, 232, 0.1) !important;
        border-color: var(--sf-blue) !important;
        color: var(--sf-blue) !important;
        transform: scale(1.02);
    }

    /* Expanders Estilo Card */
    .stExpander {
        background: transparent !important;
        border: 1px solid var(--glass-border) !important;
        border-radius: 16px !important;
        margin-bottom: 20px !important;
        overflow: hidden;
    }
    .stExpander summary { background-color: var(--glass-bg) !important; padding: 10px 20px !important; }
    
    /* Titulos */
    h1 { font-weight: 600 !important; font-size: 2.2rem !important; margin-bottom: 0px !important; }
    p { opacity: 0.7; }
    </style>
    """, unsafe_allow_html=True)

# --- ESTRUCTURA DE DATOS (RESTAURADA ORIGINAL) ---
SF_PARAMS = {
    'user': 'bryan.zuniga@rappi.com',
    'account': 'hg51401',
    'authenticator': 'snowflake',
    'warehouse': 'RP_PERSONALUSER_WH',
    'database': 'FIVETRAN',
    'schema': 'PUBLIC',
    'role': 'RP_READ_ACCESS_PU_ROLE'
}

TAREAS = [
    {
        "sql": "STOCK_CH.sql",
        "sheet": "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s",
        "tab": "STOCK_CH",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "DOI_TIENDA_CH.sql",
        "sheet": "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s",
        "tab": "DOI_TIENDA_CH",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "SALES_CH.sql",
        "sheet": "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s",
        "tab": "SALES_CH",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "GOLDEN_DANI.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "Summary Golden",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_WAREHOUSE.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "WAREHOUSE_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_COUNTRY.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "COUNTRY_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_PRODUCT.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "PRODUCT_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_CITY.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "CITY_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_DETAIL.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "DETAIL_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_CATEGORY.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "CATEGORY_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_SUPPLIER.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "SUPPLIER_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_SIN_CH.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "SIN_CH_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_MODEL.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "MODEL_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_ABS.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "ABS_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_WEEK.sql",
        "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok",
        "tab": "WEEK_AVL",
        "c_start": "A3",
        "c_end": "N",
        "p_row": 3,
        "p_col": 1
    },
    {
        "sql": "SKUS_X_DIA.sql",
        "sheet": "1E4L8mssR-C1BXQd67YZnMAuSgALEXVpwQmGN1Nayxv0",
        "tab": "SKUS",
        "c_start": "A1",
        "c_end": "E",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "AVL_GOLDEN_DANI.sql",
        "sheet": "1KN6xp10n1_4WWlOBFz2AnQrcFjUyuOE5cwxAjg-bGaA",
        "tab": "AVL",
        "c_start": "A1",
        "c_end": "C",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "DOI_BY_DAY.sql",
        "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I",
        "tab": "DOI",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "WH_BY_DAY.sql",
        "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I",
        "tab": "WH",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "CITY_BY_DAY.sql",
        "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I",
        "tab": "CITY",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "SUPPLIER_BY_DAY.sql",
        "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I",
        "tab": "SUPPLIER",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "PRODUCT_BY_DAY.sql",
        "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I",
        "tab": "PRODUCT",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "TODAY_BY_DAY.sql",
        "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I",
        "tab": "CURRENT",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "DETAIL.sql",
        "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I",
        "tab": "DETAIL",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "ROQ_PO.sql",
        "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I",
        "tab": "ROQ_PO",
        "c_start": "A1",
        "c_end": "F",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "INVENTARIOS_DOS_DUENOS.sql",
        "sheet": "1TBmD3vqOmfNRAgceIvfsxHL3lkO62zSrujVF9ed4LnU",
        "tab": "BASE",
        "c_start": "A1",
        "c_end": "L",
        "p_row": 1,
        "p_col": 1
    },
    {
        "sql": "STOCK_BOLSAS.sql",
        "sheet": "1RQ48gT6PO1tb05TAHdKhL9iIuV4XTmJRTNp8qCmNf_0",
        "tab": "BASE",
        "c_start": "A1",
        "c_end": "X",
        "p_row": 1,
        "p_col": 1
    }
]

# --- LÓGICA DE TERMINAL ---
if 'logs' not in st.session_state:
    st.session_state.logs = ["Ready to sync."]

def log(msg, type="info"):
    cl = "log-success" if type == "success" else "log-error" if type == "error" else ""
    st.session_state.logs.append(f'<div class="log-entry {cl}">[{time.strftime("%H:%M:%S")}] {msg}</div>')

# --- FUNCIONES CORE ---
def get_sql_content(drive_service, file_name):
    try:
        query = f"name='{file_name}' and trashed=false"
        results = drive_service.files().list(q=query, fields='files(id, name)').execute()
        items = results.get('files', [])
        if not items: return None
        file_id = items[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        return fh.getvalue().decode('utf-8')
    except: return None

def run_task(t, drive_service, gc, cs):
    try:
        sh = gc.open_by_key(t["sheet"])
        query = get_sql_content(drive_service, t["sql"])
        if not query: return False, "SQL_ERROR"
        cs.execute(query)
        df = pd.DataFrame(cs.fetchall(), columns=[col[0] for col in cs.description])
        try: wks = sh.worksheet(t["tab"])
        except: wks = sh.add_worksheet(title=t["tab"], rows=1000, cols=20)
        wks.batch_clear([f"{t['c_start']}:{t['c_end']}{wks.row_count}"])
        time.sleep(1)
        set_with_dataframe(wks, df, row=t["p_row"], col=t.get("p_col", 1), include_column_header=True)
        return True, "SYNC_OK"
    except Exception as e: return False, str(e)

# --- APP UI ---
st.markdown("<h1>Snowflake Data Sync</h1>", unsafe_allow_html=True)
st.write("Sincronización automatizada de datos a nivel empresarial.")

try:
    # Carga automática de llaves
    sf_token = st.secrets["SNOWFLAKE_TOKEN"]
    google_info = json.loads(base64.b64decode(st.secrets["GOOGLE_BASE64"]).decode('utf-8'))
    creds = Credentials.from_service_account_info(google_info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly'])
    drive_service, gc = build('drive', 'v3', credentials=creds), gspread.authorize(creds)
    SF_PARAMS['password'] = sf_token

    # BOTÓN MAESTRO
    if st.button("EXECUTE ALL PIPELINES"):
        log("Initiating Master Control Pipeline...")
        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
        for t in TAREAS:
            log(f"Processing: {t['tab']}...")
            ok, msg = run_task(t, drive_service, gc, cs)
            if ok: log(f"Verified: {t['tab']}", "success")
            else: log(f"Fail: {t['tab']} - {msg}", "error")
        cs.close(); conn.close()
        log("Master Pipeline Execution Finished.", "success")

    # TERMINAL INTEGRADA
    st.markdown(f'<div class="terminal-container">{"".join(st.session_state.logs[-10:])}</div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # AGRUPACIÓN POR MUNDOS
    mundos = {}
    for tarea in TAREAS:
        sid = tarea["sheet"]
        if sid not in mundos: mundos[sid] = []
        mundos[sid].append(tarea)

    # RENDERIZADO POR EXPANDERS
    for sid, lista in mundos.items():
        nombre = NOMBRES_MUNDOS.get(sid, f"Dataset: {sid[:8]}...")
        with st.expander(f"📦 {nombre}"):
            grid = st.columns(8) # Más compacto: 8 botones por fila
            for i, t in enumerate(lista):
                with grid[i % 8]:
                    if st.button(t['tab'], key=f"t_{t['tab']}"):
                        log(f"Manual Sync: {t['tab']}...")
                        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                        ok, msg = run_task(t, drive_service, gc, cs)
                        cs.close(); conn.close()
                        if ok: log(f"Success: {t['tab']}", "success"); st.toast("OK")
                        else: log(f"Error: {msg}", "error"); st.error("Fail")

except Exception as e:
    st.error(f"System boot failed: {e}")
