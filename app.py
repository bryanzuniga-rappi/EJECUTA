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
# 1. AQUÍ CAMBIAS LOS NOMBRES DE TUS MUNDOS (SHEETS)
# =========================================================
NOMBRES_MUNDOS = {
    "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s": "Mundo 1 - Operaciones CH",
    "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok": "Mundo 2 - Golden Dani",
    "1E4L8mssR-C1BXQd67YZnMAuSgALEXVpwQmGN1Nayxv0": "Mundo 3 - SKU Management",
    "1KN6xp10n1_4WWlOBFz2AnQrcFjUyuOE5cwxAjg-bGaA": "Mundo 4 - AVL Dani",
    "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I": "Mundo 5 - Histórico Diario",
    "1TBmD3vqOmfNRAgceIvfsxHL3lkO62zSrujVF9ed4LnU": "Mundo 6 - Inventarios Dos Dueños",
    "1RQ48gT6PO1tb05TAHdKhL9iIuV4XTmJRTNp8qCmNf_0": "Mundo 7 - Stock Bolsas"
}

# --- DISEÑO HACKER / SNOWFLAKE (CSS) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&family=JetBrains+Mono&display=swap');

    html, body, [class*="css"], .stMarkdown {
        font-family: 'Poppins', sans-serif !important;
    }

    .main { background-color: #0e1117; }

    /* BOTÓN MAESTRO AZUL */
    div.stButton > button:first-child {
        background-color: #29b5e8 !important;
        color: white !important;
        border: none !important;
        font-weight: 600 !important;
        padding: 1rem !important;
        border-radius: 12px !important;
        text-transform: uppercase !important;
        letter-spacing: 2px !important;
    }

    /* BOTONES CUADRADOS DE TAREAS */
    div.stButton > button {
        background-color: #1e2229 !important;
        color: #29b5e8 !important;
        border: 2px solid #29b5e8 !important;
        border-radius: 15px !important;
        aspect-ratio: 1 / 1 !important; /* Forza la forma cuadrada */
        width: 100% !important;
        font-size: 14px !important;
        font-weight: 600 !important;
        transition: all 0.3s ease !important;
        margin-bottom: 10px;
    }
    
    div.stButton > button:hover {
        background-color: #29b5e8 !important;
        color: white !important;
        transform: scale(1.05) !important;
    }

    /* CONSOLA TIPO TERMINAL */
    .terminal-box {
        background-color: #000000;
        color: #00FF41; /* Verde Matrix */
        font-family: 'JetBrains Mono', monospace !important;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #333;
        height: 300px;
        overflow-y: auto;
        font-size: 13px;
        line-height: 1.5;
        box-shadow: inset 0 0 10px #00FF41;
    }

    /* EXPANDERS */
    .stExpander {
        border: 1px solid #29b5e8 !important;
        border-radius: 12px !important;
        background-color: #161b22 !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --- ESTRUCTURA DE DATOS (ORIGINAL) ---
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
    {"sql": "STOCK_CH.sql", "sheet": "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s", "tab": "STOCK_CH", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "DOI_TIENDA_CH.sql", "sheet": "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s", "tab": "DOI_TIENDA_CH", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "SALES_CH.sql", "sheet": "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s", "tab": "SALES_CH", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "GOLDEN_DANI.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "Summary Golden", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_WAREHOUSE.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "WAREHOUSE_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_COUNTRY.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "COUNTRY_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_PRODUCT.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "PRODUCT_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_CITY.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "CITY_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_DETAIL.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "DETAIL_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_CATEGORY.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "CATEGORY_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_SUPPLIER.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "SUPPLIER_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_SIN_CH.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "SIN_CH_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_MODEL.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "MODEL_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_ABS.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "ABS_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "AVL_GOLDEN_WEEK.sql", "sheet": "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok", "tab": "WEEK_AVL", "c_start": "A3", "c_end": "N", "p_row": 3, "p_col": 1},
    {"sql": "SKUS_X_DIA.sql", "sheet": "1E4L8mssR-C1BXQd67YZnMAuSgALEXVpwQmGN1Nayxv0", "tab": "SKUS", "c_start": "A1", "c_end": "E", "p_row": 1, "p_col": 1},
    {"sql": "AVL_GOLDEN_DANI.sql", "sheet": "1KN6xp10n1_4WWlOBFz2AnQrcFjUyuOE5cwxAjg-bGaA", "tab": "AVL", "c_start": "A1", "c_end": "C", "p_row": 1, "p_col": 1},
    {"sql": "DOI_BY_DAY.sql", "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I", "tab": "DOI", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "WH_BY_DAY.sql", "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I", "tab": "WH", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "CITY_BY_DAY.sql", "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I", "tab": "CITY", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "SUPPLIER_BY_DAY.sql", "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I", "tab": "SUPPLIER", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "PRODUCT_BY_DAY.sql", "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I", "tab": "PRODUCT", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "TODAY_BY_DAY.sql", "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I", "tab": "CURRENT", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "DETAIL.sql", "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I", "tab": "DETAIL", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "ROQ_PO.sql", "sheet": "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I", "tab": "ROQ_PO", "c_start": "A1", "c_end": "F", "p_row": 1, "p_col": 1},
    {"sql": "INVENTARIOS_DOS_DUENOS.sql", "sheet": "1TBmD3vqOmfNRAgceIvfsxHL3lkO62zSrujVF9ed4LnU", "tab": "BASE", "c_start": "A1", "c_end": "L", "p_row": 1, "p_col": 1},
    {"sql": "STOCK_BOLSAS.sql", "sheet": "1RQ48gT6PO1tb05TAHdKhL9iIuV4XTmJRTNp8qCmNf_0", "tab": "BASE", "c_start": "A1", "c_end": "X", "p_row": 1, "p_col": 1}
]

# --- INICIALIZAR HISTORIAL DE TERMINAL ---
if 'logs' not in st.session_state:
    st.session_state.logs = ["> System initialized...", "> Waiting for commands..."]

def add_log(text):
    timestamp = time.strftime("%H:%M:%S")
    st.session_state.logs.append(f"[{timestamp}] {text}")

# --- FUNCIONES CORE ---
def get_sql_content(drive_service, file_name):
    try:
        query = f"name='{file_name}' and trashed=false"
        results = drive_service.files().list(q=query, spaces='drive', corpora='allDrives', includeItemsFromAllDrives=True, supportsAllDrives=True, fields='files(id, name)').execute()
        items = results.get('files', [])
        if not items: return None
        file_id = items[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False: _, done = downloader.next_chunk()
        return fh.getvalue().decode('utf-8')
    except Exception as e:
        return None

def run_task(t, drive_service, gc, cs):
    try:
        sh = gc.open_by_key(t["sheet"])
        query = get_sql_content(drive_service, t["sql"])
        if not query: return False, "SQL NOT FOUND"
        cs.execute(query)
        df = pd.DataFrame(cs.fetchall(), columns=[col[0] for col in cs.description])
        try:
            wks = sh.worksheet(t["tab"])
        except gspread.exceptions.WorksheetNotFound:
            wks = sh.add_worksheet(title=t["tab"], rows=1000, cols=20)
        wks.batch_clear([f"{t['c_start']}:{t['c_end']}{wks.row_count}"])
        time.sleep(1)
        set_with_dataframe(wks, df, row=t["p_row"], col=t.get("p_col", 1), include_column_header=True)
        return True, f"SUCCESS: {t['tab']}"
    except Exception as e:
        return False, str(e)

# --- APP START ---
st.title("❄️ DATA SYNC PRO: COMMAND CENTER")

try:
    sf_token = st.secrets["SNOWFLAKE_TOKEN"]
    encoded_json = st.secrets["GOOGLE_BASE64"].strip()
    decoded_bytes = base64.b64decode(encoded_json)
    google_info = json.loads(decoded_bytes.decode('utf-8'))
    
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly']
    creds = Credentials.from_service_account_info(google_info, scopes=scopes)
    drive_service = build('drive', 'v3', credentials=creds)
    gc = gspread.authorize(creds)
    SF_PARAMS['password'] = sf_token

    # --- BOTÓN MAESTRO ---
    if st.button("EXECUTE ALL PROTOCOLS (MASTER SYNC)"):
        add_log("ALERT: MASTER PIPELINE INITIATED")
        conn = snowflake.connector.connect(**SF_PARAMS)
        cs = conn.cursor()
        for t in TAREAS:
            add_log(f"RUNNING: {t['tab']}...")
            success, msg = run_task(t, drive_service, gc, cs)
            if success: add_log(f"OK: {t['tab']} sync completed.")
            else: add_log(f"CRITICAL ERROR IN {t['tab']}: {msg}")
        cs.close()
        conn.close()
        add_log("SYSTEM: ALL TASKS FINISHED")

    # --- TERMINAL BOX (HACKER STYLE) ---
    st.markdown(f'''
        <div class="terminal-box">
            {"<br>".join(st.session_state.logs[-15:])}
        </div>
    ''', unsafe_allow_html=True)

    st.markdown("---")

    # --- AGRUPACIÓN POR MUNDOS ---
    mundos = {}
    for tarea in TAREAS:
        s_id = tarea["sheet"]
        if s_id not in mundos: mundos[s_id] = []
        mundos[s_id].append(tarea)

    # --- RENDERIZADO DE MUNDOS ---
    for s_id, lista in mundos.items():
        # Busca el nombre personalizado o usa el ID por defecto
        nombre_mundo = NOMBRES_MUNDOS.get(s_id, f"Mundo ID: {s_id[:10]}...")
        
        with st.expander(f"📁 {nombre_mundo}"):
            # Cuadrícula de botones cuadrados (6 columnas para que se vean compactos)
            cols = st.columns(6) 
            for j, t in enumerate(lista):
                with cols[j % 6]:
                    if st.button(t['tab'], key=f"square_{t['tab']}_{s_id}"):
                        add_log(f"MANUAL OVERRIDE: {t['tab']}")
                        with st.spinner("Executing..."):
                            conn = snowflake.connector.connect(**SF_PARAMS)
                            cs = conn.cursor()
                            success, msg = run_task(t, drive_service, gc, cs)
                            cs.close()
                            conn.close()
                            if success: 
                                add_log(f"SYNC SUCCESS: {t['tab']}")
                                st.toast("Success!")
                            else: 
                                add_log(f"ERROR: {msg}")
                                st.error("Failed")

except Exception as e:
    st.error(f"FATAL BOOT ERROR: {e}")

# Botón para limpiar terminal
if st.sidebar.button("Clear Terminal Logs"):
    st.session_state.logs = ["> Logs cleared..."]
    st.rerun()
