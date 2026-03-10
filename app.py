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
st.set_page_config(page_title="SnowSync Enterprise", page_icon="❄️", layout="wide")

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

# --- DISEÑO APPLE PREMIUM / SNOWFLAKE (CSS) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&family=JetBrains+Mono&display=swap');

    html, body, [class*="css"], .stMarkdown {
        font-family: 'Poppins', sans-serif !important;
        background-color: #000000 !important;
    }

    .stApp {
        background: radial-gradient(circle at 50% -20%, #1a2a3a 0%, #000000 100%) !important;
    }

    /* HEADER LIMPIO FLOTANTE */
    .app-header { 
        display: flex; align-items: center; justify-content: center;
        padding: 50px 0 30px 0; background: transparent !important; gap: 30px;
    }
    .big-snowflake { font-size: 6rem; color: #29b5e8; opacity: 0.8; }
    .title-text-group { text-align: left; }
    .app-header h1 { font-size: 3.2rem !important; font-weight: 600 !important; color: #FFFFFF !important; margin: 0 !important; }
    .app-header p { color: #29b5e8; letter-spacing: 8px; font-size: 0.8rem; text-transform: uppercase; margin: 0 !important; }

    /* BOTÓN MAESTRO: IZQUIERDA */
    div.stButton > button:first-child {
        background: #29b5e8 !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 15px 40px !important;
        font-size: 1rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 2px !important;
        width: 350px !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 10px 25px rgba(41, 181, 232, 0.2) !important;
    }
    div.stButton > button:first-child:hover {
        background: #3ac0f2 !important;
        transform: translateY(-2px);
    }

    /* BOTÓN LIMPIAR: DERECHA */
    div.stButton > button[key="clear_log"] {
        background: transparent !important;
        color: #555 !important;
        border: 1px solid #333 !important;
        border-radius: 8px !important;
        padding: 4px 15px !important;
        font-size: 0.65rem !important;
        float: right !important;
    }

    /* BOTONES TAREA RECTANGULARES 16:9 */
    div.stButton > button {
        background-color: rgba(255, 255, 255, 0.03) !important;
        color: #ffffff !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 14px !important;
        height: 75px !important;
        width: 100% !important;
        font-size: 13px !important;
        font-weight: 500 !important;
        transition: all 0.3s ease !important;
        display: block !important;
        white-space: normal !important;
        word-wrap: break-word !important;
    }
    div.stButton > button:hover {
        border-color: #29b5e8 !important;
        background: rgba(41, 181, 232, 0.05) !important;
        transform: translateY(-2px) !important;
    }

    /* CONSOLA AZUL SNOWFLAKE */
    .terminal-box {
        background-color: #050505;
        color: #29b5e8;
        font-family: 'JetBrains Mono', monospace !important;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #1a1a1a;
        height: 300px;
        overflow-y: auto;
        font-size: 13px;
        line-height: 1.5;
        box-shadow: inset 0 0 10px rgba(0,0,0,0.5);
    }

    /* EXPANDERS */
    .stExpander {
        border: none !important;
        background: rgba(255, 255, 255, 0.02) !important;
        border-radius: 20px !important;
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
    st.session_state.logs = ["> SnowSync Kernel Online.", "> System Ready."]

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

# --- UI START ---
st.markdown('<div class="app-header"><div class="big-snowflake">❄️</div><div class="title-text-group"><h1>SnowSync</h1><p>Enterprise Edition</p></div></div>', unsafe_allow_html=True)

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

    # --- BARRA DE COMANDO ---
    col_l, col_r = st.columns([3, 1])
    with col_l:
        if st.button("EJECUTAR MASIVO", key="masivo_btn"):
            add_log("MASTER SYNC INITIATED...")
            conn = snowflake.connector.connect(**SF_PARAMS)
            cs = conn.cursor()
            for t in TAREAS:
                add_log(f"Syncing: {t['tab']}")
                success, msg = run_task(t, drive_service, gc, cs)
            cs.close()
            conn.close()
            add_log("GLOBAL PROTOCOL COMPLETED.")
            st.rerun()

    with col_r:
        if st.button("Limpiar Consola", key="clear_log"):
            st.session_state.logs = ["> Logs flushed."]
            st.rerun()

    # --- TERMINAL BOX ---
    st.markdown(f'''
        <div class="terminal-box">
            {"<br>".join(st.session_state.logs[-12:])}
        </div>
    ''', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- AGRUPACIÓN POR MUNDOS ---
    mundos = {}
    for tarea in TAREAS:
        s_id = tarea["sheet"]
        if s_id not in mundos: mundos[s_id] = []
        mundos[s_id].append(tarea)

    # --- RENDERIZADO DE MUNDOS ---
    for s_id, lista in mundos.items():
        nombre_mundo = NOMBRES_MUNDOS.get(s_id, f"Mundo ID: {s_id[:10]}...")
        with st.expander(f"{nombre_mundo}"):
            cols = st.columns(6) 
            for j, t in enumerate(lista):
                with cols[j % 6]:
                    if st.button(t['tab'], key=f"btn_{t['tab']}_{s_id}"):
                        add_log(f"Manual Override: {t['tab']}")
                        conn = snowflake.connector.connect(**SF_PARAMS)
                        cs = conn.cursor()
                        success, msg = run_task(t, drive_service, gc, cs)
                        cs.close()
                        conn.close()
                        add_log(f"Process ended: {msg}")
                        st.rerun()

except Exception as e:
    st.error(f"FATAL ERROR: {e}")
