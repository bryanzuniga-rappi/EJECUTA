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
# CONFIGURACIÓN DE NOMBRES DE MUNDOS
# =========================================================
NOMBRES_MUNDOS = {
    "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s": "Operaciones CH",
    "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok": "Golden Engine",
    "1E4L8mssR-C1BXQd67YZnMAuSgALEXVpwQmGN1Nayxv0": "SKUs Inventory",
    "1KN6xp10n1_4WWlOBFz2AnQrcFjUyuOE5cwxAjg-bGaA": "AVL Analytics",
    "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I": "Daily Records",
    "1TBmD3vqOmfNRAgceIvfsxHL3lkO62zSrujVF9ed4LnU": "Master Stocks",
    "1RQ48gT6PO1tb05TAHdKhL9iIuV4XTmJRTNp8qCmNf_0": "Bags Supply"
}

# --- CSS APPLE ULTIMATE V7 (CONSOLA REAL + GHOST PILL) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&family=JetBrains+Mono&display=swap');

    html, body, [class*="css"], .stMarkdown {
        font-family: 'Poppins', sans-serif !important;
        background-color: #000000 !important;
    }

    .stApp {
        background: radial-gradient(circle at 50% -20%, #111b27 0%, #000000 100%) !important;
    }

    /* HEADER SIN NADA DETRÁS */
    .app-header { 
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 40px 0; 
        background: transparent !important;
        gap: 30px;
    }
    .big-snowflake { font-size: 6rem; color: #29b5e8; opacity: 0.8; }
    .title-text-group { text-align: left; }
    .app-header h1 { font-size: 3.2rem !important; font-weight: 600 !important; color: #FFFFFF !important; margin: 0 !important; }
    .app-header p { color: #29b5e8; letter-spacing: 8px; font-size: 0.8rem; text-transform: uppercase; margin: 0 !important; }

    /* BOTÓN MAESTRO: CAPSULE GHOST PILL */
    div.stButton > button:first-child {
        background: transparent !important;
        color: #29b5e8 !important;
        border: 1px solid rgba(41, 181, 232, 0.4) !important;
        border-radius: 50px !important;
        padding: 10px 80px !important;
        font-size: 0.85rem !important;
        text-transform: uppercase !important;
        letter-spacing: 4px !important;
        transition: all 0.5s ease !important;
        margin: 0 auto !important;
        display: block !important;
        width: fit-content !important;
    }
    div.stButton > button:first-child:hover {
        background: rgba(41, 181, 232, 0.1) !important;
        border-color: #29b5e8 !important;
        box-shadow: 0 0 20px rgba(41, 181, 232, 0.2) !important;
    }

    /* CONSOLA CONVENCIONAL */
    .terminal-window {
        background-color: #050505;
        color: #29b5e8;
        font-family: 'JetBrains Mono', monospace !important;
        padding: 20px;
        border: 1px solid #1a1a1a;
        border-radius: 12px;
        height: 250px;
        overflow-y: auto;
        margin: 10px auto;
        max-width: 98%;
        box-shadow: inset 0 0 10px rgba(0,0,0,0.8);
    }
    .log-line { line-height: 1.6; font-size: 13px; margin: 2px 0; border-left: 2px solid #1a1a1a; padding-left: 10px; }
    .cursor { display: inline-block; width: 8px; height: 15px; background-color: #29b5e8; animation: blink 1s infinite; }
    @keyframes blink { 50% { opacity: 0; } }

    /* BOTONES DE TAREA 16:9 */
    [data-testid="stColumn"] div.stButton > button {
        background: rgba(255, 255, 255, 0.02) !important;
        border: 1px solid rgba(255, 255, 255, 0.08) !important;
        color: #d1d1d1 !important;
        border-radius: 12px !important;
        height: 85px !important; 
        width: 155px !important;
        font-size: 0.75rem !important;
        transition: all 0.3s ease !important;
    }
    [data-testid="stColumn"] div.stButton > button:hover {
        border-color: rgba(41, 181, 232, 0.4) !important;
        color: #ffffff !important;
        transform: translateY(-2px);
    }

    /* BOTÓN LIMPIAR */
    .clear-btn-container { text-align: right; margin-right: 2%; margin-bottom: -10px; }
    .clear-btn-container button {
        background: transparent !important;
        color: #555 !important;
        border: none !important;
        font-size: 0.7rem !important;
        text-transform: uppercase !important;
    }
    .clear-btn-container button:hover { color: #29b5e8 !important; }

    /* EXPANDERS */
    .stExpander { border: none !important; background: rgba(255, 255, 255, 0.01) !important; border-radius: 20px !important; }
    </style>
    """, unsafe_allow_html=True)

# --- ESTRUCTURA DE DATOS ORIGINAL (SIN CAMBIOS) ---
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

# --- LÓGICA DE LOGS ---
if 'logs' not in st.session_state: st.session_state.logs = ["> SnowSync Kernel initialized."]
def add_log(msg): st.session_state.logs.append(msg)

# --- CORE FUNCTIONS (PASO A PASO) ---
def get_sql_content(drive_service, file_name):
    try:
        add_log(f"Searching Drive: {file_name}")
        st.rerun() # Fuerza visualización del log actual
    except: pass
    
    try:
        query = f"name='{file_name}' and trashed=false"
        results = drive_service.files().list(q=query, fields='files(id, name)').execute()
        items = results.get('files', [])
        if not items: 
            add_log(f"FAIL: {file_name} not found.")
            return None
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
        add_log(f"--- STARTING: {t['tab']} ---")
        sh = gc.open_by_key(t["sheet"])
        query = get_sql_content(drive_service, t["sql"])
        if not query: return False
        
        add_log(f"Snowflake: Processing SQL query...")
        st.rerun()
        cs.execute(query)
        df = pd.DataFrame(cs.fetchall(), columns=[col[0] for col in cs.description])
        
        try: wks = sh.worksheet(t["tab"])
        except: wks = sh.add_worksheet(title=t["tab"], rows=1000, cols=20)
        
        add_log(f"Sheets: Clearing destination range...")
        wks.batch_clear([f"{t['c_start']}:{t['c_end']}{wks.row_count}"])
        
        add_log(f"Sheets: Inserting {len(df)} records...")
        st.rerun()
        time.sleep(0.5)
        set_with_dataframe(wks, df, row=t["p_row"], col=t.get("p_col", 1), include_column_header=True)
        add_log(f"VERIFIED: {t['tab']} sync complete.")
        st.rerun()
        return True
    except Exception as e:
        add_log(f"ERROR: {str(e)[:50]}...")
        return False

# --- UI START ---
st.markdown('<div class="app-header"><div class="big-snowflake">❄️</div><div class="title-text-group"><h1>SnowSync</h1><p>Enterprise Edition</p></div></div>', unsafe_allow_html=True)

try:
    sf_token = st.secrets["SNOWFLAKE_TOKEN"]
    google_info = json.loads(base64.b64decode(st.secrets["GOOGLE_BASE64"]).decode('utf-8'))
    creds = Credentials.from_service_account_info(google_info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly'])
    drive_service, gc = build('drive', 'v3', credentials=creds), gspread.authorize(creds)
    SF_PARAMS['password'] = sf_token

    # BOTÓN MAESTRO GHOST
    if st.button("EJECUTAR MASIVO"):
        add_log(">>> MASTER SYNC INITIATED")
        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
        for t in TAREAS:
            run_task(t, drive_service, gc, cs)
        cs.close(); conn.close()
        add_log(">>> GLOBAL PIPELINE FINISHED")
        st.rerun()

    # BARRA DE HERRAMIENTAS CONSOLA
    st.markdown('<div class="clear-btn-container">', unsafe_allow_html=True)
    if st.button("Clear Console", key="clear_log"):
        st.session_state.logs = ["> Console cleared."]
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # CONSOLA PROFESIONAL AZUL
    log_content = "".join([f'<div class="log-line">{l}</div>' for l in st.session_state.logs[-12:]])
    st.markdown(f'<div class="terminal-window">{log_content}<span class="cursor"></span></div>', unsafe_allow_html=True)

    # AGRUPACIÓN
    mundos = {}
    for tarea in TAREAS:
        sid = tarea["sheet"]
        if sid not in mundos: mundos[sid] = []
        mundos[sid].append(tarea)

    # RENDERIZADO
    for sid, lista in mundos.items():
        nombre = NOMBRES_MUNDOS.get(sid, sid[:8])
        with st.expander(f"{nombre}"):
            if st.button(f"Ejecutar {nombre} Completo", key=f"m_{sid}"):
                conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                for t in lista: run_task(t, drive_service, gc, cs)
                cs.close(); conn.close()
            
            st.markdown("<br>", unsafe_allow_html=True)
            cols = st.columns(8) 
            for i, t in enumerate(lista):
                with cols[i % 8]:
                    if st.button(t['tab'].replace('_', ' '), key=f"btn_{t['tab']}_{sid}"):
                        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                        run_task(t, drive_service, gc, cs)
                        cs.close(); conn.close()

except Exception as e:
    st.error(f"Error: {e}")
