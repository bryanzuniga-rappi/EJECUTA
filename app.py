import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import io
import snowflake.connector
import json
import time
from datetime import datetime, timedelta
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
    "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s": "OPERACIONES CH",
    "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok": "GOLDEN ENGINE",
    "1E4L8mssR-C1BXQd67YZnMAuSgALEXVpwQmGN1Nayxv0": "SKU MANAGEMENT",
    "1KN6xp10n1_4WWlOBFz2AnQrcFjUyuOE5cwxAjg-bGaA": "AVL ANALYTICS",
    "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I": "DAILY RECORDS",
    "1TBmD3vqOmfNRAgceIvfsxHL3lkO62zSrujVF9ed4LnU": "MASTER STOCKS",
    "1RQ48gT6PO1tb05TAHdKhL9iIuV4XTmJRTNp8qCmNf_0": "BAGS SUPPLY"
}

# --- CSS APPLE V17 (ESTANDARIZACIÓN TOTAL & FIX ESTRUCTURA) ---
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

    /* HEADER */
    .app-header { 
        display: flex; align-items: center; justify-content: center;
        padding: 40px 0 20px 0; background: transparent !important; gap: 25px;
    }
    .big-snowflake { font-size: 5rem; color: #29b5e8; opacity: 0.8; }
    .title-text-group { text-align: left; }
    .app-header h1 { font-size: 2.8rem !important; font-weight: 600 !important; color: #FFFFFF !important; margin: 0 !important; }
    .app-header p { color: #29b5e8; letter-spacing: 6px; font-size: 0.75rem; text-transform: uppercase; margin: 0 !important; }

    /* BARRA DE COMANDO */
    div.stButton > button[key="masivo_btn"] {
        background: #29b5e8 !important;
        color: white !important;
        border-radius: 10px !important;
        padding: 10px 40px !important;
        font-size: 0.9rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 2px !important;
        width: auto !important;
        min-width: 250px;
    }

    div.stButton > button[key="clear_log"] {
        background: transparent !important;
        color: #555 !important;
        border: 1px solid #333 !important;
        border-radius: 8px !important;
        padding: 4px 15px !important;
        font-size: 0.65rem !important;
        float: right !important;
    }

    /* BOTONES INTERNOS: ESTANDARIZACIÓN 16:9 */
    [data-testid="stExpander"] div.stButton > button {
        background-color: rgba(255, 255, 255, 0.03) !important;
        color: #ffffff !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 12px !important;
        
        /* Geometría Fija */
        height: 75px !important;
        min-height: 75px !important;
        max-height: 75px !important;
        width: 100% !important;
        
        /* Centrado */
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-align: center !important;
        
        font-size: 11px !important;
        text-transform: uppercase;
        line-height: 1.1 !important;
        padding: 8px !important;

        overflow: hidden !important;
        white-space: normal !important;
        word-wrap: break-word !important;
    }
    
    [data-testid="stExpander"] div.stButton > button:hover {
        border-color: #29b5e8 !important;
        background: rgba(41, 181, 232, 0.1) !important;
    }

    /* CONSOLA */
    .terminal-box {
        background-color: #050505;
        color: #29b5e8;
        font-family: 'JetBrains Mono', monospace !important;
        padding: 15px;
        border-radius: 12px;
        border: 1px solid #1a1a1a;
        height: 280px;
        overflow-y: auto;
        font-size: 13px;
    }
    .cursor {
        display: inline-block;
        width: 8px;
        height: 14px;
        background-color: #29b5e8;
        margin-left: 4px;
        animation: blink 1s infinite;
    }
    @keyframes blink { 50% { opacity: 0; } }

    .stExpander { border: none !important; background: rgba(255, 255, 255, 0.015) !important; border-radius: 15px !important; }
    </style>
    """, unsafe_allow_html=True)

# =========================================================
# ESTRUCTURA DE DATOS ORIGINAL (RESPETADA AL 100%)
# =========================================================
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

# --- LÓGICA DE LOGS (UTC-6) ---
if 'logs' not in st.session_state: st.session_state.logs = ["› SnowSync Kernel Online."]

def add_log(msg):
    mx_time = datetime.utcnow() - timedelta(hours=6)
    timestamp = mx_time.strftime("%H:%M:%S")
    st.session_state.logs.append(f"› {timestamp} | {msg}")

# --- CORE FUNCTIONS ---
def get_sql_content(drive_service, file_name):
    try:
        query = f"name='{file_name}' and trashed=false"
        results = drive_service.files().list(q=query, spaces='drive', corpora='allDrives', includeItemsFromAllDrives=True, supportsAllDrives=True, fields='files(id, name)').execute()
        items = results.get('files', [])
        if not items: return None
        file_id = items[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO(); downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False: _, done = downloader.next_chunk()
        return fh.getvalue().decode('utf-8')
    except: return None

def run_task(t, drive_service, gc, cs):
    try:
        sh = gc.open_by_key(t["sheet"])
        query = get_sql_content(drive_service, t["sql"])
        if not query: return False, "SQL NOT FOUND"
        cs.execute(query)
        df = pd.DataFrame(cs.fetchall(), columns=[col[0] for col in cs.description])
        try: wks = sh.worksheet(t["tab"])
        except: wks = sh.add_worksheet(title=t["tab"], rows=1000, cols=20)
        wks.batch_clear([f"{t['c_start']}:{t['c_end']}{wks.row_count}"])
        time.sleep(1)
        set_with_dataframe(wks, df, row=t["p_row"], col=t.get("p_col", 1), include_column_header=True)
        return True, f"SUCCESS: {t['tab']}"
    except Exception as e: return False, str(e)

# --- UI START ---
st.markdown('<div class="app-header"><div class="big-snowflake">❄️</div><div class="title-text-group"><h1>SnowSync</h1><p>Enterprise Edition</p></div></div>', unsafe_allow_html=True)

try:
    sf_token = st.secrets["SNOWFLAKE_TOKEN"]
    google_info = json.loads(base64.b64decode(st.secrets["GOOGLE_BASE64"]).decode('utf-8'))
    creds = Credentials.from_service_account_info(google_info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly'])
    drive_service, gc = build('drive', 'v3', credentials=creds), gspread.authorize(creds)
    SF_PARAMS['password'] = sf_token

    # BARRA DE COMANDO
    col_l, col_r = st.columns([1, 1]) # Ratio 1:1 para dividir la pantalla a la mitad
    
    with col_l:
        # El CSS 'masivo_btn' ya tiene el ancho definido, aquí se queda pegado a la izquierda
        if st.button("EJECUTAR MASIVO", key="masivo_btn"):
            add_log("MASTER SYNC INITIATED...")
            conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
            for t in TAREAS:
                run_task(t, drive_service, gc, cs)
                add_log(f"Synced: {t['tab']}")
            cs.close(); conn.close()
            st.rerun()

    with col_r:
        # Creamos un div con alineación derecha para asegurar que se pegue al borde
        st.markdown('<div style="text-align: right;">', unsafe_allow_html=True)
        if st.button("Limpiar Consola", key="clear_log"):
            st.session_state.logs = ["› Logs flushed."]; st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    # CONSOLA
    log_content = "<br>".join(st.session_state.logs[-12:])
    st.markdown(f'<div class="terminal-box">{log_content}<span class="cursor"></span></div>', unsafe_allow_html=True)

    # MUNDOS
    mundos = {}
    for tarea in TAREAS:
        sid = tarea["sheet"]
        if sid not in mundos: mundos[sid] = []
        mundos[sid].append(tarea)

    for sid, lista in mundos.items():
        nombre_mundo = NOMBRES_MUNDOS.get(sid, sid[:8])
        with st.expander(nombre_mundo):
            cols = st.columns(4)
            for i, t in enumerate(lista):
                with cols[i % 4]:
                    if st.button(t['tab'].replace('_', ' '), key=f"btn_{t['tab']}_{sid}"):
                        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                        run_task(t, drive_service, gc, cs)
                        cs.close(); conn.close(); add_log(f"Manual Sync {t['tab']} OK"); st.rerun()

except Exception as e:
    st.error(f"Error: {e}")
