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

# --- CSS APPLE V11 (STABLE & CLEAN) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&family=JetBrains+Mono&display=swap');

    html, body, [class*="css"], .stMarkdown {
        font-family: 'Poppins', sans-serif !important;
        background-color: #000000 !important;
    }

    .stApp {
        background: radial-gradient(circle at 50% -20%, #1a2a3a 0%, #000000 100%) !important;
    }

    /* HEADER TRANSPARENTE TOTAL */
    .app-header { 
        display: flex; align-items: center; justify-content: center;
        padding: 50px 0 30px 0; background: transparent !important; gap: 30px;
    }
    .big-snowflake { font-size: 6rem; color: #29b5e8; opacity: 0.8; }
    .title-text-group { text-align: left; }
    .app-header h1 { font-size: 3.2rem !important; font-weight: 600 !important; color: #FFFFFF !important; margin: 0 !important; }
    .app-header p { color: #29b5e8; letter-spacing: 8px; font-size: 0.8rem; text-transform: uppercase; margin: 0 !important; }

    /* BARRA DE COMANDO */
    .command-bar { display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px; }

    /* BOTÓN MASIVO IZQUIERDA */
    div.stButton > button[key="masivo_btn"] {
        background: #29b5e8 !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 12px 40px !important;
        font-size: 0.9rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 2px !important;
        width: 320px !important;
        box-shadow: 0 10px 20px rgba(41, 181, 232, 0.15) !important;
    }

    /* BOTÓN LIMPIAR DERECHA PEQUEÑO */
    div.stButton > button[key="clear_log"] {
        background: transparent !important;
        color: #555 !important;
        border: 1px solid #333 !important;
        border-radius: 8px !important;
        padding: 4px 15px !important;
        font-size: 0.65rem !important;
        text-transform: uppercase !important;
    }

    /* BOTONES TAREA RECTANGULARES 16:9 */
    [data-testid="stColumn"] div.stButton > button {
        background: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        color: #ffffff !important;
        border-radius: 14px !important;
        height: 75px !important;
        width: 100% !important;
        font-size: 0.75rem !important;
        font-weight: 500 !important;
        transition: all 0.3s ease !important;
        display: block !important;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    [data-testid="stColumn"] div.stButton > button:hover {
        border-color: #29b5e8 !important;
        background: rgba(41, 181, 232, 0.05) !important;
        transform: translateY(-2px);
    }

    /* CONSOLA */
    .terminal-window {
        background-color: #050505;
        color: #29b5e8;
        font-family: 'JetBrains Mono', monospace !important;
        padding: 15px;
        border: 1px solid #1a1a1a;
        border-radius: 12px;
        height: 250px;
        overflow-y: auto;
        margin-top: 10px;
        box-shadow: inset 0 0 10px rgba(0,0,0,0.5);
    }
    .log-line { line-height: 1.6; font-size: 13px; margin: 2px 0; border-bottom: 1px solid rgba(255,255,255,0.01); }
    </style>
    """, unsafe_allow_html=True)

# =========================================================
# ESTRUCTURA DE DATOS ORIGINAL (INTACTA)
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

# --- LOGS ---
if 'logs' not in st.session_state:
    st.session_state.logs = ["› SnowSync Kernel Ready."]

def add_log(msg):
    st.session_state.logs.append(f"› {time.strftime('%H:%M:%S')} | {msg}")

# --- CORE FUNCTIONS ---
def get_sql_content(drive_service, file_name):
    query = f"name='{file_name}' and trashed=false"
    results = drive_service.files().list(q=query, fields='files(id, name)').execute()
    items = results.get('files', [])
    if not items: return None
    request = drive_service.files().get_media(fileId=items[0]['id'])
    fh = io.BytesIO(); downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: _, done = downloader.next_chunk()
    return fh.getvalue().decode('utf-8')

def run_task(t, drive_service, gc, cs, console_placeholder):
    try:
        sh = gc.open_by_key(t["sheet"])
        query = get_sql_content(drive_service, t["sql"])
        if not query: return False
        cs.execute(query)
        df = pd.DataFrame(cs.fetchall(), columns=[col[0] for col in cs.description])
        try: wks = sh.worksheet(t["tab"])
        except: wks = sh.add_worksheet(title=t["tab"], rows=1000, cols=20)
        wks.batch_clear([f"{t['c_start']}:{t['c_end']}{wks.row_count}"])
        set_with_dataframe(wks, df, row=t["p_row"], col=t.get("p_col", 1), include_column_header=True)
        return True
    except: return False

# --- UI START ---
st.markdown('<div class="app-header"><div class="big-snowflake">❄️</div><div class="title-text-group"><h1>SnowSync</h1><p>Enterprise Edition</p></div></div>', unsafe_allow_html=True)

try:
    sf_token = st.secrets["SNOWFLAKE_TOKEN"]
    google_info = json.loads(base64.b64decode(st.secrets["GOOGLE_BASE64"]).decode('utf-8'))
    creds = Credentials.from_service_account_info(google_info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly'])
    drive_service, gc = build('drive', 'v3', credentials=creds), gspread.authorize(creds)
    SF_PARAMS['password'] = sf_token

    # BARRA DE COMANDO: MASIVO (IZQ) | LIMPIAR (DER)
    col_l, col_empty, col_r = st.columns([2, 1, 1])
    with col_l:
        execute_masivo = st.button("EJECUTAR MASIVO", key="masivo_btn")
    with col_r:
        if st.button("Clear Console", key="clear_log"):
            st.session_state.logs = ["› Logs flushed."]; st.rerun()

    # CONSOLA
    console_placeholder = st.empty()
    def render_console():
        content = "".join([f'<div class="log-line">{l}</div>' for l in st.session_state.logs[-10:]])
        console_placeholder.markdown(f'<div class="terminal-window">{content}</div>', unsafe_allow_html=True)
    
    render_console()

    # LÓGICA EJECUCIÓN MASIVA
    if execute_masivo:
        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
        for t in TAREAS:
            add_log(f"Syncing: {t['tab']}...")
            render_console()
            run_task(t, drive_service, gc, cs, console_placeholder)
            add_log(f"Done: {t['tab']}")
            render_console()
        cs.close(); conn.close()
        st.rerun()

    # MUNDOS
    mundos = {}
    for tarea in TAREAS:
        sid = tarea["sheet"]
        if sid not in mundos: mundos[sid] = []
        mundos[sid].append(tarea)

    for sid, lista in mundos.items():
        nombre = NOMBRES_MUNDOS.get(sid, sid[:8])
        with st.expander(f"{nombre}"):
            if st.button(f"Sync World: {nombre}", key=f"m_{sid}"):
                conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                for t in lista:
                    add_log(f"Processing: {t['tab']}")
                    render_console()
                    run_task(t, drive_service, gc, cs, console_placeholder)
                cs.close(); conn.close()
                st.rerun()
            
            st.markdown("<br>", unsafe_allow_html=True)
            cols = st.columns(6) 
            for i, t in enumerate(lista):
                with cols[i % 6]:
                    if st.button(t['tab'].replace('_', ' '), key=f"btn_{t['tab']}_{sid}"):
                        add_log(f"Manual Sync: {t['tab']}")
                        render_console()
                        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                        run_task(t, drive_service, gc, cs, console_placeholder)
                        cs.close(); conn.close()
                        st.rerun()

except Exception as e:
    st.error(f"Error: {e}")
