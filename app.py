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

# --- CSS APPLE MINIMALISTA V6 ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&family=JetBrains+Mono&display=swap');

    html, body, [class*="css"], .stMarkdown {
        font-family: 'Poppins', sans-serif !important;
        background-color: #000000 !important;
    }

    /* Fondo base con degradado sutil */
    .stApp {
        background: radial-gradient(circle at 50% -20%, #111b27 0%, #000000 100%) !important;
    }

    /* TÍTULO LIMPIO (SIN RECUADRO) */
    .app-header { 
        text-align: center; 
        padding: 60px 0 40px 0; 
        background: transparent !important;
    }
    .app-header h1 {
        font-size: 3.5rem !important;
        font-weight: 600 !important;
        letter-spacing: -2px !important;
        color: #FFFFFF !important;
        margin: 0 !important;
        background: none !important;
    }
    .app-header p {
        color: #29b5e8;
        letter-spacing: 8px;
        font-size: 0.8rem;
        text-transform: uppercase;
        margin-top: 5px !important;
        opacity: 0.8;
    }

    /* BOTÓN MAESTRO: GHOST STYLE (MÁS PROFESIONAL) */
    div.stButton > button:first-child {
        background: transparent !important;
        color: #29b5e8 !important;
        border: 1px solid rgba(41, 181, 232, 0.5) !important;
        border-radius: 100px !important;
        padding: 12px 60px !important;
        font-weight: 400 !important;
        font-size: 0.9rem !important;
        text-transform: uppercase !important;
        letter-spacing: 3px !important;
        transition: all 0.6s cubic-bezier(0.165, 0.84, 0.44, 1) !important;
        margin: 0 auto !important;
        display: block !important;
    }
    
    div.stButton > button:first-child:hover {
        background: rgba(41, 181, 232, 0.1) !important;
        border-color: #29b5e8 !important;
        color: white !important;
        transform: scale(1.02);
        box-shadow: 0 0 25px rgba(41, 181, 232, 0.15) !important;
    }

    /* BOTONES DE TAREA: RECTANGULARES 16:9 LIMPIOS */
    [data-testid="stColumn"] div.stButton > button {
        background: rgba(255, 255, 255, 0.02) !important;
        border: 1px solid rgba(255, 255, 255, 0.08) !important;
        color: #d1d1d1 !important;
        border-radius: 12px !important;
        height: 85px !important; 
        width: 155px !important;
        min-width: 155px !important;
        max-width: 155px !important;
        font-size: 0.75rem !important;
        font-weight: 400 !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-align: center !important;
        padding: 10px !important;
        white-space: normal !important;
        word-wrap: break-word !important;
        transition: all 0.3s ease !important;
        margin: 0 auto !important;
    }
    
    [data-testid="stColumn"] div.stButton > button:hover {
        background: rgba(41, 181, 232, 0.05) !important;
        border-color: rgba(41, 181, 232, 0.4) !important;
        color: #ffffff !important;
        transform: scale(1.04);
    }

    /* CONSOLA */
    .console-card {
        background: rgba(0, 0, 0, 0.2) !important;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.03);
        border-radius: 15px;
        padding: 15px;
        font-family: 'JetBrains Mono', monospace;
        color: #29b5e8;
        height: 140px;
        overflow-y: auto;
        margin: 40px auto;
        max-width: 900px;
        font-size: 0.75rem;
    }

    /* EXPANDERS */
    .stExpander {
        border: none !important;
        background: rgba(255, 255, 255, 0.01) !important;
        border-radius: 20px !important;
        margin-bottom: 8px !important;
    }
    .stExpander summary { font-weight: 400 !important; color: #888 !important; font-size: 1rem !important; }
    .stExpander summary:hover { color: white !important; }
    
    /* BOTÓN SYNC ALL MUNDO */
    .mundo-sync-container button {
        background: transparent !important;
        color: #29b5e8 !important;
        border: 1px solid rgba(41, 181, 232, 0.2) !important;
        border-radius: 30px !important;
        font-size: 0.7rem !important;
        height: 32px !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --- ESTRUCTURA DE DATOS ORIGINAL (SIN CAMBIOS) ---
SF_PARAMS = {'user': 'bryan.zuniga@rappi.com', 'account': 'hg51401', 'authenticator': 'snowflake', 'warehouse': 'RP_PERSONALUSER_WH', 'database': 'FIVETRAN', 'schema': 'PUBLIC', 'role': 'RP_READ_ACCESS_PU_ROLE'}

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

# --- LOGS ---
if 'logs' not in st.session_state: st.session_state.logs = ["› SnowSync Kernel Online"]
def log(msg): st.session_state.logs.append(f"› {time.strftime('%H:%M:%S')} | {msg}")

# --- CORE FUNCTIONS ---
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
        if not query: return False
        cs.execute(query)
        df = pd.DataFrame(cs.fetchall(), columns=[col[0] for col in cs.description])
        try: wks = sh.worksheet(t["tab"])
        except: wks = sh.add_worksheet(title=t["tab"], rows=1000, cols=20)
        wks.batch_clear([f"{t['c_start']}:{t['c_end']}{wks.row_count}"])
        time.sleep(1)
        set_with_dataframe(wks, df, row=t["p_row"], col=t.get("p_col", 1), include_column_header=True)
        return True
    except: return False

# --- UI START ---
st.markdown('<div class="app-header"><h1>SnowSync</h1><p>Enterprise Edition</p></div>', unsafe_allow_html=True)

try:
    sf_token = st.secrets["SNOWFLAKE_TOKEN"]
    google_info = json.loads(base64.b64decode(st.secrets["GOOGLE_BASE64"]).decode('utf-8'))
    creds = Credentials.from_service_account_info(google_info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly'])
    drive_service, gc = build('drive', 'v3', credentials=creds), gspread.authorize(creds)
    SF_PARAMS['password'] = sf_token

    # BOTÓN MAESTRO GHOST STYLE
    if st.button("EJECUTAR MASIVO"):
        log("MASTER SYNC INITIATED...")
        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
        for t in TAREAS:
            run_task(t, drive_service, gc, cs)
            log(f"Updated: {t['tab']}")
        cs.close(); conn.close()
        log("PIPELINE COMPLETE.")
        st.rerun()

    # CONSOLA
    st.markdown(f'<div class="console-card">{"<br>".join(st.session_state.logs[-6:])}</div>', unsafe_allow_html=True)

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
            st.markdown('<div class="mundo-sync-container">', unsafe_allow_html=True)
            if st.button(f"Sync Group: {nombre}", key=f"m_{sid}"):
                conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                for t in lista:
                    run_task(t, drive_service, gc, cs)
                cs.close(); conn.close()
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            cols = st.columns(8) 
            for i, t in enumerate(lista):
                with cols[i % 8]:
                    clean_name = t['tab'].replace('_', ' ')
                    if st.button(clean_name, key=f"btn_{t['tab']}_{sid}"):
                        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                        run_task(t, drive_service, gc, cs)
                        cs.close(); conn.close()
                        st.toast(f"Success: {t['tab']}")
                        st.rerun()

except Exception as e:
    st.error(f"Initialization Error: {e}")
