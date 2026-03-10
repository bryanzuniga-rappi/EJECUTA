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
st.set_page_config(page_title="SnowSync Pro", page_icon="❄️", layout="wide")

# --- DICCIONARIO DE NOMBRES DE MUNDOS ---
NOMBRES_MUNDOS = {
    "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s": "Operaciones CH",
    "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok": "Golden Engine",
    "1E4L8mssR-C1BXQd67YZnMAuSgALEXVpwQmGN1Nayxv0": "SKUs Inventory",
    "1KN6xp10n1_4WWlOBFz2AnQrcFjUyuOE5cwxAjg-bGaA": "AVL Analytics",
    "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I": "Daily Records",
    "1TBmD3vqOmfNRAgceIvfsxHL3lkO62zSrujVF9ed4LnU": "Master Stocks",
    "1RQ48gT6PO1tb05TAHdKhL9iIuV4XTmJRTNp8qCmNf_0": "Bags Supply"
}

# --- CSS PROFESIONAL APPLE + SNOWFLAKE AZUL ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&family=JetBrains+Mono&display=swap');

    html, body, [class*="css"] {
        font-family: 'Poppins', sans-serif !important;
        background-color: #000000 !important;
    }

    /* Títulos */
    h1 { font-weight: 600 !important; letter-spacing: -1.5px !important; color: white; }
    
    /* Botón Master (Pipeline General) */
    div.stButton > button:first-child {
        background-color: #29b5e8 !important;
        color: white !important;
        border: none !important;
        padding: 15px 40px !important;
        border-radius: 50px !important;
        font-weight: 600 !important;
        margin: 0 auto 30px auto !important;
        display: block !important;
        box-shadow: 0 0 20px rgba(41, 181, 232, 0.3) !important;
    }

    /* Botones de Tarea (Cuadrados Uniformes) */
    [data-testid="stColumn"] div.stButton > button {
        background: rgba(255, 255, 255, 0.05) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        color: #ffffff !important;
        border-radius: 18px !important;
        height: 110px !important;
        width: 110px !important;
        font-size: 0.75rem !important;
        transition: all 0.3s ease !important;
        margin-bottom: 15px;
    }
    [data-testid="stColumn"] div.stButton > button:hover {
        border-color: #29b5e8 !important;
        background: rgba(41, 181, 232, 0.1) !important;
        transform: translateY(-3px);
    }

    /* Botón de Ejecutar Mundo Completo */
    .mundo-sync-btn button {
        background-color: transparent !important;
        color: #29b5e8 !important;
        border: 1px solid #29b5e8 !important;
        border-radius: 30px !important;
        font-size: 0.8rem !important;
        padding: 5px 20px !important;
        height: auto !important;
        width: auto !important;
        aspect-ratio: auto !important;
    }

    /* Consola Hacker */
    .terminal-output {
        background: rgba(10, 10, 10, 0.8) !important;
        backdrop-filter: blur(15px);
        border: 1px solid #29b5e8;
        border-radius: 15px;
        padding: 20px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 13px;
        color: #29b5e8;
        height: 220px;
        overflow-y: auto;
        margin-bottom: 30px;
        box-shadow: inset 0 0 15px rgba(41, 181, 232, 0.1);
    }

    /* Acordeones (Expanders) */
    .stExpander {
        border: none !important;
        background: rgba(255, 255, 255, 0.02) !important;
        border-radius: 20px !important;
        margin-bottom: 10px !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --- ESTRUCTURA DE DATOS (ORIGINAL INTACTA) ---
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

# --- SESSION LOGS ---
if 'logs' not in st.session_state:
    st.session_state.logs = ["> SnowSync Kernel Loaded.", "> System Ready."]

def log(msg):
    st.session_state.logs.append(f"> {time.strftime('%H:%M:%S')} | {msg}")

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
        if not query: return False, "SQL_ERROR"
        cs.execute(query)
        df = pd.DataFrame(cs.fetchall(), columns=[col[0] for col in cs.description])
        try: wks = sh.worksheet(t["tab"])
        except: wks = sh.add_worksheet(title=t["tab"], rows=1000, cols=20)
        wks.batch_clear([f"{t['c_start']}:{t['c_end']}{wks.row_count}"])
        time.sleep(1)
        set_with_dataframe(wks, df, row=t["p_row"], col=t.get("p_col", 1), include_column_header=True)
        return True, "SUCCESS"
    except Exception as e: return False, str(e)

# --- INICIO UI ---
st.markdown("<h1 style='text-align: center;'>SnowSync <span style='color:#29b5e8; font-weight:300;'>Enterprise</span></h1>", unsafe_allow_html=True)

try:
    sf_token = st.secrets["SNOWFLAKE_TOKEN"]
    google_info = json.loads(base64.b64decode(st.secrets["GOOGLE_BASE64"]).decode('utf-8'))
    creds = Credentials.from_service_account_info(google_info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly'])
    drive_service, gc = build('drive', 'v3', credentials=creds), gspread.authorize(creds)
    SF_PARAMS['password'] = sf_token

    # BOTÓN PIPELINE GENERAL
    if st.button("🚀 INITIALIZE MASTER PIPELINE"):
        log("BROADCASTING GLOBAL SYNC PROTOCOL...")
        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
        for t in TAREAS:
            log(f"Syncing {t['tab']}...")
            ok, err = run_task(t, drive_service, gc, cs)
            if ok: log(f"OK: {t['tab']}")
            else: log(f"ERR: {t['tab']} ({err})")
        cs.close(); conn.close()
        log("GLOBAL PIPELINE COMPLETE.")

    # CONSOLA HACKER
    st.markdown(f'<div class="terminal-output">{"<br>".join(st.session_state.logs[-10:])}</div>', unsafe_allow_html=True)

    # AGRUPACIÓN POR MUNDOS
    mundos = {}
    for tarea in TAREAS:
        sid = tarea["sheet"]
        if sid not in mundos: mundos[sid] = []
        mundos[sid].append(tarea)

    # RENDERIZADO DE SECCIONES PLEGABLES
    for sid, lista in mundos.items():
        nombre = NOMBRES_MUNDOS.get(sid, f"Dataset {sid[:6]}")
        
        with st.expander(f"📁 {nombre} ({len(lista)} tasks)"):
            # Botón de Ejecución de Mundo Completo
            col_mundo_btn, _ = st.columns([1, 4])
            with col_mundo_btn:
                st.markdown('<div class="mundo-sync-btn">', unsafe_allow_html=True)
                if st.button(f"Run All {nombre}", key=f"world_{sid}"):
                    log(f"Initiating Group Sync: {nombre}")
                    conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                    for t in lista:
                        run_task(t, drive_service, gc, cs)
                        log(f"OK: {t['tab']}")
                    cs.close(); conn.close()
                    log(f"Group {nombre} Finished.")
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            
            # Botones Cuadrados Individuales
            cols = st.columns(8) # 8 por fila para perfecta simetría
            for i, t in enumerate(lista):
                with cols[i % 8]:
                    label = t['tab'].replace('_', ' ')
                    if st.button(f"⚡\n{label[:12]}", key=f"ind_{t['tab']}_{sid}"):
                        log(f"Individual Override: {t['tab']}...")
                        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                        run_task(t, drive_service, gc, cs)
                        cs.close(); conn.close()
                        log(f"Done: {t['tab']}")
                        st.toast(f"{t['tab']} Updated")
                        st.rerun()

except Exception as e:
    st.error(f"Critical System Failure: {e}")

if st.sidebar.button("Reset Console"):
    st.session_state.logs = ["> Kernel Reset."]; st.rerun()
