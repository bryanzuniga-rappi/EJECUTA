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

# --- CSS ULTRA PRO (APPLE DESIGN LANGUAGE) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&family=JetBrains+Mono&display=swap');

    html, body, [class*="css"] {
        font-family: 'Poppins', sans-serif !important;
        background-color: #000000 !important;
    }

    /* Contenedor Principal */
    .stApp {
        background: radial-gradient(circle at 50% -20%, #1a2a3a 0%, #000000 100%);
    }

    /* Títulos Estilo Apple */
    .app-header {
        text-align: center;
        padding: 40px 0;
    }
    .app-header h1 {
        font-size: 3.5rem !important;
        font-weight: 600 !important;
        letter-spacing: -2px !important;
        margin-bottom: 0 !important;
        background: linear-gradient(180deg, #FFFFFF 0%, #888888 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }

    /* Botón Master (Llamativo pero elegante) */
    div.stButton > button[kind="primary"] {
        background: #29b5e8 !important;
        color: white !important;
        border: none !important;
        padding: 18px 45px !important;
        border-radius: 100px !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        margin: 20px auto !important;
        display: block !important;
        box-shadow: 0 10px 30px rgba(41, 181, 232, 0.3) !important;
        transition: all 0.4s cubic-bezier(0.165, 0.84, 0.44, 1) !important;
    }
    div.stButton > button[kind="primary"]:hover {
        transform: scale(1.05);
        box-shadow: 0 15px 40px rgba(41, 181, 232, 0.5) !important;
    }

    /* Botones de Tarea (Los "App Icons") */
    [data-testid="stColumn"] div.stButton > button {
        background: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(255, 255, 255, 0.08) !important;
        color: #ffffff !important;
        border-radius: 24px !important;
        height: 100px !important;
        width: 100px !important;
        font-size: 0.7rem !important;
        line-height: 1.2 !important;
        padding: 5px !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-align: center !important;
        transition: all 0.3s ease !important;
        margin: 0 auto !important;
    }
    
    [data-testid="stColumn"] div.stButton > button:hover {
        background: rgba(41, 181, 232, 0.15) !important;
        border-color: #29b5e8 !important;
        transform: translateY(-5px) scale(1.05);
        box-shadow: 0 10px 20px rgba(0,0,0,0.5) !important;
    }

    /* Botones de Mundo (Ejecutar grupo) */
    .mundo-btn button {
        background: transparent !important;
        color: #29b5e8 !important;
        border: 1px solid #29b5e8 !important;
        border-radius: 50px !important;
        padding: 5px 25px !important;
        height: auto !important;
        width: auto !important;
        font-size: 0.8rem !important;
    }

    /* Consola de Comando */
    .console-card {
        background: rgba(15, 15, 15, 0.6) !important;
        backdrop-filter: blur(20px);
        border: 1px solid #222;
        border-radius: 20px;
        padding: 20px;
        font-family: 'JetBrains Mono', monospace;
        color: #29b5e8;
        height: 200px;
        overflow-y: auto;
        margin-bottom: 50px;
        box-shadow: inset 0 0 20px rgba(0,0,0,1);
    }

    /* Expanders */
    .stExpander {
        border: none !important;
        background: rgba(255, 255, 255, 0.02) !important;
        border-radius: 28px !important;
        margin-bottom: 15px !important;
    }
    .stExpander summary { font-weight: 500 !important; color: #eee !important; padding: 15px !important; }
    </style>
    """, unsafe_allow_html=True)

# --- TAREAS (SIN TOCAR TU ESTRUCTURA) ---
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

# --- LÓGICA DE LOGS ---
if 'logs' not in st.session_state:
    st.session_state.logs = ["> SnowSync Enterprise OS v2.0 initialized.", "> Secure link to Snowflake established."]

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
        if not query: return False, "SQL_MISSING"
        cs.execute(query)
        df = pd.DataFrame(cs.fetchall(), columns=[col[0] for col in cs.description])
        try: wks = sh.worksheet(t["tab"])
        except: wks = sh.add_worksheet(title=t["tab"], rows=1000, cols=20)
        wks.batch_clear([f"{t['c_start']}:{t['c_end']}{wks.row_count}"])
        time.sleep(1)
        set_with_dataframe(wks, df, row=t["p_row"], col=t.get("p_col", 1), include_column_header=True)
        return True, "SUCCESS"
    except Exception as e: return False, str(e)

# --- APP START ---
st.markdown('<div class="app-header"><h1>SnowSync</h1><p style="color:#29b5e8; font-weight:300; letter-spacing:4px;">ENTERPRISE</p></div>', unsafe_allow_html=True)

try:
    # Auth
    sf_token = st.secrets["SNOWFLAKE_TOKEN"]
    google_info = json.loads(base64.b64decode(st.secrets["GOOGLE_BASE64"]).decode('utf-8'))
    creds = Credentials.from_service_account_info(google_info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly'])
    drive_service, gc = build('drive', 'v3', credentials=creds), gspread.authorize(creds)
    SF_PARAMS['password'] = sf_token

    # NIVEL 1: MASTER SYNC
    if st.button("INITIALIZE MASTER PIPELINE", kind="primary"):
        log("BROADCASTING GLOBAL COMMAND...")
        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
        for t in TAREAS:
            log(f"Syncing: {t['tab']}...")
            ok, err = run_task(t, drive_service, gc, cs)
            if ok: log(f"OK: {t['tab']}")
            else: log(f"FAILED: {t['tab']}")
        cs.close(); conn.close()
        log("ALL SYSTEMS UPDATED.")

    # NIVEL 2: CONSOLA
    st.markdown(f'<div class="console-card">{"<br>".join(st.session_state.logs[-8:])}</div>', unsafe_allow_html=True)

    # AGRUPACIÓN POR MUNDOS
    mundos = {}
    for tarea in TAREAS:
        sid = tarea["sheet"]
        if sid not in mundos: mundos[sid] = []
        mundos[sid].append(tarea)

    # NIVEL 3: RENDERIZADO POR MUNDOS (EXPANDIBLES)
    for sid, lista in mundos.items():
        nombre = NOMBRES_MUNDOS.get(sid, f"Dataset {sid[:6]}")
        with st.expander(f"📁 {nombre} ({len(lista)} tasks)"):
            
            # Botón de Sincronizar Mundo Completo
            col_mundo, _ = st.columns([1, 4])
            with col_mundo:
                st.markdown('<div class="mundo-btn">', unsafe_allow_html=True)
                if st.button(f"Sync all {nombre}", key=f"world_{sid}"):
                    log(f"Syncing group: {nombre}")
                    conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                    for t in lista:
                        run_task(t, drive_service, gc, cs)
                        log(f"Synced {t['tab']}")
                    cs.close(); conn.close()
                    log(f"Group {nombre} Done.")
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # Botones de Consulta (Individuales)
            cols = st.columns(10) # 10 columnas para que sean pequeños y uniformes
            for i, t in enumerate(lista):
                with cols[i % 10]:
                    # Limpiamos nombre para el icono
                    display_name = t['tab'].replace('_', ' ')[:10]
                    if st.button(f"⚡\n{display_name}", key=f"ind_{t['tab']}_{sid}"):
                        log(f"Individual override: {t['tab']}")
                        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                        ok, err = run_task(t, drive_service, gc, cs)
                        cs.close(); conn.close()
                        if ok: log(f"Verified: {t['tab']}"); st.toast("OK")
                        else: log(f"Error: {err}")
                        st.rerun()

except Exception as e:
    st.error(f"Kernel Panic: {e}")

if st.sidebar.button("Clear Log"):
    st.session_state.logs = ["> Logs flushed."]; st.rerun()
