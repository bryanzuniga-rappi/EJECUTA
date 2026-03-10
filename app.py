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
    "1UR_0V7tkpqOTnmeQ9zVbproWiZk3xncUBSD6Ft2XU6s": "Operaciones CH",
    "1exrUKZgpgKIPR7LWPOYtNK6lT6HddtrQvEwrmA_F8Ok": "Golden Dani",
    "1E4L8mssR-C1BXQd67YZnMAuSgALEXVpwQmGN1Nayxv0": "SKUs Management",
    "1KN6xp10n1_4WWlOBFz2AnQrcFjUyuOE5cwxAjg-bGaA": "AVL Engine",
    "17epSRURcXCYcnwcdKJhgwFQimaHYb0EYH7tt-e6Km7I": "Daily History",
    "1TBmD3vqOmfNRAgceIvfsxHL3lkO62zSrujVF9ed4LnU": "Inventarios",
    "1RQ48gT6PO1tb05TAHdKhL9iIuV4XTmJRTNp8qCmNf_0": "Stock Bolsas"
}

# --- CSS APPLE DESIGN SYSTEM V4 ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&family=JetBrains+Mono:wght@400&display=swap');

    /* Variables de Diseño Apple */
    :root {
        --bg-color: #000000;
        --sf-blue: #29b5e8;
        --card-bg: rgba(28, 28, 30, 0.7);
        --glass-border: rgba(255, 255, 255, 0.08);
    }

    html, body, [class*="css"] {
        font-family: 'Poppins', sans-serif !important;
        background-color: var(--bg-color) !important;
    }

    .stApp {
        background: radial-gradient(circle at 2% 2%, #1a1a1a 0%, #000000 100%);
    }

    /* TITULO */
    h1 {
        font-weight: 600 !important;
        letter-spacing: -1.2px !important;
        font-size: 2.8rem !important;
        margin-bottom: 30px !important;
        color: white;
    }

    /* BOTÓN MAESTRO (MINIMALISTA) */
    div.stButton > button:first-child {
        background: white !important;
        color: black !important;
        border: none !important;
        padding: 12px 40px !important;
        border-radius: 30px !important;
        font-weight: 500 !important;
        font-size: 0.9rem !important;
        margin: 0 auto !important;
        display: block !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 4px 12px rgba(255, 255, 255, 0.1);
    }
    div.stButton > button:first-child:hover {
        transform: scale(1.03);
        box-shadow: 0 8px 24px rgba(255, 255, 255, 0.2);
    }

    /* TARJETAS DE MUNDOS */
    .mundo-card {
        background: var(--card-bg);
        backdrop-filter: blur(20px);
        border: 1px solid var(--glass-border);
        border-radius: 24px;
        padding: 25px;
        margin-bottom: 25px;
    }
    .mundo-title {
        color: #8E8E93;
        font-size: 0.8rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 20px;
    }

    /* BOTONES DE TAREA CUADRADOS E IGUALES */
    [data-testid="stColumn"] div.stButton > button {
        background: rgba(255, 255, 255, 0.05) !important;
        border: 1px solid var(--glass-border) !important;
        color: white !important;
        border-radius: 16px !important;
        /* Proporciones fijas */
        height: 100px !important;
        width: 100px !important;
        margin: 0 auto !important;
        font-size: 0.7rem !important;
        font-weight: 500 !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        transition: all 0.2s ease !important;
    }
    [data-testid="stColumn"] div.stButton > button:hover {
        background: rgba(255, 255, 255, 0.1) !important;
        border-color: var(--sf-blue) !important;
    }

    /* TERMINAL STATUS BAR */
    .status-bar {
        background: rgba(41, 181, 232, 0.05);
        border: 1px solid rgba(41, 181, 232, 0.2);
        padding: 10px 20px;
        border-radius: 12px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
        color: var(--sf-blue);
        margin: 20px 0;
    }
    </style>
    """, unsafe_allow_html=True)

# --- TAREAS (ESTRUCTURA ORIGINAL) ---
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

# --- LÓGICA DE ESTADO ---
if 'status' not in st.session_state:
    st.session_state.status = "System Ready"

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

# --- UI START ---
st.markdown("<h1>System <span style='color:#555;'>Console</span></h1>", unsafe_allow_html=True)

try:
    sf_token = st.secrets["SNOWFLAKE_TOKEN"]
    google_info = json.loads(base64.b64decode(st.secrets["GOOGLE_BASE64"]).decode('utf-8'))
    creds = Credentials.from_service_account_info(google_info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly'])
    drive_service, gc = build('drive', 'v3', credentials=creds), gspread.authorize(creds)
    SF_PARAMS['password'] = sf_token

    # BARRA DE ESTADO MINIMALISTA
    st.markdown(f'<div class="status-bar">● {st.session_state.status}</div>', unsafe_allow_html=True)

    # BOTÓN CENTRAL
    if st.button("Initialize Master Sync"):
        st.session_state.status = "Executing Global Sync..."
        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
        for t in TAREAS:
            ok, msg = run_task(t, drive_service, gc, cs)
        cs.close(); conn.close()
        st.session_state.status = "Pipeline Complete"
        st.rerun()

    st.markdown("<br><br>", unsafe_allow_html=True)

    # AGRUPACIÓN POR MUNDOS
    mundos = {}
    for tarea in TAREAS:
        sid = tarea["sheet"]
        if sid not in mundos: mundos[sid] = []
        mundos[sid].append(tarea)

    # RENDERIZADO ESTILO APPLE CARDS
    for sid, lista in mundos.items():
        nombre = NOMBRES_MUNDOS.get(sid, sid[:10])
        
        # Inyectamos el contenedor del mundo
        st.markdown(f'<div class="mundo-card"><div class="mundo-title">{nombre}</div>', unsafe_allow_html=True)
        
        # Grid de botones cuadrados
        cols = st.columns(10) # 10 columnas para que sean pequeños y uniformes
        for i, t in enumerate(lista):
            with cols[i % 10]:
                if st.button(t['tab'][:8], key=f"btn_{t['tab']}_{sid}"):
                    st.session_state.status = f"Syncing {t['tab']}..."
                    conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                    run_task(t, drive_service, gc, cs)
                    cs.close(); conn.close()
                    st.session_state.status = "Idle"
                    st.toast(f"Updated {t['tab']}")
                    st.rerun()
        
        st.markdown('</div>', unsafe_allow_html=True)

except Exception as e:
    st.error(f"Booting sequence failed: {e}")
