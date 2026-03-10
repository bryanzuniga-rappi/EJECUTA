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
# CONFIGURACIÓN DE NOMBRES DE MUNDOS (EDITA AQUÍ)
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

# --- DISEÑO APPLE PREMIUM (CSS) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&family=JetBrains+Mono&display=swap');

    html, body, [class*="css"], .stMarkdown {
        font-family: 'Poppins', sans-serif !important;
        background-color: #000000 !important;
    }

    .stApp {
        background: radial-gradient(circle at 50% -20%, #111b27 0%, #000000 100%);
    }

    /* Títulos */
    .app-header { text-align: center; padding: 30px 0; }
    .app-header h1 { font-size: 3rem !important; font-weight: 600 !important; color: white; margin-bottom: 0px !important; }
    .app-header p { color: #29b5e8; letter-spacing: 5px; font-size: 0.8rem; text-transform: uppercase; }

    /* BOTÓN MASIVO (SNOWFLAKE AZUL + ANIMACIÓN) */
    div.stButton > button:first-child {
        background: linear-gradient(135deg, #1fa2d4 0%, #157396 100%) !important;
        color: white !important;
        border-radius: 50px !important;
        padding: 18px 60px 18px 90px !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        text-transform: uppercase !important;
        letter-spacing: 2px !important;
        border: none !important;
        box-shadow: 0 10px 30px rgba(31, 162, 212, 0.1) !important;
        transition: all 0.4s ease !important;
        position: relative;
        margin: 20px auto !important;
        display: block !important;
    }
    
    div.stButton > button:first-child::before {
        content: '';
        position: absolute; left: 35px; top: 50%; transform: translateY(-50%);
        width: 32px; height: 32px; background-color: white;
        -webkit-mask-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M444.6 288.1L400 320l44.6 31.9c10.4 7.4 12.8 21.8 5.4 32.2-7.4 10.4-21.8 12.8-32.2 5.4L352 344v60.9c0 12.8-10.4 23.1-23.1 23.1-12.8 0-23.1-10.4-23.1-23.1V344L256 320l-49.8 24v60.9c0 12.8-10.4 23.1-23.1 23.1-12.8 0-23.1-10.4-23.1-23.1V344L114.1 389.5c-10.4 7.4-24.8 5-32.2-5.4-7.4-10.4-5-24.8 5.4-32.2L112 320L67.4 288.1c-10.4-7.4-12.8-21.8-5.4-32.2 7.4-10.4 21.8-12.8 32.2-5.4L112 272l49.8-24v-60.9c0-12.8 10.4-23.1 23.1-23.1 12.8 0 23.1 10.4 23.1 23.1V272L256 296l49.8-24v-60.9c0-12.8 10.4-23.1 23.1-23.1s23.1 10.4 23.1 23.1V272l49.8 45.5 44.6-31.9c10.4-7.4 24.8-5 32.2 5.4 7.4 10.4 5 24.8-5.4 32.2z"/></svg>');
        mask-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M444.6 288.1L400 320l44.6 31.9c10.4 7.4 12.8 21.8 5.4 32.2-7.4 10.4-21.8 12.8-32.2 5.4L352 344v60.9c0 12.8-10.4 23.1-23.1 23.1-12.8 0-23.1-10.4-23.1-23.1V344L256 320l-49.8 24v60.9c0 12.8-10.4 23.1-23.1 23.1-12.8 0-23.1-10.4-23.1-23.1V344L114.1 389.5c-10.4 7.4-24.8 5-32.2-5.4-7.4-10.4-5-24.8 5.4-32.2L112 320L67.4 288.1c-10.4-7.4-12.8-21.8-5.4-32.2 7.4-10.4 21.8-12.8 32.2-5.4L112 272l49.8-24v-60.9c0-12.8 10.4-23.1 23.1-23.1 12.8 0 23.1 10.4 23.1 23.1V272L256 296l49.8-24v-60.9c0-12.8 10.4-23.1 23.1-23.1s23.1 10.4 23.1 23.1V272l49.8 45.5 44.6-31.9c10.4-7.4 24.8-5 32.2 5.4 7.4 10.4 5 24.8-5.4 32.2z"/></svg>');
        -webkit-mask-repeat: no-repeat; mask-repeat: no-repeat;
        transition: transform 0.6s ease;
    }
    
    div.stButton > button:first-child:hover {
        transform: scale(1.02);
        box-shadow: 0 15px 45px rgba(31, 162, 212, 0.2) !important;
    }
    div.stButton > button:first-child:hover::before { transform: translateY(-50%) rotate(180deg); }

    /* BOTONES DE TAREA (CUADRADOS UNIFORMES) */
    [data-testid="stColumn"] div.stButton > button {
        background: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(255, 255, 255, 0.08) !important;
        color: #ffffff !important;
        border-radius: 18px !important;
        height: 110px !important;
        width: 110px !important;
        min-width: 110px !important;
        max-width: 110px !important;
        font-size: 0.75rem !important;
        font-weight: 500 !important;
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
        border-color: #29b5e8 !important;
        background: rgba(41, 181, 232, 0.05) !important;
        transform: translateY(-3px);
    }

    /* CONSOLA */
    .console-card {
        background: rgba(0, 0, 0, 0.4) !important;
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 20px;
        padding: 20px;
        font-family: 'JetBrains Mono', monospace;
        color: #29b5e8;
        height: 160px;
        overflow-y: auto;
        margin-bottom: 30px;
        font-size: 0.8rem;
    }

    /* EXPANDERS MUNDOS */
    .stExpander {
        border: none !important;
        background: rgba(255, 255, 255, 0.015) !important;
        border-radius: 24px !important;
        margin-bottom: 12px !important;
    }
    .stExpander summary { font-weight: 600 !important; color: white !important; font-size: 1.1rem !important; }
    
    /* BOTÓN SYNC ALL MUNDO */
    .mundo-sync-container button {
        background: transparent !important;
        color: #29b5e8 !important;
        border: 1px solid #29b5e8 !important;
        border-radius: 30px !important;
        font-size: 0.7rem !important;
        padding: 2px 20px !important;
        height: 32px !important;
        width: auto !important;
        min-width: 0 !important;
    }
    </style>
    """, unsafe_allow_html=True)

# =========================================================
# ESTRUCTURA DE DATOS ORIGINAL (SIN CAMBIOS)
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

# --- LÓGICA DE LOGS ---
if 'logs' not in st.session_state:
    st.session_state.logs = ["> SnowSync Enterprise initialized."]

def log(msg):
    st.session_state.logs.append(f"> {time.strftime('%H:%M:%S')} | {msg}")

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

    # NIVEL 1: BOTÓN MAESTRO
    if st.button("EJECUTAR MASIVO"):
        log("MASTER PROTOCOL INITIATED...")
        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
        for t in TAREAS:
            run_task(t, drive_service, gc, cs)
            log(f"Sync: {t['tab']}")
        cs.close(); conn.close()
        log("PIPELINE COMPLETE.")
        st.rerun()

    # NIVEL 2: CONSOLA
    st.markdown(f'<div class="console-card">{"<br>".join(st.session_state.logs[-8:])}</div>', unsafe_allow_html=True)

    # AGRUPACIÓN (LÓGICA INTERNA)
    mundos = {}
    for tarea in TAREAS:
        sid = tarea["sheet"]
        if sid not in mundos: mundos[sid] = []
        mundos[sid].append(tarea)

    # NIVEL 3: RENDERIZADO POR MUNDOS
    for sid, lista in mundos.items():
        nombre = NOMBRES_MUNDOS.get(sid, sid[:8])
        with st.expander(f"{nombre}"):
            
            # Botón de Mundo Completo
            st.markdown('<div class="mundo-sync-container">', unsafe_allow_html=True)
            if st.button(f"Ejecutar {nombre} Completo", key=f"m_{sid}"):
                log(f"Iniciando Mundo: {nombre}")
                conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                for t in lista:
                    run_task(t, drive_service, gc, cs)
                    log(f"OK: {t['tab']}")
                cs.close(); conn.close()
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # Grid de Botones Individuales (CUADRADOS ESTÁNDAR)
            cols = st.columns(10) 
            for i, t in enumerate(lista):
                with cols[i % 10]:
                    clean_name = t['tab'].replace('_', ' ')
                    if st.button(clean_name, key=f"btn_{t['tab']}_{sid}"):
                        log(f"Iniciando: {t['tab']}")
                        conn = snowflake.connector.connect(**SF_PARAMS); cs = conn.cursor()
                        run_task(t, drive_service, gc, cs)
                        cs.close(); conn.close()
                        log(f"Finalizado: {t['tab']}")
                        st.toast(f"Updated {t['tab']}")
                        st.rerun()

except Exception as e:
    st.error(f"Initialization Error: {e}")
