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
st.set_page_config(page_title="Data Sync Pro", page_icon="🚀", layout="wide")

# --- DISEÑO PROFESIONAL (CSS CUSTOM) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap');

    /* Fuente Global */
    html, body, [class*="css"], .stButton > button {
        font-family: 'Poppins', sans-serif !important;
    }

    /* Fondo y Título */
    .main {
        background-color: #0e1117;
    }
    h1 {
        color: #FFFFFF;
        font-weight: 600 !important;
        letter-spacing: -1px;
    }

    /* Estilo de Botón Maestro (EJECUTA TODO) */
    div.stButton > button:first-child {
        background-color: #FF4B4B;
        color: white;
        border: none;
        font-weight: 600;
        padding: 0.75rem 1rem;
        border-radius: 10px;
        transition: all 0.3s ease;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    div.stButton > button:first-child:hover {
        background-color: #FF2B2B;
        transform: scale(1.01);
        box-shadow: 0 4px 15px rgba(255, 75, 75, 0.4);
    }

    /* Estilo de Botones Individuales */
    div.stButton > button {
        background-color: #262730;
        color: #E0E0E0;
        border: 1px solid #3d414b;
        border-radius: 8px;
        font-weight: 400;
        transition: all 0.2s ease;
    }
    div.stButton > button:hover {
        border-color: #FF4B4B;
        color: #FF4B4B;
        background-color: #1e1f26;
    }
    
    /* Divider Personalizado */
    hr {
        margin: 2em 0;
        border: 0;
        border-top: 1px solid #3d414b;
    }
    </style>
    """, unsafe_allow_html=True)

# --- ESTRUCTURA DE DATOS (SIN CAMBIOS) ---
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
    }
]

# --- FUNCIONES CORE (SIN CAMBIOS) ---
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
        st.error(f"Error Drive {file_name}: {e}")
        return None

def run_task(t, drive_service, gc, cs):
    try:
        sh = gc.open_by_key(t["sheet"])
        query = get_sql_content(drive_service, t["sql"])
        if not query: return False, "SQL no encontrado"
        cs.execute(query)
        df = pd.DataFrame(cs.fetchall(), columns=[col[0] for col in cs.description])
        try:
            wks = sh.worksheet(t["tab"])
        except gspread.exceptions.WorksheetNotFound:
            wks = sh.add_worksheet(title=t["tab"], rows=1000, cols=20)
        wks.batch_clear([f"{t['c_start']}:{t['c_end']}{wks.row_count}"])
        time.sleep(1)
        set_with_dataframe(wks, df, row=t["p_row"], col=t.get("p_col", 1), include_column_header=True)
        return True, f"Sync {t['tab']} OK"
    except Exception as e:
        return False, str(e)

# --- FLUJO PRINCIPAL ---
st.write("### 🚀 Data Sync Engine")
st.title("Snowflake ➔ Google Sheets")

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

    # Sección Principal de Control
    with st.container():
        if st.button("⚡ EJECUTAR PIPELINE COMPLETO", use_container_width=True):
            with st.status("Procesando todas las tareas...", expanded=True) as status:
                conn = snowflake.connector.connect(**SF_PARAMS)
                cs = conn.cursor()
                for t in TAREAS:
                    st.write(f"🔄 Sincronizando: **{t['tab']}**")
                    success, msg = run_task(t, drive_service, gc, cs)
                    if not success: st.error(f"❌ {t['tab']}: {msg}")
                cs.close()
                conn.close()
                status.update(label="✅ Sincronización terminada con éxito", state="complete")

    st.markdown("---")
    st.write("### 🛠️ Tareas Individuales")
    
    # Cuadrícula de tareas
    cols = st.columns(4)
    for i, t in enumerate(TAREAS):
        with cols[i % 4]:
            if st.button(f"📥 {t['tab']}", key=f"btn_{i}", use_container_width=True):
                with st.spinner("Conectando..."):
                    conn = snowflake.connector.connect(**SF_PARAMS)
                    cs = conn.cursor()
                    success, msg = run_task(t, drive_service, gc, cs)
                    cs.close()
                    conn.close()
                    if success: st.toast(f"✅ {msg}", icon='🔥')
                    else: st.error(msg)
except Exception as e:
    st.error(f"🚨 Initialization Error: {e}")
