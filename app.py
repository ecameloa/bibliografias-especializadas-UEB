# -*- coding: utf-8 -*-
# Herramienta para la elaboración de bibliografías especializadas
# Universidad El Bosque

import io
import time
import math
import requests
import numpy as np
import pandas as pd
import streamlit as st
from unidecode import unidecode

# =========================
# CONFIGURACIÓN / CONSTANTES
# =========================

# URLs oficiales (Digital / Física)
URL_DIGITAL = ("https://biblioteca.unbosque.edu.co/sites/default/files/"
               "Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx")
URL_FISICA = ("https://biblioteca.unbosque.edu.co/sites/default/files/"
              "Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx")

# Logos (oscuro / claro)
DARK_LOGO  = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png"
LIGHT_LOGO = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20ORG.png"

# Columnas sugeridas por defecto
DEFAULT_SEARCH_COL1 = "Título"
DEFAULT_SEARCH_COL2 = "Temáticas"
DEFAULT_DUP_DIGITAL = "Url OA"
DEFAULT_DUP_FISICA  = "No. Topográfico"

# =========================
# CSS: mejoras de UI
# =========================
st.set_page_config(page_title="Herramienta para bibliografías especializadas", layout="wide")

st.markdown("""
<style>
/* Sidebar más ancho y estable */
[data-testid="stSidebar"] {min-width: 340px; max-width: 420px;}
/* Evitar recortes en notificaciones */
[data-testid="stNotification"] p, [data-testid="stNotification"] div { white-space: normal !important; }
/* Reducir padding vertical en sidebar para que quepa más info */
section[data-testid="stSidebar"] div.block-container { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)

# =========================
# ESTADO
# =========================
ss = st.session_state
ss.setdefault("tema", "dark")  # "dark" o "light"
ss.setdefault("digital_loading", False)
ss.setdefault("fisica_loading",  False)
ss.setdefault("digital_ok", False)
ss.setdefault("fisica_ok",  False)
ss.setdefault("df_digital", None)
ss.setdefault("df_fisica",  None)
ss.setdefault("tematicas_df", None)   # 2 columnas: termino, normalizado
ss.setdefault("excluir_df",   None)   # 1 columna: termino_excluir
ss.setdefault("df_resultados", None)
ss.setdefault("bitacora", None)
# ================ UTILIDADES ================

def normalize_txt(x: str) -> str:
    if pd.isna(x):
        return ""
    x = str(x)
    x = unidecode(x)  # elimina tildes
    return x

def clear_for_new_search():
    """Limpia resultados pero NO borra las bases grandes ya cargadas."""
    for k in ["df_resultados", "bitacora", "tematicas_df", "excluir_df"]:
        ss.pop(k, None)
    st.success("Listo para una nueva búsqueda. Las bases oficiales se mantienen en memoria.")

def _read_excel_from_bytes(b: io.BytesIO) -> pd.DataFrame:
    b.seek(0)
    # Deja que pandas detecte el engine (openpyxl instalado)
    return pd.read_excel(b)

def download_excel_with_progress(url: str, label: str, retry: int = 2) -> pd.DataFrame:
    """Descarga con barra de progreso por bytes, con reintentos."""
    attempt = 0
    last_exc = None
    while attempt <= retry:
        try:
            r = requests.get(url, stream=True, timeout=60)
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0)) or None

            progress = st.progress(0, text=f"Descargando {label}…")
            buf = io.BytesIO()
            downloaded = 0
            chunk_size = 1024 * 512  # 512 KB

            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    buf.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = int(downloaded / total * 100)
                        progress.progress(min(pct, 100), text=f"{label}: {pct}%")

            progress.progress(100, text=f"{label}: 100%")
            df = _read_excel_from_bytes(buf)
            return df

        except Exception as e:
            last_exc = e
            attempt += 1
            st.warning(f"Fallo descargando {label}. Reintentando ({attempt}/{retry})…")
            time.sleep(1.5)

    raise RuntimeError(f"No se pudo descargar {label}: {last_exc}")

def load_aux_temas(file) -> pd.DataFrame:
    """Temáticas: col1 = término, col2 = normalizado"""
    df = pd.read_excel(file)
    if df.shape[1] < 2:
        raise ValueError("La plantilla de Temáticas debe tener 2 columnas: término y normalizado.")
    df = df.iloc[:, :2].copy()
    df.columns = ["termino", "normalizado"]
    df["termino"] = df["termino"].astype(str).str.strip()
    df["normalizado"] = df["normalizado"].astype(str).str.strip()
    return df

def load_aux_excluir(file) -> pd.DataFrame:
    """Términos a excluir: 1 columna."""
    df = pd.read_excel(file)
    if df.shape[1] < 1:
        raise ValueError("La plantilla de Términos a excluir debe tener al menos 1 columna.")
    df = df.iloc[:, :1].copy()
    df.columns = ["excluir"]
    df["excluir"] = df["excluir"].astype(str).str.strip()
    return df

def find_matches(df, cols_busqueda, tematicas_df, fuente: str):
    """
    Busca coincidencias de 'tematicas_df.termino' en columnas definidas.
    Devuelve df_resultados parcial + bitácora parcial por término.
    """
    if df is None or df.empty:
        return pd.DataFrame(), pd.DataFrame(columns=["Fuente", "Término", "Resultados"])

    if tematicas_df is None or tematicas_df.empty:
        return pd.DataFrame(), pd.DataFrame(columns=["Fuente", "Término", "Resultados"])

    # normaliza columns para buscar
    df_norm = df.copy()
    col_map = {}
    for col in cols_busqueda:
        if col in df_norm.columns:
            key = f"_norm_{col}"
            df_norm[key] = df_norm[col].astype(str).map(normalize_txt).str.lower()
            col_map[col] = key

    resultados = []
    bit_tmp = []

    for _, row in tematicas_df.iterrows():
        termino = row["termino"]
        term_norm = normalize_txt(termino).lower()
        mask_total = pd.Series(False, index=df_norm.index)
        col_hit = None

        for col, norm_col in col_map.items():
            # contención simple (puedes mejorar con regex si quieres)
            hit = df_norm[norm_col].str.contains(term_norm, na=False)
            if hit.any() and col_hit is None:
                col_hit = col  # registramos primera columna donde coincida
            mask_total = mask_total | hit

        df_hits = df.loc[mask_total].copy()
        if not df_hits.empty:
            df_hits["Temática"] = termino
            df_hits["Temática normalizada"] = row["normalizado"]
            df_hits["Columna de coincidencia"] = col_hit if col_hit else ""
            resultados.append(df_hits)

            bit_tmp.append({"Fuente": fuente, "Término": termino, "Resultados": int(df_hits.shape[0])})
        else:
            # también registramos 0 resultados
            bit_tmp.append({"Fuente": fuente, "Término": termino, "Resultados": 0})

    df_final = pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()
    bit_df = pd.DataFrame(bit_tmp, columns=["Fuente", "Término", "Resultados"])
    return df_final, bit_df

def remove_duplicates(df, colname):
    """Elimina duplicados por una columna si existe."""
    if df is None or df.empty:
        return df
    if colname and (colname in df.columns):
        df = df.drop_duplicates(subset=[colname], keep="first")
    return df

def highlight_workbook(writer, df_result, cols_highlight, excluir_list):
    """Crea XLSX con resaltado amarillo en términos a excluir + hoja Bitácora."""
    wb  = writer.book
    ws1 = wb.add_worksheet("Resultados")
    ws2 = wb.add_worksheet("Bitácora")
    # Formatos
    fmt_header = wb.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1})
    fmt_cell = wb.add_format({"border": 1})
    fmt_yellow = wb.add_format({"bg_color": "#FFF68F", "border": 1})

    # === Hoja Resultados ===
    cols = list(df_result.columns)
    for j, c in enumerate(cols):
        ws1.write(0, j, c, fmt_header)

    # data
    excl_norm = [normalize_txt(x).lower() for x in excluir_list]
    col_idx_to_check = [cols.index(c) for c in cols_highlight if c in cols]

    for i in range(df_result.shape[0]):
        for j, c in enumerate(cols):
            val = df_result.iat[i, j]
            fmt = fmt_cell
            if j in col_idx_to_check:
                text = normalize_txt(val).lower()
                if any(x in text for x in excl_norm if x):
                    fmt = fmt_yellow
            ws1.write(i+1, j, val, fmt)

    # auto width básico
    for j, c in enumerate(cols):
        maxw = max(10, min(60, int(df_result[c].astype(str).map(len).max() if not df_result.empty else 10)))
        ws1.set_column(j, j, maxw)

    # === Hoja Bitácora ===
    bit = ss.get("bitacora")
    if bit is None or bit.empty:
        bit = pd.DataFrame(columns=["Fuente", "Término", "Resultados"])
    cols_b = list(bit.columns)
    for j, c in enumerate(cols_b):
        ws2.write(0, j, c, fmt_header)

    for i in range(bit.shape[0]):
        for j, c in enumerate(cols_b):
            ws2.write(i+1, j, bit.iat[i, j], fmt_cell)

    for j, c in enumerate(cols_b):
        maxw = max(10, min(40, int(bit[c].astype(str).map(len).max() if not bit.empty else 10)))
        ws2.set_column(j, j, maxw)

def export_xlsx_with_highlight(df_result, excluir_df):
    bio = io.BytesIO()
    excluir_list = excluir_df["excluir"].tolist() if (excluir_df is not None and not excluir_df.empty) else []
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        # Lo escribimos también con pandas por si alguien abre tablas desde aquí
        df_result.to_excel(writer, sheet_name="Resultados", index=False)
        # Luego re-escribimos con formato (misma hoja)
        # Re-crear Libro y hojas con nuestro formateo:
        writer.close()  # cierra para poder rehacer con highlight
    # Volvemos a crear para formateo
    bio2 = io.BytesIO()
    with pd.ExcelWriter(bio2, engine="xlsxwriter") as writer2:
        highlight_workbook(writer2, df_result, ["Título", "Temáticas"], excluir_list)
    bio2.seek(0)
    return bio2

# ========================= UI: SIDEBAR =========================

# Tema (opcional)
st.sidebar.caption("Tema")
c1, c2 = st.sidebar.columns([1,1])
with c1:
    if st.button("Oscuro", use_container_width=True):
        ss["tema"] = "dark"
with c2:
    if st.button("Claro", use_container_width=True):
        ss["tema"] = "light"

logo_url = DARK_LOGO if ss["tema"] == "dark" else LIGHT_LOGO
st.sidebar.image(logo_url, use_container_width=True)
st.sidebar.caption("Biblioteca Juan Roa Vásquez")

# Fuente de datos
fuente = st.sidebar.radio("Fuente de datos", ("Desde web oficial", "Subir archivos"))

if fuente == "Subir archivos" and (ss["digital_ok"] or ss["fisica_ok"]):
    st.sidebar.warning(
        "Estás cambiando a archivos locales. **Dejas de usar la versión oficial**. "
        "Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app."
    )

colA, colB = st.sidebar.columns(2)
with colA:
    st.button(
        "Digital (oficial)",
        key="btn_dig",
        disabled=ss["digital_loading"] or ss["digital_ok"] or (fuente != "Desde web oficial"),
        help="Descarga desde la web oficial",
        on_click=lambda: ss.update(digital_loading=True)
    )
    if ss["digital_loading"]:
        st.caption("⏳ Descargando digital…")
    elif ss["digital_ok"]:
        st.success("✅ Digital cargada")

with colB:
    st.button(
        "Física (oficial)",
        key="btn_fis",
        disabled=ss["fisica_loading"] or ss["fisica_ok"] or (fuente != "Desde web oficial"),
        help="Descarga desde la web oficial",
        on_click=lambda: ss.update(fisica_loading=True)
    )
    if ss["fisica_loading"]:
        st.caption("⏳ Descargando física…")
    elif ss["fisica_ok"]:
        st.success("✅ Física cargada")

st.sidebar.markdown("---")
st.sidebar.subheader("Plantillas oficiales:")
st.sidebar.markdown(
    "- [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx)  \n"
    "- [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx)"
)

st.sidebar.markdown("---")
st.sidebar.subheader("Archivos auxiliares (obligatorios)")
tem_file = st.sidebar.file_uploader("Temáticas (.xlsx, col1= término, col2= normalizado)", type=["xlsx"])
exc_file  = st.sidebar.file_uploader("Términos a excluir (.xlsx, 1ra columna)", type=["xlsx"])

st.sidebar.markdown("---")
st.sidebar.button("🧹 Nueva búsqueda (mantener bases oficiales)", on_click=clear_for_new_search)
st.sidebar.info("Para **reiniciar completamente**, refresca la página o cierra la pestaña.")

# ========================= UI: MAIN =========================

st.title("Herramienta para la elaboración de bibliografías especializadas")

with st.container(border=True):
    st.markdown(
        "- **Objetivo:** permitir la autogestión por programa/asignatura/tema y resaltar términos a excluir para depuración manual.  \n"
        "- Usa siempre las bases oficiales (Digital/Física) o súbelas **manualmente** en la barra lateral.  \n"
        "- **Plantillas:** [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx) "
        "y [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx).  \n"
        "- Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app.  \n"
        "- El proceso puede tardar algunos minutos; puedes seguir usando tu equipo."
    )

# Estado de bases cargadas
ok_badges = []
if ss["digital_ok"]: ok_badges.append("Digital ✅")
if ss["fisica_ok"]:  ok_badges.append("Física ✅")
if ok_badges:
    st.success("Bases cargadas: " + " • ".join(ok_badges))

# ============= AUTO DESCARGA =============
if fuente == "Desde web oficial":
    # Si no están cargadas todavía, dispara descarga de ambas
    need_dig = not ss["digital_ok"]
    need_fis = not ss["fisica_ok"]

    if need_dig or need_fis:
        with st.status("Descargando bases desde la web oficial…", expanded=True) as status:
            if need_dig:
                ss["digital_loading"] = True
                try:
                    df_d = download_excel_with_progress(URL_DIGITAL, "Digital")
                    ss["df_digital"] = df_d
                    ss["digital_ok"] = True
                    st.write("✅ Digital cargada")
                except Exception as e:
                    st.error(f"No se pudo descargar la base Digital: {e}")
                finally:
                    ss["digital_loading"] = False

            if need_fis:
                ss["fisica_loading"] = True
                try:
                    df_f = download_excel_with_progress(URL_FISICA, "Física")
                    ss["df_fisica"] = df_f
                    ss["fisica_ok"] = True
                    st.write("✅ Física cargada")
                except Exception as e:
                    st.error(f"No se pudo descargar la base Física: {e}")
                finally:
                    ss["fisica_loading"] = False

            if ss["digital_ok"] and ss["fisica_ok"]:
                status.update(label="¡Bases oficiales cargadas en memoria!", state="complete")
            else:
                status.update(label="Carga incompleta. Revisa los mensajes.", state="error")

else:
    # Subir manual de Digital / Física si el usuario lo decide
    st.info("Fuente de datos: **Subir archivos**. (Opcionalmente conserva las oficiales ya cargadas).")

# ========================= CARGA AUXILIARES =========================
if tem_file is not None:
    try:
        ss["tematicas_df"] = load_aux_temas(tem_file)
        st.success(f"Temáticas cargadas: {ss['tematicas_df'].shape[0]} términos.")
    except Exception as e:
        st.error(f"Error leyendo Temáticas: {e}")

if exc_file is not None:
    try:
        ss["excluir_df"] = load_aux_excluir(exc_file)
        st.success(f"Términos a excluir cargados: {ss['excluir_df'].shape[0]} términos.")
    except Exception as e:
        st.error(f"Error leyendo Términos a excluir: {e}")

# ========================= CONFIGURACIÓN DE BÚSQUEDA =========================
st.subheader("Configuración de búsqueda y duplicados")

# Detecta columnas si ya hay DataFrames
cols_dig = list(ss["df_digital"].columns) if (ss["df_digital"] is not None) else []
cols_fis = list(ss["df_fisica"].columns)  if (ss["df_fisica"]  is not None) else []

col1, col2, col3, col4 = st.columns([1.2, 1.2, 1.2, 1.2])
with col1:
    col_busq_1 = st.selectbox("Búsqueda principal por:", options=(cols_dig or [DEFAULT_SEARCH_COL1]),
                              index=(cols_dig.index(DEFAULT_SEARCH_COL1) if DEFAULT_SEARCH_COL1 in cols_dig else 0))
with col2:
    col_busq_2 = st.selectbox("Búsqueda complementaria por:", options=(cols_dig or [DEFAULT_SEARCH_COL2]),
                              index=(cols_dig.index(DEFAULT_SEARCH_COL2) if DEFAULT_SEARCH_COL2 in cols_dig else 0))
with col3:
    dup_dig = st.selectbox("Columna de duplicados en Digital:", options=(cols_dig or [DEFAULT_DUP_DIGITAL]),
                           index=(cols_dig.index(DEFAULT_DUP_DIGITAL) if DEFAULT_DUP_DIGITAL in cols_dig else 0))
with col4:
    dup_fis = st.selectbox("Columna de duplicados en Física:", options=(cols_fis or [DEFAULT_DUP_FISICA]),
                           index=(cols_fis.index(DEFAULT_DUP_FISICA) if DEFAULT_DUP_FISICA in cols_fis else 0))

st.caption("Consejo: por defecto la búsqueda se realiza en **Título** y **Temáticas**. Puedes elegir otras dos columnas si lo necesitas.")

# ========================= EJECUCIÓN DE BÚSQUEDA =========================
todo_ok_para_buscar = ( (ss["df_digital"] is not None or ss["df_fisica"] is not None)
                        and (ss["tematicas_df"] is not None)
                        and (ss["excluir_df"] is not None) )

if not todo_ok_para_buscar:
    st.info("Cargando las bases desde la web oficial… o usa la barra lateral para subir archivos manualmente.")

# Botón
run_search = st.button("🚀 Iniciar búsqueda", disabled=not todo_ok_para_buscar)

if run_search and todo_ok_para_buscar:
    with st.status("Buscando coincidencias…", expanded=True) as status:
        status.write("Normalizando y preparando…")
        # 1) Digital
        df_dig_res, bit_dig = pd.DataFrame(), pd.DataFrame(columns=["Fuente", "Término", "Resultados"])
        if ss["df_digital"] is not None and not ss["df_digital"].empty:
            status.write("Buscando en **Digital**…")
            df_dig_res, bit_dig = find_matches(
                ss["df_digital"],
                cols_busqueda=[col_busq_1, col_busq_2],
                tematicas_df=ss["tematicas_df"],
                fuente="Digital"
            )
            # Duplicados Digital
            df_dig_res = remove_duplicates(df_dig_res, dup_dig)

        # 2) Física
        df_fis_res, bit_fis = pd.DataFrame(), pd.DataFrame(columns=["Fuente", "Término", "Resultados"])
        if ss["df_fisica"] is not None and not ss["df_fisica"].empty:
            status.write("Buscando en **Física**…")
            df_fis_res, bit_fis = find_matches(
                ss["df_fisica"],
                cols_busqueda=[col_busq_1, col_busq_2],
                tematicas_df=ss["tematicas_df"],
                fuente="Física"
            )
            # Duplicados Física
            df_fis_res = remove_duplicates(df_fis_res, dup_fis)

        # Unimos resultados
        df_final = pd.concat([df_dig_res, df_fis_res], ignore_index=True) if (not df_dig_res.empty or not df_fis_res.empty) else pd.DataFrame()
        # Bitácora: unimos y aseguramos también términos con 0
        bit_total = pd.concat([bit_dig, bit_fis], ignore_index=True)
        # Asegurar cero por cada término y fuente que no haya sacado nada
        all_terms = ss["tematicas_df"]["termino"].unique().tolist()
        fuentes = ["Digital", "Física"]
        rows_zero = []
        for fu in fuentes:
            terms_conteo = set(bit_total.loc[bit_total["Fuente"]==fu, "Término"].tolist())
            for t in all_terms:
                if t not in terms_conteo:
                    rows_zero.append({"Fuente": fu, "Término": t, "Resultados": 0})
        if rows_zero:
            bit_total = pd.concat([bit_total, pd.DataFrame(rows_zero)], ignore_index=True)

        # Guardamos en sesión
        ss["df_resultados"] = df_final
        # Orden bitácora: por Fuente y luego por Resultados desc
        ss["bitacora"] = bit_total.sort_values(by=["Fuente","Resultados","Término"], ascending=[True, False, True]).reset_index(drop=True)

        if df_final.empty:
            status.update(label="Sin resultados para los términos dados.", state="error")
        else:
            status.update(label="Búsqueda finalizada.", state="complete")

# ========================= RESULTADOS =========================
df_out = ss.get("df_resultados")
if df_out is not None:
    st.subheader("Resultados (vista previa)")
    st.write(f"Filas: **{df_out.shape[0]:,}**")
    st.dataframe(df_out.head(200), use_container_width=True)

    # Bitácora visible siempre (incluye términos con 0)
    st.subheader("🧾 Bitácora (por término y fuente)")
    st.dataframe(ss["bitacora"], use_container_width=True, height=300)

    # Descargas
    colx, coly = st.columns(2)
    with colx:
        # CSV (sin NaN -> celdas en blanco)
        csv_bytes = df_out.fillna("").to_csv(index=False).encode("utf-8-sig")
        st.download_button("📄 Descargar CSV", data=csv_bytes, file_name="resultados.csv", mime="text/csv")

    with coly:
        # XLSX con resaltado
        try:
            bio_xlsx = export_xlsx_with_highlight(df_out.fillna(""), ss.get("excluir_df"))
            st.download_button("📘 Descargar Excel (con resaltado)", data=bio_xlsx.getvalue(),
                               file_name="resultados_con_resaltado.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            st.error(f"No se pudo generar el Excel con resaltado: {e}")
