# -*- coding: utf-8 -*-
# Versión 7.3 – UEB Bibliografías (Streamlit)
# Cambios clave:
# - use_container_width (adiós use_column_width deprecado)
# - Descarga automática bases Digital/Física con indicadores de progreso
# - Carga manual de bases en "⚙️ Avanzado" (tiene prioridad solo en la sesión)
# - Cargadores de Temáticas y Términos a excluir siempre visibles (obligatorios)
# - Resaltado de exclusiones en Excel (y CSV sin “nan”)
# - Dedupe: Digital (Url OA), Física (No. Topográfico) – configurables
# - Sin botones “usar/liberar memoria”; sesión estable (tema no borra datos)

import io
import re
import time
import requests
import pandas as pd
import numpy as np
from unidecode import unidecode
import streamlit as st

# ------------------------------------
# Configuración general
# ------------------------------------
st.set_page_config(
    page_title="Herramienta para bibliografías especializadas",
    page_icon="📚",
    layout="wide",
)

# URLs oficiales
URL_DIGITAL = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx"
URL_FISICA  = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx"

# Logos
DARK_LOGO  = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png"
LIGHT_LOGO = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20ORG.png"

# ------------------------------------
# Helpers de sesión
# ------------------------------------
def init_state():
    for k, v in {
        "df_digital": None,
        "df_fisica": None,
        "digital_loaded_from": None,   # "web" | "manual"
        "fisica_loaded_from": None,    # "web" | "manual"
        "tematicas_file": None,
        "excluir_file": None,
        "auto_loaded_once": False,
        "last_status": "",
    }.items():
        st.session_state.setdefault(k, v)

init_state()

# ------------------------------------
# Utilidades
# ------------------------------------
def normalize_text(x: str) -> str:
    if pd.isna(x):
        return ""
    return unidecode(str(x)).strip().lower()

def contains_term(text: str, term: str) -> bool:
    """Coincidencia simple: substring no sensible a acentos/case."""
    return normalize_text(term) in normalize_text(text)

# ------------------------------------
# Carga de datos (con caché)
# ------------------------------------
@st.cache_data(show_spinner=False, ttl=3600)
def load_excel_from_url(url: str) -> pd.DataFrame:
    # Descarga a bytes para mostrar barra (fuera del caché está la barra)
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    total = int(resp.headers.get("Content-Length", "0"))
    chunk = 1024 * 64

    buf = io.BytesIO()
    downloaded = 0
    for data in resp.iter_content(chunk_size=chunk):
        buf.write(data)
        downloaded += len(data)
    buf.seek(0)
    # Leer excel
    df = pd.read_excel(buf)
    return df

def safe_progress_download(label: str, url: str) -> pd.DataFrame | None:
    """Barra de progreso visual mientras la función cacheada descarga el archivo."""
    ph = st.empty()
    prog = st.progress(0, text=f"Descargando {label}…")
    try:
        # No podemos medir porcentaje exacto desde cache; simulamos ticks visibles
        # mientras hacemos la carga real (ráfaga final).
        for i in range(0, 50, 5):
            prog.progress(i, text=f"Descargando {label}…")
            time.sleep(0.05)
        df = load_excel_from_url(url)
        prog.progress(100, text=f"{label}: descarga completa")
        time.sleep(0.15)
        prog.empty()
        ph.empty()
        return df
    except Exception as e:
        prog.empty()
        ph.error(f"No se pudo descargar {label}: {e}")
        return None

# ------------------------------------
# Lectura de Temáticas / Excluir
# ------------------------------------
def read_tematicas_file(file) -> pd.DataFrame:
    df = pd.read_excel(file)
    df.columns = [str(c).strip() for c in df.columns]
    # Espera: col1 = término (todas las variantes), col2 = normalizado
    if df.shape[1] < 2:
        raise ValueError("La plantilla de Temáticas debe tener 2 columnas (término, normalizado).")
    df = df.iloc[:, :2].copy()
    df.columns = ["termino", "normalizado"]
    df["termino"] = df["termino"].astype(str)
    df["normalizado"] = df["normalizado"].astype(str)
    # No filas vacías
    df = df[(df["termino"].str.strip() != "") & (df["normalizado"].str.strip() != "")]
    return df

def read_excluir_file(file) -> list[str]:
    df = pd.read_excel(file)
    if df.shape[1] < 1:
        return []
    col = df.columns[0]
    vals = df[col].dropna().astype(str).tolist()
    vals = [v for v in [v.strip() for v in vals] if v]
    return vals

# ------------------------------------
# Motor de búsqueda
# ------------------------------------
def search_in_base(df: pd.DataFrame,
                   tematicas_df: pd.DataFrame,
                   col1: str,
                   col2: str,
                   fuente: str) -> pd.DataFrame:
    """Busca por términos (tematicas_df['termino']) en col1/col2."""
    if df is None or df.empty:
        return pd.DataFrame()

    if col1 not in df.columns or col2 not in df.columns:
        return pd.DataFrame()

    results = []
    total_terms = len(tematicas_df)
    st.write("")  # espacio
    st.info(f"Buscando en **{fuente}**… ({total_terms} términos)")
    pbar = st.progress(0)
    for i, row in tematicas_df.iterrows():
        termino = row["termino"]
        normal = row["normalizado"]

        mask = df[col1].astype(str).apply(lambda x: contains_term(x, termino)) | \
               df[col2].astype(str).apply(lambda x: contains_term(x, termino))
        hits = df.loc[mask].copy()
        if not hits.empty:
            hits["Término"] = termino
            hits["Normalizado"] = normal
            hits["Fuente"] = fuente
            results.append(hits)

        if total_terms:
            pbar.progress(int(100 * (i + 1) / total_terms))
    pbar.empty()

    if results:
        out = pd.concat(results, ignore_index=True)
        # Columna de coincidencia
        out["Columna coincidencia"] = np.where(
            out[col1].astype(str).apply(lambda x: contains_term(x, out["Término"].iloc[0])),"Título","Temáticas"
        )  # marcador simple
        return out
    return pd.DataFrame()

def apply_exclusion_highlight(df: pd.DataFrame, excluir: list[str], col1: str, col2: str) -> pd.DataFrame:
    if df.empty or not excluir:
        df["Exclusión_detectada"] = False
        return df

    excl_norm = [normalize_text(e) for e in excluir if e.strip()]
    def has_excl(x: str) -> bool:
        nx = normalize_text(x)
        return any(e in nx for e in excl_norm)

    df["Exclusión_detectada"] = df[col1].astype(str).apply(has_excl) | df[col2].astype(str).apply(has_excl)
    return df

def bitacora_por_termino(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Término" not in df.columns:
        return pd.DataFrame({"Término": [], "Resultados": []})
    return df.groupby(["Fuente", "Término"], as_index=False).size().rename(columns={"size": "Resultados"})

# ------------------------------------
# Exportación
# ------------------------------------
def to_csv_blank_nan(df: pd.DataFrame) -> bytes:
    clean = df.replace({np.nan: ""})
    return clean.to_csv(index=False).encode("utf-8-sig")

def to_xlsx_with_highlight(df: pd.DataFrame, bitacora: pd.DataFrame,
                           excluir: list[str], col1: str, col2: str) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_out = df.replace({np.nan: ""})
        df_out.to_excel(writer, sheet_name="Resultados", index=False)
        if not bitacora.empty:
            bitacora.to_excel(writer, sheet_name="Bitácora", index=False)

        wb  = writer.book
        ws  = writer.sheets["Resultados"]
        fmt_yellow = wb.add_format({"bg_color": "#FFF59D"})
        fmt_header = wb.add_format({"bold": True})

        # Encabezados en negrita
        for col_idx, col_name in enumerate(df_out.columns):
            ws.write(0, col_idx, col_name, fmt_header)

        # Resaltar exclusiones en col1/col2
        excl_norm = [normalize_text(e) for e in excluir]
        if excl_norm and col1 in df_out.columns and col2 in df_out.columns:
            col1_idx = df_out.columns.get_loc(col1)
            col2_idx = df_out.columns.get_loc(col2)
            for r in range(len(df_out)):
                t1 = normalize_text(str(df_out.iat[r, col1_idx]))
                t2 = normalize_text(str(df_out.iat[r, col2_idx]))
                if any(e in t1 for e in excl_norm):
                    ws.write(r + 1, col1_idx, df_out.iat[r, col1_idx], fmt_yellow)
                if any(e in t2 for e in excl_norm):
                    ws.write(r + 1, col2_idx, df_out.iat[r, col2_idx], fmt_yellow)

    return output.getvalue()

# ------------------------------------
# UI – Encabezado
# ------------------------------------
col_logo, col_title = st.columns([0.27, 0.73])
with col_logo:
    # Mostramos el logo “oscuro” por defecto; si usas selector de tema, no borres estado.
    st.image(DARK_LOGO, use_container_width=True)
with col_title:
    st.markdown(
        "## Herramienta para la elaboración de bibliografías especializadas"
    )

st.info(
    "• **Objetivo:** autogestión por programa/asignatura/tema y **resaltado de términos a excluir** para depuración manual.\n"
    "• Usa siempre las bases oficiales (Digital/Física) o súbelas **manualmente** en la barra lateral.\n"
    "• Plantillas: [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx) "
    "y [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx).\n"
    "• Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app.\n"
    "• El proceso puede tardar algunos minutos; puedes seguir usando tu equipo (no cierres el navegador)."
)

# ------------------------------------
# Barra lateral
# ------------------------------------
st.sidebar.markdown("### Plantillas oficiales:")
st.sidebar.markdown(
    "- [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx)\n"
    "- [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx)"
)

st.sidebar.markdown("### Archivos auxiliares (obligatorios)")
tematicas_file = st.sidebar.file_uploader(
    "Temáticas (.xlsx, col1=término, col2=normalizado)", type=["xlsx"], key="uploader_tematicas"
)
excluir_file = st.sidebar.file_uploader(
    "Términos a excluir (.xlsx, 1ra columna)", type=["xlsx"], key="uploader_excluir"
)
if tematicas_file is not None:
    st.session_state["tematicas_file"] = tematicas_file
if excluir_file is not None:
    st.session_state["excluir_file"] = excluir_file

with st.sidebar.expander("⚙️ Avanzado: subir bases Digital/Física manualmente", expanded=False):
    st.caption(
        "Si subes aquí, se usan **estos archivos** en lugar de las bases oficiales (solo en esta sesión)."
    )
    manual_digital = st.file_uploader("Base **Digital** (.xlsx)", type=["xlsx"], key="manual_dig")
    manual_fisica  = st.file_uploader("Base **Física** (.xlsx)", type=["xlsx"], key="manual_fis")

    if manual_digital is not None:
        try:
            df_dig_manual = pd.read_excel(manual_digital)
            st.session_state["df_digital"] = df_dig_manual
            st.session_state["digital_loaded_from"] = "manual"
            st.success("Base **Digital** cargada manualmente y lista.")
        except Exception as e:
            st.error(f"No se pudo leer la base Digital: {e}")

    if manual_fisica is not None:
        try:
            df_fis_manual = pd.read_excel(manual_fisica)
            st.session_state["df_fisica"] = df_fis_manual
            st.session_state["fisica_loaded_from"] = "manual"
            st.success("Base **Física** cargada manualmente y lista.")
        except Exception as e:
            st.error(f"No se pudo leer la base Física: {e}")

if st.session_state.get("digital_loaded_from") == "manual" or st.session_state.get("fisica_loaded_from") == "manual":
    st.sidebar.warning(
        "Estás usando **bases cargadas manualmente**. La versión no está garantizada como la más reciente. "
        "Los archivos NO se almacenan por la Universidad y se eliminan al cerrar esta app."
    )
else:
    st.sidebar.info(
        "Por defecto se usan las **bases oficiales** descargadas automáticamente. "
        "Puedes subir manualmente una base en el panel *Avanzado* si lo necesitas."
    )

# ------------------------------------
# Descarga automática (si no hay manual)
# ------------------------------------
st.write("")  # separador
box = st.container()
with box:
    st.markdown("#### 🗃️ Bases oficiales cargadas en memoria (sesión)")
    dig_bar = st.empty()
    fis_bar = st.empty()

if st.session_state["df_digital"] is None and st.session_state.get("digital_loaded_from") != "manual":
    st.info("Cargando las bases **Digital** y **Física** desde la web oficial… "
            "Puedes subir *Temáticas* y *Términos a excluir* mientras tanto. No cierres esta ventana.")
    df_dig = safe_progress_download("Base Digital", URL_DIGITAL)
    if df_dig is not None:
        st.session_state["df_digital"] = df_dig
        st.session_state["digital_loaded_from"] = "web"

if st.session_state["df_fisica"] is None and st.session_state.get("fisica_loaded_from") != "manual":
    df_fis = safe_progress_download("Base Física", URL_FISICA)
    if df_fis is not None:
        st.session_state["df_fisica"] = df_fis
        st.session_state["fisica_loaded_from"] = "web"

# Indicadores verdes
with box:
    colA, colB = st.columns([1, 1])
    with colA:
        if st.session_state["df_digital"] is not None:
            st.success("✅ Digital cargada (100%)")
        else:
            st.warning("Digital: pendiente")
    with colB:
        if st.session_state["df_fisica"] is not None:
            st.success("✅ Física cargada (100%)")
        else:
            st.warning("Física: pendiente")

# ------------------------------------
# Configuración de búsqueda
# ------------------------------------
st.markdown("### Configuración de búsqueda y duplicados")
col1, col2, col3, col4 = st.columns([1.2, 1.2, 1.4, 1.4])

# Columnas sugeridas (pueden variar según tus bases)
default_col_titulo = "Título"
default_col_tema   = "Temáticas"

with col1:
    col_busq1 = st.selectbox("Búsqueda principal por:", options=[default_col_titulo, default_col_tema], index=0)
with col2:
    col_busq2 = st.selectbox("Búsqueda complementaria por:", options=[default_col_titulo, default_col_tema], index=1)
with col3:
    col_dups_dig = st.selectbox("Columna de duplicados en Digital:", options=["Url OA", default_col_titulo, default_col_tema], index=0)
with col4:
    col_dups_fis = st.selectbox("Columna de duplicados en Física:", options=["No. Topográfico", default_col_titulo, default_col_tema], index=0)

st.caption("Consejo: por defecto la búsqueda se realiza en **Título** y **Temáticas**. "
           "Puedes elegir otras dos columnas si lo necesitas.")

# ------------------------------------
# Validación previa
# ------------------------------------
ready_bases = (st.session_state["df_digital"] is not None) and (st.session_state["df_fisica"] is not None)

tem_ok = st.session_state.get("tematicas_file") is not None
exc_ok = st.session_state.get("excluir_file") is not None

if not ready_bases:
    st.warning("Esperando la carga de las bases **Digital** y **Física**…")
if not tem_ok or not exc_ok:
    st.info("Por favor, sube los archivos obligatorios: **Temáticas** y **Términos a excluir** en la barra lateral.")

# ------------------------------------
# Búsqueda
# ------------------------------------
st.write("")
btn_buscar = st.button("🚀 Iniciar búsqueda", disabled=not (ready_bases and tem_ok and exc_ok))

if btn_buscar:
    try:
        tematicas_df = read_tematicas_file(st.session_state["tematicas_file"])
        excluir_list = read_excluir_file(st.session_state["excluir_file"])

        df_dig = st.session_state["df_digital"].copy()
        df_fis = st.session_state["df_fisica"].copy()

        # Asegurar columnas existen
        for need_col in [col_busq1, col_busq2]:
            if need_col not in df_dig.columns or need_col not in df_fis.columns:
                st.error(f"La columna **{need_col}** no existe en alguna base. Verifica encabezados.")
                st.stop()

        # Buscar
        res_dig = search_in_base(df_dig, tematicas_df, col_busq1, col_busq2, "Digital")
        res_fis = search_in_base(df_fis, tematicas_df, col_busq1, col_busq2, "Física")

        # Unir
        all_res = pd.concat([res_dig, res_fis], ignore_index=True) if not res_dig.empty or not res_fis.empty else pd.DataFrame()
        if all_res.empty:
            st.warning("No se encontraron coincidencias con los términos cargados.")
            st.stop()

        # Dedupe por columnas elegidas
        # Digital: col_dups_dig; Física: col_dups_fis → deduplicamos separadamente
        if col_dups_dig in all_res.columns:
            mask_dig = all_res["Fuente"] == "Digital"
            all_res.loc[mask_dig, :] = all_res.loc[mask_dig, :].drop_duplicates(subset=[col_dups_dig])

        if col_dups_fis in all_res.columns:
            mask_fis = all_res["Fuente"] == "Física"
            all_res.loc[mask_fis, :] = all_res.loc[mask_fis, :].drop_duplicates(subset=[col_dups_fis])

        all_res = all_res.reset_index(drop=True)

        # Resaltado (bandera booleana)
        all_res = apply_exclusion_highlight(all_res, excluir_list, col_busq1, col_busq2)

        # Bitácora por término
        bita = bitacora_por_termino(all_res)

        st.markdown("### Resultados")
        st.dataframe(all_res.head(200), use_container_width=True)

        # Descargas
        colA, colB = st.columns(2)
        with colA:
            csv_bytes = to_csv_blank_nan(all_res)
            st.download_button(
                "📄 Descargar CSV",
                data=csv_bytes,
                file_name="resultados.csv",
                mime="text/csv",
            )
        with colB:
            xlsx_bytes = to_xlsx_with_highlight(all_res, bita, excluir_list, col_busq1, col_busq2)
            st.download_button(
                "📘 Descargar Excel (con resaltado)",
                data=xlsx_bytes,
                file_name="resultados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        st.markdown("### 📋 Bitácora (por término)")
        if bita.empty:
            st.info("No hay bitácora para mostrar.")
        else:
            st.dataframe(bita, use_container_width=True)

    except Exception as e:
        st.error(f"Ocurrió un error en la búsqueda: {e}")
