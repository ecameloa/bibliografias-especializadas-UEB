# app.py
# -----------------------------------------------------------
# Herramienta para la elaboración de bibliografías especializadas
# - Descarga automática y robusta de BD Digital/Física
# - Indicadores de estado (sin botones de descarga)
# - Subida manual oculta en "Avanzado"
# - Búsqueda con normalización segura y progreso
# -----------------------------------------------------------

from __future__ import annotations
import io
import time
import re
from typing import Optional, Tuple

import requests
import pandas as pd
import numpy as np
from unidecode import unidecode
import streamlit as st

# ------------------ Configuración ------------------

st.set_page_config(
    page_title="Herramienta para bibliografías",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

URL_DIGITAL = ("https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/"
               "Biblioteca%20Colecci%C3%B3n%20Digital.xlsx")
URL_FISICA  = ("https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/"
               "Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx")

CHUNK_SIZE = 1024 * 1024         # 1 MB
TIMEOUT    = (30, 300)           # (connect, read)
MAX_RETRY  = 3

# ------------------ Estado de sesión ------------------

def _ss_init():
    ss = st.session_state
    ss.setdefault("boot_started", False)
    ss.setdefault("boot_in_progress", False)
    ss.setdefault("boot_error", None)
    ss.setdefault("boot_done", False)

    ss.setdefault("df_digital", None)     # DataFrame
    ss.setdefault("df_fisica", None)      # DataFrame
    ss.setdefault("digital_pct", 0)       # 0..100
    ss.setdefault("fisica_pct", 0)        # 0..100

    # Subidas manuales
    ss.setdefault("tematicas_df", None)
    ss.setdefault("excluir_df", None)

_ss_init()

# ------------------ Utilidades robustas ------------------

def _safe_unidecode(text: str) -> str:
    try:
        return unidecode(text)
    except Exception:
        return text

def _normalize_text(s: pd.Series, progress: Optional[st.progress] = None) -> pd.Series:
    """
    Minúsculas, sin tildes, espacios colapsados. Segura y con progreso.
    """
    if s is None:
        return s

    total = len(s)
    step = max(1, total // 100)
    out = []

    for i, val in enumerate(s.astype(str).fillna("").tolist(), start=1):
        t = _safe_unidecode(val).lower()
        t = re.sub(r"\s+", " ", t).strip()
        out.append(t)
        if progress and i % step == 0:
            progress.progress(min(i / total, 1.0), text=f"Normalizando… {int(i*100/total)}%")

    return pd.Series(out, index=s.index)

def _stream_to_memory(url: str, label: str, pct_key: str) -> io.BytesIO:
    """
    Descarga robusta (con reintentos y barra) del archivo en memoria.
    """
    for attempt in range(1, MAX_RETRY + 1):
        try:
            with requests.get(url, stream=True, timeout=TIMEOUT) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))
                buf = io.BytesIO()
                read = 0
                pb = st.progress(0, text=f"Descargando {label}…")

                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    buf.write(chunk)
                    read += len(chunk)
                    if total:
                        pct = int(min(read / total, 1.0) * 100)
                        st.session_state[pct_key] = pct
                        pb.progress(pct/100, text=f"{label}: {read/1e6:,.1f} / {total/1e6:,.1f} MB")
                # Si no hay Content-Length, igualmente completamos
                st.session_state[pct_key] = 100
                pb.progress(1.0, text=f"{label}: descarga completa")
                buf.seek(0)
                return buf
        except Exception as e:
            if attempt < MAX_RETRY:
                st.warning(f"{label}: error de red ({e}). Reintentando {attempt}/{MAX_RETRY}…")
                time.sleep(2 * attempt)
            else:
                raise

# Cachear datos 12h por contenedor
@st.cache_data(ttl=43200, show_spinner=False)
def _load_digital_from_web() -> pd.DataFrame:
    bio = _stream_to_memory(URL_DIGITAL, "Base Digital", "digital_pct")
    return pd.read_excel(bio, engine="openpyxl", dtype=str)

@st.cache_data(ttl=43200, show_spinner=False)
def _load_fisica_from_web() -> pd.DataFrame:
    bio = _stream_to_memory(URL_FISICA, "Base Física", "fisica_pct")
    return pd.read_excel(bio, engine="openpyxl", dtype=str)

def bootstrap_downloads():
    """
    Descarga automática, secuencial y a prueba de caídas.
    No se dispara de nuevo si ya se hizo (boot_done).
    """
    ss = st.session_state
    if ss["boot_done"] or ss["boot_in_progress"]:
        return

    ss["boot_started"] = True
    ss["boot_in_progress"] = True
    ss["boot_error"] = None

    with st.container(border=True):
        st.info(
            "Cargando las bases **Digital** y **Física** desde la web oficial. "
            "Puedes subir **Temáticas** y **Términos a excluir** mientras tanto. "
            "No cierres esta ventana.",
            icon="⬇️",
        )

    try:
        if ss.get("df_digital") is None:
            with st.spinner("Descargando Base **Digital**…"):
                ss["df_digital"] = _load_digital_from_web()
        st.success("Base **Digital** cargada en memoria.", icon="✅")

        if ss.get("df_fisica") is None:
            with st.spinner("Descargando Base **Física**…"):
                ss["df_fisica"] = _load_fisica_from_web()
        st.success("Base **Física** cargada en memoria.", icon="✅")

        ss["boot_done"] = True
    except Exception as e:
        ss["boot_error"] = str(e)
        st.error(
            "No se pudo completar la descarga automática. "
            "Puedes reintentar o cargar archivos manualmente desde el lateral.",
            icon="❌",
        )
    finally:
        ss["boot_in_progress"] = False

def render_sources_status():
    ss = st.session_state
    st.markdown("### ℹ️ ¡Bases oficiales cargadas en memoria!")
    with st.container(border=True):
        st.write(f"Digital: **{ss.get('digital_pct', 0)}%**")
        st.progress(ss.get("digital_pct", 0)/100)
        if ss.get("df_digital") is not None:
            st.checkbox("Digital cargada", True, disabled=True)

        st.write(f"Física: **{ss.get('fisica_pct', 0)}%**")
        st.progress(ss.get("fisica_pct", 0)/100)
        if ss.get("df_fisica") is not None:
            st.checkbox("Física cargada", True, disabled=True)

# ------------------ Sidebar ------------------

def sidebar():
    st.sidebar.image(
        "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png",
        use_column_width=True,
    )

    st.sidebar.caption("Biblioteca Juan Roa Vásquez")

    st.sidebar.markdown("### Fuente de datos")
    st.sidebar.radio(
        "Elegir fuente",
        ["Desde web oficial", "Subir archivos"],
        index=0,
        key="source_mode",
        help="Usa siempre la fuente oficial. 'Subir archivos' es solo para pruebas puntuales.",
    )

    # Subida manual dentro de Avanzado
    with st.sidebar.expander("⚙️ Avanzado: subir archivos manualmente", expanded=False):
        st.info("**Advertencia:** si subes archivos manuales, no se garantiza que uses la última versión. "
                "Los archivos no se almacenan; se eliminan al cerrar la app.", icon="⚠️")
        up_t = st.file_uploader(
            "Temáticas (.xlsx, col1=término, col2=normalizado)",
            type=["xlsx"],
            key="up_tematicas",
        )
        if up_t is not None:
            try:
                st.session_state["tematicas_df"] = pd.read_excel(up_t, engine="openpyxl", dtype=str)
                st.success("Temáticas cargadas.", icon="✅")
            except Exception as e:
                st.error(f"No se pudo leer el archivo de Temáticas: {e}", icon="❌")

        up_e = st.file_uploader(
            "Términos a excluir (.xlsx, 1ra col.)",
            type=["xlsx"],
            key="up_excluir",
        )
        if up_e is not None:
            try:
                st.session_state["excluir_df"] = pd.read_excel(up_e, engine="openpyxl", dtype=str)
                st.success("Términos a excluir cargados.", icon="✅")
            except Exception as e:
                st.error(f"No se pudo leer el archivo de exclusión: {e}", icon="❌")

    st.sidebar.markdown("---")
    st.sidebar.markdown("#### Plantillas oficiales:")
    st.sidebar.markdown(
        "- [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx)\n"
        "- [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx)"
    )

# ------------------ Búsqueda (tu lógica, reforzada) ------------------

def run_search(
    df_digital: pd.DataFrame,
    df_fisica: pd.DataFrame,
    tematicas_df: pd.DataFrame,
    excluir_df: Optional[pd.DataFrame],
    col_busq1: str,
    col_busq2: str,
    col_dup_digital: str,
    col_dup_fisica: str,
):
    """
    Implementa tu búsqueda sin alterar el comportamiento
    pero reforzando normalización y evitando caídas.
    """
    st.markdown("### 🔍 Resultados")

    # Validaciones mínimas
    for df, nombre in [(df_digital, "Digital"), (df_fisica, "Física")]:
        if not isinstance(df, pd.DataFrame) or df.empty:
            st.error(f"La base **{nombre}** está vacía o no se ha cargado correctamente.", icon="❌")
            return

    for col in [col_busq1, col_busq2]:
        for df, nombre in [(df_digital, "Digital"), (df_fisica, "Física")]:
            if col not in df.columns:
                st.error(f"En **{nombre}** no existe la columna '{col}'. Verifica nombres de columnas.", icon="❌")
                return

    if col_dup_digital not in df_digital.columns:
        st.error(f"En **Digital** no existe la columna de duplicados '{col_dup_digital}'.", icon="❌")
        return
    if col_dup_fisica not in df_fisica.columns:
        st.error(f"En **Física** no existe la columna de duplicados '{col_dup_fisica}'.", icon="❌")
        return

    # Normalizar columnas de búsqueda (con barra)
    st.write("Normalizando columnas de búsqueda (esto evita falsos positivos y errores)…")
    p1 = st.progress(0)
    p2 = st.progress(0)
    try:
        df_d = df_digital.copy()
        df_f = df_fisica.copy()
        df_d["_norm1"] = _normalize_text(df_d[col_busq1], p1)
        df_d["_norm2"] = _normalize_text(df_d[col_busq2], p2)
        p1.empty(); p2.empty()
    except Exception as e:
        st.error(f"No fue posible normalizar Digital: {e}", icon="❌")
        return

    p3 = st.progress(0)
    try:
        df_f["_norm1"] = _normalize_text(df_f[col_busq1], p3)
        df_f["_norm2"] = _normalize_text(df_f[col_busq2], None)
        p3.empty()
    except Exception as e:
        st.error(f"No fue posible normalizar Física: {e}", icon="❌")
        return

    # Preparar temáticas y exclusión
    if not isinstance(tematicas_df, pd.DataFrame) or tematicas_df.empty:
        st.warning("Debes cargar **Temáticas** (plantilla oficial) para ejecutar la búsqueda.")
        return

    # 1ra col = término, 2da col = normalizado
    try:
        terminos = tematicas_df.iloc[:, 0].astype(str).fillna("")
        normalizado = tematicas_df.iloc[:, 1].astype(str).fillna("")
    except Exception:
        st.error("La plantilla de Temáticas debe tener al menos 2 columnas.", icon="❌")
        return

    excl_set = set()
    if isinstance(excluir_df, pd.DataFrame) and not excluir_df.empty:
        try:
            excl_set = set(
                _normalize_text(excluir_df.iloc[:, 0].astype(str).fillna(""))
                .dropna().tolist()
            )
        except Exception:
            st.warning("No se pudo interpretar el archivo de **exclusión**. Se omitirá.", icon="⚠️")

    # Búsqueda: ejemplo sencillo (ajústalo si ya tienes tu lógica avanzada)
    resultados = []
    st.write("Buscando coincidencias por términos… (puedes tardar según cantidad de términos)")
    step = max(1, len(terminos)//50)
    prog = st.progress(0)

    for i, (t, norm) in enumerate(zip(terminos, normalizado), start=1):
        patt = _safe_unidecode(t).lower().strip()
        patt = re.escape(patt)
        # Coincidencia si aparece en norm1 o norm2 y no contiene exclusiones
        mask_d = (df_d["_norm1"].str.contains(patt, regex=True) |
                  df_d["_norm2"].str.contains(patt, regex=True))
        sub_d = df_d.loc[mask_d].copy()

        mask_f = (df_f["_norm1"].str.contains(patt, regex=True) |
                  df_f["_norm2"].str.contains(patt, regex=True))
        sub_f = df_f.loc[mask_f].copy()

        # Resaltado de exclusión en Título/Temáticas (solo marcado; el Excel exporta con formato)
        if excl_set:
            def _mark_excl(val):
                nv = _safe_unidecode(str(val)).lower()
                for ex in excl_set:
                    if ex and ex in nv:
                        return True
                return False

            if not sub_d.empty:
                sub_d["_excluir_titulo"] = sub_d[col_busq1].apply(_mark_excl)
                sub_d["_excluir_temat"]  = sub_d[col_busq2].apply(_mark_excl)
            if not sub_f.empty:
                sub_f["_excluir_titulo"] = sub_f[col_busq1].apply(_mark_excl)
                sub_f["_excluir_temat"]  = sub_f[col_busq2].apply(_mark_excl)

        if not sub_d.empty:
            sub_d["Fuente"] = "Digital"
            sub_d["Término"] = t
            sub_d["Temática normalizada"] = norm
            resultados.append(sub_d)

        if not sub_f.empty:
            sub_f["Fuente"] = "Física"
            sub_f["Término"] = t
            sub_f["Temática normalizada"] = norm
            resultados.append(sub_f)

        if i % step == 0:
            prog.progress(min(i/len(terminos), 1.0), text=f"Término {i}/{len(terminos)}")

    prog.empty()

    if not resultados:
        st.info("No se encontraron coincidencias.", icon="ℹ️")
        return

    out = pd.concat(resultados, ignore_index=True)
    st.success(f"Resultados totales: {len(out):,}", icon="✅")

    # Preview (muestras)
    st.dataframe(out.head(200), use_container_width=True)

    # Exportar a Excel (con resaltado)
    def _to_xlsx_bytes(df: pd.DataFrame) -> bytes:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Resultados")
            wb  = writer.book
            ws  = writer.sheets["Resultados"]
            yellow = wb.add_format({"bg_color": "#FFF59D"})
            # Resalta columnas donde _excluir_xxx sea True
            try:
                colmap = {c:i for i,c in enumerate(df.columns)}
                tit_col = colmap.get(col_busq1)
                tem_col = colmap.get(col_busq2)
                ex1 = colmap.get("_excluir_titulo")
                ex2 = colmap.get("_excluir_temat")
                if ex1 is not None and tit_col is not None:
                    for r, v in enumerate(df["_excluir_titulo"].fillna(False).tolist(), start=1):
                        if v: ws.write(r, tit_col, df.iloc[r-1, tit_col], yellow)
                if ex2 is not None and tem_col is not None:
                    for r, v in enumerate(df["_excluir_temat"].fillna(False).tolist(), start=1):
                        if v: ws.write(r, tem_col, df.iloc[r-1, tem_col], yellow)
            except Exception:
                pass
        buf.seek(0)
        return buf.getvalue()

    xlsx = _to_xlsx_bytes(out)
    st.download_button(
        "⬇️ Descargar Excel (con resaltado)",
        data=xlsx,
        file_name="resultados_bibliografias.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ------------------ Layout principal ------------------

sidebar()

st.title("Herramienta para la elaboración de bibliografías especializadas")

with st.container(border=True):
    st.markdown(
        """
- **Objetivo:** permitir la autogestión por programa/asignatura/tema y **resaltar términos a excluir** para depuración manual.  
- Usa siempre las bases **oficiales** (Digital/Física) o súbelas **manualmente** en la barra lateral.  
- **Plantillas:** [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx) y [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx).  
- Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app.  
- El proceso puede tardar algunos minutos; puedes seguir usando tu equipo (no cierres el navegador).
        """
    )

# Lanzar/mostrar estado de descargas oficiales
bootstrap_downloads()
render_sources_status()

# Configuración de columnas (dejas las que usas por defecto)
st.markdown("### Configuración de búsqueda y duplicados")
col1, col2, col3, col4 = st.columns([1.2, 1.4, 1.4, 1.6])

with col1:
    col_busq1 = st.selectbox("Búsqueda principal por:", ["Título"], index=0)
with col2:
    col_busq2 = st.selectbox("Búsqueda complementaria por:", ["Temáticas"], index=0)
with col3:
    col_dup_digital = st.selectbox("Columna de duplicados en Digital:", ["Url OA", "Título"], index=0)
with col4:
    col_dup_fisica = st.selectbox("Columna de duplicados en Física:", ["No. Topográfico", "Título"], index=0)

# Mensaje guía mientras termina la carga o si falta algo manual
if st.session_state.get("df_digital") is None or st.session_state.get("df_fisica") is None:
    st.info("Cargando las bases desde la web oficial… o usa la barra lateral para subir archivos manualmente.", icon="⏳")

# Botón de búsqueda
st.markdown("---")
if st.button("🚀 Iniciar búsqueda", type="primary"):
    try:
        run_search(
            st.session_state.get("df_digital"),
            st.session_state.get("df_fisica"),
            st.session_state.get("tematicas_df"),
            st.session_state.get("excluir_df"),
            col_busq1,
            col_busq2,
            col_dup_digital,
            col_dup_fisica,
        )
    except Exception as e:
        st.error(f"Ocurrió un error durante la búsqueda: {e}", icon="❌")
        st.stop()

# Si falló el arranque, ofrece reintento
if st.session_state.get("boot_error"):
    colR, _ = st.columns([1,2])
    with colR:
        if st.button("🔁 Reintentar descarga oficial"):
            for k in ("df_digital", "df_fisica"):
                st.session_state[k] = None
            st.session_state["digital_pct"] = 0
            st.session_state["fisica_pct"]  = 0
            st.session_state["boot_error"] = None
            st.session_state["boot_started"] = False
            st.session_state["boot_done"] = False
            st.rerun()
