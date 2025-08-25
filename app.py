# -*- coding: utf-8 -*-
# v7.3.3 – fix: hoja por defecto (sheet=0), UI chips, renombres y robustez de columnas
import io
import time
import requests
import pandas as pd
import streamlit as st
from unidecode import unidecode

# ================== Ajustes generales ==================
st.set_page_config(
    page_title="Herramienta para la elaboración de bibliografías",
    page_icon="📚",
    layout="wide",
)

# URLs oficiales
URL_DIGITAL = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx"
URL_FISICA  = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx"

# ================== Estado ==================
def _init_state():
    ss = st.session_state
    ss.setdefault("digital_ok", False)
    ss.setdefault("fisica_ok", False)
    ss.setdefault("digital_loading", False)
    ss.setdefault("fisica_loading", False)
    ss.setdefault("df_digital", None)
    ss.setdefault("df_fisica", None)
    ss.setdefault("tematicas_bytes", None)
    ss.setdefault("excluir_bytes", None)
    ss.setdefault("auto_kickoff", False)
_init_state()

# ================== Helpers ==================
def info_box(title: str, body: str):
    st.markdown(
        f"""
        <div style="
          background:#0d2840;
          border:1px solid #164a72;
          color:#cfe8ff;
          padding:14px 16px;
          border-radius:8px;">
          <b>{title}</b><br>{body}
        </div>
        """,
        unsafe_allow_html=True,
    )

@st.cache_data(ttl=60 * 60, show_spinner=False)
def read_excel_from_bytes(b: bytes, sheet=0) -> pd.DataFrame:
    """Lee SIEMPRE la primera hoja (sheet=0) → devuelve DataFrame."""
    return pd.read_excel(io.BytesIO(b), sheet_name=sheet)

@st.cache_data(ttl=60 * 60, show_spinner=False)
def read_excel_from_url_bytes(b: bytes, sheet=0) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(b), sheet_name=sheet)

def stream_download(url: str, status_area: st.delta_generator.DeltaGenerator, bar: st.progress) -> bytes:
    with requests.get(url, stream=True, timeout=30) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0)) or None
        chunk = 1024 * 512
        got = 0
        buf = io.BytesIO()
        for part in r.iter_content(chunk_size=chunk):
            buf.write(part)
            got += len(part)
            if total:
                bar.progress(min(1.0, got / total))
        status_area.write("Descarga completa. Verificando archivo…")
        return buf.getvalue()

# ================== Sidebar ==================
with st.sidebar:
    st.image(
        "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png",
        use_container_width=True,
    )

    st.markdown("### Plantillas oficiales:")
    st.markdown("- [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx)")
    st.markdown("- [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx)")

    st.markdown("### Archivos auxiliares (obligatorios)")
    bases_listas = st.session_state.digital_ok and st.session_state.fisica_ok

    tem_file = st.file_uploader(
        "Temáticas (.xlsx, col1=término, col2=normalizado)",
        type=["xlsx"],
        disabled=not bases_listas,
        key="tematicas_upl",
    )
    if tem_file is not None:
        st.session_state.tematicas_bytes = tem_file.read()
    st.caption("✅ Temáticas cargadas" if st.session_state.tematicas_bytes else "📝 Pendiente")

    ex_file = st.file_uploader(
        "Términos a excluir (.xlsx, 1ra columna)",
        type=["xlsx"],
        disabled=not bases_listas,
        key="excluir_upl",
    )
    if ex_file is not None:
        st.session_state.excluir_bytes = ex_file.read()
    st.caption("✅ Términos a excluir cargados" if st.session_state.excluir_bytes else "📝 Pendiente")

    st.divider()
    with st.expander("⚙️ Avanzado: subir **bases** Digital/Física manualmente"):
        st.caption(
            "Por defecto se usan las **bases oficiales** descargadas automáticamente. "
            "Si subes manualmente, reemplaza **sólo en esta sesión**."
        )
        up_dig = st.file_uploader("Reemplazar **Base de datos de la colección Digital** (.xlsx)", type=["xlsx"], key="dig_manual")
        up_fis = st.file_uploader("Reemplazar **Base de datos de la colección Física** (.xlsx)", type=["xlsx"], key="fis_manual")

# ================== Encabezado ==================
st.markdown("# Herramienta para la elaboración de bibliografías")
info = (
    "Objetivo: autogestión por programa/asignatura/tema y resaltado de **términos a excluir** para depuración manual. "
    "Usa siempre las bases oficiales (Digital/Física) o súbelas **manualmente** en la barra lateral. "
    "Plantillas: [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx) "
    "y [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx). "
    "Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app. "
    "El proceso puede tardar algunos minutos; puedes seguir usando tu equipo."
)
info_box("ℹ️ Información", info)

# ================== Panel de estado ==================
panel = st.container(border=True)
with panel:
    st.subheader("Bases oficiales cargadas en memoria (sesión)")
    place_status = st.empty()
    pb_d = st.progress(0, text="Base de datos de la colección Digital")
    pb_f = st.progress(0, text="Base de datos de la colección Física")

# ================== Lectura de bases (oficial / manual) ==================
def load_official_digital():
    st.session_state.digital_loading = True
    with st.status("Descargando **Base de datos de la colección Digital**…", expanded=True) as s:
        txt = st.empty()
        bar = st.progress(0)
        try:
            b = stream_download(URL_DIGITAL, txt, bar)
            df = read_excel_from_url_bytes(b, sheet=0)  # <-- DataFrame
            st.session_state.df_digital = df
            st.session_state.digital_ok = True
            pb_d.progress(1.0, text="Base de datos de la colección Digital (100%)")
            s.update(label="Base de datos de la colección Digital lista ✔️", state="complete")
        except Exception as e:
            s.update(label=f"Error en Digital: {e}", state="error")
            st.session_state.digital_ok = False
    st.session_state.digital_loading = False

def load_official_fisica():
    st.session_state.fisica_loading = True
    with st.status("Descargando **Base de datos de la colección Física**…", expanded=True) as s:
        txt = st.empty()
        bar = st.progress(0)
        try:
            b = stream_download(URL_FISICA, txt, bar)
            df = read_excel_from_url_bytes(b, sheet=0)  # <-- DataFrame
            st.session_state.df_fisica = df
            st.session_state.fisica_ok = True
            pb_f.progress(1.0, text="Base de datos de la colección Física (100%)")
            s.update(label="Base de datos de la colección Física lista ✔️", state="complete")
        except Exception as e:
            s.update(label=f"Error en Física: {e}", state="error")
            st.session_state.fisica_ok = False
    st.session_state.fisica_loading = False

# reemplazos manuales (si los hay)
if st.session_state.get("dig_manual") is not None:
    try:
        df_man = read_excel_from_bytes(st.session_state.dig_manual.read(), sheet=0)
        st.session_state.df_digital = df_man
        st.session_state.digital_ok = True
        pb_d.progress(1.0, text="Base de datos de la colección Digital (manual)")
    except Exception as e:
        st.warning(f"No se pudo leer la base Digital manual: {e}")

if st.session_state.get("fis_manual") is not None:
    try:
        df_man = read_excel_from_bytes(st.session_state.fis_manual.read(), sheet=0)
        st.session_state.df_fisica = df_man
        st.session_state.fisica_ok = True
        pb_f.progress(1.0, text="Base de datos de la colección Física (manual)")
    except Exception as e:
        st.warning(f"No se pudo leer la base Física manual: {e}")

# arranque automático (solo una vez)
if not st.session_state.auto_kickoff:
    st.session_state.auto_kickoff = True
    if not st.session_state.digital_ok:
        load_official_digital()
    if not st.session_state.fisica_ok:
        load_official_fisica()

# estado final del panel
if st.session_state.digital_ok and st.session_state.fisica_ok:
    place_status.success("Bases oficiales listas en memoria.", icon="✅")
else:
    if st.session_state.digital_loading or st.session_state.fisica_loading:
        place_status.info("Cargando bases desde la web oficial…", icon="⏳")
    else:
        place_status.warning("Alguna base no se cargó correctamente.", icon="⚠️")

# deshabilitar uploaders hasta que ambas bases estén listas
if not (st.session_state.digital_ok and st.session_state.fisica_ok):
    st.info("Los cargadores de **Temáticas** y **Términos a excluir** se habilitan cuando las bases estén listas.", icon="ℹ️")

# ================== Sección de búsqueda (integra tu lógica) ==================
st.divider()
st.subheader("Configuración de búsqueda y duplicados")

bases_listas = st.session_state.digital_ok and st.session_state.fisica_ok
if bases_listas:
    df_dig = st.session_state.df_digital
    df_fis = st.session_state.df_fisica

    # defensivo: si por alguna razón no son DataFrames, no pintar selectores
    if isinstance(df_dig, pd.DataFrame) and isinstance(df_fis, pd.DataFrame):
        cols1 = st.columns(4)
        with cols1[0]:
            col_principal = st.selectbox("Búsqueda principal por:", ["Título", "Autores", "Temáticas"], index=0)
        with cols1[1]:
            col_comp = st.selectbox("Búsqueda complementaria por:", ["Temáticas", "Título", "Autores"], index=0)
        with cols1[2]:
            try:
                col_dup_dig = st.selectbox(
                    "Columna de duplicados en **Colección Digital**",
                    list(df_dig.columns),
                    index=0
                )
            except Exception:
                col_dup_dig = st.selectbox("Columna de duplicados en **Colección Digital**", ["(sin columnas)"])
        with cols1[3]:
            try:
                col_dup_fis = st.selectbox(
                    "Columna de duplicados en **Colección Física**",
                    list(df_fis.columns),
                    index=0
                )
            except Exception:
                col_dup_fis = st.selectbox("Columna de duplicados en **Colección Física**", ["(sin columnas)"])

        st.caption("Consejo: por defecto la búsqueda se realiza en **Título** y **Temáticas**. Puedes elegir otras columnas si lo necesitas.")

        # Botón de inicio (se habilita solo si hay temáticas y excluidos)
        listo_para_buscar = st.session_state.tematicas_bytes and st.session_state.excluir_bytes
        if st.button("🚀 Iniciar búsqueda", type="primary", use_container_width=True, disabled=not listo_para_buscar):
            with st.status("Normalizando temáticas y procesando…", expanded=True) as s:
                try:
                    df_tem = pd.read_excel(io.BytesIO(st.session_state.tematicas_bytes), sheet_name=0)
                    df_exc = pd.read_excel(io.BytesIO(st.session_state.excluir_bytes), sheet_name=0)
                    s.write(f"Temáticas cargadas: {len(df_tem)} | Términos a excluir: {len(df_exc)}")
                    time.sleep(0.3)
                    # >>>>>> aquí va tu pipeline de búsqueda original <<<<<<
                    # resultados = tu_funcion_busqueda(...)
                    # st.dataframe(resultados, use_container_width=True)
                    s.update(label="Búsqueda finalizada ✔️", state="complete")
                    st.success("Búsqueda finalizada. (Integra aquí tu render de resultados / exportaciones)", icon="✅")
                except Exception as e:
                    s.update(label=f"Error en normalización o búsqueda: {e}", state="error")
    else:
        st.warning("Las bases no se cargaron correctamente (no son DataFrame). Intenta recargar la página.", icon="⚠️")
else:
    st.info("Esperando que las bases **Digital/Física** estén listas…", icon="⏳")
