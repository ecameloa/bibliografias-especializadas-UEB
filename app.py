# -*- coding: utf-8 -*-
# v7.3.2 – carga con progreso, estado “procesando”, y uploads protegidos
import io
import time
import zipfile
import requests
import pandas as pd
import numpy as np
import streamlit as st
from unidecode import unidecode

# ========= Ajustes generales =========
st.set_page_config(
    page_title="Herramienta para la elaboración de bibliografías",
    page_icon="📚",
    layout="wide",
)

# URLs oficiales (ajústalas si cambian)
URL_DIGITAL = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx"
URL_FISICA  = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx"

# ========= Estado inicial =========
def _init_state():
    ss = st.session_state
    ss.setdefault("digital_ok", False)
    ss.setdefault("fisica_ok", False)
    ss.setdefault("digital_loading", False)
    ss.setdefault("fisica_loading", False)
    ss.setdefault("df_digital", None)
    ss.setdefault("df_fisica", None)

    # uploads protegidos en memoria (bytes), para no perderlos en reruns
    ss.setdefault("tematicas_bytes", None)
    ss.setdefault("excluir_bytes", None)

    # si quieres que la descarga automática sólo se dispare 1 vez
    ss.setdefault("auto_kickoff", False)

_init_state()

# ========= Utilidades =========
@st.cache_data(ttl=60 * 60, max_entries=2, show_spinner=False)
def _read_excel_from_bytes(b: bytes, sheet: str | None = None) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(b), sheet_name=sheet)

@st.cache_data(ttl=60 * 60, max_entries=2, show_spinner=False)
def _read_excel_from_url(all_bytes: bytes, sheet: str | None = None) -> pd.DataFrame:
    # función “tonta” que sólo usa cache_data para bytes ya descargados
    return pd.read_excel(io.BytesIO(all_bytes), sheet_name=sheet)

def _stream_download(url: str, status_label: st.delta_generator.DeltaGenerator, bar: st.progress) -> bytes:
    """
    Descarga stream con progreso. Devuelve los bytes del archivo.
    """
    with requests.get(url, stream=True, timeout=30) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0)) or None
        chunk = 1024 * 512  # 512 KB
        got = 0
        buf = io.BytesIO()
        for part in r.iter_content(chunk_size=chunk):
            buf.write(part)
            got += len(part)
            if total:
                bar.progress(min(1.0, got / total))
        status_label.write("Descarga completa. Verificando archivo…")
        return buf.getvalue()

def _as_text_info_box(title: str, body: str):
    st.markdown(
        f"""
        <div style="
            background:#0d2840;
            border:1px solid #164a72;
            color:#cfe8ff;
            padding:14px 16px;
            border-radius:8px;
            ">
            <b>{title}</b><br>{body}
        </div>
        """,
        unsafe_allow_html=True,
    )

# ========= Sidebar =========
with st.sidebar:
    st.image(
        "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png",
        use_container_width=True,
    )

    st.markdown("### Plantillas oficiales:")
    st.markdown("- [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx)")
    st.markdown("- [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx)")

    st.markdown("### Archivos auxiliares (obligatorios)")
    tem_col, btn_col = st.columns([0.8, 0.2])
    with tem_col:
        tem_file = st.file_uploader(
            "Temáticas (.xlsx, col1=término, col2=normalizado)",
            type=["xlsx"],
            disabled=not (st.session_state.digital_ok and st.session_state.fisica_ok),
            key="tematicas_upl",
        )
    with btn_col:
        st.write("")  # spacing
        if st.session_state.tematicas_bytes:
            st.success("Temáticas OK", icon="✅")
        else:
            st.info("Pendiente", icon="📝")

    ex_col, btn2_col = st.columns([0.8, 0.2])
    with ex_col:
        ex_file = st.file_uploader(
            "Términos a excluir (.xlsx, 1ra columna)",
            type=["xlsx"],
            disabled=not (st.session_state.digital_ok and st.session_state.fisica_ok),
            key="excluir_upl",
        )
    with btn2_col:
        st.write("")
        if st.session_state.excluir_bytes:
            st.success("Excluidos OK", icon="✅")
        else:
            st.info("Pendiente", icon="📝")

    st.divider()
    with st.expander("⚙️ Avanzado: subir bases Digital/Física manualmente"):
        st.caption(
            "Por defecto se usan las **bases oficiales** descargadas automáticamente. "
            "Subir manualmente una base reemplaza **sólo en esta sesión**."
        )
        man_dig = st.file_uploader("Reemplazar base **Digital** (.xlsx)", type=["xlsx"], key="dig_manual")
        man_fis = st.file_uploader("Reemplazar base **Física** (.xlsx)", type=["xlsx"], key="fis_manual")

# Guardar/retener uploads en memoria para que no se pierdan en los reruns
if tem_file is not None:
    st.session_state.tematicas_bytes = tem_file.read()
if ex_file is not None:
    st.session_state.excluir_bytes = ex_file.read()

# ========= Encabezado principal =========
st.markdown(
    """
    # Herramienta para la elaboración de bibliografías
    """
)

_info = (
    "Objetivo: autogestión por programa/asignatura/tema y resaltado de **términos a excluir** para depuración manual. "
    "Usa siempre las bases oficiales (Digital/Física) o súbelas **manualmente** en la barra lateral. "
    "Plantillas: [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx) "
    "y [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx). "
    "Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app. "
    "El proceso puede tardar algunos minutos; puedes seguir usando tu equipo."
)
_as_text_info_box("ℹ️ Información", _info)

# ========= Descarga/lectura de Digital y Física =========
# Estado visual permanente + spinner de “procesando”
card = st.container(border=True)
with card:
    st.subheader("Bases oficiales cargadas en memoria (sesión)")
    place_status = st.empty()      # muestra línea “Cargando…”
    place_prog_d = st.progress(0)  # progreso Digital
    place_prog_f = st.progress(0)  # progreso Física

# Mensaje “gif-like” durante la carga
if not (st.session_state.digital_ok and st.session_state.fisica_ok):
    place_status.info(
        "Cargando las bases Digital y Física desde la web oficial… (no cierres esta ventana).",
        icon="⏳",
    )
else:
    place_status.success("Bases oficiales listas en memoria.", icon="✅")

def _load_digital_official():
    st.session_state.digital_loading = True
    with st.status("Descargando **Base Digital**…", expanded=True) as s:
        txt = st.empty()
        bar = st.progress(0)
        try:
            b = _stream_download(URL_DIGITAL, txt, bar)
            df = _read_excel_from_url(b)
            st.session_state.df_digital = df
            st.session_state.digital_ok = True
            place_prog_d.progress(1.0)
            s.update(label="Base Digital lista ✔️", state="complete")
        except Exception as e:
            s.update(label=f"Error en Digital: {e}", state="error")
            st.session_state.digital_ok = False
    st.session_state.digital_loading = False

def _load_fisica_official():
    st.session_state.fisica_loading = True
    with st.status("Descargando **Base Física**…", expanded=True) as s:
        txt = st.empty()
        bar = st.progress(0)
        try:
            b = _stream_download(URL_FISICA, txt, bar)
            df = _read_excel_from_url(b)
            st.session_state.df_fisica = df
            st.session_state.fisica_ok = True
            place_prog_f.progress(1.0)
            s.update(label="Base Física lista ✔️", state="complete")
        except Exception as e:
            s.update(label=f"Error en Física: {e}", state="error")
            st.session_state.fisica_ok = False
    st.session_state.fisica_loading = False

# Si suben manualmente, prioriza esas bases
if st.session_state.get("dig_manual") is not None:
    try:
        df_dig = _read_excel_from_bytes(st.session_state.dig_manual.read())
        st.session_state.df_digital = df_dig
        st.session_state.digital_ok = True
        place_prog_d.progress(1.0)
    except Exception as e:
        st.warning(f"No se pudo leer la base Digital manual: {e}")

if st.session_state.get("fis_manual") is not None:
    try:
        df_fis = _read_excel_from_bytes(st.session_state.fis_manual.read())
        st.session_state.df_fisica = df_fis
        st.session_state.fisica_ok = True
        place_prog_f.progress(1.0)
    except Exception as e:
        st.warning(f"No se pudo leer la base Física manual: {e}")

# Auto-carga oficial (una sola vez)
if not st.session_state.auto_kickoff:
    st.session_state.auto_kickoff = True
    if not st.session_state.digital_ok:
        _load_digital_official()
    if not st.session_state.fisica_ok:
        _load_fisica_official()

# Estado final
if st.session_state.digital_ok and st.session_state.fisica_ok:
    place_status.success("Bases oficiales listas en memoria.", icon="✅")
else:
    place_status.warning("Alguna base no se cargó correctamente.", icon="⚠️")

# ========= Habilitar uploads una vez que las bases están listas =========
if not (st.session_state.digital_ok and st.session_state.fisica_ok):
    st.info(
        "Los cargadores de **Temáticas** y **Términos a excluir** se habilitan cuando las bases estén listas.",
        icon="ℹ️",
    )

# ========= A PARTIR DE AQUÍ: tu lógica original de búsqueda (sin cambios) =========
# Usa st.session_state.df_digital / df_fisica como fuentes
# Usa st.session_state.tematicas_bytes / excluir_bytes para construir DataFrames
# (no se pierden con los reruns).
# ---------------------------------------------------------------------
# Ejemplo mínimo para no romper el flujo (reemplaza por tu lógica):
st.divider()
st.subheader("Configuración de búsqueda y duplicados")

if st.session_state.digital_ok and st.session_state.fisica_ok:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        col_principal = st.selectbox("Búsqueda principal por:", ["Título", "Autores", "Temáticas"], index=0)
    with col2:
        col_comp = st.selectbox("Búsqueda complementaria por:", ["Temáticas", "Título", "Autores"], index=0)
    with col3:
        col_dup_dig = st.selectbox("Duplicados en Digital:", list(st.session_state.df_digital.columns), index=0)
    with col4:
        col_dup_fis = st.selectbox("Duplicados en Física:", list(st.session_state.df_fisica.columns), index=0)

    st.caption("Consejo: por defecto la búsqueda se realiza en **Título** y **Temáticas**. Puedes elegir otras columnas si lo necesitas.")

    # Botón de inicio (mantén el tuyo)
    if st.button("🚀 Iniciar búsqueda", type="primary", use_container_width=True, disabled=not (st.session_state.tematicas_bytes and st.session_state.excluir_bytes)):
        with st.status("Normalizando temáticas y procesando…", expanded=True) as s:
            time.sleep(0.5)  # visual
            try:
                df_tem = pd.read_excel(io.BytesIO(st.session_state.tematicas_bytes))
                df_exc = pd.read_excel(io.BytesIO(st.session_state.excluir_bytes))
                s.write(f"Temáticas cargadas: {len(df_tem)} | Excluir: {len(df_exc)}")
                time.sleep(0.3)
                # --- Aquí va tu pipeline de búsqueda existente ---
                # resultados = tu_funcion_busqueda(...)
                # st.dataframe(resultados, use_container_width=True)
                s.update(label="Búsqueda finalizada ✔️", state="complete")
                st.success("Búsqueda finalizada. (Integra aquí tu render de resultados / exportaciones)", icon="✅")
            except Exception as e:
                s.update(label=f"Error en normalización o búsqueda: {e}", state="error")
else:
    st.info("Esperando que las bases **Digital/Física** estén listas…", icon="⏳")
