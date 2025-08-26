# -*- coding: utf-8 -*-
# Herramienta para la elaboración de bibliografías especializadas
# v7.3.2 – Endurecida contra “dict.columns”, carga oficial/manual robusta y estado estable

from __future__ import annotations
import io
import os
import time
import tempfile
from typing import Optional, Tuple, List

import requests
import pandas as pd
import streamlit as st

# =======================
# CONFIGURACIÓN DE PÁGINA
# =======================
st.set_page_config(page_title="Herramienta de bibliografías", layout="wide")

LOGO_URL_OSCURO = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png"

URL_DIGITAL = ("https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/"
               "Biblioteca%20Colecci%C3%B3n%20Digital.xlsx")
URL_FISICA = ("https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/"
              "Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx")

# =======================
# UTILIDADES
# =======================

def _safe_read_excel_bytes(xls_bytes: bytes, *, sheet_name=0) -> pd.DataFrame:
    """
    Lee un XLSX desde bytes de manera segura:
    - Fuerza sheet_name=0 (primera hoja).
    - Si por alguna razón pandas devuelve un dict (p.ej. sheet_name=None), toma la primera hoja.
    - Devuelve siempre un DataFrame (dtype=str). Lanza ValueError si no fue posible.
    """
    try:
        bio = io.BytesIO(xls_bytes)
        df_or_dict = pd.read_excel(bio, sheet_name=sheet_name, dtype=str, engine="openpyxl")
        if isinstance(df_or_dict, dict):
            # Tomar la primera hoja disponible
            if not df_or_dict:
                raise ValueError("El libro de Excel no tiene hojas.")
            first_df = next(iter(df_or_dict.values()))
            df = first_df
        else:
            df = df_or_dict
        if not hasattr(df, "columns"):
            raise ValueError("El archivo no parece una hoja de Excel válida (sin columnas).")
        # Normalizar columnas a string (evitar ints en nombres)
        df.columns = [str(c).strip() for c in df.columns]
        # Garantizar dtype str en celdas (evita comparaciones raras luego)
        df = df.astype(str)
        return df
    except Exception as e:
        raise ValueError(f"No fue posible leer el archivo Excel. Detalle: {e}") from e


def download_excel_from_url(url: str, label: str, progress_key: str) -> pd.DataFrame:
    """
    Descarga un XLSX desde URL y lo parsea seguro como DataFrame.
    Muestra progreso en UI.
    """
    st.write(f"Descargando **{label}**…")
    bar = st.progress(0, text=f"Descargando {label}…")
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", "0")) or None
            chunk = 1024 * 128
            downloaded = 0
            buf = io.BytesIO()
            last_update = time.time()
            for part in r.iter_content(chunk_size=chunk):
                if part:
                    buf.write(part)
                    downloaded += len(part)
                    if total:
                        # actualizar cada ~0.1s para no saturar
                        now = time.time()
                        if now - last_update > 0.1:
                            bar.progress(min(int(downloaded / total * 100), 100),
                                         text=f"{label}: {downloaded//1024} KB / {total//1024} KB")
                            last_update = now
            bar.progress(100, text=f"{label}: descarga completa. Verificando archivo…")
            xls_bytes = buf.getvalue()
    except Exception as e:
        bar.empty()
        raise ValueError(f"No fue posible descargar {label}. Detalle: {e}")

    # Parseo robusto del XLSX
    df = _safe_read_excel_bytes(xls_bytes, sheet_name=0)
    bar.empty()
    return df


def read_uploaded_excel(uploader, label: str) -> Optional[pd.DataFrame]:
    """
    Lee un excel subido por el usuario de manera segura (primera hoja).
    Devuelve DataFrame o None si no hay archivo.
    """
    if uploader is None:
        return None
    try:
        bytes_data = uploader.read()
        df = _safe_read_excel_bytes(bytes_data, sheet_name=0)
        return df
    except Exception as e:
        st.error(f"El archivo de **{label}** no es válido: {e}")
        return None


def ensure_session_keys():
    """
    Inicializa claves de sesión necesarias.
    """
    for k, v in {
        "df_digital": None,
        "df_fisica": None,
        "df_temat": None,
        "df_excluir": None,
        "loading_official": False,
        "digital_ready": False,
        "fisica_ready": False,
        "busqueda_listo": False,
    }.items():
        st.session_state.setdefault(k, v)


def columnas_sugeridas(df: pd.DataFrame) -> Tuple[str, str, str, str]:
    """
    Sugiere nombre de columnas por defecto según la convención esperada.
    """
    cols = [c.lower().strip() for c in df.columns]
    # Para búsqueda:
    col_titulo = next((c for c in df.columns if c.lower().strip() == "título" or c.lower().strip() == "titulo"), df.columns[0])
    col_tem = next((c for c in df.columns if "temát" in c.lower() or "temat" in c.lower()), df.columns[min(1, len(df.columns)-1)])
    # Duplicados:
    col_dup_dig = next((c for c in df.columns if c.lower().strip() == "url oa"), df.columns[0])
    col_dup_fis = next((c for c in df.columns if "topogr" in c.lower()), df.columns[0])
    return col_titulo, col_tem, col_dup_dig, col_dup_fis


def info_box():
    with st.expander("ℹ️ Información", expanded=True):
        st.markdown(
            """
- **Objetivo**: permitir la autogestión por programa/asignatura/tema y resaltar **términos a excluir** para depuración manual.  
- Usa siempre las **bases oficiales** (Digital/Física) o súbelas **manualmente** en la barra lateral.  
- **Plantillas**: [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx) y [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx).  
- Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app.  
- El proceso puede tardar algunos minutos; **puedes seguir usando tu equipo** (no cierres el navegador).
            """
        )


# =======================
# INTERFAZ
# =======================
ensure_session_keys()

# Barra lateral
with st.sidebar:
    st.image(LOGO_URL_OSCURO, use_column_width=True)
    st.markdown("**Biblioteca Juan Roa Vásquez**")

    st.markdown("### Plantillas oficiales:")
    st.markdown("- [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx)")
    st.markdown("- [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx)")

    st.markdown("### Archivos auxiliares (obligatorios)")
    up_temat = st.file_uploader("Temáticas (.xlsx, col1=término, col2=normalizado)", type=["xlsx"], key="temat_up")
    up_excl = st.file_uploader("Términos a excluir (.xlsx, 1ra columna)", type=["xlsx"], key="excl_up")

    if up_temat:
        st.session_state.df_temat = read_uploaded_excel(up_temat, "Temáticas")
    if up_excl:
        df_ex = read_uploaded_excel(up_excl, "Términos a excluir")
        if df_ex is not None:
            # Tomar SOLO la primera columna como lista de exclusión
            first_col = df_ex.columns[0]
            df_ex = df_ex[[first_col]].rename(columns={first_col: "excluir"})
            st.session_state.df_excluir = df_ex

    st.markdown("---")
    with st.expander("⚙️ Avanzado: subir bases Digital/Física manualmente"):
        st.caption("Usa esta opción solo si la descarga oficial falla.")
        up_dig = st.file_uploader("Base de datos de la colección **Digital** (.xlsx)", type=["xlsx"], key="dig_up")
        up_fis = st.file_uploader("Base de datos de la colección **Física** (.xlsx)", type=["xlsx"], key="fis_up")

        if up_dig:
            df = read_uploaded_excel(up_dig, "Colección Digital")
            if df is not None:
                st.session_state.df_digital = df
                st.session_state.digital_ready = True
                st.success("Colección Digital cargada manualmente.")
        if up_fis:
            df = read_uploaded_excel(up_fis, "Colección Física")
            if df is not None:
                st.session_state.df_fisica = df
                st.session_state.fisica_ready = True
                st.success("Colección Física cargada manualmente.")

# Encabezado y bloque de información
st.title("Herramienta para la elaboración de bibliografías especializadas")
info_box()

# Estado de carga oficial (descarga automática)
# Solo iniciar descarga si no hay nada cargado manualmente
auto_block = st.container()
with auto_block:
    if not st.session_state.digital_ready or not st.session_state.fisica_ready:
        # Mostrar un bloque informativo mientras intenta descargar
        st.info("Cargando las bases **Digital** y **Física** desde la **web oficial**… "
                "Puedes subir **Temáticas** y **Términos a excluir** mientras tanto. "
                "No cierres esta ventana.")

    # Si aún no está listo y no hay descarga en curso, dispara descarga
    if not st.session_state.loading_official and (not st.session_state.digital_ready or not st.session_state.fisica_ready):
        st.session_state.loading_official = True
        try:
            col_a, col_b = st.columns(2, gap="large")

            with col_a:
                if not st.session_state.digital_ready:
                    try:
                        st.subheader("Descargando Base de datos de la colección **Digital**…")
                        df_dig = download_excel_from_url(URL_DIGITAL, "Colección Digital", "dig_prog")
                        st.session_state.df_digital = df_dig
                        st.session_state.digital_ready = True
                        st.success("Base de datos de la **colección Digital** lista ✓")
                    except Exception as e:
                        st.error(f"No fue posible descargar la **colección Digital**: {e}")

            with col_b:
                if not st.session_state.fisica_ready:
                    try:
                        st.subheader("Descargando Base de datos de la colección **Física**…")
                        df_fis = download_excel_from_url(URL_FISICA, "Colección Física", "fis_prog")
                        st.session_state.df_fisica = df_fis
                        st.session_state.fisica_ready = True
                        st.success("Base de datos de la **colección Física** lista ✓")
                    except Exception as e:
                        st.error(f"No fue posible descargar la **colección Física**: {e}")

        finally:
            st.session_state.loading_official = False

# Caja de estado global
ready_msg = st.empty()
if st.session_state.digital_ready and st.session_state.fisica_ready:
    ready_msg.success("✅ **Bases oficiales listas en memoria (sesión).**")
else:
    ready_msg.warning("Aún faltan bases por cargar (usa la descarga oficial o la carga manual en la barra lateral).")

st.markdown("---")

# ===========================
# CONFIGURACIÓN DE BÚSQUEDA
# ===========================
st.subheader("Configuración de búsqueda y duplicados")

# Si tenemos al menos una base, sugerir columnas (si no, usar placeholders)
col_titulo_def = "Título"
col_tem_def = "Temáticas"
col_dup_dig_def = "Url OA"
col_dup_fis_def = "No. Topográfico"

if st.session_state.df_digital is not None:
    try:
        s_tit, s_tem, s_dup_d, _ = columnas_sugeridas(st.session_state.df_digital)
        col_titulo_def, col_tem_def, col_dup_dig_def = s_tit, s_tem, s_dup_d
    except Exception:
        pass

if st.session_state.df_fisica is not None:
    try:
        _, _, _, s_dup_f = columnas_sugeridas(st.session_state.df_fisica)
        col_dup_fis_def = s_dup_f
    except Exception:
        pass

c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.5, 1.5])

with c1:
    col_busq_1 = st.selectbox(
        "Búsqueda principal por:",
        options=(list(st.session_state.df_digital.columns) if st.session_state.df_digital is not None else [col_titulo_def]),
        index=0 if (st.session_state.df_digital is None) else max(0, list(st.session_state.df_digital.columns).index(col_titulo_def)) if col_titulo_def in (st.session_state.df_digital.columns if st.session_state.df_digital is not None else []) else 0
    )
with c2:
    col_busq_2 = st.selectbox(
        "Búsqueda complementaria por:",
        options=(list(st.session_state.df_digital.columns) if st.session_state.df_digital is not None else [col_tem_def]),
        index=0 if (st.session_state.df_digital is None) else max(0, list(st.session_state.df_digital.columns).index(col_tem_def)) if col_tem_def in (st.session_state.df_digital.columns if st.session_state.df_digital is not None else []) else 0
    )
with c3:
    col_dup_dig = st.selectbox(
        "Columna de duplicados en **Colección Digital**",
        options=(list(st.session_state.df_digital.columns) if st.session_state.df_digital is not None else [col_dup_dig_def]),
        index=0 if (st.session_state.df_digital is None) else max(0, list(st.session_state.df_digital.columns).index(col_dup_dig_def)) if col_dup_dig_def in (st.session_state.df_digital.columns if st.session_state.df_digital is not None else []) else 0
    )
with c4:
    col_dup_fis = st.selectbox(
        "Columna de duplicados en **Colección Física**",
        options=(list(st.session_state.df_fisica.columns) if st.session_state.df_fisica is not None else [col_dup_fis_def]),
        index=0 if (st.session_state.df_fisica is None) else max(0, list(st.session_state.df_fisica.columns).index(col_dup_fis_def)) if col_dup_fis_def in (st.session_state.df_fisica.columns if st.session_state.df_fisica is not None else []) else 0
    )

# ===========================
# BOTÓN DE BÚSQUEDA
# ===========================
st.markdown("")
btn = st.button("🚀 Iniciar búsqueda", type="primary", use_container_width=True)

# Requisitos previos
def _reqs_ok() -> bool:
    if not (st.session_state.digital_ready and st.session_state.fisica_ready):
        st.error("Debes tener **ambas** bases (Digital y Física) cargadas (oficial o manual).")
        return False
    if st.session_state.df_temat is None:
        st.error("Debes cargar **Temáticas** en la barra lateral.")
        return False
    if st.session_state.df_excluir is None:
        st.error("Debes cargar **Términos a excluir** en la barra lateral.")
        return False
    return True

result_placeholder = st.empty()

if btn:
    if _reqs_ok():
        # =============== AQUÍ ENGANCHAS TU PIPELINE REAL ==================
        # He dejado un “mock” mínimo para no romper el flujo mientras enganchas
        # el motor original. Este mock une Digital y Física y añade metadatos.
        with st.spinner("Normalizando y buscando coincidencias…"):
            time.sleep(0.8)

            df_d = st.session_state.df_digital.copy()
            df_f = st.session_state.df_fisica.copy()

            # columna fuente:
            df_d["Fuente"] = "Digital"
            df_f["Fuente"] = "Física"

            # asegurar columnas que usaremos existan (no rompe si no)
            for df in (df_d, df_f):
                for must in [col_busq_1, col_busq_2]:
                    if must not in df.columns:
                        df[must] = ""

            # “Mock” de filtro simple por exclusiones (no altera tu lógica real)
            excluir = set((st.session_state.df_excluir or pd.DataFrame(columns=["excluir"]))["excluir"].dropna().astype(str).str.strip().str.lower())
            def _not_excluded(s: pd.Series) -> pd.Series:
                join = (s[col_busq_1].fillna("") + " " + s[col_busq_2].fillna("")).str.lower()
                return ~join.apply(lambda x: any(e in x for e in excluir))

            df_d2 = df_d[_not_excluded(df_d)]
            df_f2 = df_f[_not_excluded(df_f)]

            df_out = pd.concat([df_d2, df_f2], ignore_index=True)
            # ===== FIN DEL MOCK =====
            # TODO: reubica aquí tu pipeline “bueno” conservando st.session_state y evitando recargas innecesarias.

        st.session_state.busqueda_listo = True

        with result_placeholder.container():
            st.success("✅ Búsqueda finalizada.")
            st.caption(f"Filas resultantes: {len(df_out):,}")

            st.dataframe(df_out.head(200), use_container_width=True, hide_index=True)

            # Exportar a CSV/Excel sin estilos (simple y robusto)
            cexp1, cexp2 = st.columns(2)
            with cexp1:
                csv = df_out.to_csv(index=False).encode("utf-8-sig")
                st.download_button("⬇️ Descargar CSV", data=csv, file_name="resultados.csv", mime="text/csv", use_container_width=True)
            with cexp2:
                # Excel con xlsxwriter simple
                bio = io.BytesIO()
                with pd.ExcelWriter(bio, engine="xlsxwriter") as xw:
                    df_out.to_excel(xw, sheet_name="Resultados", index=False)
                st.download_button("⬇️ Descargar Excel", data=bio.getvalue(), file_name="resultados.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)

# Pie: recordatorio de sesión
st.markdown("---")
st.caption("Las bases cargadas viven **solo durante esta sesión**. "
           "Para una nueva búsqueda, puedes **reemplazar Temáticas / Términos** sin volver a descargar las bases; "
           "si recargas el navegador, deberás cargar/descargar de nuevo.")
