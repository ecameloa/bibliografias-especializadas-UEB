# -*- coding: utf-8 -*-
# Herramienta para la elaboración de bibliografías especializadas
# v8.0 – descarga única, nueva búsqueda sin re-descargar, progreso visible, filtros/selección y citas opcionales

import io
import os
import time
import tempfile
from typing import List, Optional

import pandas as pd
import requests
import streamlit as st

# ------------------------------ CONFIGURACIÓN BÁSICA ------------------------------
st.set_page_config(page_title="Herramienta de bibliografías", layout="wide")

LOGO_URL_OSCURO = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png"
LOGO_URL_CLARO  = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20ORG.png"

URL_DIGITAL = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx"
URL_FISICA  = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx"

URL_PLANTILLA_TEMATICAS = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx"
URL_PLANTILLA_EXCLUSION = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx"

DEFAULT_COL_TITULO    = "Título"
DEFAULT_COL_TEMATICAS = "Temáticas"
DEFAULT_DUP_DIGITAL   = "Url OA"
DEFAULT_DUP_FISICA    = "No. Topográfico"

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"}

# ------------------------------ ESTADO ------------------------------
ss = st.session_state

# Bases (se conservan toda la sesión; "Nueva búsqueda" no las borra)
ss.setdefault("df_digital", None)
ss.setdefault("df_fisica", None)
ss.setdefault("bases_ready", False)

# Descarga/validación en curso
ss.setdefault("downloading", False)
ss.setdefault("progress_msg", "")

# Archivos auxiliares de cada búsqueda
ss.setdefault("tematicas_df", None)
ss.setdefault("excluir_df", None)

# Config/búsqueda y resultados
ss.setdefault("results_df", None)
ss.setdefault("bitacora_df", None)

# Para ver si el usuario ya pulsó el botón de iniciar descarga
ss.setdefault("descarga_disparada", False)

# ------------------------------ UTILIDADES ------------------------------
def normalize_text(s):
    if pd.isna(s):
        return ""
    s = str(s)
    # normalización básica sin tildes combinadas
    return (s.replace("\u0301", "")
             .replace("\u0303", "")
             .replace("\u2019", "'")
             .replace("\xa0", " ")
             .strip())

def _head_content_length(url, timeout=30):
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=UA)
        r.raise_for_status()
        cl = r.headers.get("Content-Length")
        return int(cl) if cl is not None else None
    except Exception:
        return None

def download_with_resume(url, label, container=None, max_retries=6, chunk_size=256*1024, timeout=300) -> io.BytesIO:
    """
    Descarga con reintentos, barra y tamaño total si está disponible.
    Devuelve BytesIO con el archivo.
    """
    where = container if container is not None else st
    status = where.empty()
    bar    = where.progress(0)
    info   = where.empty()

    tmp_dir = tempfile.gettempdir()
    tmp_path = os.path.join(tmp_dir, f"dl_{abs(hash(url))}.part")

    total_size = _head_content_length(url)
    attempt = 0

    while attempt < max_retries:
        attempt += 1
        try:
            downloaded = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
            headers = dict(UA)
            mode = "wb"

            if downloaded and total_size and downloaded < total_size:
                headers["Range"] = f"bytes={downloaded}-"
                mode = "ab"

            status.info(f"Descargando {label}… (intento {attempt}/{max_retries})")

            with requests.get(url, stream=True, headers=headers, timeout=timeout, allow_redirects=True) as r:
                # si pedimos rango y el servidor no lo soporta, forzamos descarga completa
                if headers.get("Range") and r.status_code == 200:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    downloaded = 0
                    mode = "wb"

                r.raise_for_status()

                content_length = r.headers.get("Content-Length")
                expected_total = downloaded + int(content_length) if content_length else total_size

                last = time.time()
                with open(tmp_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        if expected_total:
                            if time.time() - last > 0.1:
                                bar.progress(min(1.0, downloaded / expected_total))
                                info.write(f"{downloaded/1e6:,.1f} MB / {expected_total/1e6:,.1f} MB")
                                last = time.time()

            if total_size and downloaded < total_size:
                raise requests.exceptions.ChunkedEncodingError(
                    f"Descarga incompleta: {downloaded} de {total_size} bytes"
                )

            bar.progress(1.0)
            status.success(f"{label} descargado correctamente.")
            info.empty(); bar.empty(); status.empty()

            with open(tmp_path, "rb") as f:
                data = f.read()
            return io.BytesIO(data)

        except Exception as e:
            info.empty(); bar.empty()
            status.warning(f"Fallo al descargar {label}: {e}")
            if attempt < max_retries:
                time.sleep(2)
            else:
                status.error(f"No se pudo descargar {label} tras {max_retries} intentos.")
                raise
        finally:
            info.empty(); bar.empty(); status.empty()

def safe_read_excel(bio_or_file, label="archivo") -> pd.DataFrame:
    """
    Lee Excel a DataFrame (openpyxl) forzando dtype=str, rellena NaN con "".
    Protege ante objetos raros y errores comunes.
    """
    try:
        with st.spinner(f"Procesando {label}…"):
            df = pd.read_excel(bio_or_file, engine="openpyxl", dtype=str)
            if not isinstance(df, pd.DataFrame):
                raise ValueError("El archivo no es una hoja de cálculo válida.")
            df = df.fillna("")
            return df
    except Exception as e:
        raise RuntimeError(f"No fue posible procesar {label}: {e}") from e

def get_index_or_first(options: List[str], value: str) -> int:
    try:
        return options.index(value)
    except Exception:
        return 0

# ------------------------------ SIDEBAR: LOGO + PLANTILLAS + ARCHIVOS AUX ------------------------------
with st.sidebar:
    st.image(LOGO_URL_OSCURO, use_container_width=True)
    st.caption("Biblioteca Juan Roa Vásquez")

    st.markdown("### Plantillas oficiales:")
    st.markdown(f"- [Temáticas]({URL_PLANTILLA_TEMATICAS})")
    st.markdown(f"- [Términos a excluir]({URL_PLANTILLA_EXCLUSION})")

    st.markdown("### Archivos auxiliares (obligatorios)")
    # Durante la descarga/validación de bases, bloqueamos los uploaders para que el usuario no “pierda” sus archivos en los reruns
    bloqueados = ss.downloading or (not ss.bases_ready and ss.descarga_disparada)

    tem_up = st.file_uploader(
        "Temáticas (.xlsx, col1=término, col2=normalizado)",
        type=["xlsx"], key="tem_up_v8", disabled=bloqueados
    )
    exc_up = st.file_uploader(
        "Términos a excluir (.xlsx, col1)",
        type=["xlsx"], key="exc_up_v8", disabled=bloqueados
    )

    if not bloqueados:
        if tem_up is not None:
            df = safe_read_excel(tem_up, "Temáticas")
            ss.tematicas_df = df[[df.columns[0], df.columns[1]]].rename(
                columns={df.columns[0]: "termino", df.columns[1]: "normalizado"}
            ).fillna("")
            st.success(f"Temáticas cargadas: {len(ss.tematicas_df)}")
        if exc_up is not None:
            df = safe_read_excel(exc_up, "Términos a excluir")
            ss.excluir_df = df[[df.columns[0]]].rename(columns={df.columns[0]: "excluir"}).fillna("")
            st.success(f"Términos a excluir cargados: {len(ss.excluir_df)}")

    st.markdown("---")
    with st.expander("➕ Avanzado: subir bases Digital/Física manualmente", expanded=False):
        up_dig = st.file_uploader("Base de datos de la colección Digital (.xlsx)", type=["xlsx"], key="up_dig_v8")
        up_fis = st.file_uploader("Base de datos de la colección Física (.xlsx)", type=["xlsx"], key="up_fis_v8")
        if up_dig is not None:
            ss.df_digital = safe_read_excel(up_dig, "Colección Digital")
            st.success("Colección Digital (manual) cargada.")
        if up_fis is not None:
            ss.df_fisica = safe_read_excel(up_fis, "Colección Física")
            st.success("Colección Física (manual) cargada.")
        if ss.df_digital is not None and ss.df_fisica is not None:
            ss.bases_ready = True

# ------------------------------ CABECERA / INSTRUCCIONES ------------------------------
st.title("Herramienta para la elaboración de bibliografías especializadas")

with st.expander("ℹ️ Información", expanded=True):
    st.markdown(
        f"""
- **Objetivo:** permitir la autogestión por programa/asignatura/tema y resaltar **términos a excluir** para depuración manual.  
- Usa siempre las bases oficiales (Digital/Física) o súbelas **manualmente** en la barra lateral.  
- **Plantillas:** [Temáticas]({URL_PLANTILLA_TEMATICAS}) y [Términos a excluir]({URL_PLANTILLA_EXCLUSION}).  
- Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app.  
- El proceso puede tardar algunos minutos; **puedes seguir usando tu equipo** (no cierres el navegador).
        """
    )

# ------------------------------ DESCARGA SECUENCIAL (ÚNICA) DE BASES OFICIALES ------------------------------
st.markdown("#### Bases oficiales")
desc_area = st.container()

if not ss.bases_ready:
    # Botón único de descarga secuencial (Digital -> Física)
    col_btn = st.columns([1, 2, 1])[1]
    with col_btn:
        bt = st.button("⬇️ Descargar bases oficiales (única vez por sesión)", type="primary", disabled=ss.downloading)
    if bt and not ss.downloading:
        ss.descarga_disparada = True
        ss.downloading = True
        ss.progress_msg = "Descargando Base de datos de la colección Digital…"

    # Proceso de descarga paso a paso
    if ss.downloading:
        with desc_area:
            st.info("Cargando las bases Digital y Física desde la web oficial… Puedes subir **Temáticas** y **Términos a excluir** mientras tanto. No cierres esta ventana.")
            # DIGITAL
            st.subheader("Descargando Base de datos de la colección Digital…")
            zona_dig = st.container()
            try:
                bio_d = download_with_resume(URL_DIGITAL, "Colección Digital", container=zona_dig)
                st.caption("Colección Digital: descarga completa. Verificando archivo…")
                ss.df_digital = safe_read_excel(bio_d, "Colección Digital")
                st.success("Base de datos de la colección Digital lista ✓")
            except Exception as e:
                st.error(f"No fue posible descargar la base Digital: {e}")
                ss.downloading = False

            # FÍSICA
            st.subheader("Descargando Base de datos de la colección Física…")
            zona_fis = st.container()
            try:
                bio_f = download_with_resume(URL_FISICA, "Colección Física", container=zona_fis)
                st.caption("Colección Física: descarga completa. Verificando archivo…")
                ss.df_fisica = safe_read_excel(bio_f, "Colección Física")
                st.success("Base de datos de la colección Física lista ✓")
            except Exception as e:
                st.error(f"No fue posible descargar la base Física: {e}")
                ss.downloading = False

            if ss.df_digital is not None and ss.df_fisica is not None:
                ss.bases_ready = True
                st.success("✅ Bases oficiales listas en memoria.")
                ss.downloading = False
                ss.progress_msg = ""

else:
    st.success("✅ Bases oficiales listas en memoria (sesión).")
    st.caption("Tip: usa **Nueva búsqueda** para repetir con otras temáticas sin volver a descargar las bases.")

# Si aún no hay bases, no avanzamos
if not ss.bases_ready:
    st.stop()

# ------------------------------ NUEVA BÚSQUEDA (sin borrar bases) ------------------------------
col_nb = st.columns([1, 1, 4])[0]
with col_nb:
    if st.button("🧪 Nueva búsqueda"):
        # limpiamos sólo insumos/resultados de la búsqueda
        for k in ("tematicas_df", "excluir_df", "results_df", "bitacora_df"):
            ss[k] = None
        st.toast("Listo. Carga nuevas Temáticas y Términos a excluir para buscar.")

# ------------------------------ VALIDACIONES PREVIAS ------------------------------
if ss.tematicas_df is None or ss.excluir_df is None:
    st.warning("Carga **Temáticas** y **Términos a excluir** (barra lateral) para continuar.")
    st.stop()

# ------------------------------ CONFIGURACIÓN DE BÚSQUEDA Y DUPLICADOS ------------------------------
st.subheader("Configuración de búsqueda y duplicados")

cols_dig = list(ss.df_digital.columns)
cols_fis = list(ss.df_fisica.columns)
common_cols = sorted(set(cols_dig + cols_fis))

c1, c2, c3, c4 = st.columns([1, 1, 1, 1])

with c1:
    col_busq1 = st.selectbox(
        "Búsqueda principal por:",
        options=common_cols,
        index=get_index_or_first(common_cols, DEFAULT_COL_TITULO),
        key="col_busq1_v8",
    )

with c2:
    col_busq2 = st.selectbox(
        "Búsqueda complementaria por:",
        options=common_cols,
        index=get_index_or_first(common_cols, DEFAULT_COL_TEMATICAS),
        key="col_busq2_v8",
    )

with c3:
    col_dup_dig = st.selectbox(
        "Columna de duplicados en Colección Digital:",
        options=cols_dig,
        index=get_index_or_first(cols_dig, DEFAULT_DUP_DIGITAL),
        key="dup_dig_v8",
    )

with c4:
    col_dup_fis = st.selectbox(
        "Columna de duplicados en Colección Física:",
        options=cols_fis,
        index=get_index_or_first(cols_fis, DEFAULT_DUP_FISICA),
        key="dup_fis_v8",
    )

st.caption("Por defecto la búsqueda se realiza en “Título” y “Temáticas”. Puedes elegir otras dos columnas si lo necesitas.")

st.markdown("---")

# ------------------------------ MOTOR DE BÚSQUEDA (TU LÓGICA EXISTENTE) ------------------------------
def _prepara_columnas(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).fillna("")

def _buscar(df: pd.DataFrame, fuente: str, col1: str, col2: str, tem_df: pd.DataFrame,
            barra, estado, total_steps: int, offset: int) -> pd.DataFrame:
    res = []
    tem = tem_df.copy()
    tem["termino"] = tem["termino"].astype(str).fillna("")
    tem["normalizado"] = tem["normalizado"].astype(str).fillna("")
    N = len(tem)
    t0 = time.time()

    for i, row in tem.iterrows():
        term = normalize_text(row["termino"])
        if term:
            m1 = df[col1].map(lambda s: term in normalize_text(s))
            m2 = df[col2].map(lambda s: term in normalize_text(s))
            md = df[m1 | m2].copy()
            if not md.empty:
                md["Temática"] = row["termino"]
                md["Temática normalizada"] = row["normalizado"]
                md["Columna de coincidencia"] = None
                md.loc[m1[m1].index, "Columna de coincidencia"] = col1
                md.loc[m2[m2].index, "Columna de coincidencia"] = md["Columna de coincidencia"].fillna(col2)
                md["Fuente"] = fuente
                res.append(md)

        frac = (i + 1) / max(N, 1)
        elapsed = time.time() - t0
        est_total = elapsed / max(frac, 1e-6)
        est_rem = max(0, int(est_total - elapsed))
        barra.progress(min(1.0, (offset + i + 1) / total_steps))
        estado.info(f"{fuente}: {i+1}/{N} términos • transcurrido: {int(elapsed)} s • restante: {est_rem} s")

    if res:
        return pd.concat(res, ignore_index=True)
    return pd.DataFrame()

def ejecutar_busqueda():
    excluye = [str(x).strip() for x in ss.excluir_df["excluir"].tolist() if str(x).strip() != ""]
    barra = st.progress(0)
    estado = st.empty()

    DF_D = ss.df_digital.copy()
    DF_F = ss.df_fisica.copy()

    _prepara_columnas(DF_D, [col_busq1, col_busq2, col_dup_dig])
    _prepara_columnas(DF_F, [col_busq1, col_busq2, col_dup_fis])

    total = len(ss.tematicas_df) * 2
    res_d = _buscar(DF_D, "Digital", col_busq1, col_busq2, ss.tematicas_df, barra, estado, total_steps=total, offset=0)
    res_f = _buscar(DF_F, "Física",  col_busq1, col_busq2, ss.tematicas_df, barra, estado, total_steps=total, offset=len(ss.tematicas_df))

    if not res_d.empty and col_dup_dig in res_d.columns:
        res_d = res_d.drop_duplicates(subset=[col_dup_dig], keep="first")
    if not res_f.empty and col_dup_fis in res_f.columns:
        res_f = res_f.drop_duplicates(subset=[col_dup_fis], keep="first")

    res = pd.concat([res_d, res_f], ignore_index=True) if not (res_d.empty and res_f.empty) else pd.DataFrame()

    # Persistimos
    ss.results_df = res

    # Bitácora con ceros
    tem = ss.tematicas_df[["termino", "normalizado"]].drop_duplicates().reset_index(drop=True)
    fuentes = pd.DataFrame({"Fuente": ["Digital", "Física"]})
    grid = fuentes.assign(key=1).merge(tem.assign(key=1), on="key").drop("key", axis=1)

    if res.empty:
        counts = pd.DataFrame(columns=["Fuente", "Temática", "Temática normalizada", "Resultados"])
    else:
        counts = (res
                  .groupby(["Fuente", "Temática", "Temática normalizada"], dropna=False)
                  .size().reset_index(name="Resultados"))

    bit = (grid.merge(counts, how="left",
                      left_on=["Fuente", "termino", "normalizado"],
                      right_on=["Fuente", "Temática", "Temática normalizada"])
              .drop(columns=["Temática", "Temática normalizada"], errors="ignore")
              .rename(columns={"termino": "Término", "normalizado": "Normalizado"}))

    bit["Resultados"] = bit["Resultados"].fillna(0).astype(int)
    bit = bit.sort_values(["Fuente", "Resultados", "Término"], ascending=[True, False, True]).reset_index(drop=True)
    ss.bitacora_df = bit

    barra.progress(1.0)
    estado.empty()
    st.success("Búsqueda finalizada.")

# ------------------------------ BOTÓN DE BÚSQUEDA ------------------------------
if st.button("🚀 Iniciar búsqueda", type="primary"):
    try:
        ejecutar_busqueda()
    except Exception as e:
        st.error(f"Ocurrió un problema durante la búsqueda: {e}")

# ------------------------------ RESULTADOS Y Filtros / Selección ------------------------------
st.subheader("Resultados")
if ss.results_df is None or ss.results_df.empty:
    st.info("Aún no hay resultados. Ejecuta la búsqueda.")
else:
    res = ss.results_df.copy()

    # Filtros rápidos (opcionales, no alteran el motor)
    colf1, colf2, colf3 = st.columns([1, 1, 2])
    with colf1:
        filtro_fuente = st.multiselect("Fuente", options=sorted(res["Fuente"].dropna().unique().tolist()), default=None)
    with colf2:
        col_tema_norm = "Temática normalizada"
        temas_norm = sorted(res[col_tema_norm].dropna().unique().tolist()) if col_tema_norm in res.columns else []
        filtro_tema = st.multiselect("Temática normalizada", options=temas_norm, default=None)
    with colf3:
        tipon_col = "Tipo de ítem normalizado mat especial"
        tipo_opts = sorted(res.get(tipon_col, pd.Series(dtype=str)).dropna().unique().tolist())
        filtro_tipo = st.multiselect("Tipo normalizado", options=tipo_opts, default=None)

    if filtro_fuente:
        res = res[res["Fuente"].isin(filtro_fuente)]
    if filtro_tema:
        res = res[res[col_tema_norm].isin(filtro_tema)]
    if filtro_tipo and tipon_col in res.columns:
        res = res[res[tipon_col].isin(filtro_tipo)]

    # Columna de selección para exportaciones parciales
    res_view = res.copy()
    if "__Seleccionar__" not in res_view.columns:
        res_view.insert(0, "__Seleccionar__", False)

    # Control de filas visibles (para no recargar el front)
    cva, cvb = st.columns([1, 1])
    with cva:
        show_all = st.checkbox("Mostrar todas las filas (Vista)", value=False)
    with cvb:
        limit = st.number_input("Filas a mostrar (Vista)", min_value=50, max_value=10000, value=200, step=50)

    if not show_all:
        res_view_show = res_view.head(int(limit))
    else:
        res_view_show = res_view

    # Editor para marcar selección
    res_view_show = st.data_editor(
        res_view_show,
        use_container_width=True,
        height=520,
        column_config={
            "__Seleccionar__": st.column_config.CheckboxColumn("Seleccionar"),
        },
        disabled=[c for c in res_view_show.columns if c != "__Seleccionar__"],
        key="data_editor_res_v8",
    )

    # Determinar seleccionados (de la vista)
    seleccion_mask = res_view_show["__Seleccionar__"] if "__Seleccionar__" in res_view_show.columns else pd.Series(False, index=res_view_show.index)
    seleccionados = res_view_show[seleccion_mask].drop(columns=["__Seleccionar__"], errors="ignore")

    # ---------------- Exportaciones ----------------
    st.markdown("##### Exportaciones")
    colx1, colx2, colx3 = st.columns([1.2, 1.2, 2])

    # CSV completo (filtrado por los filtros, no por la selección)
    with colx1:
        st.download_button(
            "⬇️ CSV (todo lo filtrado)",
            data=res.fillna("").to_csv(index=False).encode("utf-8"),
            file_name="resultados_filtrados.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # Excel con resaltado + Bitácora
    with colx2:
        excluye = [str(x).strip() for x in ss.excluir_df["excluir"].tolist() if str(x).strip() != ""]
        import xlsxwriter  # se usa sólo aquí
        xbio = io.BytesIO()
        writer = pd.ExcelWriter(xbio, engine="xlsxwriter")
        res.to_excel(writer, index=False, sheet_name="Resultados")
        wb = writer.book
        ws = writer.sheets["Resultados"]
        fmt = wb.add_format({"bg_color": "#FFF599"})

        cols = list(res.columns)
        col_tit = cols.index(DEFAULT_COL_TITULO) + 1 if DEFAULT_COL_TITULO in cols else None
        col_tem = cols.index(DEFAULT_COL_TEMATICAS) + 1 if DEFAULT_COL_TEMATICAS in cols else None
        excl_norm = [normalize_text(x) for x in excluye]

        for r in range(1, len(res) + 1):
            if col_tit:
                v = normalize_text(res.iloc[r - 1, col_tit - 1])
                if any(t in v for t in excl_norm):
                    ws.write(r, col_tit - 1, res.iloc[r - 1, col_tit - 1], fmt)
            if col_tem:
                v = normalize_text(res.iloc[r - 1, col_tem - 1])
                if any(t in v for t in excl_norm):
                    ws.write(r, col_tem - 1, res.iloc[r - 1, col_tem - 1], fmt)

        if ss.bitacora_df is not None:
            ss.bitacora_df.to_excel(writer, index=False, sheet_name="Bitácora")

        writer.close(); xbio.seek(0)
        st.download_button(
            "⬇️ Excel (resaltado + Bitácora)",
            data=xbio.getvalue(),
            file_name="resultados_resaltados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    # Citas APA simplificadas para seleccionados (opcional)
    def build_apa(row: pd.Series) -> str:
        """
        Generador APA simplificado según reglas indicadas por el usuario.
        No intenta cubrir todos los casos APA: sólo los campos disponibles y reglas pedidas.
        """
        tit   = str(row.get("Título", "")).strip()
        aut   = str(row.get("Autor(es)", "")).strip()
        edit  = str(row.get("Editorial", "")).strip()
        anio  = str(row.get("Año de Publicación", "")).strip()
        bd    = str(row.get("Base de datos", "")).strip()
        url   = str(row.get("Url OA", "") or row.get("Url de acceso", "")).strip()
        isbn  = str(row.get("ISBN", "")).strip()
        issn  = str(row.get("ISSN1", "")).strip()
        tipo  = str(row.get("Tipo de ítem normalizado mat especial", "")).strip()
        topog = str(row.get("No. Topográfico", "")).strip()

        partes = []

        # Autor: sólo si no es "NO APLICA"
        if aut and aut.upper() != "NO APLICA":
            partes.append(f"{aut}.")
        # Año (s.f. aceptado)
        if anio and anio.upper() != "NO APLICA":
            partes.append(f"({anio}).")
        # Título (siempre)
        if tit:
            partes.append(f"{tit}.")
        # Editorial
        if edit:
            partes.append(f"{edit}.")
        elif edit == "":
            partes.append("s.e.")

        # Acceso / Fuente
        acc = []
        if bd:
            acc.append(f"Disponible en {bd}")
        # Digital: URL
        if url:
            acc.append(url)
        # Físico: No. Topográfico
        if topog and topog.upper() != "NO APLICA":
            acc.append(f"No. Topográfico: {topog}")
        if acc:
            partes.append("; ".join(acc) + ".")

        # ISBN / ISSN (si existen)
        extras = []
        if isbn and isbn.upper() != "NO APLICA":
            extras.append(f"ISBN: {isbn}")
        if issn and issn.upper() != "NO APLICA":
            extras.append(f"ISSN: {issn}")
        if extras:
            partes.append(" ".join(extras) + ".")

        return " ".join([p for p in partes if p]).replace("..", ".")

    with colx3:
        if seleccionados is not None and not seleccionados.empty:
            citas = [build_apa(r) for _, r in seleccionados.iterrows()]
            txt = "\n\n".join(c for c in citas if c.strip())
            if not txt:
                st.info("Selecciona filas para generar citas.")
            else:
                st.download_button(
                    "🧾 Citas APA (seleccionados)",
                    data=txt.encode("utf-8"),
                    file_name="citas_apa.txt",
                    mime="text/plain",
                    use_container_width=True,
                )
        else:
            st.caption("Marca filas en la columna **Seleccionar** para exportar citas de esa selección.")

# ------------------------------ BITÁCORA ------------------------------
st.subheader("📑 Bitácora por término")
if ss.bitacora_df is None or ss.bitacora_df.empty:
    st.info("Aún no hay bitácora. Ejecuta la búsqueda.")
else:
    st.dataframe(ss.bitacora_df, use_container_width=True, height=360)
    st.download_button(
        "Descargar bitácora (.csv)",
        data=ss.bitacora_df.to_csv(index=False).encode("utf-8"),
        file_name="bitacora_por_termino.csv",
        mime="text/csv",
    )
