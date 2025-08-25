# -*- coding: utf-8 -*-
# Herramienta para la elaboración de bibliografías especializadas
# v7.4.1 – sin botones de memoria + auto-carga estable en Cloud

import io
import os
import time
import tempfile
import requests
import pandas as pd
import streamlit as st

# ============ CONFIG GENERAL ============
st.set_page_config(page_title="Herramienta de bibliografías", layout="wide")

LOGO_URL = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png"

# URLs oficiales
URL_DIGITAL = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx"
URL_FISICA  = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx"

URL_PLANTILLA_TEMATICAS = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx"
URL_PLANTILLA_EXCLUSION = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx"

# Columnas por defecto (las mismas que funcionaban)
DEFAULT_COL_TITULO    = "Título"
DEFAULT_COL_TEMATICAS = "Temáticas"
DEFAULT_DUP_DIGITAL   = "Url OA"
DEFAULT_DUP_FISICA    = "No. Topográfico"

UA = {"User-Agent": "Mozilla/5.0"}

# ============ ESTADO ============
ss = st.session_state
for k, v in {
    "df_digital": None,
    "df_fisica": None,
    "tematicas_df": None,
    "excluir_df": None,
    "auto_started": False,
    "loading_digital": False,
    "loading_fisica": False,
    "processing_digital": False,
    "processing_fisica": False,
    "results_df": None,
    "bitacora_df": None,
}.items():
    ss.setdefault(k, v)

# ============ UTILIDADES ============
def normalize_text(s):
    if pd.isna(s):
        return ""
    s = str(s)
    return (s.replace("\u0301","")
             .replace("\u0303","")
             .replace("\u2019","'")
             .replace("\xa0"," "))

def _head_content_length(url, timeout=20):
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=UA)
        r.raise_for_status()
        cl = r.headers.get("Content-Length")
        return int(cl) if cl is not None else None
    except Exception:
        return None

def download_with_resume(url, label, max_retries=6, chunk_size=256*1024, timeout=120, container=None):
    """Descarga robusta con reintentos y progreso."""
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
            if downloaded and total_size and downloaded < total_size:
                headers["Range"] = f"bytes={downloaded}-"
                mode = "ab"
            else:
                mode = "wb"

            status.info(f"Descargando {label}… (intento {attempt}/{max_retries})")

            with requests.get(url, stream=True, headers=headers, timeout=timeout, allow_redirects=True) as r:
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

                        if expected_total and (time.time() - last) > 0.15:
                            bar.progress(min(1.0, downloaded/expected_total))
                            mb = downloaded/1e6
                            if expected_total:
                                info.write(f"{mb:,.1f} MB / {expected_total/1e6:,.1f} MB")
                            else:
                                info.write(f"{mb:,.1f} MB")
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

def read_excel_from_bytes(bio, label="archivo"):
    with st.spinner(f"Procesando {label}…"):
        bio.seek(0)
        df = pd.read_excel(bio, engine="openpyxl", dtype=str)
        df = df.fillna("")
    return df

def _safe_index(names, target):
    try:
        return names.index(target)
    except Exception:
        return 0

# ============ SIDEBAR ============
with st.sidebar:
    st.image(LOGO_URL, use_container_width=True)
    st.caption("Biblioteca Juan Roa Vásquez")
    st.markdown("### Plantillas oficiales:")
    st.markdown(f"- [Temáticas]({URL_PLANTILLA_TEMATICAS})")
    st.markdown(f"- [Términos a excluir]({URL_PLANTILLA_EXCLUSION})")

    st.markdown("### Archivos auxiliares (obligatorios)")
    tem_container = st.container()
    exc_container = st.container()

    st.markdown("---")
    with st.expander("⚙️ Avanzado: subir bases Digital/Física manualmente", expanded=False):
        up_dig = st.file_uploader("Base de datos de la colección Digital (.xlsx)", type=["xlsx"], key="up_dig_manual")
        up_fis = st.file_uploader("Base de datos de la colección Física (.xlsx)",  type=["xlsx"], key="up_fis_manual")
        if up_dig is not None:
            ss.df_digital = read_excel_from_bytes(up_dig, "base Digital (manual)")
            st.success("Base de datos de la colección Digital cargada (manual).")
        if up_fis is not None:
            ss.df_fisica = read_excel_from_bytes(up_fis, "base Física (manual)")
            st.success("Base de datos de la colección Física cargada (manual).")

# ============ CABECERA ============
st.markdown(
    f"""
### Herramienta para la elaboración de bibliografías especializadas

<div style="padding:12px;border-radius:8px;background:#0f172a;">
<ul>
<li><b>Objetivo</b>: autogestión por programa/asignatura/tema y resaltado de <b>términos a excluir</b> para depuración manual.</li>
<li>Usa siempre las bases oficiales (Digital/Física) o súbelas <b>manual­mente</b> en la barra lateral.</li>
<li>Plantillas: <a href="{URL_PLANTILLA_TEMATICAS}">Temáticas</a> y <a href="{URL_PLANTILLA_EXCLUSION}">Términos a excluir</a>.</li>
<li>Los archivos adjuntos <b>no se almacenan</b> por la Universidad y se eliminan al cerrar la app.</li>
<li>El proceso puede tardar algunos minutos; puedes seguir usando tu equipo (no cierres el navegador).</li>
</ul>
</div>
""",
    unsafe_allow_html=True,
)

# ============ AUTO-CARGA (una sola vez) ============
if not ss.auto_started and (ss.df_digital is None or ss.df_fisica is None):
    ss.auto_started = True
    st.info("Cargando las bases Digital y Física desde la web oficial… Puedes continuar leyendo las instrucciones.")

    # Digital
    try:
        ss.loading_digital = True
        bio = download_with_resume(URL_DIGITAL, "Colección Digital", container=st)
        ss.processing_digital = True
        ss.df_digital = read_excel_from_bytes(bio, "base de datos de la colección Digital")
    finally:
        ss.loading_digital = False
        ss.processing_digital = False

    # Física
    try:
        ss.loading_fisica = True
        bio = download_with_resume(URL_FISICA, "Colección Física", container=st)
        ss.processing_fisica = True
        ss.df_fisica = read_excel_from_bytes(bio, "base de datos de la colección Física")
    finally:
        ss.loading_fisica = False
        ss.processing_fisica = False

# Indicadores
ok_dig = ss.df_digital is not None
ok_fis = ss.df_fisica is not None

st.subheader("Bases oficiales cargadas en memoria (sesión)")
if ok_dig and ok_fis:
    st.success("✅ Bases oficiales listas en memoria.")
else:
    dig_txt = "en proceso…" if ss.loading_digital or ss.processing_digital else ("pendiente" if not ok_dig else "lista")
    fis_txt = "en proceso…" if ss.loading_fisica or ss.processing_fisica else ("pendiente" if not ok_fis else "lista")
    st.info(f"Base de datos de la colección Digital: **{dig_txt}**  •  "
            f"Base de datos de la colección Física: **{fis_txt}**")

# Mostrar uploads solo cuando ambas estén listas (evita que se “pierdan”).
if ok_dig and ok_fis:
    with st.sidebar:
        with tem_container:
            tem_up = st.file_uploader(
                "Temáticas (.xlsx, col1=término, col2=normalizado)", type=["xlsx"], key="tem_up_ready")
            if tem_up is not None:
                df = read_excel_from_bytes(tem_up, "Temáticas")
                ss.tematicas_df = df[[df.columns[0], df.columns[1]]].rename(
                    columns={df.columns[0]:"termino", df.columns[1]:"normalizado"}).fillna("")
                st.success(f"Temáticas cargadas: {len(ss.tematicas_df)}")

        with exc_container:
            exc_up = st.file_uploader(
                "Términos a excluir (.xlsx, col1)", type=["xlsx"], key="exc_up_ready")
            if exc_up is not None:
                df = read_excel_from_bytes(exc_up, "Términos a excluir")
                ss.excluir_df = df[[df.columns[0]]].rename(columns={df.columns[0]:"excluir"}).fillna("")
                st.success(f"Términos a excluir cargados: {len(ss.excluir_df)}")
else:
    st.warning("Descargando/Procesando bases oficiales… al terminar podrás cargar Temáticas y Términos a excluir.")
    st.stop()

# Requisitos mínimos
if ss.tematicas_df is None or ss.excluir_df is None:
    st.error("Debes cargar **Temáticas** y **Términos a excluir** (en la barra lateral) antes de buscar.")
    st.stop()

# ============ CONFIGURACIÓN ============
st.subheader("Configuración de búsqueda y duplicados")

cols_dig = list(ss.df_digital.columns)
cols_fis = list(ss.df_fisica.columns)
common_cols = sorted(set(cols_dig + cols_fis))

c1, c2, c3, c4 = st.columns([1,1,1,1])
with c1:
    col_busq1 = st.selectbox("Búsqueda principal por:", options=common_cols,
                             index=_safe_index(common_cols, DEFAULT_COL_TITULO))
with c2:
    col_busq2 = st.selectbox("Búsqueda complementaria por:", options=common_cols,
                             index=_safe_index(common_cols, DEFAULT_COL_TEMATICAS))
with c3:
    col_dup_dig = st.selectbox("Columna de duplicados en Colección Digital:", options=cols_dig,
                               index=_safe_index(cols_dig, DEFAULT_DUP_DIGITAL))
with c4:
    col_dup_fis = st.selectbox("Columna de duplicados en Colección Física:", options=cols_fis,
                               index=_safe_index(cols_fis, DEFAULT_DUP_FISICA))

st.caption("Por defecto la búsqueda se realiza en “Título” y “Temáticas”. Puedes elegir otras dos columnas si lo necesitas.")

# ============ BÚSQUEDA (pipeline intacto) ============
st.markdown("---")
if st.button("🚀 Iniciar búsqueda", type="primary"):
    excluye = [str(x).strip() for x in ss.excluir_df["excluir"].tolist() if str(x).strip()!=""]

    barra = st.progress(0)
    estado = st.empty()

    DF_D = ss.df_digital.copy()
    DF_F = ss.df_fisica.copy()

    for df, dup_col in ((DF_D,col_dup_dig),(DF_F,col_dup_fis)):
        for c in (col_busq1, col_busq2, dup_col):
            if c in df.columns:
                df[c] = df[c].astype(str).fillna("")

    def buscar(df, fuente, total_steps, offset):
        res = []
        tem = ss.tematicas_df.copy()
        tem["termino"]      = tem["termino"].astype(str).fillna("")
        tem["normalizado"]  = tem["normalizado"].astype(str).fillna("")
        N = len(tem)
        t0 = time.time()

        for i, row in tem.iterrows():
            term = normalize_text(row["termino"])
            if term:
                m1 = df[col_busq1].map(lambda s: term in normalize_text(s))
                m2 = df[col_busq2].map(lambda s: term in normalize_text(s))
                md = df[m1 | m2].copy()
                if not md.empty:
                    md["Temática"]                 = row["termino"]
                    md["Temática normalizada"]     = row["normalizado"]
                    md["Columna de coincidencia"]  = None
                    md.loc[m1[m1].index, "Columna de coincidencia"] = col_busq1
                    md.loc[m2[m2].index, "Columna de coincidencia"] = md["Columna de coincidencia"].fillna(col_busq2)
                    md["Fuente"] = fuente
                    res.append(md)

            frac = (i + 1) / max(N, 1)
            elapsed = time.time() - t0
            est_total = elapsed / max(frac, 1e-6)
            est_rem = max(0, int(est_total - elapsed))
            # actualizar cada ~0.1s para no bloquear
            if (i % 2) == 0:
                barra.progress(min(1.0, (offset + i + 1) / total_steps))
                estado.info(f"{fuente}: normalizando/buscando {i+1}/{N} • transcurrido: {int(elapsed)} s • restante: {est_rem} s")

        if res:
            return pd.concat(res, ignore_index=True)
        return pd.DataFrame()

    total = len(ss.tematicas_df) * 2
    res_d = buscar(DF_D, "Digital", total, 0)
    res_f = buscar(DF_F, "Física",  total, len(ss.tematicas_df))

    if not res_d.empty and col_dup_dig in res_d.columns:
        res_d = res_d.drop_duplicates(subset=[col_dup_dig], keep="first")
    if not res_f.empty and col_dup_fis in res_f.columns:
        res_f = res_f.drop_duplicates(subset=[col_dup_fis], keep="first")

    res = pd.concat([res_d, res_f], ignore_index=True) if not (res_d.empty and res_f.empty) else pd.DataFrame()
    ss.results_df = res

    # Bitácora con ceros
    tem = ss.tematicas_df[["termino","normalizado"]].drop_duplicates().reset_index(drop=True)
    fuentes = pd.DataFrame({"Fuente":["Digital","Física"]})
    grid = fuentes.assign(key=1).merge(tem.assign(key=1), on="key").drop("key", axis=1)

    if res.empty:
        counts = pd.DataFrame(columns=["Fuente","Temática","Temática normalizada","Resultados"])
    else:
        counts = (res
                  .groupby(["Fuente","Temática","Temática normalizada"], dropna=False)
                  .size().reset_index(name="Resultados"))

    bit = (grid.merge(counts, how="left",
                      left_on=["Fuente","termino","normalizado"],
                      right_on=["Fuente","Temática","Temática normalizada"])
                .drop(columns=["Temática","Temática normalizada"], errors="ignore")
                .rename(columns={"termino":"Término","normalizado":"Normalizado"}))

    bit["Resultados"] = bit["Resultados"].fillna(0).astype(int)
    bit = bit.sort_values(["Fuente","Resultados","Término"], ascending=[True, False, True]).reset_index(drop=True)
    ss.bitacora_df = bit

    barra.progress(1.0)
    estado.empty()
    st.success("Búsqueda finalizada.")

# ============ RESULTADOS ============
st.subheader("Resultados")
if ss.results_df is None or ss.results_df.empty:
    st.info("Aún no hay resultados. Ejecuta la búsqueda.")
else:
    res = ss.results_df

    col_a, col_b = st.columns([1,1])
    with col_a:
        show_all = st.checkbox("Mostrar todas las filas", value=False)
    with col_b:
        limit = st.number_input("Filas a mostrar (si no muestras todas):", min_value=50, max_value=10000, value=200, step=50)

    if show_all:
        st.dataframe(res, use_container_width=True, height=560)
    else:
        st.dataframe(res.head(int(limit)), use_container_width=True, height=560)

    st.download_button(
        "⬇️ Descargar CSV (todos los resultados)",
        data=res.fillna("").to_csv(index=False).encode("utf-8"),
        file_name="resultados.csv",
        mime="text/csv"
    )

    excluye = [str(x).strip() for x in ss.excluir_df["excluir"].tolist() if str(x).strip()!=""]
    if excluye:
        import xlsxwriter
        xbio = io.BytesIO()
        writer = pd.ExcelWriter(xbio, engine="xlsxwriter")
        res.to_excel(writer, index=False, sheet_name="Resultados")
        wb = writer.book; ws = writer.sheets["Resultados"]
        fmt = wb.add_format({"bg_color":"#FFF599"})

        cols = list(res.columns)
        col_tit = cols.index(DEFAULT_COL_TITULO) + 1 if DEFAULT_COL_TITULO in cols else None
        col_tem = cols.index(DEFAULT_COL_TEMATICAS) + 1 if DEFAULT_COL_TEMATICAS in cols else None
        excl_norm = [normalize_text(x) for x in excluye]

        for r in range(1, len(res)+1):
            if col_tit:
                v = normalize_text(res.iloc[r-1, col_tit-1])
                if any(t in v for t in excl_norm):
                    ws.write(r, col_tit-1, res.iloc[r-1, col_tit-1], fmt)
            if col_tem:
                v = normalize_text(res.iloc[r-1, col_tem-1])
                if any(t in v for t in excl_norm):
                    ws.write(r, col_tem-1, res.iloc[r-1, col_tem-1], fmt)

        if ss.bitacora_df is not None:
            ss.bitacora_df.to_excel(writer, index=False, sheet_name="Bitácora")

        writer.close(); xbio.seek(0)
        st.download_button(
            "⬇️ Descargar Excel (con resaltado y bitácora)",
            data=xbio.getvalue(),
            file_name="resultados_resaltados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Carga un “Listado de términos a excluir” para obtener el Excel con resaltado amarillo.")

# ============ BITÁCORA ============
st.subheader("📑 Bitácora por término")
if ss.bitacora_df is None or ss.bitacora_df.empty:
    st.info("Aún no hay bitácora. Ejecuta la búsqueda.")
else:
    bit = ss.bitacora_df
    st.dataframe(bit, use_container_width=True, height=380)
    st.download_button(
        "Descargar bitácora (.csv)",
        data=bit.to_csv(index=False).encode("utf-8"),
        file_name="bitacora_por_termino.csv",
        mime="text/csv"
    )
