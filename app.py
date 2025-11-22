# -*- coding: utf-8 -*-
# Herramienta para la elaboración de bibliografías especializadas
# v8.2 – Método A estable + Método B avanzado, sin tocar el motor de búsqueda original

import io
import os
import time
import tempfile
from typing import List, Dict, Any

import pandas as pd
import requests
import streamlit as st

# ---------------------------------- CONFIGURACIÓN ----------------------------------
st.set_page_config(page_title="Herramienta de bibliografías", layout="wide")

# Logos (el tema claro/oscuro lo manejas en config.toml; aquí sólo usamos uno)
LOGO_URL = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png"

# URLs oficiales (Digital/Física y plantillas)
URL_DIGITAL = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx"
URL_FISICA = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx"

URL_PLANTILLA_TEMATICAS = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx"
URL_PLANTILLA_EXCLUSION = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx"

# Valores por defecto para búsqueda/duplicados (Método A)
DEFAULT_COL_TITULO = "Título"
DEFAULT_COL_TEMATICAS = "Temáticas"
DEFAULT_DUP_DIGITAL = "Url OA"
DEFAULT_DUP_FISICA = "No. Topográfico"

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
}

# Columnas a OMITIR en exportaciones CSV/XLSX
EXPORT_DROP_COLS = {
    "Fecha de actualización",
    "Tipo de ítem normalizado mat especial",
    "Formato",
    "Prioridad Búsqueda",
}

# Renombres para exportación
EXPORT_RENAME = {
    "Temáticas": "Temáticas catalogadas por el Editor",
    "Temática": "Término de búsqueda",
    "Temática normalizada": "Término de búsqueda normalizado",
    "Url en LOCATE/IDEA": "Url de acceso",
}

# ---------------------------------- ESTADO ----------------------------------
ss = st.session_state

# Bases (se conservan toda la sesión; “Nueva búsqueda” no las borra)
ss.setdefault("df_digital", None)
ss.setdefault("df_fisica", None)
ss.setdefault("bases_ready", False)

# Descarga/sincronización
ss.setdefault("downloading", False)
ss.setdefault("descarga_disparada", False)

# Insumos de búsqueda (Método A)
ss.setdefault("tematicas_df", None)
ss.setdefault("excluir_df", None)

# Resultados y bitácora
ss.setdefault("results_df", None)
ss.setdefault("bitacora_df", None)
ss.setdefault("last_method", None)  # "A" o "B"

# ---------------------------------- UTILIDADES ----------------------------------
def normalize_text(s):
    if pd.isna(s):
        return ""
    s = str(s)
    return (
        s.replace("\u0301", "")
        .replace("\u0303", "")
        .replace("\u2019", "'")
        .replace("\xa0", " ")
        .strip()
    )


def _head_content_length(url, timeout=30):
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=UA)
        r.raise_for_status()
        cl = r.headers.get("Content-Length")
        return int(cl) if cl is not None else None
    except Exception:
        return None


def download_with_resume(
    url, label, container=None, max_retries=5, chunk_size=256 * 1024, timeout=300
) -> io.BytesIO:
    """
    Descarga con barra y reintentos. Devuelve BytesIO.
    """
    where = container if container is not None else st
    status = where.empty()
    bar = where.progress(0)
    info = where.empty()

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

            with requests.get(
                url,
                stream=True,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
            ) as r:
                if headers.get("Range") and r.status_code == 200:
                    # el servidor no aceptó rango → reinicia total
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
                        if expected_total and time.time() - last > 0.1:
                            bar.progress(min(1.0, downloaded / expected_total))
                            info.write(
                                f"{downloaded/1e6:,.1f} MB / {expected_total/1e6:,.1f} MB"
                            )
                            last = time.time()

            if total_size and downloaded < total_size:
                raise requests.exceptions.ChunkedEncodingError(
                    f"Descarga incompleta: {downloaded} de {total_size} bytes"
                )

            bar.progress(1.0)
            status.success(f"{label} descargado correctamente.")
            info.empty()
            bar.empty()
            status.empty()

            with open(tmp_path, "rb") as f:
                data = f.read()
            return io.BytesIO(data)

        except Exception as e:
            info.empty()
            bar.empty()
            status.warning(f"Fallo al descargar {label}: {e}")
            if attempt < max_retries:
                time.sleep(2)
            else:
                status.error(
                    f"No se pudo descargar {label} tras {max_retries} intentos."
                )
                raise
        finally:
            info.empty()
            bar.empty()
            status.empty()


def safe_read_excel(bio_or_file, label="archivo") -> pd.DataFrame:
    """
    Lee Excel a DataFrame (openpyxl), dtype=str, sin NaN.
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


def resolve_column(df: pd.DataFrame, canonical: str) -> str:
    """
    Busca una columna cuyo nombre (strip, lower) coincida con el canonical dado.
    Útil para casos como 'Autor(es) ' con espacio al final.
    """
    target = canonical.strip().lower()
    for c in df.columns:
        if c.strip().lower() == target:
            return c
    return canonical  # si no existe, devolvemos el canonical (se comprobará más adelante)


# (Mejor esfuerzo) cambiar el texto “Browse files” por “Cargar listado”
st.markdown(
    """
<style>
button[title="Browse files"]{visibility: hidden;}
button[title="Browse files"]::after{
    content:" Cargar listado";
    visibility: visible;
    display:inline-block;
    padding:0.25rem 0.75rem;
    background:#2e7d32;
    color:white;
    border-radius:6px;
}
</style>
""",
    unsafe_allow_html=True,
)

# ---------------------------------- SIDEBAR ----------------------------------
with st.sidebar:
    st.image(LOGO_URL, use_container_width=True)
    st.caption("Elaborado por David Camelo para la Biblioteca de la Universidad El Bosque")

    st.markdown("### Plantillas oficiales:")
    st.markdown(f"- [Temáticas]({URL_PLANTILLA_TEMATICAS})")
    st.markdown(f"- [Términos a excluir]({URL_PLANTILLA_EXCLUSION})")

    st.markdown("### Archivos auxiliares (obligatorios)")
    # Mientras sincroniza bases, congelamos uploaders para evitar re-runs que “pierdan” el archivo
    bloqueados = ss.downloading or (not ss.bases_ready and ss.descarga_disparada)

    tem_up = st.file_uploader(
        "Temáticas (.xlsx, col1=término, col2=normalizado)",
        type=["xlsx"],
        key="tem_up_v82",
        disabled=bloqueados,
    )
    exc_up = st.file_uploader(
        "Términos a excluir (.xlsx, col1)",
        type=["xlsx"],
        key="exc_up_v82",
        disabled=bloqueados,
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
            ss.excluir_df = df[[df.columns[0]]].rename(
                columns={df.columns[0]: "excluir"}
            ).fillna("")
            st.success(f"Términos a excluir cargados: {len(ss.excluir_df)}")

    st.markdown("---")
    with st.expander("➕ Avanzado: subir bases Digital/Física manualmente", expanded=False):
        up_dig = st.file_uploader(
            "Base de datos de la colección Digital (.xlsx)",
            type=["xlsx"],
            key="up_dig_v82",
        )
        up_fis = st.file_uploader(
            "Base de datos de la colección Física (.xlsx)",
            type=["xlsx"],
            key="up_fis_v82",
        )
        if up_dig is not None:
            ss.df_digital = safe_read_excel(up_dig, "Colección Digital")
            st.success("Colección Digital (manual) cargada.")
        if up_fis is not None:
            ss.df_fisica = safe_read_excel(up_fis, "Colección Física")
            st.success("Colección Física (manual) cargada.")
        if ss.df_digital is not None and ss.df_fisica is not None:
            ss.bases_ready = True

# ---------------------------------- CABECERA ----------------------------------
st.title("Herramienta para la elaboración de bibliografías especializadas")

with st.expander("ℹ️ Información", expanded=True):
    st.markdown(
        f"""
- **Objetivo:** permitir la autogestión por programa/asignatura/tema y resaltar **términos a excluir** para depuración manual.  
- Usa siempre las bases oficiales (Digital/Física) o súbelas **manualmente** en la barra lateral.  
- **Plantillas:** [Temáticas]({URL_PLANTILLA_TEMATICAS}) y [Términos a excluir]({URL_PLANTILLA_EXCLUSION}).  
- Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app.  
- El proceso puede tardar algunos minutos; **puedes seguir usando tu equipo** (no cierres el navegador).
        """,
        help="Sección informativa general.",
    )

with st.expander("🧭 Paso a paso (recomendado)", expanded=True):
    st.markdown(
        f"""
**1) Sincronización (obligatoria una sola vez por sesión).**  
Haga clic en **“Sincronizar bases de datos oficiales”** (botón más abajo). Este paso conecta las colecciones **Digital** y **Física** con su última versión.  
> Este proceso tarda ~5 minutos. No cierre esta ventana.

**2) Cargue sus temáticas (Método A).**  
Descargue la plantilla de [Temáticas]({URL_PLANTILLA_TEMATICAS}).  
La **columna 1** incluye variaciones del término (con/sin tildes, otros idiomas).  
La **columna 2** agrupa/normaliza el término, que será el que verás en los resultados.

**3) Cargue términos a excluir (Método A).**  
Use la plantilla de [Términos a excluir]({URL_PLANTILLA_EXCLUSION}). Sirve para evitar falsos positivos (p. ej., buscar “ecología” sin recuperar “ginecología”).

**4) Parámetros de búsqueda.**  
Por defecto la búsqueda se hace en **Título** y **Temáticas** y se eliminan duplicados por **Url OA** (Digital) y **No. Topográfico** (Física). Puedes cambiarlos si lo necesitas (Método A).

**5) Ejecute e interprete.**  
Pulsa **Iniciar búsqueda** (Método A) o **Iniciar búsqueda avanzada** (Método B).  
Verás una tabla (vista de hasta 200 filas por defecto).  
Puedes **filtrar**, **marcar filas** y **exportar** en CSV/XLSX o **citas APA** (beta).

**6) Exportaciones.**  
El Excel del Método A incluye la **bitácora por término** y resalta coincidencias con **términos a excluir**.  
Las exportaciones “solo seleccionados” respetan lo marcado en la tabla.

**7) Nueva búsqueda.**  
Pulsa **Nueva búsqueda** para cargar otras temáticas y términos **sin re-sincronizar** las bases.  
Al cerrar la pestaña, la sesión se pierde (no se guarda nada).
        """
    )

# ---------------------------------- SINCRONIZACIÓN DE BASES ----------------------------------
st.markdown("#### Bases de datos de las colecciones de la Biblioteca")

if not ss.bases_ready:
    bcol = st.columns([1, 2, 1])[1]
    with bcol:
        btn_sync = st.button(
            "🔄 Sincronizar bases de datos oficiales",
            type="primary",
            use_container_width=True,
            disabled=ss.downloading or ss.descarga_disparada,
        )
    if btn_sync and not ss.downloading:
        ss.descarga_disparada = True
        ss.downloading = True

    if ss.downloading:
        st.info(
            "Sincronizando colecciones **Digital** y **Física**… "
            "Puedes cargar **Temáticas** y **Términos a excluir** mientras tanto. No cierres esta ventana."
        )

        # Digital
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

        # Física
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
            ss.downloading = False
            st.success("✅ Bases oficiales listas en memoria.")
else:
    st.success("✅ Bases oficiales listas en memoria (sesión).")
    st.caption("Consejo: usa **Nueva búsqueda** para repetir con otras temáticas sin re-sincronizar.")

# Si no hay bases, paramos aquí
if not ss.bases_ready:
    st.stop()

# ---------------------------------- NUEVA BÚSQUEDA ----------------------------------
col_nb = st.columns([1, 1, 4])[0]
with col_nb:
    if st.button("🧪 Nueva búsqueda", use_container_width=True):
        for k in ("tematicas_df", "excluir_df", "results_df", "bitacora_df", "last_method"):
            ss[k] = None
        st.toast(
            "Listo. Carga nuevas Temáticas/Términos (Método A) o define condiciones (Método B)."
        )

# ---------------------------------- SELECCIÓN DE MODO ----------------------------------
modo = st.radio(
    "Modo de búsqueda",
    [
        "Método A – listado de temáticas",
        "Método B – búsqueda avanzada (experimental)",
    ],
    horizontal=True,
)

# ---------------------------------- FUNCIONES COMUNES ----------------------------------
def _prepara_columnas(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).fillna("")


def _prep_export(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Renombrar columnas específicas (si existen)
    out = out.rename(columns={k: v for k, v in EXPORT_RENAME.items() if k in out.columns})
    # Unificar URL de acceso en digital/físico si aplica
    if "Url en LOCATE/IDEA" in out.columns and "Url de acceso" not in out.columns:
        out = out.rename(columns={"Url en LOCATE/IDEA": "Url de acceso"})
    # Omitir columnas administrativas
    drop_cols = [c for c in EXPORT_DROP_COLS if c in out.columns]
    if drop_cols:
        out = out.drop(columns=drop_cols)
    return out.fillna("")


def build_apa(row: pd.Series) -> str:
    """
    Generador APA simplificado con los campos disponibles.
    Intenta localizar la columna de autor aunque tenga espacios al final.
    """
    tit = str(row.get("Título", "")).strip()

    # Buscar columna de autor flexible: "Autor(es)" o similar
    autor_col = None
    for c in row.index:
        if c.strip().lower().startswith("autor"):
            autor_col = c
            break
    aut = str(row.get(autor_col, "")).strip() if autor_col else ""

    edit = str(row.get("Editorial", "")).strip()
    anio = str(row.get("Año de Publicación", "")).strip()
    bd = str(row.get("Base de datos", "")).strip()
    url = str(row.get("Url OA", "") or row.get("Url de acceso", "")).strip()
    isbn = str(row.get("ISBN", "")).strip()
    issn = str(row.get("ISSN1", "")).strip()
    topog = str(row.get("No. Topográfico", "")).strip()

    partes = []
    if aut and aut.upper() != "NO APLICA":
        partes.append(f"{aut}.")
    if anio and anio.upper() != "NO APLICA":
        partes.append(f"({anio}).")
    if tit:
        partes.append(f"{tit}.")
    if edit:
        partes.append(f"{edit}.")
    elif edit == "":
        partes.append("s.e.")

    acc = []
    if bd:
        acc.append(f"Disponible en {bd}")
    if url:
        acc.append(url)
    if topog and topog.upper() != "NO APLICA":
        acc.append(f"No. Topográfico: {topog}")
    if acc:
        partes.append("; ".join(acc) + ".")

    extras = []
    if isbn and isbn.upper() != "NO APLICA":
        extras.append(f"ISBN: {isbn}")
    if issn and issn.upper() != "NO APLICA":
        extras.append(f"ISSN: {issn}")
    if extras:
        partes.append(" ".join(extras) + ".")

    return " ".join([p for p in partes if p]).replace("..", ".")


# ======================================================================================
#                                     MÉTODO A
#          (Listado de temáticas, motor de búsqueda original v8.0 / v8.1)
# ======================================================================================
def metodo_a():
    # Validaciones propias del Método A
    if ss.tematicas_df is None or ss.excluir_df is None:
        st.warning(
            "Para el **Método A** debes cargar **Temáticas** y **Términos a excluir** "
            "en la barra lateral."
        )
        st.stop()

    st.subheader("Configuración de búsqueda y duplicados (Método A)")

    cols_dig = list(ss.df_digital.columns)
    cols_fis = list(ss.df_fisica.columns)
    common_cols = sorted(set(cols_dig + cols_fis))

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])

    with c1:
        col_busq1 = st.selectbox(
            "Búsqueda principal por",
            options=common_cols,
            index=get_index_or_first(common_cols, DEFAULT_COL_TITULO),
            key="col_busq1_v82",
        )

    with c2:
        col_busq2 = st.selectbox(
            "Búsqueda complementaria por",
            options=common_cols,
            index=get_index_or_first(common_cols, DEFAULT_COL_TEMATICAS),
            key="col_busq2_v82",
        )

    with c3:
        col_dup_dig = st.selectbox(
            "Columna de duplicados en Colección Digital",
            options=cols_dig,
            index=get_index_or_first(cols_dig, DEFAULT_DUP_DIGITAL),
            key="dup_dig_v82",
        )

    with c4:
        col_dup_fis = st.selectbox(
            "Columna de duplicados en Colección Física",
            options=cols_fis,
            index=get_index_or_first(cols_fis, DEFAULT_DUP_FISICA),
            key="dup_fis_v82",
        )

    st.caption(
        "Por defecto se usan “Título” y “Temáticas”, y duplicados por “Url OA” / “No. Topográfico”. "
        "Puedes cambiarlo si lo necesitas."
    )

    st.markdown("---")

    def _buscar(
        df: pd.DataFrame,
        fuente: str,
        col1: str,
        col2: str,
        tem_df: pd.DataFrame,
        barra,
        estado,
        total_steps: int,
        offset: int,
    ) -> pd.DataFrame:
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
                    md.loc[m2[m2].index, "Columna de coincidencia"] = md[
                        "Columna de coincidencia"
                    ].fillna(col2)
                    md["Fuente"] = fuente
                    res.append(md)

            frac = (i + 1) / max(N, 1)
            elapsed = time.time() - t0
            est_total = elapsed / max(frac, 1e-6)
            est_rem = max(0, int(est_total - elapsed))
            barra.progress(min(1.0, (offset + i + 1) / total_steps))
            estado.info(
                f"{fuente}: {i+1}/{N} términos • transcurrido: {int(elapsed)} s • restante: {est_rem} s"
            )

        if res:
            return pd.concat(res, ignore_index=True)
        return pd.DataFrame()

    def ejecutar_busqueda_a():
        excluye = [
            str(x).strip()
            for x in ss.excluir_df["excluir"].tolist()
            if str(x).strip() != ""
        ]
        barra = st.progress(0)
        estado = st.empty()

        DF_D = ss.df_digital.copy()
        DF_F = ss.df_fisica.copy()

        _prepara_columnas(DF_D, [col_busq1, col_busq2, col_dup_dig])
        _prepara_columnas(DF_F, [col_busq1, col_busq2, col_dup_fis])

        total = len(ss.tematicas_df) * 2
        res_d = _buscar(
            DF_D,
            "Digital",
            col_busq1,
            col_busq2,
            ss.tematicas_df,
            barra,
            estado,
            total_steps=total,
            offset=0,
        )
        res_f = _buscar(
            DF_F,
            "Física",
            col_busq1,
            col_busq2,
            ss.tematicas_df,
            barra,
            estado,
            total_steps=total,
            offset=len(ss.tematicas_df),
        )

        if not res_d.empty and col_dup_dig in res_d.columns:
            res_d = res_d.drop_duplicates(subset=[col_dup_dig], keep="first")
        if not res_f.empty and col_dup_fis in res_f.columns:
            res_f = res_f.drop_duplicates(subset=[col_dup_fis], keep="first")

        res = (
            pd.concat([res_d, res_f], ignore_index=True)
            if not (res_d.empty and res_f.empty)
            else pd.DataFrame()
        )

        # Persistimos
        ss.results_df = res
        ss.last_method = "A"

        # Bitácora con ceros
        tem = (
            ss.tematicas_df[["termino", "normalizado"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        fuentes = pd.DataFrame({"Fuente": ["Digital", "Física"]})
        grid = fuentes.assign(key=1).merge(
            tem.assign(key=1), on="key"
        ).drop("key", axis=1)

        if res.empty:
            counts = pd.DataFrame(
                columns=[
                    "Fuente",
                    "Temática",
                    "Temática normalizada",
                    "Resultados",
                ]
            )
        else:
            counts = (
                res.groupby(
                    ["Fuente", "Temática", "Temática normalizada"], dropna=False
                )
                .size()
                .reset_index(name="Resultados")
            )

        bit = (
            grid.merge(
                counts,
                how="left",
                left_on=["Fuente", "termino", "normalizado"],
                right_on=["Fuente", "Temática", "Temática normalizada"],
            )
            .drop(columns=["Temática", "Temática normalizada"], errors="ignore")
            .rename(columns={"termino": "Término", "normalizado": "Normalizado"})
        )

        bit["Resultados"] = bit["Resultados"].fillna(0).astype(int)
        bit = bit.sort_values(
            ["Fuente", "Resultados", "Término"], ascending=[True, False, True]
        ).reset_index(drop=True)
        ss.bitacora_df = bit

        barra.progress(1.0)
        estado.empty()
        st.success("Búsqueda finalizada (Método A).")

    # BOTÓN MÉTODO A
    if st.button("🚀 Iniciar búsqueda (Método A)", type="primary", use_container_width=True):
        try:
            ejecutar_busqueda_a()
        except Exception as e:
            st.error(f"Ocurrió un problema durante la búsqueda: {e}")


# ======================================================================================
#                                     MÉTODO B
#                        (Búsqueda avanzada tipo “descubridor”)
# ======================================================================================
def metodo_b():
    st.subheader("Búsqueda avanzada (Método B – experimental)")

    # Colecciones a incluir
    c_fuentes, c_tipos = st.columns([1, 1])

    with c_fuentes:
        colecciones = st.multiselect(
            "Colecciones a incluir",
            ["Digital", "Física"],
            default=["Digital", "Física"],
        )

    # Tipos normalizados (Libro, Revista, etc.)
    tipo_col = "Tipo de ítem normalizado mat especial"
    tipos_all_series = []
    if ss.df_digital is not None and tipo_col in ss.df_digital.columns:
        tipos_all_series.append(ss.df_digital[tipo_col])
    if ss.df_fisica is not None and tipo_col in ss.df_fisica.columns:
        tipos_all_series.append(ss.df_fisica[tipo_col])

    if tipos_all_series:
        tipos_all = (
            pd.concat(tipos_all_series).dropna().astype(str).sort_values().unique().tolist()
        )
    else:
        tipos_all = []

    with c_tipos:
        tipos_sel = st.multiselect(
            "Tipo de ítem normalizado",
            options=tipos_all,
            default=tipos_all,  # TODOS seleccionados por defecto
            help="Si no seleccionas nada, se usarán todos los tipos disponibles.",
        )

    st.write(
        "Define una o varias condiciones. Se aplican en orden y se combinan "
        "con **Y (AND)**, **O (OR)** o **NO (NOT)**."
    )

    num_cond = st.number_input(
        "Número de condiciones",
        min_value=1,
        max_value=6,
        value=1,
        step=1,
    )

    CAMPOS = [
        "Cualquier campo",
        "Título",
        "Autor(es)",
        "Temáticas",
        "Editorial",
        "Año de Publicación",
    ]
    OPERADORES = ["Contiene", "No contiene", "Frase exacta", "Comienza con"]

    condiciones: List[Dict[str, Any]] = []
    for i in range(int(num_cond)):
        st.markdown(f"**Condición {i+1}**")
        c1, c2, c3, c4 = st.columns([0.9, 1.1, 1.0, 1.4])

        with c1:
            if i == 0:
                conector = st.selectbox(
                    "Conector",
                    ["(primera)"],
                    key=f"conn_{i}_v82",
                    disabled=True,
                )
            else:
                conector = st.selectbox(
                    "Conector",
                    ["Y (AND)", "O (OR)", "NO (NOT)"],
                    key=f"conn_{i}_v82",
                )

        with c2:
            campo = st.selectbox(
                "Campo",
                CAMPOS,
                key=f"campo_{i}_v82",
            )

        with c3:
            operador = st.selectbox(
                "Operador",
                OPERADORES,
                key=f"op_{i}_v82",
            )

        with c4:
            valor = st.text_input("Valor", key=f"valor_{i}_v82")

        condiciones.append(
            {
                "conector": conector,
                "campo": campo,
                "operador": operador,
                "valor": valor,
            }
        )

    st.markdown("")
    if st.button("✏️ Iniciar búsqueda avanzada (Método B)", use_container_width=True):
        try:
            ejecutar_busqueda_b(
                colecciones=colecciones,
                tipos_sel=tipos_sel,
                condiciones=condiciones,
            )
        except Exception as e:
            st.error(f"Ocurrió un problema durante la búsqueda avanzada: {e}")


def _cond_mask_for_df(
    df: pd.DataFrame, campo: str, operador: str, valor: str
) -> pd.Series:
    """
    Construye la máscara booleana para una condición sobre un DataFrame concreto.
    """
    if df.empty or not valor:
        return pd.Series(False, index=df.index)

    valor_norm = str(valor).strip().lower()

    # Resolver columnas "canónicas"
    col_titulo = resolve_column(df, "Título")
    col_autor = resolve_column(df, "Autor(es)")
    col_tem = resolve_column(df, "Temáticas")
    col_edit = resolve_column(df, "Editorial")
    col_anio = resolve_column(df, "Año de Publicación")

    if campo == "Título":
        cols = [col_titulo]
    elif campo == "Autor(es)":
        cols = [col_autor]
    elif campo == "Temáticas":
        cols = [col_tem]
    elif campo == "Editorial":
        cols = [col_edit]
    elif campo == "Año de Publicación":
        cols = [col_anio]
    else:  # Cualquier campo
        cols = [col_titulo, col_autor, col_tem, col_edit, col_anio]

    # Filtrar columnas que realmente existan
    cols = [c for c in cols if c in df.columns]

    if not cols:
        return pd.Series(False, index=df.index)

    base_mask = pd.Series(False, index=df.index)

    for c in cols:
        serie = df[c].astype(str).fillna("").str.lower()
        if operador == "Contiene" or operador == "No contiene":
            m = serie.str.contains(valor_norm, na=False)
        elif operador == "Frase exacta":
            m = serie == valor_norm
        elif operador == "Comienza con":
            m = serie.str.startswith(valor_norm, na=False)
        else:
            m = pd.Series(False, index=df.index)
        base_mask = base_mask | m

    if operador == "No contiene":
        return ~base_mask
    else:
        return base_mask


def ejecutar_busqueda_b(
    colecciones: List[str],
    tipos_sel: List[str],
    condiciones: List[Dict[str, Any]],
):
    frames = []
    tipo_col = "Tipo de ítem normalizado mat especial"

    for fuente, df_base in (("Digital", ss.df_digital), ("Física", ss.df_fisica)):
        if fuente not in colecciones:
            continue
        if df_base is None or df_base.empty:
            continue

        df = df_base.copy()

        # Filtro por tipo normalizado (si hay selección)
        if tipos_sel and tipo_col in df.columns:
            df = df[df[tipo_col].isin(tipos_sel)]

        if df.empty:
            continue

        mask_global = None

        for idx, cond in enumerate(condiciones):
            valor = str(cond.get("valor", "")).strip()
            if not valor:
                continue

            campo = cond.get("campo", "Cualquier campo")
            operador = cond.get("operador", "Contiene")
            conector = cond.get("conector", "(primera)")

            cond_mask = _cond_mask_for_df(df, campo, operador, valor)

            if mask_global is None:
                # Primera condición
                if conector.startswith("NO"):
                    mask_global = ~cond_mask
                else:
                    mask_global = cond_mask
            else:
                if conector.startswith("Y"):
                    mask_global = mask_global & cond_mask
                elif conector.startswith("O"):
                    mask_global = mask_global | cond_mask
                elif conector.startswith("NO"):
                    # Interpretamos NO como "AND NOT" sobre el acumulado
                    mask_global = mask_global & (~cond_mask)
                else:
                    mask_global = mask_global & cond_mask

        if mask_global is None:
            continue

        sub = df[mask_global].copy()
        if sub.empty:
            continue

        sub["Fuente"] = fuente
        frames.append(sub)

    if frames:
        res = pd.concat(frames, ignore_index=True)
    else:
        res = pd.DataFrame()

    # Eliminamos duplicados por Url OA / No. Topográfico si existen
    if not res.empty:
        if DEFAULT_DUP_DIGITAL in res.columns:
            res = res.drop_duplicates(subset=[DEFAULT_DUP_DIGITAL, DEFAULT_DUP_FISICA],
                                      keep="first")

    ss.results_df = res
    ss.bitacora_df = None
    ss.last_method = "B"

    st.success(f"Búsqueda avanzada finalizada. Resultados: {len(res):,}")


# ======================================================================================
#                  EJECUTAR SEGÚN EL MODO SELECCIONADO (A o B)
# ======================================================================================
if modo.startswith("Método A"):
    metodo_a()
else:
    metodo_b()

# ---------------------------------- RESULTADOS + FILTROS/SELECCIÓN ----------------------------------
st.subheader("Resultados")

if ss.results_df is None or ss.results_df.empty:
    st.info("Aún no hay resultados. Ejecuta la búsqueda en el modo seleccionado.")
else:
    res = ss.results_df.copy()

    # Filtros rápidos comunes
    colf1, colf2, colf3 = st.columns([1, 1, 2])
    with colf1:
        filtro_fuente = st.multiselect(
            "Fuente",
            options=sorted(res["Fuente"].dropna().unique().tolist())
            if "Fuente" in res.columns
            else [],
            default=None,
        )
    with colf2:
        col_tema_norm = "Temática normalizada"
        temas_norm = (
            sorted(res[col_tema_norm].dropna().unique().tolist())
            if col_tema_norm in res.columns
            else []
        )
        filtro_tema = st.multiselect(
            "Temática normalizada",
            options=temas_norm,
            default=None,
        )
    with colf3:
        tipon_col = "Tipo de ítem normalizado mat especial"
        tipo_opts = (
            sorted(res.get(tipon_col, pd.Series(dtype=str)).dropna().unique().tolist())
            if tipon_col in res.columns
            else []
        )
        filtro_tipo = st.multiselect(
            "Tipo normalizado",
            options=tipo_opts,
            default=None,
        )

    if filtro_fuente:
        res = res[res["Fuente"].isin(filtro_fuente)]
    if filtro_tema and col_tema_norm in res.columns:
        res = res[res[col_tema_norm].isin(filtro_tema)]
    if filtro_tipo and tipon_col in res.columns:
        res = res[res[tipon_col].isin(filtro_tipo)]

    st.caption(f"Filas totales (después de filtros): **{len(res):,}**")

    # Columna de selección (checkbox)
    res_view = res.copy()
    if "__Seleccionar__" not in res_view.columns:
        res_view.insert(0, "__Seleccionar__", False)

    cva, cvb = st.columns([1, 1])
    with cva:
        show_all = st.checkbox("Mostrar todas las filas (Vista)", value=False)
    with cvb:
        limit = st.number_input(
            "Filas a mostrar (Vista)",
            min_value=50,
            max_value=10000,
            value=200,
            step=50,
        )

    res_view_show = res_view if show_all else res_view.head(int(limit))

    res_view_show = st.data_editor(
        res_view_show,
        use_container_width=True,
        height=520,
        column_config={
            "__Seleccionar__": st.column_config.CheckboxColumn("Seleccionar"),
        },
        disabled=[c for c in res_view_show.columns if c != "__Seleccionar__"],
        key="data_editor_res_v82",
    )

    seleccion_mask = (
        res_view_show["__Seleccionar__"]
        if "__Seleccionar__" in res_view_show.columns
        else pd.Series(False, index=res_view_show.index)
    )
    seleccionados = res_view_show[seleccion_mask].drop(
        columns=["__Seleccionar__"], errors="ignore"
    )
    st.caption(f"Seleccionados en la vista: **{len(seleccionados):,}**")

    # ---------------------------------- EXPORTACIONES ----------------------------------
    st.markdown("##### Exportaciones")

    colx1, colx2, colx3, colx4, colx5 = st.columns([1.2, 1.2, 1.6, 1.6, 2])

    # CSV completo (filtrado)
    with colx1:
        st.download_button(
            "⬇️ CSV (todo lo filtrado)",
            data=_prep_export(res).to_csv(index=False).encode("utf-8"),
            file_name="resultados_filtrados.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # CSV de seleccionados
    with colx2:
        st.download_button(
            "⬇️ CSV (solo seleccionados)",
            data=_prep_export(
                seleccionados if not seleccionados.empty else res.head(0)
            ).to_csv(index=False).encode("utf-8"),
            file_name="resultados_seleccionados.csv",
            mime="text/csv",
            disabled=seleccionados.empty,
            use_container_width=True,
        )

    # Excel completo / resaltado + Bitácora SOLO para Método A
    with colx3:
        if ss.last_method == "A":
            excluye = [
                str(x).strip()
                for x in ss.excluir_df["excluir"].tolist()
                if str(x).strip() != ""
            ]
            import xlsxwriter

            xbio = io.BytesIO()
            writer = pd.ExcelWriter(xbio, engine="xlsxwriter")

            res_x = _prep_export(res)
            res_x.to_excel(writer, index=False, sheet_name="Resultados")
            wb = writer.book
            ws = writer.sheets["Resultados"]
            fmt = wb.add_format({"bg_color": "#FFF599"})

            cols = list(res_x.columns)
            # Localizar columnas de título y temáticas después de renombres
            try:
                col_tit_idx = cols.index(
                    EXPORT_RENAME.get(DEFAULT_COL_TITULO, DEFAULT_COL_TITULO)
                )
            except ValueError:
                try:
                    col_tit_idx = cols.index(DEFAULT_COL_TITULO)
                except ValueError:
                    col_tit_idx = None

            try:
                col_tem_idx = cols.index(
                    EXPORT_RENAME.get(DEFAULT_COL_TEMATICAS, DEFAULT_COL_TEMATICAS)
                )
            except ValueError:
                try:
                    col_tem_idx = cols.index(DEFAULT_COL_TEMATICAS)
                except ValueError:
                    col_tem_idx = None

            excl_norm = [normalize_text(x) for x in excluye]

            for r in range(1, len(res_x) + 1):
                if col_tit_idx is not None:
                    v = normalize_text(res_x.iloc[r - 1, col_tit_idx])
                    if any(t in v for t in excl_norm):
                        ws.write(r, col_tit_idx, res_x.iloc[r - 1, col_tit_idx], fmt)
                if col_tem_idx is not None:
                    v = normalize_text(res_x.iloc[r - 1, col_tem_idx])
                    if any(t in v for t in excl_norm):
                        ws.write(r, col_tem_idx, res_x.iloc[r - 1, col_tem_idx], fmt)

            if ss.bitacora_df is not None:
                ss.bitacora_df.to_excel(
                    writer, index=False, sheet_name="Bitácora"
                )

            writer.close()
            xbio.seek(0)
            st.download_button(
                "⬇️ Excel (filtrado + resaltado + Bitácora)",
                data=xbio.getvalue(),
                file_name="resultados_filtrados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        else:
            # Método B: Excel simple con los resultados filtrados
            bio_all = io.BytesIO()
            with pd.ExcelWriter(bio_all, engine="xlsxwriter") as w_all:
                _prep_export(res).to_excel(w_all, index=False, sheet_name="Resultados")
            bio_all.seek(0)
            st.download_button(
                "⬇️ Excel (todo lo filtrado)",
                data=bio_all.getvalue(),
                file_name="resultados_filtrados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    # Excel de seleccionados (sin resaltado)
    with colx4:
        if not seleccionados.empty:
            sel_x = _prep_export(seleccionados)
            bio_sel = io.BytesIO()
            with pd.ExcelWriter(bio_sel, engine="xlsxwriter") as wsel:
                sel_x.to_excel(wsel, index=False, sheet_name="Seleccionados")
            bio_sel.seek(0)
            st.download_button(
                "⬇️ Excel (solo seleccionados)",
                data=bio_sel.getvalue(),
                file_name="resultados_seleccionados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        else:
            st.download_button(
                "⬇️ Excel (solo seleccionados)",
                data=b"",
                file_name="resultados_seleccionados.xlsx",
                disabled=True,
                use_container_width=True,
            )

    # Citas APA (beta) sobre seleccionados
    with colx5:
        if not seleccionados.empty:
            citas = [build_apa(r) for _, r in seleccionados.iterrows()]
            txt = "\n\n".join(c for c in citas if c.strip())
            st.download_button(
                "🧾 Citas APA (seleccionados)",
                data=txt.encode("utf-8"),
                file_name="citas_apa.txt",
                mime="text/plain",
                use_container_width=True,
            )
        else:
            st.download_button(
                "🧾 Citas APA (seleccionados)",
                data="".encode("utf-8"),
                file_name="citas_apa.txt",
                mime="text/plain",
                use_container_width=True,
                disabled=True,
            )

# ---------------------------------- BITÁCORA ----------------------------------
st.subheader("📑 Bitácora por término")

if ss.last_method != "A":
    st.info(
        "La bitácora detallada se genera únicamente con el **Método A (listado de temáticas)**."
    )
elif ss.bitacora_df is None or ss.bitacora_df.empty:
    st.info("Aún no hay bitácora. Ejecuta la búsqueda con el **Método A**.")
else:
    st.dataframe(ss.bitacora_df, use_container_width=True, height=360)
    st.download_button(
        "Descargar bitácora (.csv)",
        data=ss.bitacora_df.to_csv(index=False).encode("utf-8"),
        file_name="bitacora_por_termino.csv",
        mime="text/csv",
        use_container_width=True,
    )
