# -*- coding: utf-8 -*-
# Herramienta para la elaboración de bibliografías especializadas
# v8.2 – Modo A (plantillas) + Modo B (búsqueda avanzada), sin tocar el motor base

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

LOGO_URL = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png"

URL_DIGITAL = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx"
URL_FISICA = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx"

URL_PLANTILLA_TEMATICAS = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx"
URL_PLANTILLA_EXCLUSION = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx"

DEFAULT_COL_TITULO = "Título"
DEFAULT_COL_TEMATICAS = "Temáticas"
DEFAULT_DUP_DIGITAL = "Url OA"
DEFAULT_DUP_FISICA = "No. Topográfico"

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
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

# Bases
ss.setdefault("df_digital", None)
ss.setdefault("df_fisica", None)
ss.setdefault("bases_ready", False)

# Descarga
ss.setdefault("downloading", False)
ss.setdefault("descarga_disparada", False)

# Insumos método A
ss.setdefault("tematicas_df", None)
ss.setdefault("excluir_df", None)

# Resultados
ss.setdefault("results_df", None)
ss.setdefault("bitacora_df", None)

# Modo de búsqueda: "A" (plantillas) o "B" (avanzada)
ss.setdefault("modo_busqueda", "A")

# Condiciones para método B (lista de dicts)
ss.setdefault(
    "condiciones_b",
    [
        {
            "op": "Y",  # operador con la condición anterior (primera se ignora)
            "campo": "Título",
            "modo": "Contiene",
            "valor": "",
        }
    ],
)

# ---------------------------------- UTILIDADES ----------------------------------
def normalize_text(s: Any) -> str:
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
    """Descarga con barra y reintentos. Devuelve BytesIO."""
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
    Además, limpia espacios en los nombres de columnas.
    """
    try:
        with st.spinner(f"Procesando {label}…"):
            df = pd.read_excel(bio_or_file, engine="openpyxl", dtype=str)
            if not isinstance(df, pd.DataFrame):
                raise ValueError("El archivo no es una hoja de cálculo válida.")
            df = df.fillna("")
            df.columns = [str(c).strip() for c in df.columns]
            return df
    except Exception as e:
        raise RuntimeError(f"No fue posible procesar {label}: {e}") from e


def get_index_or_first(options: List[str], value: str) -> int:
    try:
        return options.index(value)
    except Exception:
        return 0


def find_column_by_label(all_columns: List[str], label: str) -> str | None:
    """Busca una columna coincidiendo por nombre normalizado (lower + strip)."""
    target = label.strip().lower()
    for c in all_columns:
        if str(c).strip().lower() == target:
            return c
    return None


def get_value_by_alias(row: pd.Series, label: str) -> str:
    """Devuelve el valor de una columna identificada por label normalizado."""
    target = label.strip().lower()
    for c in row.index:
        if str(c).strip().lower() == target:
            return str(row[c]).strip()
    return ""


# CSS para cambiar "Browse files" → "Cargar listado" (mejor esfuerzo)
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
    st.caption(
        "Elaborado por David Camelo para la Biblioteca de la Universidad El Bosque"
    )

    st.markdown("### Plantillas oficiales:")
    st.markdown(f"- [Temáticas]({URL_PLANTILLA_TEMATICAS})")
    st.markdown(f"- [Términos a excluir]({URL_PLANTILLA_EXCLUSION})")

    st.markdown("### Archivos auxiliares (obligatorios, solo Modo A)")
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
            ss.tematicas_df = (
                df[[df.columns[0], df.columns[1]]]
                .rename(columns={df.columns[0]: "termino", df.columns[1]: "normalizado"})
                .fillna("")
            )
            st.success(f"Temáticas cargadas: {len(ss.tematicas_df)}")
        if exc_up is not None:
            df = safe_read_excel(exc_up, "Términos a excluir")
            ss.excluir_df = (
                df[[df.columns[0]]]
                .rename(columns={df.columns[0]: "excluir"})
                .fillna("")
            )
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

**2) Modo A (listados) – Cargar temáticas.**  
Descargue la plantilla de [Temáticas]({URL_PLANTILLA_TEMATICAS}).  
La **columna 1** incluye variaciones del término (con/sin tildes, otros idiomas).  
La **columna 2** agrupa/normaliza el término, que será el que verás en los resultados.

**3) Modo A – Términos a excluir.**  
Use la plantilla de [Términos a excluir]({URL_PLANTILLA_EXCLUSION}). Sirve para evitar falsos positivos (p. ej., buscar “ecología” sin recuperar “ginecología”).

**4) Modo A – Parámetros.**  
Por defecto la búsqueda se hace en **Título** y **Temáticas** y se eliminan duplicados por **Url OA** (Digital) y **No. Topográfico** (Física). Puedes cambiarlos si lo necesitas.

**5) Modo B (búsqueda avanzada por campos).**  
En el selector de modo, elija **“Búsqueda avanzada (Método B)”** para definir condiciones por **Título, Autor(es), Temáticas, Editorial, Año**, etc.  
Puede combinar condiciones con **Y / O / NO** y aplicar filtros por tipo de ítem.

**6) Ejecute e interprete.**  
Pulsa **Iniciar búsqueda** (Modo A) o **Ejecutar búsqueda avanzada** (Modo B).  
Verás una tabla (vista de hasta 200 filas por defecto). Puedes **filtrar**, **marcar filas** y **exportar**.

**7) Nueva búsqueda.**  
Pulsa **Nueva búsqueda** para cargar otros insumos **sin re-sincronizar** las bases.  
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
            "Puedes cargar **Temáticas** y **Términos a excluir** mientras tanto. "
            "No cierres esta ventana."
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
    st.caption(
        "Consejo: usa **Nueva búsqueda** para repetir con otras temáticas sin re-sincronizar."
    )

if not ss.bases_ready:
    st.stop()

# ---------------------------------- NUEVA BÚSQUEDA ----------------------------------
col_nb = st.columns([1, 1, 4])[0]
with col_nb:
    if st.button("🧪 Nueva búsqueda", use_container_width=True):
        for k in ("tematicas_df", "excluir_df", "results_df", "bitacora_df"):
            ss[k] = None
        # dejamos bases intactas
        st.toast(
            "Listo. Carga nuevas Temáticas/Términos o define nuevas condiciones en el Modo B."
        )

# ---------------------------------- SELECCIÓN DE MODO ----------------------------------
st.markdown("### Modo de búsqueda")

modo_label = st.radio(
    "Elige cómo quieres buscar:",
    (
        "Listado de temáticas (Método A, plantillas)",
        "Búsqueda avanzada por campos (Método B)",
    ),
    index=0 if ss.modo_busqueda == "A" else 1,
)
ss.modo_busqueda = "A" if "Listado" in modo_label else "B"

# ---------------------------------- MODO A: PLANTILLAS (motor v8.0) ----------------------------------
if ss.modo_busqueda == "A":
    # Validaciones de insumos
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
        "Por defecto se usan “Título” y “Temáticas”, y duplicados por “Url OA” / "
        "“No. Topográfico”. Puedes cambiarlo si lo necesitas."
    )

    st.markdown("---")

    # ---- Funciones del motor A ----
    def _prepara_columnas(df: pd.DataFrame, cols: List[str]):
        for c in cols:
            if c in df.columns:
                df[c] = df[c].astype(str).fillna("")

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
                f"{fuente}: {i+1}/{N} términos • transcurrido: {int(elapsed)} s "
                f"• restante: {est_rem} s"
            )

        if res:
            return pd.concat(res, ignore_index=True)
        return pd.DataFrame()

    def ejecutar_busqueda_modo_a(
        col_busq1: str, col_busq2: str, col_dup_dig: str, col_dup_fis: str
    ):
        excluye = [
            str(x).strip()
            for x in (ss.excluir_df["excluir"].tolist() if ss.excluir_df is not None else [])
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

        ss.results_df = res

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
                columns=["Fuente", "Temática", "Temática normalizada", "Resultados"]
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
        st.success("Búsqueda finalizada (Modo A).")

    # Botón de búsqueda A
    if st.button("🚀 Iniciar búsqueda (Método A)", type="primary", use_container_width=True):
        try:
            ejecutar_busqueda_modo_a(col_busq1, col_busq2, col_dup_dig, col_dup_fis)
        except Exception as e:
            st.error(f"Ocurrió un problema durante la búsqueda: {e}")

# ---------------------------------- MODO B: BÚSQUEDA AVANZADA ----------------------------------
else:
    st.subheader("Búsqueda avanzada por campos (Método B)")

    all_cols = sorted(
        set(list(ss.df_digital.columns) + list(ss.df_fisica.columns))
    )

    campos_disponibles = [
        "Título",
        "Autor(es)",
        "Temáticas",
        "Editorial",
        "Año de Publicación",
    ]
    modos_disponibles = ["Contiene", "No contiene", "Frase exacta", "Comienza con"]

    st.markdown(
        "Define una o varias condiciones. Puedes combinar con **Y**, **O** o **NO**. "
        "La primera condición no necesita operador."
    )

    nuevas_cond: List[Dict[str, str]] = []
    for idx, cond in enumerate(ss.condiciones_b):
        col1, col2, col3, col4 = st.columns([0.6, 1.2, 1.2, 3])

        with col1:
            if idx == 0:
                st.markdown("Operador")
                st.caption("— (primera condición)")
                op = "Y"
            else:
                op = st.selectbox(
                    "Operador",
                    options=["Y", "O", "NO"],
                    index=get_index_or_first(["Y", "O", "NO"], cond.get("op", "Y")),
                    key=f"op_b_{idx}",
                )

        with col2:
            campo = st.selectbox(
                "Campo",
                options=campos_disponibles,
                index=get_index_or_first(campos_disponibles, cond.get("campo", "Título")),
                key=f"campo_b_{idx}",
            )

        with col3:
            modo = st.selectbox(
                "Coincidencia",
                options=modos_disponibles,
                index=get_index_or_first(modos_disponibles, cond.get("modo", "Contiene")),
                key=f"modo_b_{idx}",
            )

        with col4:
            valor = st.text_input(
                "Texto",
                value=cond.get("valor", ""),
                key=f"valor_b_{idx}",
            )

        nuevas_cond.append(
            {"op": op, "campo": campo, "modo": modo, "valor": valor}
        )

    ss.condiciones_b = nuevas_cond

    col_btn1, col_btn2 = st.columns([1, 1])
    with col_btn1:
        if st.button("➕ Agregar condición"):
            ss.condiciones_b.append(
                {"op": "Y", "campo": "Título", "modo": "Contiene", "valor": ""}
            )
    with col_btn2:
        if st.button("➖ Quitar última condición") and len(ss.condiciones_b) > 1:
            ss.condiciones_b.pop()

    # Filtro por tipo normalizado
    tipon_col = find_column_by_label(
        all_cols, "Tipo de ítem normalizado mat especial"
    )
    filtro_tipo = []
    if tipon_col:
        dfD_t = ss.df_digital.copy()
        dfF_t = ss.df_fisica.copy()
        tipos_opts = sorted(
            set(
                dfD_t.get(tipon_col, pd.Series(dtype=str)).dropna().unique().tolist()
            ).union(
                set(
                    dfF_t.get(tipon_col, pd.Series(dtype=str))
                    .dropna()
                    .unique()
                    .tolist()
                )
            )
        )
        filtro_tipo = st.multiselect(
            "Filtrar por tipo de ítem normalizado",
            options=tipos_opts,
            default=[],
        )

    st.markdown("---")

    def ejecutar_busqueda_modo_b(
        condiciones: List[Dict[str, str]], filtro_tipo: List[str]
    ):
        dfD = ss.df_digital.copy()
        dfD["Fuente"] = "Digital"
        dfF = ss.df_fisica.copy()
        dfF["Fuente"] = "Física"
        base = pd.concat([dfD, dfF], ignore_index=True)

        for c in base.columns:
            base[c] = base[c].astype(str).fillna("")

        if not condiciones:
            st.warning("Debes definir al menos una condición.")
            return

        # Índice de columnas normalizadas
        col_map = {str(c).strip().lower(): c for c in base.columns}

        def resolve_col(label: str) -> str | None:
            return col_map.get(label.strip().lower())

        mask = pd.Series(True, index=base.index)
        any_applied = False

        for idx, cond in enumerate(condiciones):
            texto = cond.get("valor", "").strip()
            if not texto:
                continue

            campo_label = cond.get("campo", "Título")
            modo = cond.get("modo", "Contiene")
            op = cond.get("op", "Y")

            col_name = resolve_col(campo_label)
            if not col_name:
                continue

            serie = base[col_name].astype(str)

            txt_norm = normalize_text(texto).lower()

            def cmp(v: str) -> bool:
                v_norm = normalize_text(v).lower()
                if modo == "Contiene":
                    return txt_norm in v_norm
                elif modo == "No contiene":
                    return txt_norm not in v_norm
                elif modo == "Frase exacta":
                    return v_norm == txt_norm
                elif modo == "Comienza con":
                    return v_norm.startswith(txt_norm)
                else:
                    return txt_norm in v_norm

            cond_mask = serie.map(cmp)

            if idx == 0:
                mask = cond_mask
            else:
                if op == "Y":
                    mask = mask & cond_mask
                elif op == "O":
                    mask = mask | cond_mask
                elif op == "NO":
                    mask = mask & (~cond_mask)
                else:
                    mask = mask & cond_mask

            any_applied = True

        if not any_applied:
            st.warning("No hay condiciones válidas de búsqueda (todas vacías).")
            return

        if filtro_tipo and tipon_col:
            mask = mask & base[tipon_col].isin(filtro_tipo)

        res = base[mask].copy()

        # Aseguramos columnas usadas en filtros posteriores
        for col in ["Temática", "Temática normalizada"]:
            if col not in res.columns:
                res[col] = ""

        ss.results_df = res

        # Bitácora simple por Fuente
        if res.empty:
            ss.bitacora_df = pd.DataFrame(
                columns=["Fuente", "Resultados"]
            )
        else:
            bit = (
                res.groupby(["Fuente"], dropna=False)
                .size()
                .reset_index(name="Resultados")
                .sort_values(["Fuente"])
                .reset_index(drop=True)
            )
            ss.bitacora_df = bit

        st.success(f"Búsqueda avanzada finalizada (Modo B). Resultados: {len(res):,}")

    if st.button(
        "🚀 Ejecutar búsqueda avanzada (Método B)",
        type="primary",
        use_container_width=True,
    ):
        try:
            ejecutar_busqueda_modo_b(ss.condiciones_b, filtro_tipo)
        except Exception as e:
            st.error(f"Ocurrió un problema durante la búsqueda avanzada: {e}")

# ---------------------------------- RESULTADOS + FILTROS/SELECCIÓN ----------------------------------
st.subheader("Resultados")

if ss.results_df is None or ss.results_df.empty:
    st.info("Aún no hay resultados. Ejecuta la búsqueda en el modo que prefieras.")
else:
    res = ss.results_df.copy()

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
            "Temática normalizada", options=temas_norm, default=None
        )
    with colf3:
        tipon_col_res = find_column_by_label(
            list(res.columns), "Tipo de ítem normalizado mat especial"
        )
        tipo_opts = (
            sorted(
                res.get(tipon_col_res, pd.Series(dtype=str)).dropna().unique().tolist()
            )
            if tipon_col_res
            else []
        )
        filtro_tipo_res = st.multiselect(
            "Tipo normalizado", options=tipo_opts, default=None
        )

    if filtro_fuente:
        res = res[res["Fuente"].isin(filtro_fuente)]
    if filtro_tema and "Temática normalizada" in res.columns:
        res = res[res["Temática normalizada"].isin(filtro_tema)]
    if filtro_tipo_res and tipon_col_res:
        res = res[res[tipon_col_res].isin(filtro_tipo_res)]

    st.caption(f"Filas totales (después de filtros): **{len(res):,}**")

    res_view = res.copy()
    if "__Seleccionar__" not in res_view.columns:
        res_view.insert(0, "__Seleccionar__", False)

    cva, cvb = st.columns([1, 1])
    with cva:
        show_all = st.checkbox("Mostrar todas las filas (Vista)", value=False)
    with cvb:
        limit = st.number_input(
            "Filas a mostrar (Vista)", min_value=50, max_value=10000, value=200, step=50
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

    # --------- Helpers de exportación ---------
    def _prep_export(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out = out.rename(
            columns={k: v for k, v in EXPORT_RENAME.items() if k in out.columns}
        )
        if "Url en LOCATE/IDEA" in out.columns and "Url de acceso" not in out.columns:
            out = out.rename(columns={"Url en LOCATE/IDEA": "Url de acceso"})
        drop_cols = [c for c in EXPORT_DROP_COLS if c in out.columns]
        if drop_cols:
            out = out.drop(columns=drop_cols)
        return out.fillna("")

    # ---------------------------------- EXPORTACIONES ----------------------------------
    st.markdown("##### Exportaciones")
    colx1, colx2, colx3, colx4, colx5 = st.columns([1.2, 1.2, 1.6, 1.6, 2])

    with colx1:
        st.download_button(
            "⬇️ CSV (todo lo filtrado)",
            data=_prep_export(res).to_csv(index=False).encode("utf-8"),
            file_name="resultados_filtrados.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with colx2:
        st.download_button(
            "⬇️ CSV (solo seleccionados)",
            data=_prep_export(seleccionados if not seleccionados.empty else res.head(0))
            .to_csv(index=False)
            .encode("utf-8"),
            file_name="resultados_seleccionados.csv",
            mime="text/csv",
            disabled=seleccionados.empty,
            use_container_width=True,
        )

    with colx3:
        excluye = []
        if ss.excluir_df is not None:
            excluye = [
                str(x).strip()
                for x in ss.excluir_df.get("excluir", pd.Series(dtype=str)).tolist()
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

        # Columnas de título y temáticas después del renombrado
        col_tit_name = (
            EXPORT_RENAME.get(DEFAULT_COL_TITULO, DEFAULT_COL_TITULO)
            if DEFAULT_COL_TITULO in cols
            or EXPORT_RENAME.get(DEFAULT_COL_TITULO, DEFAULT_COL_TITULO) in cols
            else None
        )
        col_tem_name = (
            EXPORT_RENAME.get(DEFAULT_COL_TEMATICAS, DEFAULT_COL_TEMATICAS)
            if DEFAULT_COL_TEMATICAS in cols
            or EXPORT_RENAME.get(DEFAULT_COL_TEMATICAS, DEFAULT_COL_TEMATICAS) in cols
            else None
        )

        col_tit = cols.index(col_tit_name) + 1 if col_tit_name in cols else None
        col_tem = cols.index(col_tem_name) + 1 if col_tem_name in cols else None

        excl_norm = [normalize_text(x) for x in excluye]

        for r in range(1, len(res_x) + 1):
            if col_tit is not None:
                v = normalize_text(res_x.iloc[r - 1, col_tit - 1])
                if any(t in v for t in excl_norm):
                    ws.write(r, col_tit - 1, res_x.iloc[r - 1, col_tit - 1], fmt)
            if col_tem is not None:
                v = normalize_text(res_x.iloc[r - 1, col_tem - 1])
                if any(t in v for t in excl_norm):
                    ws.write(r, col_tem - 1, res_x.iloc[r - 1, col_tem - 1], fmt)

        if ss.bitacora_df is not None and not ss.bitacora_df.empty:
            ss.bitacora_df.to_excel(writer, index=False, sheet_name="Bitácora")

        writer.close()
        xbio.seek(0)
        st.download_button(
            "⬇️ Excel (filtrado + resaltado + Bitácora)",
            data=xbio.getvalue(),
            file_name="resultados_filtrados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

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

    def build_apa(row: pd.Series) -> str:
        tit = str(row.get("Título", "")).strip()
        aut = get_value_by_alias(row, "Autor(es)")
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

    with colx5:
        if not seleccionados.empty:
            citas = [build_apa(r) for _, r in seleccionados.iterrows()]
            txt = "\n\n".join(c for c in citas if c.strip())
            st.download_button(
                "🧾 Citas APA (seleccionados) [beta]",
                data=txt.encode("utf-8"),
                file_name="citas_apa.txt",
                mime="text/plain",
                use_container_width=True,
            )
        else:
            st.download_button(
                "🧾 Citas APA (seleccionados) [beta]",
                data="".encode("utf-8"),
                file_name="citas_apa.txt",
                mime="text/plain",
                use_container_width=True,
                disabled=True,
            )

# ---------------------------------- BITÁCORA ----------------------------------
st.subheader("📑 Bitácora")
if ss.bitacora_df is None or ss.bitacora_df.empty:
    st.info("Aún no hay bitácora. Ejecuta una búsqueda para verla.")
else:
    st.dataframe(ss.bitacora_df, use_container_width=True, height=360)
    st.download_button(
        "Descargar bitácora (.csv)",
        data=ss.bitacora_df.to_csv(index=False).encode("utf-8"),
        file_name="bitacora_resultados.csv",
        mime="text/csv",
        use_container_width=True,
    )
