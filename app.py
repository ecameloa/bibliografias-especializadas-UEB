# -*- coding: utf-8 -*-
# Herramienta para la elaboración de bibliografías especializadas
# v8.2 – Métodos A/B, UI refinada, instrucciones por método

import io
import os
import time
import tempfile
from typing import List, Dict, Any

import pandas as pd
import requests
import streamlit as st

# ---------------------------------- CONFIGURACIÓN BÁSICA ----------------------------------
st.set_page_config(page_title="Herramienta de bibliografías", layout="wide")

LOGO_URL = "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png"

# URLs oficiales (Digital/Física y plantillas)
URL_DIGITAL = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx"
URL_FISICA = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx"

URL_PLANTILLA_TEMATICAS = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx"
URL_PLANTILLA_EXCLUSION = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx"

DEFAULT_COL_TITULO = "Título"
DEFAULT_COL_TEMATICAS = "Temáticas"
DEFAULT_DUP_DIGITAL = "Url OA"
DEFAULT_DUP_FISICA = "No. Topográfico"

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"  # noqa: E501
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

TIPO_NORMAL_COL = "Tipo de ítem normalizado mat especial"

# ---------------------------------- ESTADO GLOBAL ----------------------------------
ss = st.session_state

ss.setdefault("df_digital", None)
ss.setdefault("df_fisica", None)
ss.setdefault("bases_ready", False)

ss.setdefault("downloading", False)
ss.setdefault("descarga_disparada", False)

# Insumos método A
ss.setdefault("tematicas_df", None)
ss.setdefault("excluir_df", None)

# Resultados comunes (A y B)
ss.setdefault("results_df", None)
ss.setdefault("bitacora_df", None)

# Método de búsqueda actual: "A" (temáticas) o "B" (avanzada)
ss.setdefault("metodo", "A")

# Estado método B
ss.setdefault("b_num_cond", 2)
ss.setdefault("b_conds", [])  # lista de dicts con los parámetros de cada condición


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


def _head_content_length(url: str, timeout: int = 30) -> int | None:
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=UA)
        r.raise_for_status()
        cl = r.headers.get("Content-Length")
        return int(cl) if cl is not None else None
    except Exception:
        return None


def download_with_resume(
    url: str,
    label: str,
    container=None,
    max_retries: int = 5,
    chunk_size: int = 256 * 1024,
    timeout: int = 300,
) -> io.BytesIO:
    """
    Descarga con barra de progreso y reintentos. Devuelve BytesIO.
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
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    downloaded = 0
                    mode = "wb"

                r.raise_for_status()

                content_length = r.headers.get("Content-Length")
                expected_total = downloaded + int(content_length) if content_length else total_size  # noqa: E501

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


def safe_read_excel(bio_or_file, label: str = "archivo") -> pd.DataFrame:
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
    except Exception:  # pragma: no cover
        return 0


def _prepara_columnas(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).fillna("")


def _prep_export(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas y elimina las administrativas antes de exportar.
    """
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


def build_apa(row: pd.Series) -> str:
    """
    Generador APA simplificado usando los campos disponibles.
    """
    tit = str(row.get("Título", "")).strip()

    # Autor(es) puede venir con o sin espacio final
    aut = ""
    for col in ["Autor(es)", "Autor(es) "]:
        if col in row.index:
            val = str(row.get(col, "")).strip()
            if val and val.upper() != "NO APLICA":
                aut = val
                break

    edit = str(row.get("Editorial", "")).strip()
    anio = str(row.get("Año de Publicación", "")).strip()
    bd = str(row.get("Base de datos", "")).strip()
    url = str(row.get("Url OA", "") or row.get("Url de acceso", "")).strip()
    isbn = str(row.get("ISBN", "")).strip()
    issn = str(row.get("ISSN1", "")).strip()
    topog = str(row.get("No. Topográfico", "")).strip()

    partes: list[str] = []
    if aut:
        partes.append(f"{aut}.")
    if anio and anio.upper() != "NO APLICA":
        partes.append(f"({anio}).")
    if tit:
        partes.append(f"{tit}.")
    if edit:
        partes.append(f"{edit}.")
    elif edit == "":
        partes.append("s.e.")

    acc: list[str] = []
    if bd:
        acc.append(f"Disponible en {bd}")
    if url:
        acc.append(url)
    if topog and topog.upper() != "NO APLICA":
        acc.append(f"No. Topográfico: {topog}")
    if acc:
        partes.append("; ".join(acc) + ".")

    extras: list[str] = []
    if isbn and isbn.upper() != "NO APLICA":
        extras.append(f"ISBN: {isbn}")
    if issn and issn.upper() != "NO APLICA":
        extras.append(f"ISSN: {issn}")
    if extras:
        partes.append(" ".join(extras) + ".")

    return " ".join([p for p in partes if p]).replace("..", ".")


# CSS para cambiar el texto de "Browse files" (mejor esfuerzo)
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

    # Archivos auxiliares sólo para Método A
    if ss.metodo == "A":
        st.markdown("### Archivos auxiliares (obligatorios)")
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
                    columns={
                        df.columns[0]: "termino",
                        df.columns[1]: "normalizado",
                    }
                ).fillna("")
                st.success(f"Temáticas cargadas: {len(ss.tematicas_df)}")

            if exc_up is not None:
                df = safe_read_excel(exc_up, "Términos a excluir")
                ss.excluir_df = df[[df.columns[0]]].rename(
                    columns={df.columns[0]: "excluir"}
                ).fillna("")
                st.success(f"Términos a excluir cargados: {len(ss.excluir_df)}")
    else:
        st.markdown(
            "ℹ️ El **Método B** no requiere cargar plantillas. "
            "Selecciona el **Método A** si quieres usar listados de temáticas."
        )

    st.markdown("---")
    # Subir bases manualmente sólo si aún no se han cargado/sincronizado
    if not ss.bases_ready:
        with st.expander(
            "➕ Avanzado: subir bases Digital/Física manualmente", expanded=False
        ):
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
                st.success("✅ Bases oficiales listas en memoria (carga manual).")

# ---------------------------------- CUERPO PRINCIPAL ----------------------------------
st.title("Herramienta para la elaboración de bibliografías especializadas")

# --- Bloque de información general ---
with st.expander("ℹ️ Información general", expanded=True):
    st.markdown(
        f"""
- **Objetivo:** permitir la autogestión por programa/asignatura/tema y resaltar **términos a excluir** para depuración manual.  
- Usa siempre las bases oficiales (Digital/Física) o súbelas **manualmente** desde la barra lateral.  
- **Plantillas:** [Temáticas]({URL_PLANTILLA_TEMATICAS}) y [Términos a excluir]({URL_PLANTILLA_EXCLUSION}).  
- Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app.  
- El proceso puede tardar algunos minutos; **puedes seguir usando tu equipo** (no cierres el navegador).
        """
    )

# --- Sincronización de bases ---
st.markdown("#### Bases de datos de las colecciones de la Biblioteca")

if not ss.bases_ready:
    st.info(
        "Antes de buscar, sincroniza las bases de datos oficiales o carga los archivos "
        "desde la barra lateral (opción **Avanzado**)."
    )

    mid_col = st.columns([1, 2, 1])[1]
    with mid_col:
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
            "Puedes seguir usando tu equipo. No cierres esta ventana."
        )

        # Digital
        st.subheader("Descargando Base de datos de la colección Digital…")
        zona_dig = st.container()
        try:
            bio_d = download_with_resume(URL_DIGITAL, "Colección Digital", zona_dig)
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
            bio_f = download_with_resume(URL_FISICA, "Colección Física", zona_fis)
            st.caption("Colección Física: descarga completa. Verificando archivo…")
            ss.df_fisica = safe_read_excel(bio_f, "Colección Física")
            st.success("Base de datos de la colección Física lista ✓")
        except Exception as e:
            st.error(f"No fue posible descargar la base Física: {e}")
            ss.downloading = False

        if ss.df_digital is not None and ss.df_fisica is not None:
            ss.bases_ready = True
            ss.downloading = False
            st.success("✅ Bases oficiales listas en memoria (sesión).")

if not ss.bases_ready:
    st.stop()
else:
    st.success("✅ Bases oficiales listas en memoria (sesión).")

# ---------------------------------- SELECCIÓN DE MÉTODO ----------------------------------
st.markdown("### Selecciona el modo de búsqueda")

metodo_label = st.radio(
    "",
    (
        "Método A – listado de temáticas (plantilla)",
        "Método B – búsqueda avanzada tipo descubridor (experimental)",
    ),
    index=0 if ss.metodo == "A" else 1,
)
ss.metodo = "A" if metodo_label.startswith("Método A") else "B"

# --- Paso a paso según método ---
if ss.metodo == "A":
    with st.expander("🧭 Paso a paso – Método A (listado de temáticas)", expanded=True):
        st.markdown(
            f"""
**1) Sincronización (obligatoria una sola vez por sesión).**  
Si aún no lo has hecho, usa **“Sincronizar bases de datos oficiales”** o carga las bases desde *Avanzado* en la barra lateral.

**2) Cargue sus temáticas.**  
Descargue la plantilla de [Temáticas]({URL_PLANTILLA_TEMATICAS}).  
La **columna 1** incluye variaciones del término (con/sin tildes, otros idiomas).  
La **columna 2** agrupa/normaliza el término, que será el que verás en los resultados.

**3) Cargue términos a excluir.**  
Use la plantilla de [Términos a excluir]({URL_PLANTILLA_EXCLUSION}).  
Sirve para evitar falsos positivos (p. ej., buscar “ecología” sin recuperar “ginecología”).

**4) Parámetros de búsqueda.**  
Por defecto la búsqueda se hace en **Título** y **Temáticas**, y se eliminan duplicados por **Url OA** (Digital) y **No. Topográfico** (Física).  
Puedes cambiar estas columnas en la sección **Configuración de búsqueda y duplicados**.

**5) Ejecute la búsqueda.**  
Pulse **“🚀 Iniciar búsqueda (Método A)”**. Verá una tabla con los resultados (vista de hasta 200 filas por defecto).  
Podrá **filtrar**, **marcar filas** y **exportar** en CSV/XLSX o **citas APA** (beta).

**6) Exportaciones y bitácora.**  
El Excel incluye la **bitácora por término** y resalta coincidencias con **términos a excluir**.  
Las exportaciones “solo seleccionados” respetan lo marcado en la tabla.

**7) Nueva búsqueda.**  
Pulse **“Nueva búsqueda”** para cargar otras temáticas y términos **sin re-sincronizar** las bases.
Al cerrar la pestaña, la sesión se pierde (no se guarda nada).
            """
        )
else:
    with st.expander(
        "🧭 Paso a paso – Método B (búsqueda avanzada tipo descubridor)", expanded=True
    ):
        st.markdown(
            """
**1) Asegúrese de tener las bases sincronizadas.**  
Use el botón **“Sincronizar bases de datos oficiales”** (o cargue los archivos manualmente desde la barra lateral).

**2) Defina el alcance.**  
Seleccione las colecciones a incluir (Digital/Física) y, si lo desea, limite por tipo de ítem normalizado  
(p. ej., sólo **Libro**, sólo **Revista**, etc.).

**3) Construya las condiciones.**  
Cada condición tiene:
- un **conector** (primera, Y / AND, O / OR, NO / NOT),  
- un **campo** (Título, Autor(es), Temáticas, Editorial, Cualquier campo),  
- un **operador** (contiene / no contiene) y  
- un **valor** de búsqueda.

Las condiciones se aplican en orden, sobre la misma tabla de resultados:
- **Y (AND)** filtra más los resultados actuales.  
- **O (OR)** añade nuevos resultados que cumplan la condición.  
- **NO (NOT)** excluye de los resultados actuales los registros que cumplan la condición.

**4) Ejecute la búsqueda avanzada.**  
Pulse **“🚀 Iniciar búsqueda avanzada (Método B)”**. Verá una tabla con los títulos coincidentes.  
Podrá **filtrar**, **marcar filas** y **exportar** en CSV/XLSX o **citas APA** (beta).

**5) Nueva búsqueda.**  
Use el botón **“Nueva búsqueda”** para limpiar condiciones y resultados, sin volver a sincronizar las bases.
            """
        )

# ---------------------------------- NUEVA BÚSQUEDA (limpia insumos/resultados, NO bases) ----------------------------------
col_nb = st.columns([1, 1, 4])[0]
with col_nb:
    if st.button("🧪 Nueva búsqueda", use_container_width=True):
        for k in (
            "tematicas_df",
            "excluir_df",
            "results_df",
            "bitacora_df",
            "b_conds",
        ):
            ss[k] = None if k != "b_conds" else []
        st.toast("Listo. Carga nuevos términos o ajusta las condiciones para buscar de nuevo.")


# ==========================================================================================
# MÉTODO A – LISTADO DE TEMÁTICAS
# ==========================================================================================
def ejecutar_busqueda_metodo_a(col_busq1: str, col_busq2: str, col_dup_dig: str, col_dup_fis: str):  # noqa: E501
    if ss.tematicas_df is None or ss.excluir_df is None:
        st.warning(
            "Para usar el **Método A** debes cargar **Temáticas** y **Términos a excluir** "
            "en la barra lateral."
        )
        return

    excluye = [
        str(x).strip()
        for x in ss.excluir_df["excluir"].tolist()
        if str(x).strip() != ""
    ]

    barra = st.progress(0)
    estado = st.empty()

    DF_D = ss.df_digital.copy()
    DF_F = ss.df_fisica.copy()

    # Asegurar columnas como texto
    _prepara_columnas(DF_D, [col_busq1, col_busq2, col_dup_dig])
    _prepara_columnas(DF_F, [col_busq1, col_busq2, col_dup_fis])

    def _buscar(
        df: pd.DataFrame,
        fuente: str,
        tem_df: pd.DataFrame,
        offset: int,
        total_steps: int,
    ) -> pd.DataFrame:
        res_list: list[pd.DataFrame] = []
        tem = tem_df.copy()
        tem["termino"] = tem["termino"].astype(str).fillna("")
        tem["normalizado"] = tem["normalizado"].astype(str).fillna("")
        N = len(tem)
        t0 = time.time()

        for i, row in tem.iterrows():
            term = normalize_text(row["termino"])
            if term:
                m1 = DF_D[col_busq1].map(
                    lambda s: term in normalize_text(s)
                ) if fuente == "Digital" else DF_F[col_busq1].map(
                    lambda s: term in normalize_text(s)
                )
                m2 = DF_D[col_busq2].map(
                    lambda s: term in normalize_text(s)
                ) if fuente == "Digital" else DF_F[col_busq2].map(
                    lambda s: term in normalize_text(s)
                )
                src = DF_D if fuente == "Digital" else DF_F
                md = src[m1 | m2].copy()
                if not md.empty:
                    md["Temática"] = row["termino"]
                    md["Temática normalizada"] = row["normalizado"]
                    md["Columna de coincidencia"] = None
                    md.loc[m1[m1].index, "Columna de coincidencia"] = col_busq1
                    md.loc[m2[m2].index, "Columna de coincidencia"] = md[
                        "Columna de coincidencia"
                    ].fillna(col_busq2)
                    md["Fuente"] = fuente
                    res_list.append(md)

            frac = (i + 1) / max(N, 1)
            elapsed = time.time() - t0
            est_total = elapsed / max(frac, 1e-6)
            est_rem = max(0, int(est_total - elapsed))
            barra.progress(min(1.0, (offset + i + 1) / total_steps))
            estado.info(
                f"{fuente}: {i+1}/{N} términos • transcurrido: {int(elapsed)} s • restante: {est_rem} s"  # noqa: E501
            )

        if res_list:
            return pd.concat(res_list, ignore_index=True)
        return pd.DataFrame()

    total = len(ss.tematicas_df) * 2
    res_d = _buscar(
        DF_D,
        "Digital",
        ss.tematicas_df,
        offset=0,
        total_steps=total,
    )
    res_f = _buscar(
        DF_F,
        "Física",
        ss.tematicas_df,
        offset=len(ss.tematicas_df),
        total_steps=total,
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

    # --- bitácora por término ---
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
            res.groupby(["Fuente", "Temática", "Temática normalizada"], dropna=False)
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


# ==========================================================================================
# MÉTODO B – BÚSQUEDA AVANZADA
# ==========================================================================================
CAMPOS_B = {
    "Cualquier campo": None,
    "Título": "Título",
    "Autor(es)": "Autor(es)",
    "Temáticas": "Temáticas",
    "Editorial": "Editorial",
    "Año de Publicación": "Año de Publicación",
}

OPERADORES_B = ["Contiene", "No contiene"]
CONECTORES_B = ["(primera)", "Y (AND)", "O (OR)", "NO (NOT)"]


def _mask_condicion(base: pd.DataFrame, campo: str | None, operador: str, valor: str):
    """
    Genera una máscara booleana para una condición simple sobre `base`.
    """
    val_norm = normalize_text(valor).lower()

    if campo is None:
        series_list = []
        for c in [
            "Título",
            "Autor(es)",
            "Autor(es) ",
            "Temáticas",
            "Editorial",
            "Base de datos",
        ]:
            if c in base.columns:
                s = base[c].fillna("").astype(str).map(
                    lambda x: val_norm in normalize_text(x).lower()
                )
                series_list.append(s)
        if not series_list:
            mask = pd.Series(False, index=base.index)
        else:
            mask = series_list[0]
            for s in series_list[1:]:
                mask = mask | s
    else:
        if campo not in base.columns:
            return pd.Series(False, index=base.index)
        series = base[campo].fillna("").astype(str)
        mask = series.map(lambda x: val_norm in normalize_text(x).lower())

    if operador == "Contiene":
        return mask
    elif operador == "No contiene":
        return ~mask
    else:
        return mask


def ejecutar_busqueda_metodo_b(
    colecciones: list[str],
    tipos_sel: list[str],
    condiciones: List[Dict[str, Any]],
):
    # Construir tabla base Digital + Física
    DF_D = ss.df_digital.copy()
    DF_F = ss.df_fisica.copy()
    DF_D["Fuente"] = "Digital"
    DF_F["Fuente"] = "Física"
    base = pd.concat([DF_D, DF_F], ignore_index=True)

    if colecciones:
        base = base[base["Fuente"].isin(colecciones)]

    if tipos_sel and TIPO_NORMAL_COL in base.columns:
        base = base[base[TIPO_NORMAL_COL].isin(tipos_sel)]

    # Nos aseguramos que todas las columnas relevantes sean texto
    _prepara_columnas(
        base,
        [
            "Título",
            "Autor(es)",
            "Autor(es) ",
            "Temáticas",
            "Editorial",
            "Base de datos",
            "Año de Publicación",
        ],
    )

    res = None
    for idx, cond in enumerate(condiciones):
        valor = cond.get("valor", "").strip()
        if not valor:
            continue

        campo_key = cond.get("campo", "Cualquier campo")
        campo_col = CAMPOS_B.get(campo_key)
        operador = cond.get("operador", "Contiene")
        conector = cond.get("conector", "(primera)")

        mask = _mask_condicion(base, campo_col, operador, valor)

        # Primera condición: si el conector es NO se interpreta como "todo menos"
        if res is None:
            if conector.startswith("NO"):
                res = base[~mask].copy()
            else:
                res = base[mask].copy()
            continue

        # Resto de condiciones: se aplican sobre resultado actual y/o base
        if conector.startswith("Y"):
            # AND: se restringe el conjunto actual
            submask = mask.loc[res.index]
            res = res[submask].copy()
        elif conector.startswith("O"):
            # OR: se unen resultados actuales con nuevos hallazgos
            nuevos = base[mask].copy()
            res = (
                pd.concat([res, nuevos], ignore_index=True)
                .drop_duplicates()
                .reset_index(drop=True)
            )
        elif conector.startswith("NO"):
            # NOT: se excluyen del conjunto actual
            submask = mask.loc[res.index]
            res = res[~submask].copy()
        else:
            # Por si acaso, tratamos como nueva primera
            res = base[mask].copy()

    if res is None:
        res = base.copy()

    ss.results_df = res
    ss.bitacora_df = None
    st.success(f"Búsqueda avanzada finalizada. Resultados: {len(res):,}")


# ==========================================================================================
# RENDERIZADO COMÚN DE RESULTADOS + EXPORTACIONES
# ==========================================================================================
def render_resultados(con_bitacora: bool):
    st.subheader("Resultados")

    if ss.results_df is None or ss.results_df.empty:
        st.info("Aún no hay resultados. Ejecuta una búsqueda.")
        return

    res = ss.results_df.copy()

    # Filtros rápidos
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
        tipo_opts = (
            sorted(
                res.get(TIPO_NORMAL_COL, pd.Series(dtype=str))
                .dropna()
                .unique()
                .tolist()
            )
            if TIPO_NORMAL_COL in res.columns
            else []
        )
        filtro_tipo = st.multiselect(
            "Tipo normalizado", options=tipo_opts, default=None
        )

    if filtro_fuente:
        res = res[res["Fuente"].isin(filtro_fuente)]
    if filtro_tema and "Temática normalizada" in res.columns:
        res = res[res["Temática normalizada"].isin(filtro_tema)]
    if filtro_tipo and TIPO_NORMAL_COL in res.columns:
        res = res[res[TIPO_NORMAL_COL].isin(filtro_tipo)]

    st.caption(f"Filas totales (después de filtros): **{len(res):,}**")

    # Columna de selección
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
        key=f"data_editor_res_{ss.metodo}",
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

    # ---------------------------------- Exportaciones ----------------------------------
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
            )
            .to_csv(index=False)
            .encode("utf-8"),
            file_name="resultados_seleccionados.csv",
            mime="text/csv",
            disabled=seleccionados.empty,
            use_container_width=True,
        )

    # Excel completo
    with colx3:
        import xlsxwriter  # noqa: F401

        xbio = io.BytesIO()
        writer = pd.ExcelWriter(xbio, engine="xlsxwriter")

        res_x = _prep_export(res)
        res_x.to_excel(writer, index=False, sheet_name="Resultados")

        if con_bitacora:
            # Resaltado por términos a excluir (sólo Método A)
            if ss.excluir_df is not None:
                wb = writer.book
                ws = writer.sheets["Resultados"]
                fmt = wb.add_format({"bg_color": "#FFF599"})

                cols = list(res_x.columns)
                col_tit_name = (
                    EXPORT_RENAME.get(DEFAULT_COL_TITULO, DEFAULT_COL_TITULO)
                )
                col_tem_name = (
                    EXPORT_RENAME.get(DEFAULT_COL_TEMATICAS, DEFAULT_COL_TEMATICAS)
                )
                col_tit = cols.index(col_tit_name) + 1 if col_tit_name in cols else None
                col_tem = cols.index(col_tem_name) + 1 if col_tem_name in cols else None

                excluye = [
                    normalize_text(x)
                    for x in ss.excluir_df["excluir"].astype(str).tolist()
                    if str(x).strip()
                ]

                for r in range(1, len(res_x) + 1):
                    if col_tit is not None:
                        v = normalize_text(res_x.iloc[r - 1, col_tit - 1])
                        if any(t in v for t in excluye):
                            ws.write(r, col_tit - 1, res_x.iloc[r - 1, col_tit - 1], fmt)
                    if col_tem is not None:
                        v = normalize_text(res_x.iloc[r - 1, col_tem - 1])
                        if any(t in v for t in excluye):
                            ws.write(r, col_tem - 1, res_x.iloc[r - 1, col_tem - 1], fmt)

            if ss.bitacora_df is not None:
                ss.bitacora_df.to_excel(writer, index=False, sheet_name="Bitácora")

            writer.close()
            xbio.seek(0)
            st.download_button(
                "⬇️ Excel (filtrado + resaltado + Bitácora)",
                data=xbio.getvalue(),
                file_name="resultados_filtrados.xlsx",
                mime=(
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
                use_container_width=True,
            )
        else:
            writer.close()
            xbio.seek(0)
            st.download_button(
                "⬇️ Excel (todo lo filtrado)",
                data=xbio.getvalue(),
                file_name="resultados_filtrados.xlsx",
                mime=(
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
                use_container_width=True,
            )

    # Excel de seleccionados
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
                mime=(
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
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

    # Citas APA para seleccionados
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
                data=b"",
                file_name="citas_apa.txt",
                mime="text/plain",
                use_container_width=True,
                disabled=True,
            )


# ==========================================================================================
# LÓGICA PRINCIPAL POR MÉTODO
# ==========================================================================================
if ss.metodo == "A":
    # --- Validaciones Método A ---
    if ss.tematicas_df is None or ss.excluir_df is None:
        st.warning(
            "Para usar el **Método A** debes cargar **Temáticas** y **Términos a excluir** "
            "en la barra lateral."
        )
    else:
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
            "Por defecto se usan “Título” y “Temáticas”, y duplicados por "
            "“Url OA” / “No. Topográfico”. Puedes cambiarlos si lo necesitas."
        )

        st.markdown("---")

        if st.button(
            "🚀 Iniciar búsqueda (Método A)",
            type="primary",
            use_container_width=True,
        ):
            try:
                ejecutar_busqueda_metodo_a(
                    col_busq1=col_busq1,
                    col_busq2=col_busq2,
                    col_dup_dig=col_dup_dig,
                    col_dup_fis=col_dup_fis,
                )
            except Exception as e:
                st.error(f"Ocurrió un problema durante la búsqueda: {e}")

        # Mostrar resultados y bitácora
        render_resultados(con_bitacora=True)

        st.subheader("📑 Bitácora por término")
        if ss.bitacora_df is None or ss.bitacora_df.empty:
            st.info("Aún no hay bitácora. Ejecuta la búsqueda del Método A.")
        else:
            st.dataframe(ss.bitacora_df, use_container_width=True, height=360)
            st.download_button(
                "Descargar bitácora (.csv)",
                data=ss.bitacora_df.to_csv(index=False).encode("utf-8"),
                file_name="bitacora_por_termino.csv",
                mime="text/csv",
                use_container_width=True,
            )

else:
    # ======================= MÉTODO B ==========================
    st.subheader("Búsqueda avanzada (Método B – experimental)")

    # Alcance de búsqueda
    colc1, colc2 = st.columns([1, 1])
    with colc1:
        colecciones = st.multiselect(
            "Colecciones a incluir",
            options=["Digital", "Física"],
            default=["Digital", "Física"],
        )
    with colc2:
        # tipos normalizados disponibles
        todos_tipos = sorted(
            pd.concat([ss.df_digital, ss.df_fisica], ignore_index=True)
            .get(TIPO_NORMAL_COL, pd.Series(dtype=str))
            .dropna()
            .unique()
            .tolist()
        )
        tipos_sel = st.multiselect(
            "Tipo de ítem normalizado",
            options=todos_tipos,
            default=todos_tipos,  # por defecto TODOS
        )

    st.markdown(
        "Define una o varias condiciones. Se aplican en orden y se combinan con **Y (AND)**, "
        "**O (OR)** o **NO (NOT)**."
    )

    # Número de condiciones
    ss.b_num_cond = int(
        st.number_input(
            "Número de condiciones",
            min_value=1,
            max_value=5,
            value=ss.b_num_cond or 1,
            step=1,
        )
    )

    # Asegurar lista de condiciones en estado
    conds: List[Dict[str, Any]] = ss.b_conds or []
    while len(conds) < ss.b_num_cond:
        conds.append(
            {
                "conector": "(primera)" if len(conds) == 0 else "Y (AND)",
                "campo": "Título",
                "operador": "Contiene",
                "valor": "",
            }
        )
    if len(conds) > ss.b_num_cond:
        conds = conds[: ss.b_num_cond]

    # Render de cada condición
    for i in range(ss.b_num_cond):
        st.markdown(f"**Condición {i+1}**")
        c1, c2, c3, c4 = st.columns([1, 1, 1, 3])

        with c1:
            opciones_con = CONECTORES_B if i > 0 else ["(primera)"]
            conds[i]["conector"] = st.selectbox(
                "Conector",
                options=opciones_con,
                index=opciones_con.index(conds[i]["conector"])
                if conds[i]["conector"] in opciones_con
                else 0,
                key=f"b_con_{i}",
            )
        with c2:
            conds[i]["campo"] = st.selectbox(
                "Campo",
                options=list(CAMPOS_B.keys()),
                index=list(CAMPOS_B.keys()).index(conds[i]["campo"])
                if conds[i]["campo"] in CAMPOS_B
                else 0,
                key=f"b_cam_{i}",
            )
        with c3:
            conds[i]["operador"] = st.selectbox(
                "Operador",
                options=OPERADORES_B,
                index=OPERADORES_B.index(conds[i]["operador"])
                if conds[i]["operador"] in OPERADORES_B
                else 0,
                key=f"b_op_{i}",
            )
        with c4:
            conds[i]["valor"] = st.text_input(
                "Valor",
                value=conds[i]["valor"],
                key=f"b_val_{i}",
            )

        st.markdown("---")

    ss.b_conds = conds

    if st.button(
        "🚀 Iniciar búsqueda avanzada (Método B)",
        type="primary",
        use_container_width=True,
    ):
        try:
            ejecutar_busqueda_metodo_b(
                colecciones=colecciones,
                tipos_sel=tipos_sel,
                condiciones=conds,
            )
        except Exception as e:
            st.error(f"Ocurrió un problema durante la búsqueda avanzada: {e}")

    # Resultados sin bitácora ni resaltado especial
    render_resultados(con_bitacora=False)

    st.subheader("📑 Bitácora por término")
    st.info(
        "La bitácora detallada con resultados por término sólo se genera con el **Método A** "
        "(listado de temáticas)."
    )
