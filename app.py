# -*- coding: utf-8 -*-
# Herramienta para la elaboración de bibliografías especializadas
# v8.2 – Método A (listado de temáticas) + Método B (búsqueda avanzada),
#       sin tocar el motor de búsqueda original y corrigiendo Autor(es) en citas.

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
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    )
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

# Resultados
ss.setdefault("results_df", None)
ss.setdefault("bitacora_df", None)

# Método seleccionado
ss.setdefault(
    "search_method",
    "Listado de temáticas (Método A)"
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
    url,
    label,
    container=None,
    max_retries=5,
    chunk_size=256 * 1024,
    timeout=300,
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
            downloaded = (
                os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
            )
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
                expected_total = (
                    downloaded + int(content_length)
                    if content_length
                    else total_size
                )

                last = time.time()
                with open(tmp_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        if expected_total and time.time() - last > 0.1:
                            bar.progress(
                                min(1.0, downloaded / expected_total)
                            )
                            info.write(
                                f"{downloaded/1e6:,.1f} MB / "
                                f"{expected_total/1e6:,.1f} MB"
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
    Además, normaliza nombres de columnas (strip), lo que corrige
    cosas como 'Autor(es) ' → 'Autor(es)'.
    """
    try:
        with st.spinner(f"Procesando {label}…"):
            df = pd.read_excel(
                bio_or_file, engine="openpyxl", dtype=str
            )
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


def _prepara_columnas(df: pd.DataFrame, cols: List[str]):
    """Asegura tipo str y sin NaN en columnas indicadas."""
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).fillna("")


# (Opcional) Cambiar texto “Browse files” por “Cargar listado” con CSS
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
            df_t = safe_read_excel(tem_up, "Temáticas")
            ss.tematicas_df = (
                df_t[[df_t.columns[0], df_t.columns[1]]]
                .rename(
                    columns={
                        df_t.columns[0]: "termino",
                        df_t.columns[1]: "normalizado",
                    }
                )
                .fillna("")
            )
            st.success(f"Temáticas cargadas: {len(ss.tematicas_df)}")

        if exc_up is not None:
            df_e = safe_read_excel(exc_up, "Términos a excluir")
            ss.excluir_df = (
                df_e[[df_e.columns[0]]]
                .rename(columns={df_e.columns[0]: "excluir"})
                .fillna("")
            )
            st.success(f"Términos a excluir cargados: {len(ss.excluir_df)}")

    st.markdown("---")
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

**2) Cargue sus temáticas.**  
Descargue la plantilla de [Temáticas]({URL_PLANTILLA_TEMATICAS}).  
La **columna 1** incluye variaciones del término (con/sin tildes, otros idiomas).  
La **columna 2** agrupa/normaliza el término, que será el que verás en los resultados.

**3) Cargue términos a excluir.**  
Use la plantilla de [Términos a excluir]({URL_PLANTILLA_EXCLUSION}). Sirve para evitar falsos positivos (p. ej., buscar “ecología” sin recuperar “ginecología”).

**4) Parámetros de búsqueda.**  
En el **Método A** la búsqueda se hace por defecto en **Título** y **Temáticas** y se eliminan duplicados por **Url OA** (Digital) y **No. Topográfico** (Física).  
En el **Método B** puedes armar búsquedas avanzadas con operadores booleanos.

**5) Ejecute e interprete.**  
Pulsa **Iniciar búsqueda** en el método elegido. Verás una tabla (vista de hasta 200 filas por defecto).  
Puedes **filtrar**, **marcar filas** y **exportar** en CSV/XLSX o **citas APA** (beta).

**6) Exportaciones.**  
El Excel incluye la **bitácora por término** (Método A) y resalta coincidencias con **términos a excluir**.  
Las exportaciones “solo seleccionados” respetan lo marcado en la tabla.

**7) Nueva búsqueda.**  
Pulsa **Nueva búsqueda** para cargar otras temáticas y términos o para armar una nueva búsqueda avanzada **sin re-sincronizar** las bases.  
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
            "Sincronizando colecciones **Digital** y **Física**… Puedes cargar "
            "**Temáticas** y **Términos a excluir** mientras tanto. No cierres esta ventana."
        )

        # Digital
        st.subheader("Descargando Base de datos de la colección Digital…")
        zona_dig = st.container()
        try:
            bio_d = download_with_resume(
                URL_DIGITAL, "Colección Digital", container=zona_dig
            )
            st.caption(
                "Colección Digital: descarga completa. Verificando archivo…"
            )
            ss.df_digital = safe_read_excel(bio_d, "Colección Digital")
            st.success("Base de datos de la colección Digital lista ✓")
        except Exception as e:
            st.error(f"No fue posible descargar la base Digital: {e}")
            ss.downloading = False

        # Física
        st.subheader("Descargando Base de datos de la colección Física…")
        zona_fis = st.container()
        try:
            bio_f = download_with_resume(
                URL_FISICA, "Colección Física", container=zona_fis
            )
            st.caption(
                "Colección Física: descarga completa. Verificando archivo…"
            )
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
        "Consejo: usa **Nueva búsqueda** para repetir con otras temáticas o "
        "búsquedas avanzadas sin re-sincronizar."
    )

if not ss.bases_ready:
    st.stop()

# ---------------------------------- NUEVA BÚSQUEDA ----------------------------------
col_nb = st.columns([1, 1, 4])[0]
with col_nb:
    if st.button("🧪 Nueva búsqueda", use_container_width=True):
        for k in ("tematicas_df", "excluir_df", "results_df", "bitacora_df"):
            ss[k] = None
        st.toast(
            "Listo. Carga nuevas Temáticas/Términos o arma una nueva búsqueda avanzada."
        )

# ---------------------------------- MOTOR MÉTODO A (igual al de v8.0) ----------------------------------
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
            f"{fuente}: {i+1}/{N} términos • "
            f"transcurrido: {int(elapsed)} s • restante: {est_rem} s"
        )

    if res:
        return pd.concat(res, ignore_index=True)
    return pd.DataFrame()


def ejecutar_busqueda_metodo_a(
    col_busq1: str, col_busq2: str, col_dup_dig: str, col_dup_fis: str
):
    """Motor de búsqueda original (Método A)."""
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

    # Bitácora con ceros
    tem = (
        ss.tematicas_df[["termino", "normalizado"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    fuentes = pd.DataFrame({"Fuente": ["Digital", "Física"]})
    grid = (
        fuentes.assign(key=1)
        .merge(tem.assign(key=1), on="key")
        .drop("key", axis=1)
    )

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
        ["Fuente", "Resultados", "Término"],
        ascending=[True, False, True],
    ).reset_index(drop=True)
    ss.bitacora_df = bit

    barra.progress(1.0)
    estado.empty()
    st.success("Búsqueda finalizada (Método A).")


# ---------------------------------- MÉTODO B: BÚSQUEDA AVANZADA ----------------------------------
def build_base_global() -> pd.DataFrame:
    """Concatena Digital y Física en un solo DataFrame de trabajo."""
    DF_D = ss.df_digital.copy()
    DF_F = ss.df_fisica.copy()
    DF_D["Fuente"] = "Digital"
    DF_F["Fuente"] = "Física"
    base = pd.concat([DF_D, DF_F], ignore_index=True, sort=False)
    base = base.fillna("")
    # Normalizamos campos de texto de interés
    text_cols = [
        c
        for c in [
            "Título",
            "Autor(es)",
            "Temáticas",
            "Editorial",
            "Año de Publicación",
            "Base de datos",
        ]
        if c in base.columns
    ]
    _prepara_columnas(base, text_cols)
    return base


def aplicar_operador(
    serie: pd.Series, operador: str, valor: str
) -> pd.Series:
    """Construye la máscara booleana para un campo/operador/valor."""
    v_norm = normalize_text(valor).lower()
    if v_norm == "":
        return pd.Series(False, index=serie.index)

    def norm(s):
        return normalize_text(s).lower()

    if operador == "Contiene":
        return serie.map(lambda x: v_norm in norm(x))
    if operador == "No contiene":
        return serie.map(lambda x: v_norm not in norm(x))
    if operador == "Frase exacta":
        return serie.map(lambda x: norm(x) == v_norm)
    if operador == "Comienza con":
        return serie.map(lambda x: norm(x).startswith(v_norm))
    # Por defecto, Contiene
    return serie.map(lambda x: v_norm in norm(x))


def ejecutar_busqueda_metodo_b(
    condiciones: List[Dict[str, Any]],
    fuentes_sel: List[str],
    tipos_sel: List[str],
) -> None:
    """Búsqueda avanzada (Método B)."""
    base = build_base_global()

    if fuentes_sel:
        base = base[base["Fuente"].isin(fuentes_sel)]

    tipo_col = "Tipo de ítem normalizado mat especial"
    if tipos_sel and tipo_col in base.columns:
        base = base[base[tipo_col].isin(tipos_sel)]

    if base.empty:
        st.warning("No hay registros que cumplan los filtros de colección/tipo.")
        ss.results_df = pd.DataFrame()
        ss.bitacora_df = None
        return

    # Construimos máscara global
    mask = pd.Series(True, index=base.index)
    first = True

    text_cols = [
        c
        for c in [
            "Título",
            "Autor(es)",
            "Temáticas",
            "Editorial",
            "Año de Publicación",
            "Base de datos",
        ]
        if c in base.columns
    ]

    for cond in condiciones:
        campo = cond["campo"]
        operador = cond["op"]
        valor = cond["valor"]
        conector = cond["conector"]  # AND / OR / NOT

        if valor.strip() == "":
            continue  # ignoramos condiciones vacías

        if campo == "Cualquier campo":
            # OR sobre todos los campos de texto definidos
            if not text_cols:
                continue
            masks = [
                aplicar_operador(base[c], operador, valor) for c in text_cols
            ]
            cond_mask = masks[0]
            for m in masks[1:]:
                cond_mask = cond_mask | m
        else:
            if campo not in base.columns:
                continue
            cond_mask = aplicar_operador(base[campo], operador, valor)

        if first:
            # Primera condición: aplica directamente
            if operador == "No contiene":
                mask = ~cond_mask
            else:
                mask = cond_mask
            first = False
        else:
            if conector == "Y (AND)":
                mask = mask & cond_mask
            elif conector == "O (OR)":
                mask = mask | cond_mask
            elif conector == "NO (NOT)":
                mask = mask & (~cond_mask)
            else:
                mask = mask & cond_mask

    if first:
        # No hubo ninguna condición válida
        st.warning("Define al menos una condición con un valor no vacío.")
        return

    res = base[mask].copy()
    ss.results_df = res
    # Para Método B no construimos bitácora detallada
    ss.bitacora_df = pd.DataFrame()
    st.success(f"Búsqueda avanzada finalizada. Resultados: {len(res):,}")


# ---------------------------------- ELECCIÓN DE MÉTODO ----------------------------------
st.markdown("---")
st.subheader("Modo de búsqueda")

method = st.radio(
    "Seleccione el modo de búsqueda",
    [
        "Listado de temáticas (Método A)",
        "Búsqueda avanzada (Método B – experimental)",
    ],
    index=0 if ss.search_method.startswith("Listado") else 1,
)
ss.search_method = method

# ---------------------------------- MÉTODO A: CONFIG + BOTÓN ----------------------------------
if method.startswith("Listado"):

    if ss.tematicas_df is None or ss.excluir_df is None:
        st.warning(
            "Carga **Temáticas** y **Términos a excluir** en la barra lateral "
            "para usar el **Método A**. O cambia al **Método B** si quieres "
            "hacer una búsqueda avanzada sin plantillas."
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
        "Por defecto se usan “Título” y “Temáticas”, y duplicados por "
        "“Url OA” / “No. Topográfico”. Puedes cambiarlos si lo necesitas."
    )

    if st.button(
        "🚀 Iniciar búsqueda (Método A)",
        type="primary",
        use_container_width=True,
    ):
        try:
            ejecutar_busqueda_metodo_a(
                col_busq1, col_busq2, col_dup_dig, col_dup_fis
            )
        except Exception as e:
            st.error(f"Ocurrió un problema durante la búsqueda: {e}")

# ---------------------------------- MÉTODO B: UI + BOTÓN ----------------------------------
else:
    st.subheader("Búsqueda avanzada (Método B – experimental)")

    base_preview = build_base_global()
    campos_disponibles = [
        "Cualquier campo",
        "Título",
        "Autor(es)",
        "Temáticas",
        "Editorial",
        "Año de Publicación",
        "Base de datos",
    ]
    campos_disponibles = [
        c for c in campos_disponibles if c == "Cualquier campo" or c in base_preview.columns
    ]

    tipo_col = "Tipo de ítem normalizado mat especial"
    tipo_opts_all = (
        sorted(
            base_preview.get(tipo_col, pd.Series(dtype=str))
            .dropna()
            .unique()
            .tolist()
        )
        if tipo_col in base_preview.columns
        else []
    )

    col_b1, col_b2 = st.columns([1, 1])
    with col_b1:
        fuentes_sel = st.multiselect(
            "Colecciones a incluir",
            options=["Digital", "Física"],
            default=["Digital", "Física"],
            key="b_fuentes_v82",
        )
    with col_b2:
        tipos_sel = st.multiselect(
            "Tipo de ítem normalizado",
            options=tipo_opts_all,
            default=[],
            key="b_tipos_v82",
        )

    st.markdown(
        "Define una o varias condiciones. Se aplican en orden y se combinan "
        "con **Y (AND)**, **O (OR)** o **NO (NOT)**."
    )

    num_cond = st.number_input(
        "Número de condiciones",
        min_value=1,
        max_value=5,
        value=1,
        step=1,
        key="b_num_cond_v82",
    )

    condiciones: List[Dict[str, Any]] = []
    for i in range(int(num_cond)):
        st.markdown(f"**Condición {i+1}**")
        col_c1, col_c2, col_c3 = st.columns([1, 1.2, 2])
        with col_c1:
            if i == 0:
                conector = "(primera)"
                st.selectbox(
                    "Conector",
                    options=["(primera)"],
                    key=f"b_con_{i}_v82",
                    disabled=True,
                )
            else:
                conector = st.selectbox(
                    "Conector",
                    options=["Y (AND)", "O (OR)", "NO (NOT)"],
                    key=f"b_con_{i}_v82",
                )
        with col_c2:
            campo = st.selectbox(
                "Campo",
                options=campos_disponibles,
                key=f"b_campo_{i}_v82",
            )
        with col_c3:
            operador = st.selectbox(
                "Operador",
                options=[
                    "Contiene",
                    "No contiene",
                    "Frase exacta",
                    "Comienza con",
                ],
                key=f"b_op_{i}_v82",
            )
            valor = st.text_input(
                "Valor", key=f"b_val_{i}_v82", placeholder="Ej.: biología celular"
            )

        condiciones.append(
            {
                "campo": campo,
                "op": operador,
                "valor": valor,
                "conector": "AND" if i == 0 else conector.split()[0],
            }
        )

    if st.button(
        "🚀 Iniciar búsqueda avanzada (Método B)",
        type="primary",
        use_container_width=True,
    ):
        try:
            ejecutar_busqueda_metodo_b(condiciones, fuentes_sel, tipos_sel)
        except Exception as e:
            st.error(f"Ocurrió un problema durante la búsqueda avanzada: {e}")

# ---------------------------------- RESULTADOS + FILTROS/SELECCIÓN ----------------------------------
st.subheader("Resultados")

if ss.results_df is None or ss.results_df.empty:
    st.info("Aún no hay resultados. Ejecuta una búsqueda con el método elegido.")
else:
    res = ss.results_df.copy()

    # Filtros rápidos
    colf1, colf2, colf3 = st.columns([1, 1, 2])
    with colf1:
        filtro_fuente = st.multiselect(
            "Fuente",
            options=sorted(
                res.get("Fuente", pd.Series(dtype=str))
                .dropna()
                .unique()
                .tolist()
            ),
            default=None,
        )
    with colf2:
        col_tema_norm = "Temática normalizada"
        temas_norm = (
            sorted(
                res.get(col_tema_norm, pd.Series(dtype=str))
                .dropna()
                .unique()
                .tolist()
            )
            if col_tema_norm in res.columns
            else []
        )
        filtro_tema = st.multiselect(
            "Temática normalizada", options=temas_norm, default=None
        )
    with colf3:
        tipon_col = "Tipo de ítem normalizado mat especial"
        tipo_opts = sorted(
            res.get(tipon_col, pd.Series(dtype=str))
            .dropna()
            .unique()
            .tolist()
        )
        filtro_tipo = st.multiselect(
            "Tipo normalizado", options=tipo_opts, default=None
        )

    if filtro_fuente:
        res = res[res.get("Fuente", "").isin(filtro_fuente)]
    if filtro_tema and col_tema_norm in res.columns:
        res = res[res[col_tema_norm].isin(filtro_tema)]
    if filtro_tipo and tipon_col in res.columns:
        res = res[res[tipon_col].isin(filtro_tipo)]

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

    # --------- Helpers de exportación (renombres y omisiones) ---------
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

    # Excel completo con resaltado + Bitácora
    with colx3:
        excluye = (
            [str(x).strip() for x in ss.excluir_df["excluir"].tolist() if str(x).strip()]
            if ss.excluir_df is not None
            else []
        )
        import xlsxwriter  # type: ignore

        xbio = io.BytesIO()
        writer = pd.ExcelWriter(xbio, engine="xlsxwriter")

        res_x = _prep_export(res)
        res_x.to_excel(writer, index=False, sheet_name="Resultados")
        wb = writer.book
        ws = writer.sheets["Resultados"]
        fmt = wb.add_format({"bg_color": "#FFF599"})

        cols_exp = list(res_x.columns)
        col_tit_idx = (
            cols_exp.index(DEFAULT_COL_TITULO) + 1
            if DEFAULT_COL_TITULO in cols_exp
            else None
        )
        col_tem_idx = (
            cols_exp.index(DEFAULT_COL_TEMATICAS) + 1
            if DEFAULT_COL_TEMATICAS in cols_exp
            else None
        )
        excl_norm = [normalize_text(x) for x in excluye]

        for r in range(1, len(res_x) + 1):
            if col_tit_idx:
                v = normalize_text(res_x.iloc[r - 1, col_tit_idx - 1])
                if any(t in v for t in excl_norm):
                    ws.write(r, col_tit_idx - 1, res_x.iloc[r - 1, col_tit_idx - 1], fmt)
            if col_tem_idx:
                v = normalize_text(res_x.iloc[r - 1, col_tem_idx - 1])
                if any(t in v for t in excl_norm):
                    ws.write(r, col_tem_idx - 1, res_x.iloc[r - 1, col_tem_idx - 1], fmt)

        if ss.bitacora_df is not None and not ss.bitacora_df.empty:
            ss.bitacora_df.to_excel(
                writer, index=False, sheet_name="Bitácora"
            )

        writer.close()
        xbio.seek(0)
        st.download_button(
            "⬇️ Excel (filtrado + resaltado + Bitácora)",
            data=xbio.getvalue(),
            file_name="resultados_filtrados.xlsx",
            mime=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ),
            use_container_width=True,
        )

    # Excel de seleccionados (sin resaltado, más simple)
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
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
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

    # Citas APA (beta) sobre seleccionados
    def build_apa(row: pd.Series) -> str:
        """
        Generador APA simplificado con los campos disponibles.
        Usa Autor(es) si no es “NO APLICA”; si la columna venía con espacio
        al final, safe_read_excel ya la normalizó a 'Autor(es)'.
        """
        tit = str(row.get("Título", "")).strip()
        aut = str(row.get("Autor(es)", "")).strip()
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
if ss.bitacora_df is None or ss.bitacora_df.empty:
    st.info(
        "Aún no hay bitácora. La bitácora detallada se genera sólo con el "
        "**Método A (listado de temáticas)**."
    )
else:
    st.dataframe(ss.bitacora_df, use_container_width=True, height=360)
    st.download_button(
        "Descargar bitácora (.csv)",
        data=ss.bitacora_df.to_csv(index=False).encode("utf-8"),
        file_name="bitacora_por_termino.csv",
        mime="text/csv",
        use_container_width=True,
    )
