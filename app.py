# -*- coding: utf-8 -*-
# Bibliografías especializadas – UEB
# v7.4 – auto-descarga, plantillas obligatorias, búsqueda estable,
# filtros/resultados/selección/export, generador citas APA, keep-alive en procesos.

import io
import os
import re
import time
import json
import base64
import requests
import numpy as np
import pandas as pd
import streamlit as st
from unidecode import unidecode
from streamlit_autorefresh import st_autorefresh

# ----------------------------- Ajustes generales -----------------------------------

st.set_page_config(
    page_title="Herramienta para bibliografías",
    page_icon="📚",
    layout="wide",
)

ss = st.session_state

# URLs oficiales (ajústalas si cambian en tu web)
URL_DIGITAL = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20Colecci%C3%B3n%20Digital.xlsx"
URL_FISICA  = "https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Biblioteca%20BD%20Colecci%C3%B3n%20F%C3%ADsica.xlsx"

# Nombres de columnas "estándar" preferidas
COL_TITULO            = "Título"
COL_TEMATICAS         = "Temáticas"
COL_TIPO_ITEM         = "Tipo de ítem"
COL_TIPO_ITEM_NORM    = "Tipo de ítem normalizado mat especial"
COL_EDITORIAL         = "Editorial"
COL_AUTORES           = "Autor(es)"
COL_ANIO              = "Año de Publicación"
COL_BASE_DATOS        = "Base de datos"
COL_URL_OA            = "Url OA"
COL_URL_FISICA        = "Url en LOCATE/IDEA"     # Físico; lo normalizaremos a "Url de acceso"
COL_URL_ACCESO_STD    = "Url de acceso"          # Unificación para Digital/Física
COL_NO_TOPO           = "No. Topográfico"
COL_ISSN1             = "ISSN1"
COL_ISBN              = "ISBN"
COL_SJR               = "Clasificación SJR"
COL_PERM_LOC          = "permanent_location.name"
COL_BARCODE           = "Item Barcode"
COL_FORMATO           = "Formato"

# Por defecto para duplicados
DEF_DUP_DIGITAL = "Url OA"
DEF_DUP_FISICA  = "No. Topográfico"

# -------------------------------- Utilidades ----------------------------------------

def _norm_text(s):
    """Normaliza para comparación (lower + unidecode + quita espacios dobles)."""
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\n", " ").strip()
    s = unidecode(s.lower())
    s = re.sub(r"\s+", " ", s)
    return s

@st.cache_data(ttl=3600, show_spinner=False)
def download_with_resume(url, label="archivo", timeout=60):
    """Descarga con requests, cacheada, devuelve BytesIO."""
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    bio = io.BytesIO(r.content)
    bio.seek(0)
    return bio

@st.cache_data(ttl=3600, show_spinner=False)
def read_excel_from_bytes(bio, sheet=None):
    """Lee Excel desde BytesIO a DataFrame."""
    df = pd.read_excel(bio, sheet_name=sheet)
    return df

def normalize_physical_columns(df):
    """Normaliza columnas de Físico: unifica URL, etc."""
    if COL_URL_FISICA in df.columns:
        df[COL_URL_ACCESO_STD] = df[COL_URL_FISICA]
    if COL_URL_OA in df.columns and COL_URL_ACCESO_STD not in df.columns:
        df[COL_URL_ACCESO_STD] = df[COL_URL_OA]
    if COL_URL_ACCESO_STD not in df.columns:
        df[COL_URL_ACCESO_STD] = ""
    return df

def normalize_digital_columns(df):
    """Normaliza columnas de Digital a URL estándar."""
    if COL_URL_OA in df.columns:
        df[COL_URL_ACCESO_STD] = df[COL_URL_OA]
    else:
        df[COL_URL_ACCESO_STD] = ""
    return df

def show_info_panel():
    st.markdown(
        """
### ℹ️ Información

- **Objetivo**: permitir la autogestión por programa/asignatura/tema y resaltar **términos a excluir** para depuración manual.  
- Usa siempre las **bases oficiales** (Digital/Física) o súbelas **manualmente** en la barra lateral.  
- **Plantillas**: [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx) y [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx).  
- Los archivos adjuntos **no se almacenan** por la Universidad y se eliminan al cerrar la app.  
- El proceso puede tardar algunos minutos; **puedes seguir usando tu equipo** (no cierres el navegador).
        """,
        help="Guía de uso"
    )

def keepalive_if_working():
    # Activa un refresh suave solo si hay tareas largas en curso
    if ss.get("loading_digital") or ss.get("processing_digital") or ss.get("loading_fisica") or ss.get("processing_fisica"):
        st_autorefresh(interval=45000, key="keepalive")

# ----------------------------- Sidebar (plantillas & manual) ------------------------

def sidebar():
    with st.sidebar:
        st.image(
            "https://biblioteca.unbosque.edu.co/sites/default/files/Logos/Logo%201%20Blanco.png",
            use_container_width=True
        )
        st.caption("Biblioteca Juan Roa Vásquez")

        st.markdown("### Plantillas oficiales:")
        st.markdown("- [Temáticas](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx)")
        st.markdown("- [Términos a excluir](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx)")

        st.markdown("### Archivos auxiliares (obligatorios)")
        tem_file = st.file_uploader("Temáticas (.xlsx, col1=término, col2=normalizado)", type=["xlsx"], key="up_temas")
        exc_file = st.file_uploader("Términos a excluir (.xlsx, 1ra columna)", type=["xlsx"], key="up_excluir")

        st.markdown("---")
        with st.expander("🔧 Avanzado: subir bases Digital/Física manualmente"):
            man_dig = st.file_uploader("Base de datos de la colección **Digital** (.xlsx)", type=["xlsx"], key="up_dig")
            man_fis = st.file_uploader("Base de datos de la colección **Física** (.xlsx)", type=["xlsx"], key="up_fis")

        st.info(
            "Por defecto se descargan **bases oficiales** automáticamente. "
            "Puedes subir manualmente una base en el panel **Avanzado** si lo necesitas."
        )

        return tem_file, exc_file, man_dig, man_fis

# ------------------------------ Carga automática bases --------------------------------

def ensure_bases_loaded(man_dig, man_fis):
    """Garantiza que Digital/Física estén en memoria. Soporta manual y oficial."""
    # Keepalive mientras se trabaja
    keepalive_if_working()

    if "df_digital" not in ss:
        ss.df_digital = None
    if "df_fisica" not in ss:
        ss.df_fisica = None

    # Manual tiene prioridad si se sube
    if man_dig is not None:
        try:
            ss.loading_digital = True
            df = pd.read_excel(man_dig)
            ss.df_digital = normalize_digital_columns(df)
            ss.loading_digital = False
            st.success("Base de datos de la colección **Digital** (manual) cargada en memoria.")
        except Exception as e:
            ss.loading_digital = False
            st.error(f"No se pudo leer la base Digital manual: {e}")

    if man_fis is not None:
        try:
            ss.loading_fisica = True
            df = pd.read_excel(man_fis)
            ss.df_fisica = normalize_physical_columns(df)
            ss.loading_fisica = False
            st.success("Base de datos de la colección **Física** (manual) cargada en memoria.")
        except Exception as e:
            ss.loading_fisica = False
            st.error(f"No se pudo leer la base Física manual: {e}")

    # Si alguna falta, descarga oficial
    if ss.df_digital is None:
        ss.loading_digital = True
        with st.status("Descargando **Base de datos de la colección Digital**…", expanded=True):
            try:
                bio = download_with_resume(URL_DIGITAL, "Digital")
                dd = read_excel_from_bytes(bio)
                ss.df_digital = normalize_digital_columns(dd)
                st.write("Descarga completa. Verificando archivo…")
                time.sleep(0.4)
                st.success("Base de datos de la colección Digital lista ✅")
            except Exception as e:
                st.error(f"No fue posible descargar la base Digital: {e}")
            finally:
                ss.loading_digital = False

    if ss.df_fisica is None:
        ss.loading_fisica = True
        with st.status("Descargando **Base de datos de la colección Física**…", expanded=True):
            try:
                bio = download_with_resume(URL_FISICA, "Física")
                df = read_excel_from_bytes(bio)
                ss.df_fisica = normalize_physical_columns(df)
                st.write("Descarga completa. Verificando archivo…")
                time.sleep(0.4)
                st.success("Base de datos de la colección Física lista ✅")
            except Exception as e:
                st.error(f"No fue posible descargar la base Física: {e}")
            finally:
                ss.loading_fisica = False

    # Señal de listo
    if ss.df_digital is not None and ss.df_fisica is not None:
        st.success("✅ Bases oficiales listas en memoria.")
        return True
    return False

# -------------------------------- Búsqueda -------------------------------------------

def build_term_list(df_tematicas):
    """
    Construye lista de tuplas (patrón_busqueda, normalizado).
    df_tematicas: 2 columnas: col0 = término, col1 = normalizado.
    """
    terms = []
    if df_tematicas is None or df_tematicas.empty:
        return terms
    cols = df_tematicas.columns.tolist()
    tcol = cols[0]
    ncol = cols[1] if len(cols) > 1 else cols[0]
    for _, row in df_tematicas.iterrows():
        term = str(row.get(tcol, "")).strip()
        norm = str(row.get(ncol, "")).strip() or term
        if term:
            terms.append((term, norm))
    return terms

def build_exclusion_list(df_excluir):
    """Obtiene lista de términos a excluir de la primera columna."""
    if df_excluir is None or df_excluir.empty:
        return []
    col = df_excluir.columns[0]
    vals = []
    for x in df_excluir[col].tolist():
        s = str(x).strip()
        if s:
            vals.append(s.lower())
    return vals

def col_exists(df, name):
    return name in df.columns

def run_search(
    df_dig,
    df_fis,
    df_temas,
    df_excluir,
    col1_busq=COL_TITULO,
    col2_busq=COL_TEMATICAS,
    col_dup_dig=DEF_DUP_DIGITAL,
    col_dup_fis=DEF_DUP_FISICA,
):
    """
    Busca por temáticas (términos y normalizados) en Digital + Física, excluye por lista,
    agrega 'Temática normalizada' y 'Fuente' (Digital/Física), y deduplica por columnas elegidas.
    Devuelve (df_resultados, df_bitacora[term, resultados]).
    """
    # Verificaciones mínimas
    if df_dig is None or df_fis is None:
        raise ValueError("Faltan bases en memoria.")
    for df, nm in [(df_dig, "Digital"), (df_fis, "Física")]:
        if not col_exists(df, col1_busq):
            raise ValueError(f"En {nm} no existe la columna '{col1_busq}'.")
        if not col_exists(df, col2_busq):
            raise ValueError(f"En {nm} no existe la columna '{col2_busq}'.")

    # Listas de términos
    terms = build_term_list(df_temas)
    excl  = build_exclusion_list(df_excluir)

    # Pre-normaliza campos de búsqueda
    def add_norms(df):
        df = df.copy()
        df["_n1"] = df[col1_busq].apply(_norm_text)
        df["_n2"] = df[col2_busq].apply(_norm_text)
        return df

    dig = add_norms(df_dig)
    fis = add_norms(df_fis)

    # Unifico URL de acceso
    if COL_URL_ACCESO_STD not in dig.columns:
        if COL_URL_OA in dig.columns:
            dig[COL_URL_ACCESO_STD] = dig[COL_URL_OA]
        else:
            dig[COL_URL_ACCESO_STD] = ""
    if COL_URL_ACCESO_STD not in fis.columns:
        if COL_URL_FISICA in fis.columns:
            fis[COL_URL_ACCESO_STD] = fis[COL_URL_FISICA]
        else:
            fis[COL_URL_ACCESO_STD] = ""

    # Matching
    results = []
    bitacora = []
    for (term, norm) in terms:
        patt = _norm_text(term)
        # Digital
        cd = dig[(dig["_n1"].str.contains(patt, na=False)) | (dig["_n2"].str.contains(patt, na=False))].copy()
        if not cd.empty:
            cd["Temática normalizada"] = norm
            cd["Fuente"] = "Digital"
            results.append(cd)
            bitacora.append(("Digital", norm, len(cd)))
        # Física
        cf = fis[(fis["_n1"].str.contains(patt, na=False)) | (fis["_n2"].str.contains(patt, na=False))].copy()
        if not cf.empty:
            cf["Temática normalizada"] = norm
            cf["Fuente"] = "Física"
            results.append(cf)
            bitacora.append(("Física", norm, len(cf)))

    if not results:
        return pd.DataFrame(), pd.DataFrame(columns=["Fuente", "Término", "Resultados"])

    all_ = pd.concat(results, ignore_index=True)

    # Exclusión
    if excl:
        patt_ex = "|".join([re.escape(x) for x in excl])
        mask_ex = all_["_n1"].str.contains(patt_ex, na=False) | all_["_n2"].str.contains(patt_ex, na=False)
        all_ = all_.loc[~mask_ex].copy()

    # Deduplicación por fuente
    # Digital
    if col_exists(all_, col_dup_dig):
        dup_d = all_["Fuente"].eq("Digital") & all_[col_dup_dig].notna()
        all_.loc[dup_d, "_dedup_key"] = all_.loc[dup_d, col_dup_dig].astype(str)
    else:
        all_["_dedup_key"] = np.nan

    # Física
    if col_exists(all_, col_dup_fis):
        dup_f = all_["Fuente"].eq("Física") & all_[col_dup_fis].notna()
        all_.loc[dup_f, "_dedup_key_f"] = all_.loc[dup_f, col_dup_fis].astype(str)
    else:
        all_["_dedup_key_f"] = np.nan

    # Drop dup dentro de cada fuente
    before = len(all_)
    dmask = all_["Fuente"].eq("Digital")
    fmask = all_["Fuente"].eq("Física")
    all_ = pd.concat([
        all_.loc[dmask].drop_duplicates(subset=[col_dup_dig]) if col_exists(all_, col_dup_dig) else all_.loc[dmask],
        all_.loc[fmask].drop_duplicates(subset=[col_dup_fis]) if col_exists(all_, col_dup_fis) else all_.loc[fmask]
    ], ignore_index=True)
    after = len(all_)
    # Limpieza columnas auxiliares
    all_.drop(columns=["_n1", "_n2", "_dedup_key", "_dedup_key_f"], errors="ignore", inplace=True)

    # Bitácora
    bit = pd.DataFrame(bitacora, columns=["Fuente", "Término", "Resultados"])
    bit = (bit.groupby(["Fuente", "Término"], as_index=False)["Resultados"]
           .sum()
           .sort_values(["Fuente", "Resultados"], ascending=[True, False]))

    return all_, bit

# ------------------------------ Citas APA -------------------------------------------

def apa_authors(raw):
    """Heurística: 'Pérez, Juan; López, Ana' o 'Juan Pérez; Ana López' -> 'Pérez, J., & López, A.'"""
    if not raw or str(raw).strip().upper() == "NO APLICA":
        return ""
    parts = re.split(r";\s*", str(raw).strip())
    out = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if "," in p:
            ap, nm = p.split(",", 1)
            ap = ap.strip()
            nm = nm.strip()
            ini = " ".join([f"{x[0]}." for x in nm.split() if x])
            out.append(f"{ap}, {ini}")
        else:
            toks = p.split()
            if len(toks) >= 2:
                ap = toks[-1]
                ini = " ".join([f"{x[0]}." for x in toks[:-1] if x])
                out.append(f"{ap}, {ini}")
            else:
                out.append(p)
    if not out:
        return ""
    if len(out) == 1:
        return out[0]
    if len(out) == 2:
        return f"{out[0]} & {out[1]}"
    return ", ".join(out[:-1]) + f", & {out[-1]}"

def apa_year(raw):
    """Usa texto; si NO APLICA -> vacío; si viene otro texto sin año válido, respeta 's.f.' si ya se puso en la base."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if s.upper() == "NO APLICA":
        return ""
    return s  # ya viene con s.f. si aplica

def coalesce(row, *names):
    for n in names:
        if n in row and str(row[n]).strip():
            return str(row[n]).strip()
    return ""

def format_apa_row(row):
    """
    Reglas dadas:
    - Título (obligatorio)
    - Autor(es) opcional (omite si 'NO APLICA')
    - Editorial: obligatorio en teoría; si falta -> 's. e.'
    - Año de Publicación: texto; 'NO APLICA' -> vacío; 's.f.' está permitido (viene de origen).
    - Base de datos: incluir siempre 'Título disponible en …'
    - Url de acceso: **siempre** (Digital) / unificado (Físico)
    - ISBN / ISSN1 si existen (no 'NO APLICA'), tras 'Título disponible en …'
    - Físico: añadir 'Título disponible en físico, No Topográfico: …'
    - No se incluyen Temáticas, SJR, etc.
    - Tipologías normalizadas (“Material especial”, “Material didáctico”) -> cita genérica.
    """
    fuente = row.get("Fuente", "")
    tipo_norm = row.get(COL_TIPO_ITEM_NORM, "")
    tipo = row.get(COL_TIPO_ITEM, "")

    titulo = str(row.get(COL_TITULO, "")).strip().rstrip(".")
    if not titulo:
        return ""

    autores = apa_authors(row.get(COL_AUTORES, ""))
    editorial = row.get(COL_EDITORIAL, "").strip()
    if not editorial:
        editorial = "s. e."
    anio = apa_year(row.get(COL_ANIO, ""))

    base_datos = row.get(COL_BASE_DATOS, "").strip()
    url = coalesce(row, COL_URL_ACCESO_STD, COL_URL_OA, COL_URL_FISICA)
    isbn = row.get(COL_ISBN, "")
    issn = row.get(COL_ISSN1, "")
    if str(isbn).upper() == "NO APLICA":
        isbn = ""
    if str(issn).upper() == "NO APLICA":
        issn = ""
    topo = row.get(COL_NO_TOPO, "")

    # Autores + (Año). Título. Editorial.
    pref = ""
    if autores:
        pref += f"{autores} "
    if anio:
        pref += f"({anio}). "
    elif autores:
        pref += "(s. f.). "
    # Si no hubo autores ni año, no ponemos (s.f.) para no “ensuciar” referencias de Govt/Corp sin año claro:
    elif not autores:
        pref += ""

    core = f"{pref}{titulo}. {editorial}."

    # Disponibilidad
    disp = ""
    if str(fuente).lower().startswith("fís"):
        # Físico
        disp = " Título disponible en físico"
        if topo:
            disp += f", No Topográfico: {topo}"
        disp += "."
        if url:
            disp += f" {url}"
    else:
        # Digital
        if base_datos:
            disp = f" Título disponible en {base_datos}."
        else:
            disp = " Título disponible en plataforma digital."
        if url:
            disp += f" {url}"

    # Identificadores
    ident = ""
    if isbn:
        ident += f" ISBN: {isbn}."
    if issn:
        ident += f" ISSN: {issn}."

    # Material especial / didáctico -> no cambia formateo, solo caemos en genérico (ya lo es)
    return " ".join((core + disp + (" " + ident if ident else "")).split())

# ------------------------------ Resultados / UI --------------------------------------

def render_results_ui(df_result):
    st.subheader("Resultados")
    if df_result is None or df_result.empty:
        st.info("Aún no hay resultados. Ejecuta la búsqueda.")
        return

    res0 = df_result.copy()

    # Filtros
    cfa, cfb, cfc = st.columns([1.2, 1.2, 1])
    tipos_col = COL_TIPO_ITEM if COL_TIPO_ITEM in res0.columns else None
    if tipos_col:
        tipos = sorted([t for t in res0[tipos_col].dropna().unique() if str(t).strip()])
        sel_tipos = cfa.multiselect("Filtrar por **Tipo de ítem**", tipos, default=tipos)
    else:
        sel_tipos = None

    tema_norm_col = "Temática normalizada" if "Temática normalizada" in res0.columns else None
    if tema_norm_col:
        tnorms = sorted([t for t in res0[tema_norm_col].dropna().unique() if str(t).strip()])
        sel_tnorms = cfb.multiselect("Filtrar por **Temática normalizada**", tnorms, default=tnorms)
    else:
        sel_tnorms = None

    limit_view = cfc.number_input("Filas a mostrar (vista)", min_value=50, max_value=20000, value=800, step=50)

    filt = res0
    if sel_tipos is not None:
        filt = filt[filt[tipos_col].isin(sel_tipos)]
    if sel_tnorms is not None:
        filt = filt[filt[tema_norm_col].isin(sel_tnorms)]

    st.caption("Marca las filas para exportar **solo seleccionadas**, de lo contrario se exportará todo lo filtrado.")
    show = filt.copy()
    show.insert(0, "✔", False)
    view = show.head(int(limit_view)).copy()

    edited = st.data_editor(
        view,
        use_container_width=True,
        height=520,
        column_config={"✔": st.column_config.CheckboxColumn("Seleccionar")},
        hide_index=True,
        num_rows="fixed",
    )

    selected_ids = edited.index[edited["✔"]].tolist()
    selected = view.loc[selected_ids].drop(columns=["✔"], errors="ignore")
    export_df = selected if not selected.empty else filt.copy()

    col_exp1, col_exp2, col_exp3 = st.columns([1, 1, 1])

    with col_exp1:
        st.download_button(
            "⬇️ CSV (filtrado/seleccionado)",
            data=export_df.fillna("").to_csv(index=False).encode("utf-8"),
            file_name="resultados_filtrados.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with col_exp2:
        xbio = io.BytesIO()
        writer = pd.ExcelWriter(xbio, engine="xlsxwriter")
        # Dejar columnas “administrativas” también en Excel:
        export_df.to_excel(writer, index=False, sheet_name="Datos")
        writer.close()
        xbio.seek(0)
        st.download_button(
            "⬇️ Excel (filtrado/seleccionado)",
            data=xbio.getvalue(),
            file_name="resultados_filtrados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    # Citas APA (TXT)
    citas = []
    for _, r in export_df.iterrows():
        citas.append(format_apa_row(r))
    txt = "\n\n".join([c for c in citas if c]) if citas else "Sin filas seleccionadas/filtradas."

    with col_exp3:
        st.download_button(
            "📝 Citas APA (TXT)",
            data=txt.encode("utf-8"),
            file_name="citas_apa.txt",
            mime="text/plain",
            use_container_width=True,
        )

    # Vista previa (opcional)
    st.caption(f"Vista previa de {len(view)} filas (de {len(filt)} filtradas).")
    st.dataframe(view.drop(columns=["✔"], errors="ignore"), use_container_width=True, height=350)

# ------------------------------ App main --------------------------------------------

def main():
    # Estado
    if "results_df" not in ss:
        ss.results_df = None
    if "bitacora_df" not in ss:
        ss.bitacora_df = None

    tem_file, exc_file, man_dig, man_fis = sidebar()

    st.markdown("## Herramienta para la elaboración de bibliografías especializadas")
    show_info_panel()

    # Descarga/carga bases
    listo = ensure_bases_loaded(man_dig, man_fis)

    # Mientras “trabaja”, mantén vivo:
    keepalive_if_working()

    # Si aún no están listas, no muestres UI dependiente
    if not listo:
        st.warning("Cargando las bases Digital y Física desde la web oficial… Puedes subir **Temáticas** y **Términos a excluir** mientras tanto. No cierres esta ventana.")
        return

    # Mostrar panel de “Bases listas”
    with st.container(border=True):
        st.subheader("Bases oficiales cargadas en memoria (sesión)")
        st.markdown(f"- Base de datos de la colección **Digital**")
        st.markdown(f"- Base de datos de la colección **Física**")

    st.markdown("### Configuración de búsqueda y duplicados")

    # Defaults con fallback si faltan
    dig_cols = ss.df_digital.columns.tolist()
    fis_cols = ss.df_fisica.columns.tolist()

    # Selects
    col1, col2, col3, col4 = st.columns([1.1, 1.1, 1.2, 1.2])
    col1_busq = col1.selectbox(
        "Búsqueda principal por:",
        options=dig_cols if dig_cols else [COL_TITULO],
        index=dig_cols.index(COL_TITULO) if COL_TITULO in dig_cols else 0,
        key="sel_col1"
    )
    col2_busq = col2.selectbox(
        "Búsqueda complementaria por:",
        options=dig_cols if dig_cols else [COL_TEMATICAS],
        index=dig_cols.index(COL_TEMATICAS) if COL_TEMATICAS in dig_cols else 0,
        key="sel_col2"
    )
    col_dup_dig = col3.selectbox(
        "Columna de duplicados en **Colección Digital**",
        options=dig_cols,
        index=dig_cols.index(DEF_DUP_DIGITAL) if DEF_DUP_DIGITAL in dig_cols else 0,
        key="sel_dup_dig"
    )
    col_dup_fis = col4.selectbox(
        "Columna de duplicados en **Colección Física**",
        options=fis_cols,
        index=fis_cols.index(DEF_DUP_FISICA) if DEF_DUP_FISICA in fis_cols else 0,
        key="sel_dup_fis"
    )

    # Plantillas (obligatorias)
    df_temas = None
    df_excluir = None
    if tem_file is not None:
        try:
            df_temas = pd.read_excel(tem_file)
            st.success(f"Temáticas cargadas: {len(df_temas)}")
        except Exception as e:
            st.error(f"Error leyendo Temáticas: {e}")

    if exc_file is not None:
        try:
            df_excluir = pd.read_excel(exc_file)
            st.success(f"Términos a excluir cargados: {len(df_excluir)}")
        except Exception as e:
            st.error(f"Error leyendo Términos a excluir: {e}")

    # Botón de búsqueda
    can_search = (df_temas is not None and not df_temas.empty and
                  df_excluir is not None and not df_excluir.empty and
                  ss.df_digital is not None and ss.df_fisica is not None)

    cols_run = st.columns([1, 3, 1])
    with cols_run[1]:
        btn = st.button("🚀 Iniciar búsqueda", use_container_width=True, disabled=not can_search)

    if btn and can_search:
        with st.status("Ejecutando búsqueda (puede tardar)…", expanded=True):
            try:
                ss.processing_digital = True
                res, bit = run_search(
                    ss.df_digital, ss.df_fisica,
                    df_temas, df_excluir,
                    col1_busq=col1_busq,
                    col2_busq=col2_busq,
                    col_dup_dig=col_dup_dig,
                    col_dup_fis=col_dup_fis
                )
                ss.results_df = res
                ss.bitacora_df = bit
                ss.processing_digital = False
                if res.empty:
                    st.warning("Búsqueda finalizada, sin coincidencias con las temáticas dadas.")
                else:
                    st.success("Búsqueda finalizada ✅")
                    st.info(f"Filas resultantes: {len(res)}")
            except Exception as e:
                ss.processing_digital = False
                st.error(f"Ocurrió un error durante la búsqueda: {e}")

    # Bitácora
    st.markdown("### Bitácora")
    if ss.bitacora_df is not None and not ss.bitacora_df.empty:
        st.dataframe(ss.bitacora_df, use_container_width=True, height=220)
    else:
        st.caption("Aún no hay bitácora disponible.")

    # Resultados con filtros/selección/APA
    render_results_ui(ss.results_df)

# -------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
