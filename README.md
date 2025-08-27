# Herramienta para la elaboración de bibliografías especializadas

## 📘 Descripción
Esta aplicación, desarrollada en **Streamlit**, permite realizar búsquedas en las bases de datos de la Biblioteca de la **Universidad El Bosque** (colecciones **Digital** y **Física**), filtrarlas según temáticas, resaltar términos a excluir y exportar resultados en Excel.  

La herramienta está diseñada para apoyar la **autogestión y la obtención de bibliografías especializadas** para programas académicos, asignaturas o temas de estudio, permitiendo además la **depuración manual de resultados** mediante el resaltado de posibles términos no deseados y la **generación de citas bibliográficas en formato APA (versión beta)**.

---

## 🚀 Características principales
- **Carga única por sesión** de las bases de datos oficiales de la Biblioteca (Colección Digital y Colección Física).  
  - La primera descarga puede tardar entre **4 y 5 minutos**, según el tamaño del archivo y la conexión a internet.  
  - Una vez cargadas, las bases permanecen en memoria para realizar múltiples búsquedas sin volver a descargarlas.  

- **Motor de búsqueda rápido y estable**  
  - Coincidencia exacta de términos en las columnas seleccionadas (por defecto: **Título** y **Temáticas**).  
  - **Resultados visibles en pantalla** con desplazamiento (máx. 200 filas visibles por Streamlit, aunque en el archivo descargado se incluyen **todos** los resultados).  

- **Filtros dinámicos en la interfaz**  
  - Filtrar resultados por **Tipo de ítem** o por **Temática normalizada**.  
  - Seleccionar filas mediante **checkboxes** para exportar únicamente los resultados deseados.  
  - Opción para **exportar todos los resultados** en un solo archivo Excel.  

- **Resaltado de términos a excluir**  
  - Los títulos o temáticas que contengan palabras del listado de “Términos a excluir” se muestran **resaltados en amarillo** en los resultados.  
  - Estos resultados **no se eliminan automáticamente**; quedan resaltados para que el usuario los revise y decida si los conserva o los descarta manualmente en Excel.  

- **Generación de citas en formato APA (versión beta)**  
  - A partir de las filas seleccionadas, se generan citas usando:  
    - **Título** (obligatorio en todos los casos).  
    - **Autor(es)** (solo si no contiene “NO APLICA”).  
    - **Editorial** (si está vacío se mostrará como *s.e*).  
    - **Año de publicación** (si es “NO APLICA” se omite; si está vacío se deja en blanco; en otros casos puede contener valores como *s.f.*).  
    - **ISBN** o **ISSN1** (si contienen datos distintos a “NO APLICA”, se añaden al final de la cita con el prefijo “ISBN:” o “ISSN:”).  
    - **Url OA / Url de acceso** (campo obligatorio en la colección digital, se incluye siempre al final con el texto **“Disponible en:”**).  
    - **No. Topográfico** (solo en colección física, se incluye al final con el texto **“Disponible en físico, No. Topográfico:”**).  
  - **Campos omitidos en las citas:** “Temáticas”, “Clasificación SJR”, “Formato”, “Tipo de ítem normalizado” y “Item Barcode”.  

- **Indicadores claros de estado**  
  - Durante la carga inicial de las bases oficiales se muestra un **indicador de progreso y un mensaje de “⏳ Sincronizando bases de datos oficiales, por favor espere…”** para evitar confusión.  
  - Una vez cargadas, aparece un mensaje fijo en verde: **“✅ Bases oficiales listas en memoria”**.  
  - Los botones de descarga de las bases oficiales se **deshabilitan automáticamente** después de la carga, para evitar descargas innecesarias y consumo adicional de recursos.  

- **Nueva búsqueda**  
  - Botón **“Nueva búsqueda”** para limpiar las temáticas cargadas, términos a excluir y resultados previos, **pero conservando las bases oficiales en memoria** (no se descargan de nuevo).  

- **Interfaz amigable para usuarios no técnicos**  
  - Instrucciones claras en un cuadro informativo (con viñetas y enlaces directos).  
  - Logo institucional fijo en la parte superior izquierda de la barra lateral.  
  - Opción para alternar entre **modo claro y oscuro** sin reiniciar la aplicación.  

---

## 📂 Flujo de uso
1. **Abrir la aplicación** en [https://bibliografias-especializadas-ueb.streamlit.app/](https://bibliografias-especializadas-ueb.streamlit.app/).  
2. En la **barra lateral**:  
   - Hacer clic en **“Descargar bases de datos oficiales”** para sincronizar la colección Digital y la colección Física desde el servidor de la Biblioteca.  
   - Mientras se descargan las bases (puede tardar 4–5 minutos), el usuario puede **cargar los archivos obligatorios**:
     - **Temáticas** → [Descargar plantilla](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20Tem%C3%A1ticas.xlsx)  
     - **Términos a excluir** → [Descargar plantilla](https://biblioteca.unbosque.edu.co/sites/default/files/Formatos-Biblioteca/Plantilla%20T%C3%A9rminos%20a%20excluir.xlsx)  
     - ⚠️ **Importante:** no dejar filas vacías en estas plantillas, ya que puede alterar los resultados de búsqueda.  
3. Una vez listas, la app mostrará el mensaje **“✅ Bases oficiales listas en memoria”** y se habilitará la configuración de búsqueda.  
4. Configurar las columnas de búsqueda y duplicados (por defecto ya vienen:  
   - **Búsqueda principal por:** Título  
   - **Búsqueda complementaria por:** Temáticas  
   - **Columna de duplicados en colección Digital:** Url de acceso  
   - **Columna de duplicados en colección Física:** No. Topográfico  
5. Ejecutar la **búsqueda** → se mostrará un **indicador de progreso con barra y mensajes informativos** (incluyendo aviso de procesamiento y tiempo estimado cada 10 términos).  
6. **Filtrar y seleccionar resultados** en la tabla de la interfaz.  
   - Filtrar por **Tipo de ítem** o **Temática normalizada**.  
   - Seleccionar filas con checkboxes para exportarlas a Excel o exportar todas las coincidencias.  
7. **Exportar resultados** en formato **.xlsx** con los metadatos y resaltados incluidos.  
8. **Generar citas APA (beta)** desde los resultados seleccionados.  
9. **Nueva búsqueda** → limpia temáticas, términos a excluir y resultados, pero mantiene las bases oficiales cargadas en memoria (no es necesario volver a descargarlas en la misma sesión).  
10. **Fin de sesión** → si cierras la pestaña o el navegador, los datos cargados se eliminan automáticamente y al volver a abrir la app será necesario descargar de nuevo las bases oficiales.  

---

## 📊 Bitácora de resultados
Al finalizar cada búsqueda, se genera un **listado de todos los términos buscados**, con el número de coincidencias encontradas (incluyendo los que tuvieron **0 resultados**).  
Esto permite evaluar qué términos deben ajustarse o complementarse con otras fuentes.  

---

## ⚙️ Requisitos técnicos
- **Python 3.13+** (solo necesario si corres la app en local; en **Streamlit Cloud** ya está preinstalado).  
- Dependencias (archivo `requirements.txt`):  
  ```txt
  streamlit==1.48.1
  pandas>=2.3
  numpy>=1.26
  openpyxl>=3.1
  xlsxwriter>=3.2
  requests>=2.31
  unidecode>=1.4.0
