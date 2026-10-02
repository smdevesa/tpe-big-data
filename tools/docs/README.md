# Cómo generar el PDF de entrega (`tools/docs`)

El documento se escribe en Markdown (`docs/entrega1/diseno.md`) y un script de Python lo convierte a PDF:

```
Markdown --(pandoc)--> Typst --(compilador Typst)--> PDF
```

Todo se instala con `pip` dentro de un **venv**: no hace falta instalar pandoc, Typst ni LaTeX en el sistema.

```
docs/entrega1/                 ← contenido (lo que se edita)
├── diseno.md                  ← fuente de verdad
├── img/                       ← imágenes (logo, diagrama exportado de draw.io)
└── diagramas/*.mmd            ← diagramas en texto (Mermaid), opcionales

tools/docs/                    ← herramientas de build (no se edita para escribir el informe)
├── build_pdf.py               ← el script
├── pdf.yaml                   ← opciones: idioma, papel, márgenes, fuente
├── pdf-style.typ              ← estilo del PDF (colores, títulos, tablas, links)
├── fonts/                     ← fuente Inter incluida en el repo (licencia OFL)
└── filters/pdf.lua            ← filtro de pandoc (portada centrada, salto de página, logo)

docs/build/                    ← PDFs generados (ignorado por Git)
```

## 1. Preparar el entorno (una sola vez)

Requiere **Python 3.10 o superior**. Desde la raíz del repo:

**Windows (PowerShell)**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Si PowerShell bloquea `Activate.ps1` ("la ejecución de scripts está deshabilitada"), no hace falta activar nada:
usá siempre `.\.venv\Scripts\python` en lugar de `python`.

**macOS / Linux / Git Bash**

```bash
python3 -m venv .venv
source .venv/bin/activate          # en Git Bash: source .venv/Scripts/activate
pip install -r requirements.txt
```

Para comprobar que quedó bien:

```bash
python -c "import pypandoc, typst; print(pypandoc.get_pandoc_version(), typst.__file__)"
```

## 2. Generar el PDF

Con el venv activado, desde la raíz del repo:

```bash
python tools/docs/build_pdf.py      # -> docs/build/entrega1.pdf
```

Sin activar el venv:

```powershell
.\.venv\Scripts\python tools\docs\build_pdf.py     # Windows
```
```bash
.venv/bin/python tools/docs/build_pdf.py            # macOS / Linux
```

Opciones del script:

| Opción | Efecto |
|---|---|
| `--toc` | agrega índice de contenidos |
| `--font "Nombre"` | cambia la fuente (debe estar en `tools/docs/fonts/` o instalada en el sistema) |
| `otra_carpeta` | construye `docs/otra_carpeta/diseno.md` (p. ej. `python tools/docs/build_pdf.py entrega2`) |
| `--source archivo.md` | usa otro Markdown dentro de esa carpeta |

Los márgenes, el tamaño de papel y la letra por defecto están en `tools/docs/pdf.yaml`; colores, títulos y tablas en `tools/docs/pdf-style.typ`.

**Tipografía:** el PDF usa la fuente Inter, que viene en `tools/docs/fonts/`. Así el resultado es idéntico en cualquier máquina (si dependiera de las fuentes instaladas, Typst cae en una serif de reserva y se ve distinto). Para usar otra, copiá sus `.otf`/`.ttf` (regular, itálica, negrita y negrita itálica) a esa carpeta y cambiá `mainfont` en `tools/docs/pdf.yaml`.

## 3. Convenciones para escribir el Markdown

- **Portada:** el bloque `<div align="center"> … </div>` del principio. Se ve centrado en GitHub y en el PDF.
- **Salto de página:** `<!-- pagebreak -->` en una línea sola. Es un comentario HTML: GitHub lo ignora y el PDF corta ahí.
- **Imágenes:** ruta relativa a `docs/entrega1/`, p. ej. `![Descripción](img/arquitectura-v1.png)`. La descripción se usa como epígrafe en el PDF.
  Para fijar el tamaño usá `<img src="img/x.png" alt="..." width="240">` (así lo hace el logo).
- **Tablas:** sintaxis pipe estándar (`| a | b |`). Las celdas no admiten saltos de línea reales.
- **Código y nombres de campo:** con backticks. Los bloques de triple backtick salen en fuente monoespaciada.
- **HTML crudo:** evitarlo. El PDF solo entiende el `<div align="center">`, el `<img>` y el comentario de `pagebreak`; el resto se descarta en silencio.
- **Diagramas:** el PNG exportado de draw.io es lo que sale en el PDF. El `.mmd` (Mermaid) es una copia editable en texto; si cambian el diagrama, actualicen **ambos** o dejen uno solo.
  Para exportar de draw.io: *Archivo → Exportar como → PNG*, zoom 200 % y fondo blanco.
- **Numeración de títulos:** está escrita a mano en el Markdown (`## 1. Problema…`); no hay numeración automática.

## 4. Flujo de trabajo en equipo (Git)

```bash
git switch -c entrega1/seccion-roles        # una rama por cambio
# editar docs/entrega1/diseno.md
python tools/docs/build_pdf.py              # revisar el PDF antes de commitear
git add docs && git commit -m "docs: completa sección de roles"
git push -u origin entrega1/seccion-roles   # abrir PR y que lo revise el otro
```

- Para revisar un cambio de texto usen la pestaña **Files changed** del PR: se ve línea por línea, cosa que en Word no es posible.
- Al entregar, etiqueten la versión exacta: `git tag entrega1 && git push --tags`. La consigna evalúa "la versión disponible en el repositorio a la hora límite", y el tag la deja identificada.
- Después del feedback, los cambios van en commits aparte (por ejemplo prefijo `fix(feedback):`), porque la consigna pide marcarlos como correcciones derivadas del feedback.
- `.gitignore` excluye `.venv/` y `docs/build/`: el PDF no se versiona porque se reconstruye desde el Markdown. Lo que queda fijado en Git (y en el tag) es la fuente.
- Antes de entregar, generen el PDF desde el commit etiquetado (`git checkout entrega1`, build) y suban ese archivo, para que PDF y repo coincidan.
- Cada persona crea su propio `.venv`; `requirements.txt` (en la raíz) es lo único que se comparte.

## 5. Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `Faltan dependencias…` | El venv no está activado o no se instaló. Activalo y corré `pip install -r requirements.txt`. |
| `No module named …` pese a haber instalado | `pip` y `python` son de distintos entornos. Usá `python -m pip install -r requirements.txt`. |
| `SyntaxError` en `build_pdf.py` (en la línea `str \| None`) | Python menor a 3.10. Instalá una versión más nueva y recreá el venv. |
| Error de Typst tipo `unknown variable`, `unexpected argument` | El pandoc incluido genera sintaxis más nueva que la del compilador Typst. Actualizá: `pip install -U typst pypandoc_binary`. |
| `file not found … img/…png` | La ruta de la imagen en el Markdown es incorrecta (relativa a `docs/entrega1/`, y en Linux/macOS distingue mayúsculas). |
| `warning: unknown font family: …` | La fuente de `mainfont` no existe. Con la fuente por defecto (Inter) no debería pasar: viene en `tools/docs/fonts/`. Si cambiaste `mainfont`, copiá los `.otf`/`.ttf` a esa carpeta. |
| `.venv\Scripts\Activate.ps1 cannot be loaded…` | Política de ejecución de PowerShell. Usá `.\.venv\Scripts\python tools\docs\build_pdf.py` sin activar, o `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`. |
| Quedó un archivo `.entrega1.build.typ` en `docs/entrega1/` | El build se interrumpió a mitad. Es un temporal: borralo (está ignorado por Git). |

## 6. Vista previa mientras se escribe

En VS Code, `Ctrl+Shift+V` abre la vista previa de Markdown. Para ver los diagramas Mermaid dentro de ella
instalen la extensión *Markdown Preview Mermaid Support*. El PDF final siempre conviene mirarlo antes de entregar:
la vista previa de VS Code y el PDF no son idénticos (por ejemplo, el salto de página solo existe en el PDF).

## 7. Diagramas (Mermaid)

Los diagramas se escriben como texto en `docs/entrega1/diagramas/*.mmd` y se exportan a PNG en `docs/entrega1/img/`, que es lo que incluye el informe. Hay que re-exportar cada vez que se edita el `.mmd`:

```bash
npx -y @mermaid-js/mermaid-cli -i docs/entrega1/diagramas/arquitectura-v1.1.mmd -o docs/entrega1/img/arquitectura-v1.1.png -s 2 -b white
```

Requiere Node.js; la primera vez descarga un Chromium. Al cambiar la versión del diagrama, actualizar el título del `.mmd` (versión y fecha) y el nombre de los archivos.
