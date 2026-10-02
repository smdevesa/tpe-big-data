#!/usr/bin/env python3
"""Genera docs/build/<doc>.pdf a partir de docs/<doc>/diseno.md.

Markdown --(pandoc, vía pypandoc)--> Typst --(compilador Typst, vía librería `typst`)--> PDF

Uso (desde la raíz del repo, con el venv activado o llamando al python del venv):
    python tools/docs/build_pdf.py                 # docs/entrega1/diseno.md -> docs/build/entrega1.pdf
    python tools/docs/build_pdf.py entrega2 --toc  # otro documento, con índice
"""
import argparse
import sys
from pathlib import Path

try:
    import pypandoc
    import typst
except ImportError:
    sys.exit(
        "Faltan dependencias. Activá el venv e instalá:\n"
        "    pip install -r tools/docs/requirements.txt\n"
        "(ver tools/docs/README.md)"
    )

TOOLS = Path(__file__).resolve().parent   # tools/docs/
ROOT = TOOLS.parents[1]                    # raíz del repo
DOCS = ROOT / "docs"


def build(doc: str, source: str, toc: bool, mainfont: str | None) -> Path:
    src_dir = DOCS / doc
    md = src_dir / source
    if not md.is_file():
        sys.exit(f"No existe {md}")

    out_dir = DOCS / "build"
    out_dir.mkdir(exist_ok=True)
    pdf = out_dir / f"{doc}.pdf"

    args = [
        "--standalone",
        f"--defaults={TOOLS / 'pdf.yaml'}",
        f"--include-in-header={TOOLS / 'pdf-style.typ'}",
        f"--lua-filter={TOOLS / 'filters' / 'pdf.lua'}",
    ]
    if mainfont:
        args.append(f"--variable=mainfont:{mainfont}")
    if toc:
        args.append("--toc")

    # Las imágenes del .typ son relativas (img/...): el archivo temporal tiene que
    # vivir junto al .md y Typst se compila con esa carpeta como raíz.
    tmp = src_dir / f".{md.stem}.build.typ"
    try:
        pypandoc.convert_file(
            str(md), "typst", format="markdown", extra_args=args, outputfile=str(tmp)
        )
        typst.compile(
            str(tmp),
            output=str(pdf),
            root=str(src_dir),
            font_paths=[str(TOOLS / "fonts")],  # fuentes incluidas en el repo (tools/docs/fonts)
        )
    finally:
        tmp.unlink(missing_ok=True)
    return pdf


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("doc", nargs="?", default="entrega1", help="carpeta dentro de docs/ (default: entrega1)")
    p.add_argument("--source", default="diseno.md", help="archivo Markdown dentro de esa carpeta")
    p.add_argument("--toc", action="store_true", help="agrega índice de contenidos")
    p.add_argument("--font", help="fuente principal (sobrescribe pdf.yaml)")
    a = p.parse_args()
    pdf = build(a.doc, a.source, a.toc, a.font)
    print(f"OK -> {pdf.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
