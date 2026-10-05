// Estilo del PDF (se inyecta con --include-in-header). Solo afecta al build, no al Markdown.
#let azul = rgb("#0b5a8f")      // azul institucional (tomado del logo)
#let gris = luma(90)

// Texto
#set text(lang: "es", hyphenate: false)
#set par(justify: false, leading: 0.78em, spacing: 1.05em)

// Títulos
#show heading.where(level: 1): it => block(above: 1.4em, below: 1em)[
  #set text(size: 26pt, weight: "bold", fill: azul)
  #it.body
]
#show heading.where(level: 2): it => block(above: 2em, below: 0.9em, breakable: false)[
  #set text(size: 16pt, weight: "bold", fill: azul)
  #it.body
  #v(-0.5em)
  #line(length: 100%, stroke: 0.6pt + azul.lighten(60%))
]
#show heading.where(level: 3): it => block(above: 1.5em, below: 0.7em, breakable: false)[
  #set text(size: 12pt, weight: "bold", fill: luma(30))
  #it.body
]

// Tablas: solo líneas horizontales, encabezado con fondo suave
#set table(
  stroke: (x: none, y: 0.5pt + luma(205)),
  inset: (x: 8pt, y: 7pt),
  fill: (x, y) => if y == 0 { azul.lighten(88%) },
)
#show table.cell.where(y: 0): set text(weight: "bold", fill: azul.darken(20%))
#show table.cell: set align(left + horizon)
#show table: set text(size: 0.92em)
#show table: it => block(above: 1.2em, below: 1.2em, it)

// Código en línea y en bloque: monoespaciada, sin caja de fondo
#let mono = ("DejaVu Sans Mono", "Consolas", "Menlo", "Liberation Mono")
#show raw.where(block: false): set text(font: mono, size: 0.9em, fill: azul.darken(25%))
#show raw.where(block: true): it => block(
  fill: luma(248), inset: 10pt, radius: 3pt, width: 100%, text(font: mono, size: 0.84em, it),
)

// Citas (> …)
#show quote.where(block: true): it => block(
  inset: (left: 12pt, y: 4pt), stroke: (left: 2pt + azul.lighten(50%)), text(fill: gris, it.body),
)

// Imágenes y epígrafes
#show figure: set block(above: 1.4em, below: 1.4em)
#show figure.caption: set text(size: 0.85em, fill: gris)

// Links
#show link: set text(fill: azul)
#show link: underline
