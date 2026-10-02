-- Filtro de pandoc para el build a PDF (motor Typst).
-- Es invisible en GitHub/VS Code: usa construcciones que Markdown ya ignora o renderiza bien.
--   <!-- pagebreak -->      -> salto de página
--   <div align="center">   -> contenido centrado (portada)

function RawBlock(el)
  if el.format == 'html' and el.text:match('^%s*<!%-%-%s*pagebreak%s*%-%->%s*$') then
    return pandoc.RawBlock('typst', '#pagebreak()')
  end
end

function Div(el)
  if el.attributes['align'] == 'center' then
    local out = { pandoc.RawBlock('typst', '#align(center)[#v(3.5cm)') }
    for _, b in ipairs(el.content) do out[#out + 1] = b end
    out[#out + 1] = pandoc.RawBlock('typst', ']')
    return out
  end
end

-- <img src="..." width="240"> (HTML, para que GitHub respete el tamaño) -> imagen de pandoc
function RawInline(el)
  if el.format == 'html' then
    local src = el.text:match('<img[^>]-src="([^"]+)"')
    if src then
      local alt = el.text:match('alt="([^"]*)"') or ''
      local w = el.text:match('width="(%d+)"')
      local attrs = {}
      if w then attrs.width = (tonumber(w) * 0.75) .. 'pt' end
      return pandoc.Image({ pandoc.Str(alt) }, src, '', pandoc.Attr('', {}, attrs))
    end
  end
end

-- Tablas: ignorar los anchos de columna que pandoc deduce del Markdown (según la versión pueden salir
-- como porcentajes fijos y el texto de una columna pisa a la vecina). Sin ancho, Typst ajusta cada columna al contenido.
function Table(tbl)
  for i, spec in ipairs(tbl.colspecs) do
    tbl.colspecs[i] = { spec[1], nil }
  end
  return tbl
end
