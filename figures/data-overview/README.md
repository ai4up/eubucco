# EUBUCCO overview figure

Self-contained manuscript overview figure for the EUBUCCO dataset. The generators render
the maps/Venn as tight transparent PNGs; `build_html.py` base64-embeds them into one
standalone HTML file (all other elements — stats, bars, F1 bars, colour bars — are native
HTML/CSS so text stays crisp).

Output: **`eubucco-overview-figure.html`** (i.e. `figures/eubucco-overview-figure.html`).

## Requirements

- A Python env with `geopandas` + `pyarrow` + `matplotlib` + `matplotlib-venn`
- Two data files at the **repo root**:
  - `region-stats.parquet` — NUTS-3 stats (source + ground-truth counts per region, with geometry)
  - `city-stats.parquet` — municipality (LAU) stats with geometry, for the hero density map

## Regenerate

Run from the repo root (order: the three generators first, then the assembler):

```bash
python figures/data-overview/gen_maps.py    # source + ground-truth maps
python figures/data-overview/gen_venn.py    # source-overlap Venn
python figures/data-overview/gen_hero.py    # floorspace-density hero map
python figures/data-overview/build_html.py  # assemble the HTML
```

PNGs land in `assets/`. `build_html.py` reads them and (re)writes the HTML.
If only the map data changed, re-run the relevant generator(s) then `build_html.py`.
If only text/layout/metrics changed, just re-run `build_html.py`.

## What lives where

| File | Produces |
|------|----------|
| `gen_maps.py` | `assets/map_src_{gov,osm,msft}.png`, `assets/map_gt_{type,subtype,height,floors,construction_year}.png`; prints the sampled colour ramps + EU-wide check stats |
| `gen_venn.py` | `assets/venn.png` — source-overlap Venn (overlap % are hard-coded in the `lbl(...)` calls) |
| `gen_hero.py` | `assets/map_hero.png` — floor area ÷ land area per municipality, log scale |
| `build_html.py` | the final HTML. Hard-codes the attribute-coverage bar values and the ML evaluation metrics (per-class F1 for type/subtype, and height/floors regression) |

Update the classification F1 / regression numbers in the `F1_TYPE`, `F1_SUBTYPE`, `REG`
lists in `build_html.py`; the coverage-bar values in `BARS`; the Venn overlap in `gen_venn.py`.
Map colour ramps live in `gen_maps.py`/`gen_hero.py` and are mirrored (as sampled hex stops)
in `build_html.py` for the CSS colour bars — keep the two in sync if you change a ramp.

## Export for print (optional)

The HTML is the source of truth. To rasterise/vectorise it for a manuscript (headless Chrome):

```bash
CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
F="file://$PWD/figures/eubucco-overview-figure.html"

# high-res PNG (3x)
"$CHROME" --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor=3 \
  --window-size=1400,2050 --screenshot=overview.png "$F"

# vector PDF (type/bars stay sharp & selectable)
"$CHROME" --headless=new --disable-gpu --no-pdf-header-footer --print-to-pdf=overview.pdf "$F"
```
