"""Assemble the EUBUCCO overview figure.

Reads the PNGs in assets/ (produced by gen_maps.py, gen_venn.py, gen_hero.py),
base64-embeds them, and writes a single self-contained HTML file to
../eubucco-overview-figure.html. All non-map elements (stats, bars, F1 bars,
colour bars) are native HTML/CSS so text stays crisp.
"""
import base64
from pathlib import Path

HERE = Path(__file__).resolve().parent
ASSETS = HERE / "assets"
OUT = HERE.parent / "eubucco-overview-figure.html"


def uri(name):
    b = base64.b64encode((ASSETS / f"{name}.png").read_bytes()).decode()
    return f"data:image/png;base64,{b}"

IMG = {k: uri(k) for k in [
    "map_hero", "venn", "map_src_gov", "map_src_osm", "map_src_msft",
    "map_gt_type", "map_gt_subtype", "map_gt_height", "map_gt_floors", "map_gt_construction_year",
]}

MAKO = ["#dcefeb", "#b5dfd7", "#82c9c0", "#52abac", "#3486a0", "#2f6595", "#31447f", "#2f2a63", "#241a45"]
GREENS = ["#e3f1ec", "#cee9e0", "#b5e0d2", "#95d2c0", "#6ebea8", "#4ba88f", "#37967e", "#297e69", "#1f6353"]
HERO = ["#eef4f2", "#b1dfd7", "#6ec1b9", "#3f9ea5", "#2f7999", "#315086", "#2e295f", "#1e1532", "#120c22"]
mako_grad = ", ".join(MAKO)
green_grad = ", ".join(GREENS)
hero_grad = ", ".join(HERO)

# attribute coverage bars: name, (ground_truth, merged, ml), total. In-bar labels only
# for segments >= 10 % (keeps narrow merged slivers unlabelled but still visible).
BARS = [
    ("Type", (38.1, 7.4, 54.5), 100.0),
    ("Subtype", (17.3, 4.2, 78.5), 100.0),
    ("Height", (43.2, 0.1, 56.7), 100.0),
    ("Floors", (16.6, 3.4, 79.9), 100.0),
    ("Construction year", (15.6, 0.3, 0.0), 15.9),
]


def bar_row(name, parts, total):
    gt, mg, es = parts
    segs = ""
    for val, cls, tcol in [(gt, "gt", "#ffffff"), (mg, "mg", "#0f3b31"), (es, "es", "#22564a")]:
        if val <= 0:
            continue
        txt = f'{val:g}%' if val >= 10.0 else ""
        segs += (f'<span class="seg {cls}" style="width:{val}%;color:{tcol}">'
                 f'<em>{txt}</em></span>')
    return (f'<div class="bar-row"><span class="bar-name">{name}</span>'
            f'<span class="bar-track">{segs}</span>'
            f'<span class="bar-tot">{total:g}%</span></div>')

bars_html = "\n".join(bar_row(*b) for b in BARS)

# Data source of attribute VALUES, all 5 core attributes combined, among values that
# come from an external dataset (i.e. ground-truth + merged; excludes ML-estimated).
# Counts from eubucco_lat_lon.parquet <attr>_source columns (gov* / osm / msft):
#   gov 364,897,821 · osm 91,583,766 · msft 15,696,556  (472,178,143 sourced values;
#   full split incl. ML-estimated: ML 64.8% · gov 27.2% · osm 6.8% · msft 1.2%).
SRCDIST = [("Governmental", 77.3, "gov"), ("OpenStreetMap", 19.4, "osm"), ("Microsoft", 3.3, "msft")]
srcbar_html = "".join(f'<span class="seg {cls}" style="width:{val}%"></span>' for _, val, cls in SRCDIST)
srclegend_html = "".join(
    f'<li><i class="{cls}"></i>{name} <b>{round(val)}%</b></li>' for name, val, cls in SRCDIST)

# classification F1 per class for the released type & subtype attributes
# (effective F1; subtype accounts for two-stage error propagation)
F1_TYPE = [("Residential", 93), ("Non-residential", 90)]
F1_SUBTYPE = [
    ("Detached", 91), ("Semi-detached", 85), ("Apartment", 84), ("Terraced", 82),
    ("Others", 79), ("Commercial", 79), ("Agricultural", 69), ("Industrial", 62), ("Public", 62),
]


def f1_rows(items):
    return "\n".join(
        f'<div class="f1-row"><span class="f1-name">{n}</span>'
        f'<span class="f1-track"><span class="f1-fill" style="width:{v}%"></span></span>'
        f'<span class="f1-val">0.{v}</span></div>'
        for n, v in items)

f1_type_html = f1_rows(F1_TYPE)
f1_subtype_html = f1_rows(F1_SUBTYPE)

REG = [("Height", "1.43&thinsp;m", "2.43&thinsp;m", "0.63"),
       ("Floor count", "0.23", "0.44", "0.83")]
reg_html = "\n".join(
    f'<div class="reg2-row"><span class="rname">{a}</span>'
    f'<span class="rvals">MAE {mae} &middot; RMSE {rmse} &middot; R&sup2; {r2}</span></div>'
    for a, mae, rmse, r2 in REG)

SRC_MAPS = [
    ("Governmental", "#7dd3a0", "map_src_gov", "62%"),
    ("OpenStreetMap", "#e8b54a", "map_src_osm", "17%"),
    ("Microsoft", "#5cb1c9", "map_src_msft", "20%"),
]
src_maps_html = "\n".join(
    f'<figure class="mapfig"><figcaption><span class="dot" style="background:{col}"></span>'
    f'{title}<b>{share}</b></figcaption><img src="{IMG[img]}" alt="{title} share map"></figure>'
    for title, col, img, share in SRC_MAPS)

GT_MAPS = [
    ("Type", "map_gt_type", "38%"),
    ("Subtype", "map_gt_subtype", "17%"),
    ("Height", "map_gt_height", "43%"),
    ("Floors", "map_gt_floors", "17%"),
    ("Construction year", "map_gt_construction_year", "16%"),
]
gt_maps_html = "\n".join(
    f'<figure class="mapfig"><figcaption>{title}<b>{share}</b></figcaption>'
    f'<img src="{IMG[img]}" alt="{title} ground-truth map"></figure>'
    for title, img, share in GT_MAPS)

HTML = f"""<title>EUBUCCO — Dataset overview</title>
<style>
  :root {{
    --ground:#ffffff; --panel:#f7faf9;
    --ink:#14201c; --muted:#5f6f6a; --faint:#8a9793;
    --line:rgba(20,32,28,.13); --line-soft:rgba(20,32,28,.07);
    --accent:#2f8f78; --blue:#3486a0;
    --gov:#7dd3a0; --msft:#5cb1c9; --osm:#e8b54a;
    --gt:#2f8f78; --mg:#79c4b1; --es:#bfe3d9;
    --sans:-apple-system,BlinkMacSystemFont,"Segoe UI","Helvetica Neue",Arial,sans-serif;
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background: transparent; }} /* or #e9edeb */
  .fig {{
    max-width:1440px; margin:26px auto; background:var(--ground);
    color:var(--ink); font-family:var(--sans);
    border:1px solid var(--line); border-radius:14px; overflow:hidden;
    -webkit-font-smoothing:antialiased;
  }}
  .kicker {{ font-size:.72rem; letter-spacing:.19em; text-transform:uppercase;
    font-weight:700; color:var(--accent); margin:0 0 8px; }}
  .phead {{ display:flex; align-items:baseline; gap:10px; margin:0 0 14px; }}
  .plet {{ flex:none; width:1.4rem; height:1.4rem; border-radius:50%;
    background:var(--ink); color:#fff; font-size:.78rem; font-weight:700;
    display:inline-flex; align-items:center; justify-content:center; transform:translateY(2px); }}
  .ptitle {{ font-size:1.06rem; font-weight:750; letter-spacing:-.01em; margin:0; }}
  .psub {{ font-size:.82rem; color:var(--muted); margin:2px 0 0; }}

  /* heading + legend on one row (panels b & c) */
  .phead-row {{ display:flex; align-items:center; justify-content:space-between;
    gap:28px; margin:0 0 16px; }}
  .phead-row .phead {{ margin:0; }}
  .phead-row .scale {{ flex:0 0 auto; width:300px; margin:14px 28px 0 0; }}
  .phead-row .scale .ramp, .phead-row .scale .ends {{ width:100%; }}

  /* ---------- top band ---------- */
  .top {{ display:grid; grid-template-columns:0.95fr 1.05fr; align-items:stretch;
    border-bottom:1px solid var(--line); }}
  .topleft {{ padding:38px 36px; display:flex; flex-direction:column; gap:28px; justify-content:center; }}
  .lead-h1 {{ font-size:2.15rem; font-weight:800; letter-spacing:-.028em; margin:0; line-height:1.06; }}
  .lead {{ margin:.65rem 0 0; font-size:.97rem; color:var(--muted); line-height:1.58; }}
  .stats {{ display:grid; grid-template-columns:repeat(3,1fr); gap:0;
    border:1px solid var(--line); border-radius:11px; overflow:hidden; }}
  .stat {{ padding:18px 20px; border-left:1px solid var(--line-soft); }}
  .stat:first-child {{ border-left:0; }}
  .stat .num {{ font-size:1.9rem; font-weight:800; letter-spacing:-.03em; line-height:1;
    font-variant-numeric:tabular-nums; }}
  .stat .num .plus {{ color:var(--accent); }}
  .stat .lbl {{ display:block; margin-top:7px; font-size:.74rem; color:var(--muted); line-height:1.3; }}

  .topright {{ position:relative; border-left:1px solid var(--line);
    display:flex; align-items:center; justify-content:center; background:#fbfdfc; }}
  .topright img {{ width:100%; height:auto; display:block; }}
  .herocap {{ position:absolute; top:6%; left:4%; width:36%; max-width:340px; }}
  .herocap .htext {{ font-size:.84rem; color:var(--muted); line-height:1.5; margin:0 0 14px; }}
  .heroscale {{ display:flex; flex-direction:column; gap:4px; max-width:176px; }}
  .heroscale .ramp {{ height:9px; border-radius:999px; border:1px solid var(--line-soft); }}
  .heroscale .ticks {{ display:flex; justify-content:space-between; font-size:.66rem;
    color:var(--faint); font-variant-numeric:tabular-nums; }}
  .heroscale .cap {{ font-size:.68rem; color:var(--muted); line-height:1.3; }}

  /* ---------- generic panel ---------- */
  .panel {{ padding:28px 32px 32px; border-bottom:1px solid var(--line); }}
  .panel:last-child {{ border-bottom:0; }}

  /* sources */
  .srcwrap {{ display:grid; grid-template-columns:.6fr 2.4fr; gap:24px; align-items:center; }}
  .vennbox {{ text-align:center; }}
  .vennbox img {{ width:100%; max-width:300px; height:auto; }}
  .vennbox .vcap {{ font-size:.75rem; color:var(--muted); margin:6px 0 0; line-height:1.4; }}
  .maprow {{ display:grid; grid-template-columns:repeat(3,1fr); gap:16px; }}
  .mapfig {{ margin:0; }}
  .mapfig img {{ width:100%; height:auto; display:block; }}
  .mapfig figcaption {{ display:flex; align-items:center; justify-content:center; gap:7px;
    font-size:.87rem; font-weight:650; margin-bottom:3px; }}
  .mapfig figcaption b {{ color:var(--muted); font-weight:700; font-variant-numeric:tabular-nums; }}
  .mapfig figcaption .dot {{ width:.64rem; height:.64rem; border-radius:50%; flex:none; }}
  .gtrow {{ display:grid; grid-template-columns:repeat(5,1fr); gap:0; }}
  .gtrow .mapfig {{ margin-inline:-18px; }}
  .gtrow .mapfig:first-child {{ margin-left:0; }}
  .gtrow .mapfig:last-child {{ margin-right:0; }}

  .scale {{ display:flex; flex-direction:column; align-items:center; gap:5px; margin:18px auto 0; }}
  .scale .ramp {{ width:min(320px,55%); height:10px; border-radius:999px; border:1px solid var(--line-soft); }}
  .scale .ends {{ width:min(320px,55%); display:flex; justify-content:space-between;
    font-size:.72rem; color:var(--faint); font-variant-numeric:tabular-nums; }}
  .scale .cap {{ font-size:.78rem; color:var(--muted); }}

  /* ---------- attributes row: coverage | evaluation ---------- */
  .attgrid {{ display:grid; grid-template-columns:1fr 1fr; }}
  .att-l {{ display:flex; flex-direction:column; padding:6px 60px 6px 6px; }}
  .att-r {{ display:flex; flex-direction:column; padding:6px 6px 6px 60px; border-left:1px solid var(--line); }}
  .att-body {{ flex:1; display:flex; flex-direction:column; justify-content:flex-start;
    max-width:552px; padding-top:8px; }}

  /* single decomposed source-composition bar + legend */
  .srcbar {{ display:flex; height:22px; border-radius:999px; overflow:hidden; margin-left: calc(5.4rem + 12px); margin-right: calc(2.8rem + 12px);}}
  .srcbar .seg {{ height:100%; }}
  .srcbar .seg.gov {{ background:var(--gov); }}
  .srcbar .seg.osm {{ background:var(--osm); }}
  .srcbar .seg.msft {{ background:var(--msft); }}
  .src-legend {{ list-style:none; display:flex; justify-content: center; flex-wrap:wrap; gap:6px 18px; margin:10px 0 0; padding:0; }}
  .src-legend li {{ display:flex; align-items:center; gap:7px; font-size:.79rem; color:var(--muted); }}
  .src-legend i {{ width:.8rem; height:.8rem; border-radius:3px; flex:none; }}
  .src-legend i.gov {{ background:var(--gov); }} .src-legend i.osm {{ background:var(--osm); }}
  .src-legend i.msft {{ background:var(--msft); }}
  .src-legend b {{ color:var(--ink); font-weight:800; font-variant-numeric:tabular-nums; }}

  .cov {{ display:flex; flex-direction:column; gap:12px; }}
  .bar-row {{ display:flex; align-items:center; gap:12px; }}
  .bar-name {{ flex:none; width:5.4rem; font-size:.85rem; font-weight:700; text-align:right; }}
  .bar-track {{ flex:1; display:flex; height:24px; border-radius:999px; overflow:hidden; background:#ececec; }}
  .bar-track .seg {{ height:100%; display:flex; align-items:center; justify-content:center; }}
  .bar-track .seg em {{ font-style:normal; font-size:.74rem; font-weight:600; font-variant-numeric:tabular-nums; }}
  .seg.gt {{ background:var(--gt); }} .seg.mg {{ background:var(--mg); }} .seg.es {{ background:var(--es); }}
  .bar-tot {{ flex:none; width:2.8rem; font-size:.85rem; font-weight:800; text-align:left;
    font-variant-numeric:tabular-nums; }}
  .cov-legend {{ list-style:none; display:flex; justify-content: center; flex-wrap:wrap; gap:8px 18px; margin:16px 0 0; padding:0; }}
  .cov-legend li {{ display:flex; align-items:center; gap:7px; font-size:.79rem; color:var(--muted); }}
  .cov-legend i {{ width:.8rem; height:.8rem; border-radius:3px; display:inline-block; flex:none; }}
  .cov-legend i.gt {{ background:var(--gt); }} .cov-legend i.mg {{ background:var(--mg); }}
  .cov-legend i.es {{ background:var(--es); }}

  /* evaluation: per-class F1 bars + soft regression readout */
  .evcap {{ font-size:.71rem; letter-spacing:.09em; text-transform:uppercase; font-weight:700;
    color:var(--muted); margin:0 0 11px; }}
  .evcap .u {{ letter-spacing:0; text-transform:none; font-weight:500; color:var(--faint); font-size:.73rem; }}
  .f1 {{ display:flex; flex-direction:column; gap:8px; }}
  .f1-grp {{ font-size:.72rem; font-weight:700; color:var(--ink); margin:0 0 1px; }}
  .f1-row {{ display:flex; align-items:center; gap:13px; }}
  .f1-name {{ flex:none; width:6.9rem; font-size:.8rem; color:var(--ink); text-align:right; }}
  .f1-track {{ flex:1; height:9px; background:#eaeeed; border-radius:3px; overflow:hidden; }}
  .f1-fill {{ display:block; height:100%; background:var(--blue); border-radius:3px; }}
  .f1-val {{ flex:none; width:2.3rem; text-align:right; font-size:.8rem; font-weight:700;
    color:var(--ink); font-variant-numeric:tabular-nums; }}
  .reg2 {{ display:flex; flex-direction:column; }}
  .reg2-row {{ display:flex; justify-content:space-between; align-items:baseline; gap:14px;
    padding:8px 0; border-bottom:1px solid var(--line-soft); }}
  .reg2-row:last-child {{ border-bottom:0; }}
  .reg2-row .rname {{ font-size:.84rem; font-weight:700; }}
  .reg2-row .rvals {{ font-size:.8rem; color:var(--muted); font-variant-numeric:tabular-nums; }}

  @media (max-width:900px) {{
    .top {{ grid-template-columns:1fr; }}
    .topright {{ border-left:0; border-top:1px solid var(--line); }}
    .herocap {{ position:static; width:auto; max-width:none; padding:22px 22px 4px; }}
    .srcwrap, .attgrid {{ grid-template-columns:1fr; }}
    .att-l {{ padding:0; }}
    .att-r {{ padding:24px 0 0; border-left:0; border-top:1px solid var(--line); margin-top:24px; }}
    .att-body {{ max-width:none; }}
    .gtrow {{ grid-template-columns:repeat(3,1fr); }}
    .panel,.topleft {{ padding-left:22px; padding-right:22px; }}
    .phead-row {{ flex-direction:column; align-items:flex-start; gap:14px; }}
    .phead-row .scale {{ width:100%; }}
  }}
</style>

<div class="fig">
  <!-- top band: stats (left) | building stock (right) -->
  <div class="top">
    <div class="topleft">
      <div>
        <p class="kicker">Dataset overview</p>
        <h1 class="lead-h1">EUBUCCO v1.0 &mdash; A harmonized European building stock database</h1>
        <p class="lead">Building footprints integrated across multiple data sources and enriched with five harmonized core attributes, providing complete attribute coverage, source provenance, and uncertainty estimates.</p>
      </div>
      <div class="stats">
        <div class="stat"><span class="num">322<span class="plus">M+</span></span>
          <span class="lbl">individual buildings</span></div>
        <div class="stat"><span class="num">30</span>
          <span class="lbl">countries <br>(EU-27 + UK, NO, CH)</span></div>
        <div class="stat"><span class="num">57</span>
          <span class="lbl">open datasets harmonized</span></div>
      </div>
    </div>

    <div class="topright">
      <img src="{IMG['map_hero']}" alt="Floor-space density across Europe">
      <div class="herocap">
        <div class="phead"><span class="plet">a</span>
          <h2 class="ptitle">Building stock distribution</h2></div>
        <p class="htext">Estimated floor&#8209;space density for <br>322 million buildings,
          aggregated to 98,676&nbsp;municipalities (LAU).</p>
        <div class="heroscale">
          <div class="ramp" style="background:linear-gradient(90deg,{hero_grad})"></div>
          <div class="ticks"><span>0.002</span><span>0.03</span><span>0.4</span></div>
          <div class="cap">m&sup2; floor space per m&sup2; of land</div>
        </div>
      </div>
    </div>
  </div>

  <!-- b — footprint sources -->
  <section class="panel">
    <div class="phead-row">
      <div class="phead"><span class="plet">b</span>
        <div><h2 class="ptitle">Footprint source composition</h2>
          <p class="psub">Overlap between building footprint sources and their regional contributions across the study area.</p></div>
      </div>
      <div class="scale">
        <div class="cap">Share of a region&rsquo;s buildings from this source [%]</div>
        <div class="ramp" style="background:linear-gradient(90deg,{mako_grad})"></div>
        <div class="ends"><span>0</span><span>100</span></div>
      </div>
    </div>
    <div class="srcwrap">
      <div class="vennbox">
        <img src="{IMG['venn']}" alt="Venn diagram of building source overlap">
        <p class="vcap">Estimated overlap between sources [%]</p>
      </div>
      <div class="maprow">{src_maps_html}</div>
    </div>
  </section>

  <!-- c — ground-truth geography -->
  <section class="panel">
    <div class="phead-row">
      <div class="phead"><span class="plet">c</span>
        <div><h2 class="ptitle">Geographic coverage of ground-truth attributes</h2>
          <p class="psub">Regional coverage of building attributes obtained from governmental sources and OpenStreetMap.</p></div>
      </div>
      <div class="scale">
        <div class="cap">Ground-truth coverage [%]</div>
        <div class="ramp" style="background:linear-gradient(90deg,{green_grad})"></div>
        <div class="ends"><span>0</span><span>100</span></div>
      </div>
    </div>
    <div class="gtrow" style="margin-top:20px;">{gt_maps_html}</div>
  </section>

  <!-- d + e — attribute coverage | ML evaluation -->
  <section class="panel">
    <div class="attgrid">
      <div class="att-l">
        <div class="phead"><span class="plet">e</span>
          <div><h2 class="ptitle">Attribute provenance</h2>
            <p class="psub">Provenance of the five core building attributes by enrichment approach and source dataset.</p></div>
        </div>
        <div class="att-body">
          <p class="evcap" style="margin-top:20px;">By enrichment approach</p>
          <div class="cov">{bars_html}</div>
          <ul class="cov-legend">
            <li><i class="gt"></i> Ground truth <span style="color:var(--faint)"></span></li>
            <li><i class="mg"></i> Merged <span style="color:var(--faint)"></span></li>
            <li><i class="es"></i> ML estimated</li>
          </ul>
          <p class="evcap" style="margin-top:44px;">By source dataset <span class="u">(across all attributes)</span></p>
          <div class="srcbar">{srcbar_html}</div>
          <ul class="src-legend">{srclegend_html}</ul>
        </div>
      </div>

      <div class="att-r">
        <div class="phead"><span class="plet">e</span>
          <div><h2 class="ptitle">Attribute reliability</h2>
            <p class="psub">Evaluation of ML-estimated attributes using 20% held-out ground-truth labels.</p></div>
        </div>
        <div class="att-body">
          <p class="evcap" style="margin-top:0px;">Regression</p>
          <div class="reg2">{reg_html}</div>
          <p class="evcap" style="margin-top:22px;">Classification <span class="u">(F1 per class)</span></p>
          <div class="f1">
            <p class="f1-grp">Type</p>
            {f1_type_html}
            <p class="f1-grp" style="margin-top:8px;">Subtype</p>
            {f1_subtype_html}
          </div>
        </div>
      </div>
    </div>
  </section>

</div>
"""

OUT.write_text(HTML)
print("wrote", OUT, len(HTML), "bytes")