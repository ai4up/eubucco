#!/usr/bin/env python3
"""
EUBUCCO v1.0 processing-pipeline figure — scientific graphical abstract.
Six numbered stages, flat/typographic, with schematic footprint illustrations
of conflation + rubbersheeting (illus.py).
"""
import illus

# --------------------------------------------------------------------------- #
W = 1680
FONT = "Helvetica, Arial, sans-serif"
MONO = "'Courier New', monospace"

INK, SUB, FAINT = "#16202e", "#4f5868", "#9aa2b1"
HAIR, CELL, CANVAS = "#e6eaf0", "#e7ebf1", "#ffffff"

C_RET, C_PRE, C_CONF = "#3a6491", "#2e857c", "#b9802f"
C_PRED, C_CONF2, C_REL = "#6d5b95", "#8a7f5a", "#a8536b"
C_GOV, C_OSM, C_MS = illus.C_GOV, illus.C_OSM, illus.C_MS

# base font sizes (bumped for readability)
F_BODY = 12.7
F_CELLTITLE = 15
F_STAGETITLE = 21


def _h2rgb(h):
    h = h.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def mix(hexc, other="#ffffff", t=0.9):
    a, b = _h2rgb(hexc), _h2rgb(other)
    return "#%02x%02x%02x" % tuple(round(a[i] + (b[i] - a[i]) * t) for i in range(3))


def tint(hexc, t=0.90):
    return mix(hexc, "#ffffff", t)


# --------------------------------------------------------------------------- #
S = []


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def rrect(x, y, w, h, r=8, fill=CANVAS, stroke=None, sw=1.0, dash=None):
    a = f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="{r}" ry="{r}" fill="{fill}"'
    if stroke:
        a += f' stroke="{stroke}" stroke-width="{sw}"'
    if dash:
        a += f' stroke-dasharray="{dash}"'
    S.append(a + "/>")


def circle(cx, cy, r, fill, stroke=None, sw=1):
    a = f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r:.1f}" fill="{fill}"'
    if stroke:
        a += f' stroke="{stroke}" stroke-width="{sw}"'
    S.append(a + "/>")


def line(x1, y1, x2, y2, color=HAIR, sw=1.0, dash=None):
    a = f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{color}" stroke-width="{sw}"'
    if dash:
        a += f' stroke-dasharray="{dash}"'
    S.append(a + "/>")


def txt(x, y, s, size=12, fill=INK, weight="normal", anchor="start",
        family=FONT, ls=None, italic=False):
    a = (f'<text x="{x:.1f}" y="{y:.1f}" font-family="{family}" font-size="{size}" '
         f'fill="{fill}" font-weight="{weight}" text-anchor="{anchor}"')
    if ls is not None:
        a += f' letter-spacing="{ls}"'
    if italic:
        a += ' font-style="italic"'
    S.append(a + f">{esc(s)}</text>")


def _tw(s, size, weight="normal"):
    # calibrated to real Helvetica metrics (~0.46 body avg); kept slightly
    # above to stay conservative so wrapped lines never cross the cell border.
    f = 0.52 if weight == "bold" else 0.475
    return len(s) * size * f


def wrap(s, maxw, size, weight="normal"):
    words, lines, cur = s.split(), [], ""
    for wd in words:
        trial = (cur + " " + wd).strip()
        if _tw(trial, size, weight) <= maxw or not cur:
            cur = trial
        else:
            lines.append(cur)
            cur = wd
    if cur:
        lines.append(cur)
    return lines


def mtext(x, y, s, size, maxw, fill=SUB, weight="normal", lh=1.32):
    L = wrap(s, maxw, size, weight)
    for i, ln in enumerate(L):
        txt(x, y + i * size * lh, ln, size, fill, weight)
    return y + (len(L) - 1) * size * lh


def bullets(x, y, items, size, maxw, marker, lh=1.3, vgap=9):
    cy = y
    for it in items:
        L = wrap(it, maxw - 12, size)
        rrect(x, cy - size * 0.55, 4, 4, r=0.7, fill=marker)
        for j, ln in enumerate(L):
            txt(x + 12, cy + j * size * lh, ln, size, SUB)
        cy += (len(L) - 1) * size * lh + size + vgap
    return cy - vgap


def m_bul(items, size, maxw, vgap=9, lh=1.3):
    """Measure bullets() vertical extent (offset of bottom from first baseline)."""
    cy = 0
    for it in items:
        L = max(1, len(wrap(it, maxw - 12, size)))
        cy += (L - 1) * size * lh + size + vgap
    return cy - vgap


def m_txt(s, size, maxw, lh=1.32):
    """Offset of mtext()'s last baseline from its first."""
    return (max(1, len(wrap(s, maxw, size))) - 1) * size * lh


def voff_for(bh, ch, pad=18):
    """Box bodies are top-aligned (no vertical centring)."""
    return 0


def chevron(cx, cy, color, s=11):
    S.append(f'<path d="M{cx-s*0.35:.1f},{cy-s:.1f} L{cx+s*0.55:.1f},{cy:.1f} '
             f'L{cx-s*0.35:.1f},{cy+s:.1f}" fill="none" stroke="{color}" '
             f'stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/>')


def vconnect(cx, y1, y2, color=FAINT):
    line(cx, y1, cx, y2 - 8, color, 1.9)
    S.append(f'<path d="M{cx-6},{y2-8} L{cx+6},{y2-8} L{cx},{y2} Z" fill="{color}"/>')


def smallcaps(x, y, s, color, size=11, anchor="start"):
    txt(x, y, s.upper(), size, color, "bold", anchor=anchor, ls=1.5)


def formula(x, y, w, s, color, size=12.5):
    txt(x + w / 2, y, s, size, color, "bold", anchor="middle", family=MONO)


# --------------------------------------------------------------------------- #
MX, HEAD = 56, 48
CW = W - 2 * MX
IX = MX + 28
IW = CW - 56


def stage(y, h, num, accent, title, desc):
    rrect(MX, y, CW, h, r=12, fill=tint(accent, 0.955), stroke=tint(accent, 0.5), sw=1.3)
    rrect(MX + 20, y + 13, 28, 28, r=7, fill=accent)
    txt(MX + 34, y + 32, str(num), 16, "#fff", "bold", anchor="middle")
    txt(MX + 62, y + 33, title, F_STAGETITLE, INK, "bold", ls=0.2)
    txt(MX + 62 + _tw(title, F_STAGETITLE, "bold") + 18, y + 32, desc, 14, SUB, italic=True)
    line(MX + 20, y + HEAD, MX + CW - 20, y + HEAD, tint(accent, 0.4), 1.5)
    return y + HEAD


def cell(x, y, w, h, accent, letter, title, voff=0):
    rrect(x, y, w, h, r=8, fill=CANVAS, stroke=CELL, sw=1.2)
    txt(x + 16, y + 29, letter, 15, accent, "bold")
    txt(x + 34, y + 29, title, F_CELLTITLE, INK, "bold")
    line(x + 16, y + 41, x + w - 16, y + 41, HAIR, 1.0)
    return x + 16, y + 63 + voff


def tree_glyph(cx, cy, color):
    n = {"r": (cx, cy), "l1": (cx - 14, cy + 16), "l2": (cx + 14, cy + 16)}
    lv = [(cx - 21, cy + 32), (cx - 6, cy + 32), (cx + 6, cy + 32), (cx + 21, cy + 32)]
    line(*n["r"], *n["l1"], color, 1.5); line(*n["r"], *n["l2"], color, 1.5)
    line(*n["l1"], *lv[0], color, 1.5); line(*n["l1"], *lv[1], color, 1.5)
    line(*n["l2"], *lv[2], color, 1.5); line(*n["l2"], *lv[3], color, 1.5)
    for p in n.values():
        circle(*p, 3.2, color)
    for p in lv:
        circle(*p, 2.6, mix(color, "#fff", 0.4))


# --------------------------------------------------------------------------- #
#  Build
# --------------------------------------------------------------------------- #
cur = 46

txt(MX, cur + 32, "EUBUCCO v1.0", 31, INK, "bold", ls=0.3)
txt(MX + _tw("EUBUCCO v1.0", 31, "bold") + 42, cur + 32,
    "Data Processing Pipeline", 31, C_CONF, "normal", ls=0.3)
txt(MX, cur + 60,
    "From raw building-footprint sources to a harmonized, conflated and "
    "attribute-enriched building dataset.", 15, SUB)
cur += 102

# =========================================================================== #
#  STAGE 1 — DATA RETRIEVAL
# =========================================================================== #
srcs = [
    (C_GOV, "Governmental registries", "authoritative",
     ["47 datasets for EU-27 + NO, CH, UK",
      "bulk download, WFS, OGC API, FTP, or direct provision",
      "type · height · floors · construction year"]),
    (C_OSM, "OpenStreetMap", "volunteered",
     ["Geofabrik .pbf, retrieved with Pyrosm",
      "wildcard filter  building = *",
      "type (building, building:use, amenity) · height · floors (building:levels) · construction year (start_date)"]),
    (C_MS, "Microsoft GlobalMLBuildingFootprints", "aerial & satellite imagery",
     ["ML footprints over the full study area",
      "height estimates only"]),
]
cwd = (IW - 2 * 22) / 3
mb = [m_bul(items, F_BODY, cwd - 36) for *_, items in srcs]
ch = max(90 + m for m in mb) + 12
P1H = ch + 108
top = stage(cur, P1H, 1, C_RET, "Data retrieval", "Collect building footprints from multiple sources.")
cy = top + 18
for i, (col, name, kind, items) in enumerate(srcs):
    cx = IX + i * (cwd + 22)
    rrect(cx, cy, cwd, ch, r=8, fill=CANVAS, stroke=CELL, sw=1.2)
    circle(cx + 24, cy + 29, 7, col)
    txt(cx + 40, cy + 34, name, F_CELLTITLE, INK, "bold")
    smallcaps(cx + 20, cy + 56, kind, col, 10.5)
    line(cx + 18, cy + 66, cx + cwd - 18, cy + 66, HAIR, 1.0)
    bullets(cx + 20, cy + 90, items, F_BODY, cwd - 36, tint(col, 0.2))
note = "Provenance, licence and access date for every source documented in"
txt(IX, cy + ch + 30, note, 12.5, FAINT, italic=True)
txt(IX + _tw(note, 12.5) + 9, cy + ch + 30, "input-datasets-metadata.xlsx",
    12.5, mix(C_RET, "#000", 0.1), "bold", family=MONO)
vconnect(W / 2, cur + P1H, cur + P1H + 46)
cur += P1H + 46

# =========================================================================== #
#  STAGE 2 — PREPROCESSING
# =========================================================================== #
steps2 = [
    ("Geometry cleaning",
     ["convert 3D / 2.5D to 2D footprints",
      "split multipolygons into parts",
      "reproject to ETRS89 (EPSG:3035)",
      "repair or remove invalid geometries"], None),
    ("Deduplication",
     ["remove redundant intra-source footprints",
      "duplicate if IoSA > 0.25",
      "discard the smaller (sub-part) footprint"],
     "IoSA = |a ∩ b| / min(|a|,|b|)"),
    ("Admin-region mapping",
     ["centroid join with Eurostat 2016 boundaries",
      "assign NUTS3 code as region_id",
      "assign LAU code as city_id"], None),
    ("Attribute harmonization",
     ["__CROSSWALK__",
      "use-type taxonomy: binary type and detailed subtype",
      "range filters on height, floors, year",
      "drop non-building tags"], None),
    ("Rubbersheeting",
     ["correct systematic spatial misalignments of source datasets",
      "determine affine shift per H3 res-9 cell using reciprocal-NN landmark pairs"], None),
]
n, gap = 5, 22
bw = (IW - (n - 1) * gap) / n
RUB_H = 72
admin_assign = ["assign NUTS3 code as region_id", "assign LAU code as city_id"]


def _ch2(i, items, fm):
    if i == 2:
        return 53 + m_bul(admin_assign, F_BODY, bw - 28)
    if i == 3:
        return 53 + m_bul(items[1:], F_BODY, bw - 28)
    base = m_bul(items, F_BODY, bw - 28)
    if fm:
        base += 46
    if i == 4:
        base += 24 + RUB_H + 13
    return base


chs2 = [_ch2(i, items, fm) for i, (_, items, fm) in enumerate(steps2)]
bh = max(60 + c for c in chs2) 
P2H = HEAD + 18 + bh + 14
top = stage(cur, P2H, 2, C_PRE, "Preprocessing", "Clean, harmonize, and spatially align source datasets.")
by = top + 18
M = tint(C_PRE, 0.2)
for i, (title, items, fm) in enumerate(steps2):
    bx = IX + i * (bw + gap)
    vo = voff_for(bh, chs2[i])
    tx, ty = cell(bx, by, bw, bh, C_PRE, chr(97 + i), title, vo)
    if i == 2:
        rrect(tx, ty - 9, 4, 4, r=0.7, fill=M)
        txt(tx + 12, ty, "join building centroids with", F_BODY, SUB)
        txt(tx + 12, ty + 16, "Eurostat 2016 boundaries:", F_BODY, SUB)
        txt(tx + 12, ty + 31, "NUTS-regions-2016.parquet", 11.5, mix(C_PRE, "#000", .15), "bold", family=MONO)
        txt(tx + 12, ty + 46, "LAU-cities-2016.parquet", 11.5, mix(C_PRE, "#000", .15), "bold", family=MONO)
        bullets(tx, ty + 68, admin_assign, F_BODY, bw - 28, M)
    elif i == 3:
        rrect(tx, ty - 9, 4, 4, r=0.7, fill=M)
        txt(tx + 12, ty, "map use types via crosswalk", F_BODY, SUB)
        txt(tx + 12, ty + 16, "building-type-harmonization.csv", 11.5, mix(C_PRE, "#000", .15), "bold", family=MONO)
        bullets(tx, ty + 36, items[1:], F_BODY, bw - 28, M)
    else:
        yb = bullets(tx, ty, items, F_BODY, bw - 28, M)
        if fm:
            line(bx + 16, yb + 13, bx + bw - 16, yb + 13, HAIR, 1.0)
            formula(bx + 16, yb + 34, bw - 32, fm, mix(C_PRE, "#000", 0.2), 12)
        if i == 4:
            rds = RUB_H / illus.RVH
            rw = illus.RVW * rds
            rox = bx + (bw - rw) / 2
            roy = yb - 4
            S.extend(illus.rubber(rox, roy, rds))
            txt(bx + bw / 2, roy + RUB_H + 13, "same affine shift per H3 cell",
                9.5, FAINT, italic=True, anchor="middle")
    if i < n - 1:
        chevron(bx + bw + gap / 2, by + bh / 2, tint(C_PRE, 0.3))
vconnect(W / 2, cur + P2H, cur + P2H + 46)
cur += P2H + 46

# =========================================================================== #
#  STAGE 3 — CONFLATION
# =========================================================================== #
steps3 = ["candidate", "matching", "geometry", "attribute"]
titles3 = {"candidate": "Candidate pairs", "matching": "Pairwise matching",
           "geometry": "Geometry merge", "attribute": "Attribute merge"}
cand_b = ["k-NN search (k = 3)", "≤ 10 m bounding-box threshold",
          "symmetric search to capture many-to-many (m : n) links comprehensively"]
match_b = ["binary ML classifier (XGBoost)", "geometric + contextual features:"]
geom_b = ["block-level selection by priority", "keep footprints of highest priority"]
attr_lead = "Merge attributes between matching buildings across sources:"
attr_b = ["numeric → area-weighted mean", "categorical → dominant by area"]
n, gap = 4, 22
bw = (IW - (n - 1) * gap) / n
ill_w = 104
ds = ill_w / illus.SVW
ill_h = illus.SVH * ds
tW = bw - (ill_w + 14) - 20
M = tint(C_CONF, 0.25)
sub_c = mix(C_CONF, "#000", 0.1)
# measure cell content heights (extent from first baseline)
ch3 = {
    "candidate": max(m_bul(cand_b, F_BODY, tW), ill_h),
    "matching": max(m_bul(match_b, F_BODY, tW, vgap=7) + 16 + 4 * 17 + 6 + F_BODY, ill_h),
    "geometry": max(m_bul(geom_b, F_BODY, tW), ill_h),
    "attribute": max(m_txt(attr_lead, F_BODY, tW) + 22 + m_bul(attr_b, F_BODY, tW) + 60 + 12.5, ill_h),
}
bh = max(63 + ch3[m] for m in steps3) + 12
P3H = HEAD + 150 + bh + 16
top = stage(cur, P3H, 3, C_CONF, "Conflation", "Hierarchical pairwise merging of building datasets.")

# priority banner
hby = top + 16
rrect(IX, hby, IW, 42, r=8, fill=CANVAS, stroke=CELL, sw=1.2)
smallcaps(IX + 16, hby + 27, "Source priority", mix(C_CONF, "#000", 0.15))
hx = IX + 160
for i, (rk, nm, col) in enumerate([("1", "Governmental", C_GOV),
                                   ("2", "OpenStreetMap", C_OSM),
                                   ("3", "Microsoft", C_MS)]):
    circle(hx + 11, hby + 21, 10, col)
    txt(hx + 11, hby + 26, rk, 13, "#fff", "bold", anchor="middle")
    txt(hx + 28, hby + 27, nm, 14, INK, "bold")
    hx += 28 + _tw(nm, 14, "bold") + 32
    if i < 2:
        chevron(hx - 11, hby + 21, mix(C_CONF, "#000", 0.05), s=7)

# schematic legend
ly = hby + 72
lx = IX + 20


def _leg(x, label, col, kind):
    if kind == "fill":
        rrect(x, ly - 9, 12, 12, r=2, fill=tint(col, 0.25), stroke=col, sw=1.2)
    elif kind == "dot":
        rrect(x, ly - 9, 12, 12, r=2, fill="none", stroke=col, sw=1.2, dash="1.6,1.4")
    elif kind == "line":
        line(x, ly - 3, x + 14, ly - 3, col, 2.2)
    elif kind == "dline":
        line(x, ly - 3, x + 14, ly - 3, col, 1.8, dash="2.2,1.7")
    txt(x + 19, ly, label, 12, SUB)
    return x + 19 + _tw(label, 12) + 26


smallcaps(lx, ly, "Schematic", FAINT, 11)
lx += _tw("Schematic", 11, "bold") + 50
lx = _leg(lx, "target (higher priority)", illus.C_TGT, "fill")
lx = _leg(lx, "source (lower priority)", illus.C_SRC, "dot")
lx = _leg(lx, "candidate pair", illus.C_CAND, "line")
lx = _leg(lx, "accepted match", illus.C_OK, "line")
lx = _leg(lx, "rejected candidate", illus.C_NO, "dline")

by = ly + 20
for i, mode in enumerate(steps3):
    bx = IX + i * (bw + gap)
    vo = voff_for(bh, ch3[mode])
    tx, ty = cell(bx, by, bw, bh, C_CONF, chr(97 + i), titles3[mode], vo)
    S.extend(illus.scene(mode, bx + 16, ty - 2, ds))
    tX = bx + 16 + ill_w + 14
    if mode == "candidate":
        bullets(tX, ty, cand_b, F_BODY, tW, M)
    elif mode == "matching":
        yb = bullets(tX, ty, match_b, F_BODY, tW, M, vgap=7)
        subs = ["intersection: IoU, IoSA, overlap",
                "shape: area, convexity, orient.",
                "similarity: walls, centroids",
                "context: neighbour counts 5–50 m"]
        sy = yb + 4
        for s in subs:
            line(tX + 12, sy - 4, tX + 19, sy - 4, sub_c, 1.8)
            txt(tX + 25, sy, s, 11, SUB)
            sy += 17
        bullets(tX, sy + 6, ["SBFS · Optuna tuning"], F_BODY, tW, M)
    elif mode == "geometry":
        bullets(tX, ty, geom_b, F_BODY, tW, M)
    elif mode == "attribute":
        y2 = mtext(tX, ty, attr_lead, F_BODY, tW, INK)
        yb = bullets(tX, y2 + 22, attr_b, F_BODY, tW, M)
        mtext(tX, yb + 20, "source subset Sₐ* chosen by maximum overlap with target a:",
              11, tW, FAINT)
        formula(tX - 6, yb + 60, tW + 12, "Sₐ* = argmax IoU(a, S)", mix(C_CONF, "#000", 0.2), 12.5)
    if i < n - 1:
        chevron(bx + bw + gap / 2, by + bh / 2, tint(C_CONF, 0.3))

lpw = 340
lpx = MX + (CW - lpw) / 2
lpy = by + bh + 16
rrect(lpx, lpy, lpw, 28, r=14, fill=CANVAS, stroke=tint(C_CONF, 0.45))
S.append(f'<path d="M{lpx+20},{lpy+14} a7,7 0 1 1 4,5" fill="none" stroke="{C_CONF}" stroke-width="1.9" stroke-linecap="round"/>')
S.append(f'<path d="M{lpx+26},{lpy+17} l-3,4 l5,1 Z" fill="{C_CONF}"/>')
txt(lpx + lpw / 2 + 10, lpy + 19, "iterate over source pairs in priority order",
    12.5, mix(C_CONF, "#000", 0.2), "bold", anchor="middle")
vconnect(W / 2, cur + P3H, cur + P3H + 46)
cur += P3H + 46

# =========================================================================== #
#  STAGE 4 — ATTRIBUTE PREDICTION
# =========================================================================== #
n, gap = 3, 22
bw = (IW - (n - 1) * gap) / n
groups = ["building, block, and neighborhood morphology (EUBUCCO)",
          "street · address · POI (OSM · Overture)",
          "land use & built-up (CORINE · GHS)",
          "topography · climate · population (GMTED · Oxford · GHS-POP)",
          "location embeddings (SatCLIP)"]
model_b = ["gradient-boosted tree ensembles (XGBoost)",
           "ground truth from governmental and OSM sources",
           "20% stratified hold-out, targets masked",
           "seed ensemble for regression uncertainty",
           "isotonic calibration of classification probabilities"
           ]
targets = [("Height", "regression", ""), ("Floors", "regression", ""),
           ("Use type", "6-class", "residential as single class"),
           ("Residential subtype", "4-class · sequential", "")]
ch_feat = 24 + 5 * 18
ch_model = m_bul(model_b, F_BODY, bw - 100)
ch_tgt = (len(targets) - 1) * 22 + 13
bh = max(63 + c for c in (ch_feat, ch_model, ch_tgt)) + 12
P4H = HEAD + 18 + bh + 14
top = stage(cur, P4H, 4, C_PRED, "Attribute prediction",
            "Impute missing building attributes using machine learning.")
by = top + 18

bx = IX
tx, ty = cell(bx, by, bw, bh, C_PRED, "a", "Prediction targets", voff_for(bh, ch_tgt))
yy = ty
for nm, tg, note in targets:
    rrect(tx, yy - 8, 4, 4, r=0.7, fill=tint(C_PRED, 0.2))
    txt(tx + 12, yy, nm, 13.2, INK, "bold")
    if note:
        txt(tx + 12 + _tw(nm, 13.2, "bold") + 7, yy, "(" + note + ")", 10.5, SUB, italic=True)
    txt(bx + bw - 16, yy, tg, 11.5, mix(C_PRED, "#000", 0.18), anchor="end")
    yy += 22

bx = IX + (bw + gap)
tx, ty = cell(bx, by, bw, bh, C_PRED, "b", "Feature engineering", voff_for(bh, ch_feat))
txt(tx, ty, "Per-building predictors from conflated + auxiliary data:", 11.8, SUB)
yy = ty + 24
for g in groups:
    rrect(tx, yy - 7, 3.6, 3.6, r=0.6, fill=tint(C_PRED, 0.25))
    txt(tx + 13, yy, g, 12, SUB)
    yy += 18

bx = IX + 2 * (bw + gap)
tx, ty = cell(bx, by, bw, bh, C_PRED, "c", "Model training & inference", voff_for(bh, ch_model))
tree_glyph(bx + bw - 80, ty + 18, tint(C_PRED, 0.25))
bullets(tx, ty, model_b, F_BODY, bw - 100, tint(C_PRED, 0.25))

chevron(IX + bw + gap / 2, by + bh / 2, tint(C_PRED, 0.3))
chevron(IX + 2 * bw + gap + gap / 2, by + bh / 2, tint(C_PRED, 0.3))
vconnect(W / 2, cur + P4H, cur + P4H + 46)
cur += P4H + 46

# =========================================================================== #
#  STAGE 5 — ATTRIBUTE CONFIDENCE
# =========================================================================== #
cwd = (IW - 2 * 22) / 3
prov = [("ground truth", "direct from provider, confidence = NaN", SUB),
        ("merged", "combined across overlapping sources", C_CONF2),
        ("estimated", "predicted by the ML models", C_PRED)]
prov_intro = ("The way uncertainty is measured depends on the attribute provenance. Attributes are one of three kinds:")
confbox = [
    ("Uncertainty of merged attributes", C_CONF2, [
        ("categorical (type, subtype): overlap of target a with the sources bᵢ that share the dominant category",
         "Conf = |a ∩ ∪ bᵢ| / |a|"),
        ("numeric (height, floors, construction year): bounds of source values",
         "Conf_lower = minᵢ vᵢ ,  Conf_upper = maxᵢ vᵢ"),
    ]),
    ("Uncertainty of predicted attributes", C_PRED, [
        ("categorical: isotonic-calibrated class probability",
         "Conf = P_cal(class)"),
        ("numeric: seed-ensemble mean ± 95% confidence interval",
         "Conf = ȳ ± t₀.₉₇₅,₉ · s/√n"),
    ]),
]

def _confh(rows):
    yy = 62
    for body, fm in rows:
        L = max(1, len(wrap(body, cwd - 52, 11.8)))
        yy += (L - 1) * 11.8 * 1.32 + 17
        if fm:
            yy += 21
    return yy - 4


prov_h = 100 + len(prov) * 18 - 6
heights = [prov_h] + [_confh(r) for _, _, r in confbox]
cch = max(heights) + 10
P5H = HEAD + 16 + cch + 14
top = stage(cur, P5H, 5, C_CONF2, "Uncertainty estimation",
            "Quantify uncertainty of merged and predicted attributes via confidence scores.")
cyy = top + 16

# general description (left)
cx = IX
vo = 0
rrect(cx, cyy, cwd, cch, r=8, fill=CANVAS, stroke=CELL, sw=1.2)
txt(cx + 18, cyy + 29, "Attribute provenance", F_CELLTITLE, INK, "bold")
line(cx + 16, cyy + 41, cx + cwd - 16, cyy + 41, HAIR, 1.0)
mtext(cx + 18, cyy + 62 + vo, prov_intro, 12, cwd - 36, SUB)
yy = cyy + 100 + vo
for kind, desc, kc in prov:
    rrect(cx + 18, yy - 8, 4, 4, r=0.7, fill=tint(kc, 0.15))
    txt(cx + 30, yy, kind, 11.8, mix(kc, "#000", 0.2), "bold")
    txt(cx + 30 + _tw(kind, 11.8, "bold") + 7, yy, "— " + desc, 11, SUB)
    yy += 18

for j, (nm, col, rows) in enumerate(confbox):
    cx = IX + (j + 1) * (cwd + 22)
    rrect(cx, cyy, cwd, cch, r=8, fill=CANVAS, stroke=CELL, sw=1.2)
    txt(cx + 18, cyy + 29, nm, F_CELLTITLE, INK, "bold")
    line(cx + 16, cyy + 41, cx + cwd - 16, cyy + 41, HAIR, 1.0)
    yy = cyy + 62
    for body, fm in rows:
        rrect(cx + 18, yy - 7, 4, 4, r=0.7, fill=tint(col, 0.2))
        e = mtext(cx + 32, yy, body, 11.8, cwd - 52, SUB)
        yy = e + 17
        if fm:
            formula(cx + 32, yy, cwd - 64, fm, mix(col, "#000", 0.25), 12)
            yy += 21
vconnect(W / 2, cur + P5H, cur + P5H + 46)
cur += P5H + 46

# =========================================================================== #
#  STAGE 6 — RELEASE PREPARATION
# =========================================================================== #
steps6 = [
    ("Provenance & schema assembly",
     ["record data provenance per building (source dataset & source id)",
      "combine use type + residential subtype",
      "aggregate use type to binary type (residential / non-residential)",
    #   "round precision; dictionary-encode categorical columns",
      ]),
    ("ID generation",
     ["assign block-based IDs with sequential building indices",
      "adjacency recoverable without spatial queries"]),
    ("Partitioning & serialization",
     ["Parquet files with WKB geometry (EPSG:3035)",
      "NUTS2 Hive partitions · ZSTD · 10k row groups",
      "sorted by source, region, city; bbox stats for pushdown",
      ]),
]
n, gap = 3, 22
bw = (IW - (n - 1) * gap) / n
chs6 = [m_bul(items, F_BODY, bw - 26) for _, items in steps6]
bh = max(63 + c for c in chs6) + 12
P6H = HEAD + 204 + bh + 14
top = stage(cur, P6H, 6, C_REL, "Release preparation",
            "Prepare the dataset for public release.")
by = top + 16
for i, (title, items) in enumerate(steps6):
    bx = IX + i * (bw + gap)
    tx, ty = cell(bx, by, bw, bh, C_REL, chr(97 + i), title, voff_for(bh, chs6[i]))
    bullets(tx, ty, items, F_BODY, bw - 26, tint(C_REL, 0.25))
    if i < n - 1:
        chevron(bx + bw + gap / 2, by + bh / 2, tint(C_REL, 0.3))

# data-access bar
aby = by + bh + 22
smallcaps(IX, aby, "Data access & download", mix(C_REL, "#000", 0.15), 12)
aby += 12
ahw = (IW - 22) / 2
rrect(IX, aby, ahw, 60, r=8, fill=tint(C_REL, 0.95), stroke=tint(C_REL, 0.5))
dcx = IX + 26
circle(dcx, aby + 30, 9, "none", mix(C_REL, "#000", 0.1), 1.5)
line(dcx - 9, aby + 30, dcx + 9, aby + 30, mix(C_REL, "#000", 0.1), 1.5)
S.append(f'<path d="M{dcx},{aby+21} a13,9 0 0 1 0,18 a13,9 0 0 1 0,-18" fill="none" stroke="{mix(C_REL,"#000",0.1)}" stroke-width="1.5"/>')
smallcaps(IX + 46, aby + 21, "S3 object storage", mix(C_REL, "#000", 0.15), 10.5)
txt(IX + 46, aby + 40, "https://s3.eubucco.com/eubucco/v1.0/buildings/parquet/nuts_id=<ID>/<ID>.parquet",
    12, INK, family=MONO)
# txt(IX + 46, aby + 54, "S3 endpoint  s3.eubucco.com", 11.5, SUB, family=MONO)

rrect(IX + ahw + 22, aby, ahw, 60, r=8, fill=tint(C_REL, 0.95), stroke=tint(C_REL, 0.5))
gx = IX + ahw + 22 + 26
circle(gx, aby + 30, 10, "none", mix(C_REL, "#000", 0.1), 1.5)
line(gx - 10, aby + 30, gx + 10, aby + 30, mix(C_REL, "#000", 0.1), 1.3)
S.append(f'<path d="M{gx},{aby+20} a7,10 0 0 1 0,20 a7,10 0 0 1 0,-20" fill="none" stroke="{mix(C_REL,"#000",0.1)}" stroke-width="1.3"/>')
smallcaps(gx + 20, aby + 21, "Web download portal", mix(C_REL, "#000", 0.15), 10.5)
txt(gx + 20, aby + 42, "eubucco.com/files", 15, mix(C_REL, "#000", 0.2), "bold", family=MONO)
txt(gx + 20 + _tw("eubucco.com/files", 15, "bold") + 28, aby + 42,
    "download by region or country as Parquet, GeoPackage, or Shapefile", 11.5, SUB, italic=True)

# ancillary products
ayl = aby + 60 + 24
smallcaps(IX, ayl, "Auxiliary files distributed alongside the core dataset", mix(C_REL, "#000", 0.15), 12)
files = [
    ("eubucco_lat_lon.parquet", "centroids + footprint area"),
    ("city-stats.parquet", "LAU building-stock statistics"),
    ("region-stats.parquet", "NUTS3 building-stock statistics"),
    ("prediction-eval-metrics.parquet", "NUTS2 ML evaluation metrics: MAE · RMSE · R² · F1 · κ"),
]
fwd = (IW - 3 * 18) / 4
fyy = ayl + 16
for i, (fn, dsd) in enumerate(files):
    fx = IX + i * (fwd + 18)
    rrect(fx, fyy, fwd, 56, r=8, fill=CANVAS, stroke=CELL, sw=1.2)
    gx2, gy2 = fx + 18, fyy + 20
    S.append(f'<path d="M{gx2},{gy2-9} l10,0 l5,5 l0,15 l-15,0 Z" fill="none" stroke="{C_REL}" stroke-width="1.6" stroke-linejoin="round"/>')
    S.append(f'<path d="M{gx2+10},{gy2-9} l0,5 l5,0" fill="none" stroke="{C_REL}" stroke-width="1.6"/>')
    txt(fx + 42, fyy + 24, fn, 12, mix(C_REL, "#000", 0.22), "bold", family=MONO)
    txt(fx + 42, fyy + 43, dsd, 11.5, SUB)
cur += P6H + 42
H = round(cur)

# --------------------------------------------------------------------------- #
head = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" font-family="{FONT}">')
bg = f'<rect x="0" y="0" width="{W}" height="{H}" fill="{CANVAS}"/>'
with open("pipeline.svg", "w") as f:
    f.write(head + bg + "\n" + "\n".join(S) + "\n</svg>\n")
print(f"wrote pipeline.svg  ({W} x {H})")
