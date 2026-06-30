"""Synthetic, schematic footprint illustrations for the conflation steps
and the rubbersheeting step. Imported by make_pipeline.py.

Two-source schematic (clearest for the candidate/match distinction):
  target  = higher-priority source  (blue, solid)
  source  = lower-priority source   (orange, dotted)
  green line = accepted match · red line = rejected candidate
"""
import math

C_TGT = "#2f5d86"   # higher-priority (target)  – blue
C_SRC = "#d2823c"   # lower-priority (source)   – orange
C_OK = "#3f8f55"    # accepted match            – green
C_NO = "#c0566b"    # rejected candidate        – red
C_CAND = "#9aa2b1"  # candidate edge            – grey

# preprocessing illustration palette
C_BLD = "#6f8aa6"   # neutral building footprint
C_GHOST = "#b6bcc7" # removed / reference geometry
C_ACC = "#2e857c"   # preprocessing accent (teal)

# kept names for make_pipeline colour imports
C_GOV, C_OSM, C_MS = "#2f5d86", "#4d9163", "#d2823c"

SVW, SVH = 116, 64


def _rect(cx, cy, w, h, ang):
    a = math.radians(ang)
    ca, sa = math.cos(a), math.sin(a)
    pts = []
    for dx, dy in [(-w / 2, -h / 2), (w / 2, -h / 2), (w / 2, h / 2), (-w / 2, h / 2)]:
        pts.append((cx + dx * ca - dy * sa, cy + dx * sa + dy * ca))
    return pts


def _L(cx, cy, w, h, ang, cut=0.45):
    """simple L-shaped footprint"""
    a = math.radians(ang); ca, sa = math.cos(a), math.sin(a)
    raw = [(-w / 2, -h / 2), (w / 2, -h / 2), (w / 2, h / 2),
           (-w / 2 + w * cut, h / 2), (-w / 2 + w * cut, -h / 2 + h * cut),
           (-w / 2, -h / 2 + h * cut)]
    return [(cx + dx * ca - dy * sa, cy + dx * sa + dy * ca) for dx, dy in raw]


# scene buildings: (id, cx, cy, w, h, ang, sources, shape)
# sources: "B"=both, "S"=source only, "T"=target only
_DEF = [
    ("a", 25, 18, 18, 12, -9, "B", "r"),
    ("b", 57, 15, 15, 13, 7, "B", "L"),
    ("c", 89, 20, 16, 11, -5, "B", "r"),
    ("d", 22, 45, 13, 15, 12, "B", "r"),
    ("e", 60, 47, 15, 12, -7, "S", "r"),   # source-only survivor
    ("f", 95, 49, 12, 14, 5, "T", "r"),    # target-only
]
_OFF = (4.2, 3.0)     # systematic source mis-registration
_JIT = {"a": (0.6, -0.5), "b": (-0.7, 0.6), "c": (0.5, 0.4),
        "d": (-0.4, -0.6), "e": (0, 0), "f": (0, 0)}


def _build():
    tgt, src = {}, {}
    for bid, cx, cy, w, h, ang, srcs, shp in _DEF:
        mk = (lambda CX, CY, A: _L(CX, CY, w, h, A) if shp == "L" else _rect(CX, CY, w, h, A))
        if srcs in ("B", "T"):
            tgt[bid] = (mk(cx, cy, ang), (cx, cy))
        if srcs in ("B", "S"):
            jx, jy = _JIT[bid]
            ox, oy = cx + _OFF[0] + jx, cy + _OFF[1] + jy
            src[bid] = (mk(ox, oy, ang + 4), (ox, oy))
    return tgt, src


_TGT, _SRC = _build()
_MATCH = [b for b in _DEF if b[6] == "B"]   # true correspondences


def _knn_edges(k=3):
    """k nearest cross-source centroid pairs (candidate links)."""
    tc = {b: _TGT[b][1] for b in _TGT}
    sc = {b: _SRC[b][1] for b in _SRC}
    edges = set()
    for b, p in tc.items():
        near = sorted(sc.items(), key=lambda kv: math.hypot(p[0] - kv[1][0], p[1] - kv[1][1]))[:k]
        for nb, q in near:
            edges.add((("T", b), ("S", nb)))
    for b, p in sc.items():
        near = sorted(tc.items(), key=lambda kv: math.hypot(p[0] - kv[1][0], p[1] - kv[1][1]))[:k]
        for nb, q in near:
            edges.add((("T", nb), ("S", b)))
    out = []
    for (ta, a), (tb, b) in edges:
        out.append((_TGT[a][1], _SRC[b][1], a, b))
    return out


def _path(pts, ox, oy, ds):
    return "M" + " L".join(f"{ox+x*ds:.1f},{oy+y*ds:.1f}" for x, y in pts) + " Z"


def _t(p, ox, oy, ds):
    return (ox + p[0] * ds, oy + p[1] * ds)


def scene(mode, ox, oy, ds):
    S = []

    def poly(pts, fill, stroke, sw=1.0, dash=None, fop=1.0):
        a = f'<path d="{_path(pts, ox, oy, ds)}" fill="{fill}" fill-opacity="{fop}" stroke="{stroke}" stroke-width="{sw}" stroke-linejoin="round"'
        if dash:
            a += f' stroke-dasharray="{dash}"'
        S.append(a + "/>")

    def seg(p, q, color, sw, dash=None):
        a = f'<line x1="{p[0]:.1f}" y1="{p[1]:.1f}" x2="{q[0]:.1f}" y2="{q[1]:.1f}" stroke="{color}" stroke-width="{sw}"'
        if dash:
            a += f' stroke-dasharray="{dash}"'
        S.append(a + "/>")

    def dot(p, r, c):
        S.append(f'<circle cx="{p[0]:.1f}" cy="{p[1]:.1f}" r="{r}" fill="{c}"/>')

    def x_mark(p, c, s=2.4):
        S.append(f'<path d="M{p[0]-s},{p[1]-s} L{p[0]+s},{p[1]+s} M{p[0]+s},{p[1]-s} L{p[0]-s},{p[1]+s}" stroke="{c}" stroke-width="1.3"/>')

    if mode == "candidate":
        for b in _SRC:
            poly(_SRC[b][0], C_SRC, C_SRC, 1.0, "1.4,1.4", 0.08)
        for b in _TGT:
            poly(_TGT[b][0], C_TGT, C_TGT, 1.1, None, 0.10)
        for p, q, a, b in _knn_edges():
            seg(_t(p, ox, oy, ds), _t(q, ox, oy, ds), C_CAND, 0.8)
        for b in _TGT:
            dot(_t(_TGT[b][1], ox, oy, ds), 1.5, C_TGT)
        for b in _SRC:
            dot(_t(_SRC[b][1], ox, oy, ds), 1.5, C_SRC)

    elif mode == "matching":
        for b in _SRC:
            poly(_SRC[b][0], C_SRC, C_SRC, 0.9, "1.4,1.4", 0.07)
        for b in _TGT:
            poly(_TGT[b][0], C_TGT, C_TGT, 1.0, None, 0.09)
        # accepted matches (true correspondences)
        for b in _MATCH:
            bid = b[0]
            p, q = _t(_TGT[bid][1], ox, oy, ds), _t(_SRC[bid][1], ox, oy, ds)
            seg(p, q, C_OK, 1.8)
            dot(p, 1.6, C_OK); dot(q, 1.6, C_OK)
        # rejected candidates touching the unmatched buildings
        rej = [(_SRC["e"][1], _TGT["d"][1]), (_TGT["f"][1], _SRC["c"][1])]
        for p, q in rej:
            pp, qq = _t(p, ox, oy, ds), _t(q, ox, oy, ds)
            seg(pp, qq, C_NO, 1.4, "2,1.6")
            mx, my = (pp[0] + qq[0]) / 2, (pp[1] + qq[1]) / 2
            x_mark((mx, my), C_NO)

    elif mode == "geometry":
        # discarded lower-priority footprints (matched -> dropped): ghost
        for b in _MATCH:
            poly(_SRC[b[0]][0], "none", "#b6bcc7", 0.8, "2,2", 0)
        # retained: target footprints where present + source-only survivor
        for b in _TGT:
            poly(_TGT[b][0], C_TGT, C_TGT, 1.2, None, 0.34)
        poly(_SRC["e"][0], C_SRC, C_SRC, 1.2, None, 0.34)   # survives from lower-priority source

    elif mode == "attribute":
        # enrich the target footprint of one pair with source attributes
        focus = "b"
        for b in _TGT:
            fop = 0.30 if b == focus else 0.10
            poly(_TGT[b][0], C_TGT, C_TGT, 1.2 if b == focus else 0.9, None, fop)
        poly(_SRC[focus][0], C_SRC, C_SRC, 1.1, "1.6,1.4", 0.16)
        tp = _t(_TGT[focus][1], ox, oy, ds)
        sp = _t(_SRC[focus][1], ox, oy, ds)
        # attribute labels flowing from source -> target
        AMB = "#b9802f"
        labs = ["height", "type", "floors", "year"]
        bx0 = ox + SVW * ds - 4
        for i, lab in enumerate(labs):
            w = 5.0 * len(lab) + 9
            ly = oy + 7 + i * 12.5
            lx = bx0 - w
            S.append(f'<rect x="{lx:.1f}" y="{ly-7:.1f}" width="{w:.1f}" height="11" rx="5.5" fill="#fff" stroke="{AMB}" stroke-width="0.9"/>')
            S.append(f'<text x="{lx+w/2:.1f}" y="{ly+1:.1f}" font-size="6.8" text-anchor="middle" fill="{AMB}" font-family="Helvetica">{lab}</text>')
            S.append(f'<path d="M{lx:.1f},{ly-1:.1f} L{tp[0]+4:.1f},{tp[1]-2:.1f}" stroke="{AMB}" stroke-width="0.8" stroke-dasharray="1.6,1.4" opacity="0.7"/>')
        dot(sp, 1.7, C_SRC); dot(tp, 2.0, C_TGT)
    return S


# ---- rubbersheeting illustration -------------------------------------------
RVW, RVH = 132, 70


def rubber(ox, oy, ds):
    """One H3 cell: all source footprints shifted by the same affine vector."""
    S = []
    C_REF = "#9aa2b1"
    C_MOV = C_SRC

    def poly(pts, stroke, sw, dash=None, fill="none", fop=1.0):
        a = f'<path d="M{" L".join(f"{ox+x*ds:.1f},{oy+y*ds:.1f}" for x,y in pts)} Z" fill="{fill}" fill-opacity="{fop}" stroke="{stroke}" stroke-width="{sw}" stroke-linejoin="round"'
        if dash:
            a += f' stroke-dasharray="{dash}"'
        S.append(a + "/>")

    # hexagon (H3 cell)
    hx, hy, hr = 66, 35, 40
    hexpts = [(hx + hr * math.cos(math.radians(60 * i - 30)),
               hy + hr * 0.86 * math.sin(math.radians(60 * i - 30))) for i in range(6)]
    S.append(f'<path d="M{" L".join(f"{ox+x*ds:.1f},{oy+y*ds:.1f}" for x,y in hexpts)} Z" '
             f'fill="#8a7f5a" fill-opacity="0.05" stroke="#b3aa86" stroke-width="1.0" stroke-dasharray="3,2.5"/>')
    # buildings: reference position (grey dashed) + shifted source (orange) + uniform arrow
    OFF = (12, 7)
    builds = [(42, 24, 16, 11, 8), (84, 28, 14, 13, -6), (60, 52, 17, 11, 4)]
    for cx, cy, w, h, ang in builds:
        ref = _rect(cx, cy, w, h, ang)
        mov = _rect(cx - OFF[0], cy - OFF[1], w, h, ang)   # source sits offset; arrow shows correction
        poly(ref, C_REF, 0.9, "2,2")
        poly(mov, C_MOV, 1.1, None, C_MOV, 0.12)
        p = _t((cx - OFF[0], cy - OFF[1]), ox, oy, ds)
        q = _t((cx - 2.0, cy - 1.2), ox, oy, ds)
        S.append(f'<line x1="{p[0]:.1f}" y1="{p[1]:.1f}" x2="{q[0]:.1f}" y2="{q[1]:.1f}" stroke="{C_MOV}" stroke-width="1.4"/>')
        ang2 = math.atan2(q[1] - p[1], q[0] - p[0])
        for da in (2.5, -2.5):
            ax = q[0] - 4 * math.cos(ang2 + da); ay = q[1] - 4 * math.sin(ang2 + da)
            S.append(f'<line x1="{q[0]:.1f}" y1="{q[1]:.1f}" x2="{ax:.1f}" y2="{ay:.1f}" stroke="{C_MOV}" stroke-width="1.4"/>')
    return S


# ---- preprocessing illustrations -------------------------------------------
GEOM_VW, GEOM_VH = 172, 56
DEDUP_VW, DEDUP_VH = 80, 56
ADMIN_VW, ADMIN_VH = 116, 60


def _polymaker(S, ox, oy, ds):
    def P(pts, fill, stroke, sw=1.0, dash=None, fop=1.0):
        d = "M" + " L".join(f"{ox+x*ds:.1f},{oy+y*ds:.1f}" for x, y in pts) + " Z"
        a = (f'<path d="{d}" fill="{fill}" fill-opacity="{fop}" stroke="{stroke}" '
             f'stroke-width="{sw}" stroke-linejoin="round"')
        if dash:
            a += f' stroke-dasharray="{dash}"'
        S.append(a + "/>")
    return P


def _seg(S, ox, oy, ds, p, q, c, sw, dash=None):
    a = (f'<line x1="{ox+p[0]*ds:.1f}" y1="{oy+p[1]*ds:.1f}" x2="{ox+q[0]*ds:.1f}" '
         f'y2="{oy+q[1]*ds:.1f}" stroke="{c}" stroke-width="{sw}"')
    if dash:
        a += f' stroke-dasharray="{dash}"'
    S.append(a + "/>")


def _arrow(S, ox, oy, ds, x1, y1, x2, y2, color, sw=1.6, hl=6):
    ax, ay, bx, by = ox + x1 * ds, oy + y1 * ds, ox + x2 * ds, oy + y2 * ds
    S.append(f'<line x1="{ax:.1f}" y1="{ay:.1f}" x2="{bx:.1f}" y2="{by:.1f}" stroke="{color}" stroke-width="{sw}"/>')
    ang = math.atan2(by - ay, bx - ax)
    for da in (0.5, -0.5):
        hx, hy = bx - hl * math.cos(ang + da), by - hl * math.sin(ang + da)
        S.append(f'<line x1="{bx:.1f}" y1="{by:.1f}" x2="{hx:.1f}" y2="{hy:.1f}" stroke="{color}" stroke-width="{sw}"/>')


def geom_clean(ox, oy, ds):
    """Two cleaning ops: 2.5D/3D -> 2D footprint, and invalid -> repaired."""
    S = []
    P = _polymaker(S, ox, oy, ds)
    
    # 1) 2.5D / 3D building flattened to a 2D footprint
    # Base shifted left from 24 to 12
    base = _rect(12, 41, 29, 18, -6)
    roof = [(x + 5, y - 14) for x, y in base]
    P(base, C_GHOST, C_GHOST, 1.0, None, 0.10)
    for b_, r_ in zip(base, roof):
        _seg(S, ox, oy, ds, b_, r_, C_GHOST, 0.9)
    P(roof, "#ffffff", C_GHOST, 1.0, None, 0.0)
    
    # Arrow shifted left and compressed slightly to fit the new layout
    _arrow(S, ox, oy, ds, 34, 36, 44, 36, C_ACC, 1.5)
    
    # Footprint shifted left from 75 to 56
    P(_rect(60, 37, 26, 17, -2), C_BLD, C_BLD, 1.3, None, 0.30)
    
    # 2) invalid self-intersecting polygon repaired to a clean footprint
    # Bow-tie shifted left from 115 to 88
    bow = [(92, 28), (116, 28), (92, 47), (116, 47)]   # crossing vertex order -> bow-tie
    P(bow, "none", C_NO, 1.2, "1.8,1.5", 0.0)
    
    # Arrow shifted left
    _arrow(S, ox, oy, ds, 114, 37, 124, 37, C_ACC, 1.5)
    
    # Final footprint shifted left from 178 to 142
    P(_rect(140, 37, 24, 17, 3), C_BLD, C_BLD, 1.3, None, 0.30)
    
    return S


def dedup(ox, oy, ds):
    """A smaller duplicate sub-part overlapping a larger footprint is discarded."""
    S = []
    P = _polymaker(S, ox, oy, ds)
    big = _rect(36, 36, 40, 28, -4)
    P(big, C_BLD, C_BLD, 1.3, None, 0.28)
    small = _rect(48, 30, 18, 13, 11)
    P(small, C_NO, C_NO, 1.0, "1.6,1.4", 0.12)
    X, Y = ox + 48 * ds, oy + 30 * ds
    s = 4.6
    S.append(f'<path d="M{X-s},{Y-s} L{X+s},{Y+s} M{X+s},{Y-s} L{X-s},{Y+s}" stroke="{C_NO}" stroke-width="1.6"/>')
    return S


def admin(ox, oy, ds):
    """Building centroids spatially joined to nested NUTS3 / LAU boundaries."""
    S = []
    P = _polymaker(S, ox, oy, ds)
    # irregular, organic-looking administrative boundaries (not boxes / hexes)
    nuts = [(9, 27), (19, 14), (34, 10), (49, 16), (63, 8), (80, 11),
            (97, 9), (111, 21), (105, 35), (110, 47), (92, 53), (74, 49),
            (58, 56), (41, 51), (27, 53), (16, 43)]
    P(nuts, C_ACC, C_ACC, 1.1, "3,2.5", 0.04)
    lau = [(45, 24), (55, 19), (67, 21), (78, 28), (81, 38),
           (73, 47), (60, 49), (49, 45), (42, 36), (41, 29)]
    P(lau, C_ACC, C_ACC, 1.0, None, 0.10)
    for cx, cy, w, h, a in [(56, 30, 12, 9, -7), (69, 38, 10, 8, 9), (52, 40, 9, 7, 4)]:
        P(_rect(cx, cy, w, h, a), C_BLD, C_BLD, 1.0, None, 0.32)
        S.append(f'<circle cx="{ox+cx*ds:.1f}" cy="{oy+cy*ds:.1f}" r="1.7" fill="{C_ACC}"/>')
    S.append(f'<text x="{ox+5*ds:.1f}" y="{oy+10*ds:.1f}" font-size="7.5" fill="{C_ACC}" '
             f'font-family="Helvetica" font-weight="bold">NUTS3</text>')
    S.append(f'<text x="{ox+64*ds:.1f}" y="{oy+19*ds:.1f}" font-size="7" fill="#256a63" '
             f'font-family="Helvetica" font-weight="bold">LAU</text>')
    return S


if __name__ == "__main__":
    import subprocess
    ds = 2.2
    modes = ["candidate", "matching", "geometry", "attribute"]
    gap = 26
    tw, th = SVW * ds, SVH * ds
    rw = RVW * ds
    Wt = len(modes) * tw + rw + (len(modes) + 2) * gap
    Ht = max(th, RVH * ds) + 2 * gap + 26
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{Wt:.0f}" height="{Ht:.0f}" viewBox="0 0 {Wt:.0f} {Ht:.0f}" font-family="Helvetica"><rect width="100%" height="100%" fill="#fff"/>']
    x = gap
    for i, m in enumerate(modes):
        oy = gap + 16
        o.append(f'<rect x="{x-6}" y="{oy-6}" width="{tw+12}" height="{th+12}" rx="8" fill="#fcfdfe" stroke="#e6eaf0"/>')
        o.append(f'<text x="{x}" y="{oy-10}" font-size="12" font-weight="bold" fill="#16202e">{chr(97+i)} {m}</text>')
        o += scene(m, x, oy, ds)
        x += tw + gap
    oy = gap + 16
    o.append(f'<rect x="{x-6}" y="{oy-6}" width="{rw+12}" height="{RVH*ds+12}" rx="8" fill="#fcfdfe" stroke="#e6eaf0"/>')
    o.append(f'<text x="{x}" y="{oy-10}" font-size="12" font-weight="bold" fill="#16202e">rubbersheet</text>')
    o += rubber(x, oy, ds)
    o.append("</svg>")
    open("illus_preview.svg", "w").write("".join(o))
    subprocess.run(["rsvg-convert", "-w", str(int(Wt * 1.6)), "illus_preview.svg", "-o", "illus_preview.png"])
    print("wrote illus_preview.png")
