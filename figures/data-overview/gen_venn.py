"""Proportional 3-set Venn of building-source overlap, brand-coloured.

Overlap shares are estimates carried over from the source-overlap analysis; edit the
SUBSETS values below to update. Writes assets/venn.png.

Geometry uses the *weighted* venn3 so circle sizes and overlaps are roughly
area-proportional (the pairwise overlaps differ visibly instead of all matching);
everything else is styled to match the rest of the figure.
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib_venn import venn3, venn3_circles

ASSETS = Path(__file__).resolve().parent / "assets"
ASSETS.mkdir(exist_ok=True)

OSM, MSFT, GOV = "#e8b54a", "#5cb1c9", "#7dd3a0"  # brand colours

# region order for venn3: (Abc, aBc, ABc, abC, AbC, aBC, ABC)
# A = OSM, B = Microsoft, C = Governmental — sums to 100
SUBSETS = (5.2, 18.7, 24.4, 5.0, 3.5, 7.6, 35.6)
LABELS = ("OpenStreetMap", "Microsoft", "Governmental")

fig, ax = plt.subplots(figsize=(4.4, 4.2), dpi=220)
fig.patch.set_alpha(0)
ax.set_aspect("equal")
ax.axis("off")

v = venn3(subsets=SUBSETS, set_labels=LABELS, set_colors=(OSM, MSFT, GOV),
          alpha=0.62, ax=ax)
venn3_circles(subsets=SUBSETS, linewidth=1.0, color="white", ax=ax)

halo = [pe.withStroke(linewidth=2.4, foreground="white")]

# region (percentage) labels — triple intersection a touch larger/darker
region_style = {
    "111": dict(size=11.5, weight="semibold", color="#1c2b26"),
}
default_region = dict(size=9.5, weight="medium", color="#3a4844")
for key, val in zip(["100", "010", "110", "001", "101", "011", "111"], SUBSETS):
    t = v.get_label_by_id(key)
    if not t:
        continue
    st = region_style.get(key, default_region)
    t.set_text(f"{val:g}%")
    t.set_fontsize(st["size"])
    t.set_fontweight(st["weight"])
    t.set_color(st["color"])
    t.set_path_effects(halo)

# set labels, brand-tinted like before
set_colors = {"A": "#b07d13", "B": "#2f7f99", "C": "#2f8f78"}
for sid, t in zip(["A", "B", "C"], v.set_labels):
    if t:
        t.set_fontsize(9.5)
        t.set_fontweight("semibold")
        t.set_color(set_colors[sid])
        t.set_path_effects(halo)

fig.savefig(ASSETS / "venn.png", transparent=True, bbox_inches="tight", pad_inches=0.03)
print("wrote venn.png")
