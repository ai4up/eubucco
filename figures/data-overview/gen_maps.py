"""Generate EUBUCCO overview choropleth maps (NUTS-3) as tight transparent PNGs.

Reads ../../region-stats.parquet; writes assets/map_src_*.png and assets/map_gt_*.png.
Also prints the sampled colour ramps used for the HTML colour bars and verifies EU-wide stats.
"""
from pathlib import Path
import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, Normalize

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
ASSETS = HERE / "assets"
ASSETS.mkdir(exist_ok=True)
SRC = REPO / "region-stats.parquet"

gdf = gpd.read_parquet(SRC)
# geometry is EPSG:3035 (equal-area); simplify for a lighter render (1.5 km tolerance
# is invisible at continental scale)
gdf["geometry"] = gdf["geometry"].simplify(1500, preserve_topology=True)

for s in ["gov", "osm", "msft"]:
    gdf[f"{s}_share"] = 100 * gdf[f"n_{s}"] / gdf["n"]
for a in ["type", "subtype", "height", "floors", "construction_year"]:
    gdf[f"gt_{a}"] = 100 * gdf[f"n_gt_{a}"] / gdf["n"]

# source share: light aqua -> deep indigo (mako-like)
MAKO_FLOOR = "#dcefeb"
mako = LinearSegmentedColormap.from_list("mako", [
    MAKO_FLOOR, "#a8dad0", "#5cb8b0", "#3487a0", "#2e5a92", "#33306e", "#241a45",
])
# ground truth: light mint -> provenance green (#2f8f78) -> deep teal
GT_FLOOR = "#e3f1ec"
greens = LinearSegmentedColormap.from_list("gt", [
    GT_FLOOR, "#c2e5d9", "#8fd0bd", "#4fac93", "#2f8f78", "#1f6353",
])


def draw(col, cmap, fname, base, vmin=0, vmax=100):
    fig, ax = plt.subplots(figsize=(4.2, 4.6), dpi=220)
    fig.patch.set_alpha(0)
    ax.patch.set_alpha(0)
    # base = ramp floor so low/zero regions still read as filled land
    gdf.plot(ax=ax, color=base, edgecolor="none", zorder=1)
    gdf.plot(column=col, cmap=cmap, norm=Normalize(vmin, vmax), ax=ax,
             edgecolor="white", linewidth=0.12, zorder=2)
    ax.set_axis_off()
    ax.margins(0)
    ax.set_aspect("equal")
    b = gdf.total_bounds
    ax.set_xlim(b[0] - 5e4, b[2] + 5e4)
    ax.set_ylim(b[1] - 5e4, b[3] + 5e4)
    plt.subplots_adjust(0, 0, 1, 1)
    fig.savefig(ASSETS / fname, transparent=True, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print("wrote", fname)


for s in ["gov", "osm", "msft"]:
    draw(f"{s}_share", mako, f"map_src_{s}.png", MAKO_FLOOR)
for a in ["type", "subtype", "height", "floors", "construction_year"]:
    draw(f"gt_{a}", greens, f"map_gt_{a}.png", GT_FLOOR)


def ramp(cmap, n=9):
    return [matplotlib.colors.to_hex(cmap(i / (n - 1))) for i in range(n)]

print("MAKO", ramp(mako))
print("GREENS", ramp(greens))

tot = gdf["n"].sum()
print("TOTAL", int(tot))
print("SRC_SHARE", {s: round(100 * gdf[f"n_{s}"].sum() / tot, 1) for s in ["gov", "osm", "msft"]})
print("GT_SHARE", {a: round(100 * gdf[f"n_gt_{a}"].sum() / tot, 1)
                    for a in ["type", "subtype", "height", "floors", "construction_year"]})
print("N_REGIONS", len(gdf), "N_COUNTRIES", gdf["country"].nunique())
