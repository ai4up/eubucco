"""High-resolution floorspace-density hero map from city (LAU) polygons.

Reads ../../city-stats.parquet (~99k municipalities); writes assets/map_hero.png.
Colour encodes floor area per unit municipality land area (log scale) — size-invariant,
so it shows built-up intensity rather than administrative-unit granularity.
"""
from pathlib import Path
import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, LogNorm

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
ASSETS = HERE / "assets"
ASSETS.mkdir(exist_ok=True)

gdf = gpd.read_parquet(REPO / "city-stats.parquet")
gdf = gdf[gdf["floor_area"] > 0].copy()
gdf["land_area"] = gdf.geometry.area
gdf = gdf[gdf["land_area"] > 0].copy()
gdf["density"] = gdf["floor_area"] / gdf["land_area"]
gdf["geometry"] = gdf["geometry"].simplify(600, preserve_topology=True)
print("cities", len(gdf), "density range", gdf.density.min(), gdf.density.max())

HERO_FLOOR = "#eef4f2"
hero = LinearSegmentedColormap.from_list("hero", [
    HERO_FLOOR, "#a8dcd3", "#5cb8b0", "#2f8f9e", "#2f6595", "#33306e", "#201735", "#120c22",
])

vmin = gdf["density"].quantile(0.05)
vmax = gdf["density"].quantile(0.995)

fig, ax = plt.subplots(figsize=(10.5, 8.4), dpi=210)
fig.patch.set_alpha(0)
ax.patch.set_alpha(0)
gdf.plot(column="density", cmap=hero, norm=LogNorm(vmin=vmin, vmax=vmax),
         ax=ax, edgecolor="none", linewidth=0, antialiased=True)
ax.set_axis_off()
ax.set_aspect("equal")
b = gdf.total_bounds
ax.set_xlim(b[0] - 5e4, b[2] + 5e4)
ax.set_ylim(b[1] - 5e4, b[3] + 5e4)
plt.subplots_adjust(0, 0, 1, 1)
fig.savefig(ASSETS / "map_hero.png", transparent=True, bbox_inches="tight", pad_inches=0.02)
print("wrote map_hero.png  quantile vmin/vmax", round(float(vmin), 4), round(float(vmax), 4))
print("HERO", [matplotlib.colors.to_hex(hero(i / 8)) for i in range(9)])
