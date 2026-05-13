#-------------------------------------------------------------------------------
# Name:     gap_area.py
# Purpose:  Detect missing tiles (gaps) within a spatial project coverage.
#           Identifies internal holes whose area matches expected tile areas,
#           or where the physical gap distance exceeds a given threshold.
#           Includes support for rotated tiles using Minimum Rotated Rectangles.
#
# Created:  04/10/2026
# Updated:  04/17/2026 — English translation + Distance threshold logic
# Copyright: (c) EarthDefine 2025
#-------------------------------------------------------------------------------

import sys
import os
import time
import argparse
import math

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
script_name = os.path.basename(os.path.splitext(__file__)[0])

# Cascade of tile sizes to check, in order (side_m, label)
TILE_TIERS = [
    (500,  "500m x 500m"),
    (1000, "1000m x 1000m"),
    (1500, "1500m x 1500m"),
]

AREA_TOLERANCE  = 0.05   # +-5%
MAX_TILE_MULT   = 10     # detect gaps up to 10 combined tiles
DEFAULT_OUTDIR  = r"\\FS1\Software\Scripts\Lidar\tests\Lidar-236\AOI"
DEFAULT_MIN_DIST = 100.0 # Minimum gap distance in meters to consider relevant

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def area_matches(area, tile_area, tolerance, max_mult):
    """Return (True, multiplier) if area matches expected multiples."""
    for mult in range(1, max_mult + 1):
        expected = tile_area * mult
        if abs(area - expected) / expected <= tolerance:
            return True, mult
    return False, None

def get_mrr_dimensions(geom):
    """Calculate the width and height of the Minimum Rotated Rectangle."""
    rect = geom.minimum_rotated_rectangle
    # Get coordinates of the rotated rectangle corners
    x, y = rect.exterior.coords.xy
    edge_length_1 = math.hypot(x[0] - x[1], y[0] - y[1])
    edge_length_2 = math.hypot(x[1] - x[2], y[1] - y[2])
    return min(edge_length_1, edge_length_2), max(edge_length_1, edge_length_2)

# ---------------------------------------------------------------------------
# SHP mode
# ---------------------------------------------------------------------------

def detect_gaps_shp(shp_path, tile_area, tolerance, max_mult, min_gap_dist):
    try:
        import geopandas as gpd
        from shapely.geometry import Polygon
        from shapely.ops import unary_union
    except ImportError as e:
        print(f"[ERROR] Missing library: {e}")
        sys.exit(1)

    print(f"[INFO] Reading shapefile: {shp_path}")
    gdf = gpd.read_file(shp_path)

    if gdf.crs and gdf.crs.is_geographic:
        gdf = gdf.to_crs(epsg=6933)

    dissolved = unary_union(gdf.geometry)
    holes = []

    def extract_holes(geom):
        if geom.geom_type == "Polygon":
            for interior in geom.interiors:
                holes.append(Polygon(interior))
        elif geom.geom_type == "MultiPolygon":
            for part in geom.geoms:
                extract_holes(part)

    extract_holes(dissolved)
    
    gaps = []
    for i, hole in enumerate(holes):
        a = hole.area
        min_width, max_length = get_mrr_dimensions(hole)
        mrr_area = min_width * max_length
        
        matched, mult = area_matches(a, tile_area, tolerance, max_mult)
        if not matched:
            matched, mult = area_matches(mrr_area, tile_area, tolerance, max_mult)
            
        # If it doesn't match a tile size, check if it meets the minimum distance criteria
        gap_type = "Tile Match"
        if not matched and min_width >= min_gap_dist:
            matched = True
            mult = 0  # 0 indicates it's a custom distance gap, not an exact tile
            gap_type = f"Distance (> {min_gap_dist}m)"

        if matched:
            gaps.append({
                "id":        i + 1,
                "area_m2":   round(a, 2),
                "tiles_est": mult,
                "gap_type":  gap_type,
                "centroid":  hole.centroid,
                "geometry":  hole,
                "source":    "shp",
            })
    return gaps

# ---------------------------------------------------------------------------
# VRT mode
# ---------------------------------------------------------------------------

def detect_gaps_vrt(vrt_path, tile_area, tolerance, max_mult, min_gap_dist):
    try:
        import rasterio
        from rasterio.features import shapes
        from shapely.geometry import shape
    except ImportError as e:
        print(f"[ERROR] Missing library: {e}")
        sys.exit(1)

    print(f"[INFO] Reading VRT: {vrt_path}")
    gaps = []

    vrt_dir = os.path.dirname(os.path.abspath(vrt_path))
    original_cwd = os.getcwd()

    try:
        if vrt_dir:
            os.chdir(vrt_dir)

        vrt_basename = os.path.basename(vrt_path)
        with rasterio.open(vrt_basename) as src:
            print(f"[INFO] CRS    : {src.crs}")
            print(f"[INFO] Res    : {src.res}")

            try:
                print("[INFO] Generating lightweight VRT mask...")
                mask = src.dataset_mask()
            except Exception as read_error:
                print("\n" + "!"*70)
                print(f"[READ ERROR] Failed to read VRT structure/pixels.")
                print(f"The VRT file likely contains absolute paths pointing to non-existent drives.")
                print(f"Please rebuild the VRT using gdalbuildvrt.")
                print(f"Detail: {read_error}")
                print("!"*70 + "\n")
                return []

            nodata_polys = [
                shape(geom)
                for geom, val in shapes(mask, transform=src.transform)
                if val == 0 
            ]
            print(f"[INFO] NoData polygons vectorised: {len(nodata_polys)}")

            if not nodata_polys:
                return []

            max_area = max(p.area for p in nodata_polys)

            for i, poly in enumerate(nodata_polys):
                a = poly.area
                if a == max_area: 
                    continue
                
                min_width, max_length = get_mrr_dimensions(poly)
                mrr_area = min_width * max_length
                
                matched, mult = area_matches(a, tile_area, tolerance, max_mult)
                if not matched:
                    matched, mult = area_matches(mrr_area, tile_area, tolerance, max_mult)

                gap_type = "Tile Match"
                if not matched and min_width >= min_gap_dist:
                    matched = True
                    mult = 0
                    gap_type = f"Distance (>= {min_gap_dist}m)"

                if matched:
                    gaps.append({
                        "id":        i + 1,
                        "area_m2":   round(a, 2), 
                        "tiles_est": mult,
                        "gap_type":  gap_type,
                        "centroid":  poly.centroid,
                        "geometry":  poly,
                        "source":    "vrt",
                    })
        return gaps
        
    except Exception as open_error:
        print(f"\n[CRITICAL ERROR] Could not process VRT.\nDetail: {open_error}\n")
        return []
    finally:
        os.chdir(original_cwd)

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def save_gaps_polygon_shp(gaps, outdir, tag):
    try:
        import geopandas as gpd
    except ImportError:
        return
    if not gaps: return

    outpath = os.path.join(outdir, f"{tag}_gaps_poly.shp")
    gdf = gpd.GeoDataFrame(
        [{"gap_id": g["id"], "area_m2": g["area_m2"], "gap_type": g["gap_type"], "tiles_est": g["tiles_est"], "source": g["source"]} for g in gaps],
        geometry=[g["geometry"] for g in gaps],
    )
    gdf.to_file(outpath)


def save_gaps_point_shp(gaps, outdir, tag):
    try:
        import geopandas as gpd
        from shapely.geometry import Point
    except ImportError:
        return
    if not gaps: return

    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f"{tag}_gaps_points.shp")
    records, geoms = [], []
    for g in gaps:
        cx, cy = round(g["centroid"].x, 2), round(g["centroid"].y, 2)
        records.append({"gap_id": g["id"], "area_m2": g["area_m2"], "gap_type": g["gap_type"], "tiles_est": g["tiles_est"], "source": g["source"], "cx": cx, "cy": cy})
        geoms.append(Point(cx, cy))

    gdf = gpd.GeoDataFrame(records, geometry=geoms)
    gdf.to_file(outpath)


def print_report(gaps, tile_area, tier_label):
    print("\n" + "=" * 80)
    print(f"  GAP REPORT  |  Tier matched: {tier_label}  ({tile_area:,.0f} m²)")
    print("=" * 80)
    if not gaps:
        print("  No gaps matching any expected criteria were detected.")
    else:
        print(f"  {'ID':>4}  {'Area (m2)':>14}  {'Gap Type':>20}  {'Source':>6}  Centroid (x, y)")
        print("  " + "-" * 75)
        for g in gaps:
            cx, cy = round(g["centroid"].x, 1), round(g["centroid"].y, 1)
            print(f"  {g['id']:>4}  {g['area_m2']:>14,.2f}  {g['gap_type']:>20}  {g['source'].upper():>6}  ({cx}, {cy})")
        print(f"\n  Tier used     : {tier_label}\n  Gaps found    : {len(gaps)}")
    print("=" * 80)

# ---------------------------------------------------------------------------
# Cascade runner
# ---------------------------------------------------------------------------

def run_cascade(inputs, tile_tiers, tolerance, max_mult, outdir, min_gap_dist):
    for side_m, label in tile_tiers:
        tile_area = side_m * side_m
        print(f"\n[CASCADE] Trying tier: {label}  ({tile_area:,.0f} m²)")

        all_gaps = []
        for inp in inputs:
            ext = os.path.splitext(inp)[1].lower()
            tag = os.path.splitext(os.path.basename(inp))[0]

            if ext == ".shp": gaps = detect_gaps_shp(inp, tile_area, tolerance, max_mult, min_gap_dist)
            else: gaps = detect_gaps_vrt(inp, tile_area, tolerance, max_mult, min_gap_dist)
            all_gaps.extend(gaps)

        if all_gaps:
            print(f"[CASCADE] ✓ Gaps found at tier {label} — stopping cascade.")
            tag = os.path.splitext(os.path.basename(inputs[0]))[0]
            save_gaps_polygon_shp(all_gaps, outdir, tag)
            save_gaps_point_shp(all_gaps, outdir, tag)
            return all_gaps, tile_area, label
        else:
            print(f"[CASCADE] ✗ No gaps at tier {label} — trying next tier ...")

    return [], None, None

# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("inputs", nargs="+")
parser.add_argument("--outdir", default=DEFAULT_OUTDIR)
parser.add_argument("--tolerance", type=float, default=AREA_TOLERANCE)
parser.add_argument("--max_mult", type=int, default=MAX_TILE_MULT)
parser.add_argument("--min_gap_dist", type=float, default=DEFAULT_MIN_DIST, help="Minimum gap distance in meters to flag non-tile gaps")

if len(sys.argv) == 1:
    parser.print_help()
    sys.exit(1)

args = parser.parse_args()
os.makedirs(args.outdir, exist_ok=True)

stime = time.time()
print(f"\n[{script_name}] Starting ...\n  Input(s)       : {args.inputs}\n  Output dir     : {args.outdir}")
print(f"  Tolerance      : ±{args.tolerance * 100:.0f}%\n  Min Gap Dist   : {args.min_gap_dist}m\n  Max mult       : {args.max_mult}")

all_gaps, matched_area, matched_label = run_cascade(args.inputs, TILE_TIERS, args.tolerance, args.max_mult, args.outdir, args.min_gap_dist)

if matched_label: print_report(all_gaps, matched_area, matched_label)
else: print_report([], 0, "None")

print("\n>>> Dataset contains gaps — please review output files. <<<" if all_gaps else ">>> No gaps detected at any tier. Dataset appears complete. <<<")
print(f"\n[{script_name}] Finished in {time.time() - stime:.1f}s\n")
