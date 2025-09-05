#-------------------------------------------------------------------------------
# Name: cutshp.py
# Purpose: Build ONE solid (hole-free) contour of valid data from a VRT and
#          save it as a single-feature shapefile. Robust cropping (Warp→VRT),
#          diagnostics, auto-fallback to fast footprint, and safe-guards to
#          avoid writing empty shapefiles.
#
# Author: Raul,rddgamboa@earthdefine.com
# Created: 09/03/2025
#-------------------------------------------------------------------------------

import sys, os, time, argparse, xml.etree.ElementTree as ET, math, uuid
from osgeo import gdal, ogr, osr
ogr.UseExceptions(); gdal.UseExceptions()

# --------------------- optional template utils (stubs) ------------------------
try:
    import sfunctions as s
    import ufunctions as u
    import uconstants as uc
except Exception:
    class _UStub:
        def printStartTime(self):
            t = time.time()
            sys.stderr.write("Start time: %s\n" % time.strftime("%a %b %d %H:%M:%S %Y"))
            return t
        def printRunTime(self, stime):
            elapsed = time.time() - stime
            sys.stderr.write("Run time: %.1f seconds\n" % elapsed)
            sys.stderr.write("************************************************************\n")
    u = _UStub()

# ------------------------------ progress helpers ------------------------------
def _print_bar(frac, width=40, prefix=""):
    frac = max(0.0, min(1.0, float(frac or 0.0)))
    filled = int(width * frac + 1e-9)
    bar = "#" * filled + "-" * (width - filled)
    sys.stdout.write("\r%s [%s] %3d%%" % (prefix, bar, int(frac * 100)))
    sys.stdout.flush()
    if frac >= 1.0: sys.stdout.write("\n")

def _gdal_progress(complete, msg, data):
    _print_bar(complete, prefix=str(data) if data else "Processing"); return 1
def _stage(msg): sys.stdout.write("\n==> %s\n" % msg); sys.stdout.flush()

# --------------------------------- geometry -----------------------------------
def _pixel_to_geo(gt, px, py): return (gt[0]+px*gt[1]+py*gt[2], gt[3]+px*gt[4]+py*gt[5])

def _build_extent_polygon(ds):
    gt = ds.GetGeoTransform()
    w, h = ds.RasterXSize, ds.RasterYSize
    p0, p1, p2, p3 = _pixel_to_geo(gt,0,0), _pixel_to_geo(gt,w,0), _pixel_to_geo(gt,w,h), _pixel_to_geo(gt,0,h)
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x,y in (p0,p1,p2,p3,p0): ring.AddPoint_2D(x,y)
    poly = ogr.Geometry(ogr.wkbPolygon); poly.AddGeometry(ring); return poly

def _get_env_from_layer(layer, buff=0.0):
    minx, maxx, miny, maxy = layer.GetExtent()
    if buff: minx, maxx, miny, maxy = minx-buff, maxx+buff, miny-buff, maxy+buff
    return (minx, maxx, miny, maxy)

def _reproject_envelope(env, src_srs, dst_srs):
    if not src_srs or not dst_srs or src_srs.IsSame(dst_srs): return env
    try:
        src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        dst_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    except Exception: pass
    ct = osr.CoordinateTransformation(src_srs, dst_srs)
    (minX, maxX, minY, maxY) = env
    xs, ys = [], []
    for x,y in [(minX,minY),(minX,maxY),(maxX,minY),(maxX,maxY)]:
        X,Y,_ = ct.TransformPoint(x,y); xs.append(X); ys.append(Y)
    return (min(xs), max(xs), min(ys), max(ys))

def _bounds_from_ds(ds):
    gt = ds.GetGeoTransform(); w,h = ds.RasterXSize, ds.RasterYSize
    corners = [_pixel_to_geo(gt,0,0), _pixel_to_geo(gt,w,0), _pixel_to_geo(gt,0,h), _pixel_to_geo(gt,w,h)]
    xs, ys = [c[0] for c in corners], [c[1] for c in corners]
    return (min(xs), max(xs), min(ys), max(ys))

def _intersect_bounds(a,b):
    ix1, ix2 = max(a[0],b[0]), min(a[1],b[1]); iy1, iy2 = max(a[2],b[2]), min(a[3],b[3])
    return None if ix1>=ix2 or iy1>=iy2 else (ix1, ix2, iy1, iy2)

def _remove_interior_rings(g):
    """Return MultiPolygon without holes (exterior rings only)."""
    out = ogr.Geometry(ogr.wkbMultiPolygon)
    name = ogr.GeometryTypeToName(g.GetGeometryType()).lower()
    def add_exterior(p):
        if p and not p.IsEmpty():
            ext = p.GetGeometryRef(0)
            if ext: 
                np = ogr.Geometry(ogr.wkbPolygon); np.AddGeometry(ext.Clone()); out.AddGeometry(np)
    if "multipolygon" in name:
        for i in range(g.GetGeometryCount()): add_exterior(g.GetGeometryRef(i))
    elif "polygon" in name: add_exterior(g)
    return out if out.GetGeometryCount()>0 else None

def _simplify_geometry(g, tol=0.0):
    if tol and tol>0:
        try: return g.SimplifyPreserveTopology(float(tol))
        except Exception: return g
    return g

# --------------------------------- raster ops ---------------------------------
def _check_vrt_sources(vrt_path):
    missing=[]
    try:
        root = ET.parse(vrt_path).getroot(); basedir = os.path.dirname(vrt_path)
        for sf in root.iter('SourceFilename'):
            fn=(sf.text or '').strip(); 
            if not fn: continue
            if not os.path.isabs(fn): fn=os.path.normpath(os.path.join(basedir,fn))
            if not os.path.exists(fn): 
                missing.append(fn)
                if len(missing)>=1000: break
    except Exception: pass
    return missing

def _fast_tiles_union(vrt_path, debug=False):
    try:
        root = ET.parse(vrt_path).getroot(); basedir = os.path.dirname(vrt_path)
    except Exception: return None
    ds = gdal.Open(vrt_path, gdal.GA_ReadOnly); 
    if ds is None: return None
    union=None; count=0
    for sf in root.iter('SourceFilename'):
        fn=(sf.text or '').strip()
        if not fn: continue
        if not os.path.isabs(fn): fn=os.path.normpath(os.path.join(basedir,fn))
        if not os.path.exists(fn): continue
        src=gdal.Open(fn, gdal.GA_ReadOnly); 
        if src is None: continue
        poly=_build_extent_polygon(src); src=None
        if poly is None or poly.IsEmpty(): continue
        union = poly if union is None else union.Union(poly)
        count+=1
        if count%50==0: _print_bar(min(0.99,count/1000.0), prefix="Merging tiles (fast)")
    if union is None: return None
    _print_bar(1.0, prefix="Merging tiles (fast)")
    if debug:
        env=union.GetEnvelope(); sys.stdout.write(f">> FAST union envelope: {env}\n")
    return union

def _estimate_pixels(bounds, xRes, yRes):
    if not xRes or not yRes: return None
    w=max(1,int(math.ceil((bounds[1]-bounds[0])/float(abs(xRes)))))
    h=max(1,int(math.ceil((bounds[3]-bounds[2])/float(abs(yRes)))))
    return w*h, w, h

def _safe_crop_vrt(ds_full, aoi_bounds_raster, xRes, yRes, max_pixels, do_crop, debug=False):
    """Warp→/vsimem/.vrt with outputBounds = AOI∩raster; skip/limit when huge or invalid."""
    if not do_crop or aoi_bounds_raster is None: return ds_full
    r_bounds=_bounds_from_ds(ds_full); inter=_intersect_bounds(aoi_bounds_raster, r_bounds)
    if inter is None:
        if debug: sys.stdout.write(">> AOI∩raster = EMPTY; skip crop.\n")
        return ds_full
    ob=(inter[0], inter[2], inter[1], inter[3])  # (minX,minY,maxX,maxY)
    est=_estimate_pixels((ob[0], ob[2], ob[1], ob[3]), xRes, yRes)
    if est and est[0]>max_pixels:
        sys.stderr.write(f"WARNING: AOI crop ~{est[0]} px (> {max_pixels}); using full VRT.\n")
        return ds_full
    try:
        _stage("Cropping VRT to SHP AOI (Warp → /vsimem/.vrt)")
        vsiname=f"/vsimem/crop_{uuid.uuid4().hex}.vrt"
        opts=gdal.WarpOptions(format='VRT', outputBounds=ob, xRes=abs(xRes) if xRes else None,
                              yRes=abs(yRes) if yRes else None, targetAlignedPixels=True,
                              resampleAlg=gdal.GRA_NearestNeighbour, multithread=True,
                              errorThreshold=0.0, callback=_gdal_progress, callback_data="Cropping VRT")
        gdal.PushErrorHandler('CPLQuietErrorHandler')
        try: out=gdal.Warp(vsiname, ds_full, options=opts)
        finally: gdal.PopErrorHandler()
        if out is None:
            if debug: sys.stdout.write(">> Warp returned None; using full VRT.\n")
            return ds_full
        if out.RasterXSize==0 or out.RasterYSize==0:
            if debug: sys.stdout.write(">> Warp result 0x0; using full VRT.\n")
            return ds_full
        return out
    except Exception:
        if debug: sys.stdout.write(">> Exception during Warp; using full VRT.\n")
        return ds_full

def _polygonize_mask_to_geom(mask_band, srs, debug=False):
    """Polygonize mask (>0 valid) → MultiPolygon. No initial dissolve."""
    mem_drv=ogr.GetDriverByName("MEMORY"); mem_ds=mem_drv.CreateDataSource("mem")
    layer=mem_ds.CreateLayer("mask", srs=srs, geom_type=ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn("val", ogr.OFTInteger))
    gdal.PushErrorHandler('CPLQuietErrorHandler')
    try:
        gdal.Polygonize(mask_band, None, layer, 0, [], _gdal_progress, "Polygonizing mask")
    finally:
        gdal.PopErrorHandler()
    layer.SetAttributeFilter("val <> 0")
    multi=ogr.Geometry(ogr.wkbMultiPolygon); n=0
    for feat in layer:
        geom=feat.GetGeometryRef()
        if geom and not geom.IsEmpty():
            multi.AddGeometry(geom.Clone()); n+=1
    if debug: sys.stdout.write(f">> Polygonize: {n} parts\n")
    return multi if multi.GetGeometryCount()>0 else None

def _batch_dissolve(multi, batch_size=500):
    if multi is None or multi.IsEmpty(): return multi
    total=multi.GetGeometryCount()
    if total<=1: return multi
    _stage("Dissolving polygons in batches"); chunks=[]
    for i in range(0,total,batch_size):
        mp=ogr.Geometry(ogr.wkbMultiPolygon); up=min(total,i+batch_size)
        for j in range(i,up):
            g=multi.GetGeometryRef(j)
            if g and not g.IsEmpty(): mp.AddGeometry(g.Clone())
        chunks.append(mp.UnionCascaded()); _print_bar(min(0.99,(i+batch_size)/float(max(total,1))),"Dissolving")
    while len(chunks)>1:
        new=[]
        for i in range(0,len(chunks),8):
            mp=ogr.Geometry(ogr.wkbMultiPolygon)
            for g in chunks[i:i+8]:
                if g and not g.IsEmpty():
                    name=ogr.GeometryTypeToName(g.GetGeometryType()).lower()
                    if "multipolygon" in name:
                        for k in range(g.GetGeometryCount()): mp.AddGeometry(g.GetGeometryRef(k).Clone())
                    elif "polygon" in name: mp.AddGeometry(g.Clone())
            new.append(mp.UnionCascaded()); _print_bar(min(0.99,(i+8)/float(max(len(chunks),1))),"Merging chunks")
        chunks=new
    _print_bar(1.0,"Dissolving"); return chunks[0]

def _get_vrt_footprint(vrt_path, aoi_env_in_raster=None, fast=False, do_crop=True, max_pixels=500_000_000, debug=False):
    ds_full=gdal.Open(vrt_path, gdal.GA_ReadOnly)
    if ds_full is None: raise RuntimeError("Cannot open VRT: %s" % vrt_path)
    srs_raster=osr.SpatialReference(); wkt=ds_full.GetProjection()
    if wkt:
        srs_raster.ImportFromWkt(wkt)
        try: srs_raster.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        except Exception: pass
    else: srs_raster=None

    if fast:
        _stage("Generating FAST footprint (union of tile extents)")
        return _fast_tiles_union(vrt_path, debug=debug), srs_raster

    gt=ds_full.GetGeoTransform(); xRes=abs(gt[1]) if gt else None; yRes=abs(gt[5]) if gt else None
    ds=_safe_crop_vrt(ds_full,aoi_env_in_raster,xRes,yRes,max_pixels,do_crop,debug=debug)

    # Report bounds for debugging
    if debug:
        r_env=_bounds_from_ds(ds_full); sys.stdout.write(f">> Raster bounds: {r_env}\n")
        if aoi_env_in_raster: sys.stdout.write(f">> AOI (raster SRS): {aoi_env_in_raster}\n")
        if ds is not None: sys.stdout.write(f">> Working DS size: {ds.RasterXSize} x {ds.RasterYSize}\n")

    band=ds.GetRasterBand(1)
    if band is None: raise RuntimeError("VRT has no readable raster bands.")
    mask_flags=band.GetMaskFlags()
    if mask_flags & gdal.GMF_ALL_VALID:
        _stage("Mask: all valid → using cropped extent")
        return _build_extent_polygon(ds), srs_raster

    mask_band=band.GetMaskBand()
    if mask_band is None:
        _stage("No explicit mask → using cropped extent")
        return _build_extent_polygon(ds), srs_raster

    _stage("Polygonizing VRT mask")
    fp=_polygonize_mask_to_geom(mask_band, srs_raster, debug=debug)

    # Auto-fallback to FAST if polygonize ended empty
    if fp is None or fp.IsEmpty():
        _stage("Polygonize is empty → switching to FAST union of tiles")
        fast_union=_fast_tiles_union(vrt_path, debug=debug)
        if fast_union is not None and not fast_union.IsEmpty():
            return fast_union, srs_raster
        # Fallback to extent to avoid empty result
        return _build_extent_polygon(ds), srs_raster

    return fp, srs_raster

# --------------------------------- output -------------------------------------
def _write_single_feature_shp(out_path, srs, geom):
    drv=ogr.GetDriverByName("ESRI Shapefile")
    if os.path.exists(out_path): drv.DeleteDataSource(out_path)
    out_ds=drv.CreateDataSource(out_path)
    if out_ds is None: raise RuntimeError("Cannot create output: %s" % out_path)
    out_lyr=out_ds.CreateLayer(os.path.splitext(os.path.basename(out_path))[0], srs=srs, geom_type=ogr.wkbMultiPolygon)
    out_lyr.CreateField(ogr.FieldDefn("id", ogr.OFTInteger))
    f=ogr.Feature(out_lyr.GetLayerDefn()); f.SetField("id",1); f.SetGeometry(geom); out_lyr.CreateFeature(f)
    f=None; out_ds=None

# ---------------------------------- MAIN --------------------------------------
parser=argparse.ArgumentParser(
    description='Create ONE solid contour (no holes) from a VRT valid-data mask; single-feature shapefile.',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('vrt'); parser.add_argument('shp')
parser.add_argument('--buffer', type=float, default=0.0, help='AOI buffer (SHP units)')
parser.add_argument('--fast', action='store_true', help='FAST footprint (union of tile extents)')
parser.add_argument('--intersect', action='store_true', help='Intersect final footprint with dissolved SHP (if polygonal)')
parser.add_argument('--simplify', type=float, default=-1.0, help='Simplification tolerance; default≈2× pixel size')
parser.add_argument('--dissolve', action='store_true', help='Batch dissolve after polygonize')
parser.add_argument('--no-crop', action='store_true', help='Skip cropping the VRT to AOI')
parser.add_argument('--max-crop-pixels', type=int, default=500_000_000, help='Pixel limit for crop; otherwise skip')
parser.add_argument('--debug', action='store_true', help='Verbose diagnostics')
if len(sys.argv)==1: parser.print_help(); sys.exit(1)
a=parser.parse_args()

stime=u.printStartTime()
try:
    if not os.path.exists(a.vrt): raise FileNotFoundError("Missing VRT: %s" % a.vrt)
    if not os.path.exists(a.shp): raise FileNotFoundError("Missing SHP: %s" % a.shp)

    _stage("Checking VRT sources")
    missing=_check_vrt_sources(a.vrt)
    if missing:
        sys.stderr.write(f"WARNING: {len(missing)} missing sources in VRT (e.g.):\n")
        for m in missing[:5]: sys.stderr.write(f" - {m}\n")

    _stage("Reading SHP AOI")
    shp_ds=ogr.Open(a.shp,0); shp_lyr=shp_ds.GetLayer(0); shp_srs=shp_lyr.GetSpatialRef()
    try:
        if shp_srs: shp_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    except Exception: pass
    shp_env=_get_env_from_layer(shp_lyr, a.buffer)
    shp_geom_type=shp_lyr.GetGeomType()
    shp_is_poly="polygon" in ogr.GeometryTypeToName(shp_geom_type).lower()
    shp_diss=None
    if shp_is_poly and a.intersect:
        _stage("Dissolving SHP polygons (for intersection)")
        multi=ogr.Geometry(ogr.wkbMultiPolygon); total=shp_lyr.GetFeatureCount(True) or 0; done=0
        shp_lyr.ResetReading()
        for f in shp_lyr:
            g=f.GetGeometryRef()
            if g and not g.IsEmpty():
                name=ogr.GeometryTypeToName(g.GetGeometryType()).lower()
                if "multipolygon" in name:
                    for i in range(g.GetGeometryCount()): multi.AddGeometry(g.GetGeometryRef(i).Clone())
                elif "polygon" in name: multi.AddGeometry(g.Clone())
            done+=1; 
            if total>0: _print_bar(done/float(total), prefix="Reading SHP")
        shp_diss = multi.UnionCascaded() if multi.GetGeometryCount()>0 else None
        _print_bar(1.0, prefix="Reading SHP")
    shp_ds=None

    # Raster SRS and pixel size (for default simplification)
    tmp=gdal.Open(a.vrt, gdal.GA_ReadOnly); gt=tmp.GetGeoTransform() if tmp else None
    px=abs(gt[1]) if gt else 0.0; py=abs(gt[5]) if gt else 0.0; pix=max(px,py) if (px and py) else 0.0
    vrt_srs=osr.SpatialReference(); wkt=tmp.GetProjection()
    if wkt:
        vrt_srs.ImportFromWkt(wkt)
        try: vrt_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        except Exception: pass
    tmp=None
    tol = (2.0*pix) if (a.simplify is None or a.simplify<0) else a.simplify

    aoi_r = _reproject_envelope(shp_env, shp_srs, vrt_srs) if wkt else None
    if a.debug:
        sys.stdout.write(f">> SHP bbox (SHP SRS): {shp_env}\n")
        if aoi_r: sys.stdout.write(f">> SHP bbox in raster SRS: {aoi_r}\n")

    # Build footprint (exact/fast) with robust crop and auto-fallback
    fp, srs_fp = _get_vrt_footprint(a.vrt, aoi_r, fast=a.fast, do_crop=(not a.no_crop),
                                    max_pixels=a.max_crop_pixels, debug=a.debug)
    if fp is None or fp.IsEmpty(): raise RuntimeError("Footprint empty after all fallbacks.")

    _stage("Removing interior holes")
    solid=_remove_interior_rings(fp) or fp.Buffer(0)

    if a.intersect and shp_is_poly and shp_diss and not shp_diss.IsEmpty():
        _stage("Intersecting footprint with SHP")
        if srs_fp and shp_srs and (not srs_fp.IsSame(shp_srs)):
            ct=osr.CoordinateTransformation(shp_srs, srs_fp); shp_diss=shp_diss.Clone(); shp_diss.Transform(ct)
        solid = solid.Intersection(shp_diss)

    if a.dissolve: solid=_batch_dissolve(solid, batch_size=500)

    _stage("Cleaning and simplifying geometry")
    solid = solid.Buffer(0); solid = _simplify_geometry(solid, tol); solid = solid.Buffer(0)

    # ----------- DIAGNOSTICS and Safety checks (avoid 0,0 shapefiles) ----------
    if solid is None or solid.IsEmpty():
        raise RuntimeError("Final geometry is empty (after simplify/intersect). Try lowering --simplify, removing --intersect, or adding --no-crop/--fast.")
    env=solid.GetEnvelope(); area=solid.Area()
    sys.stdout.write(f">> Final envelope (current SRS): {env}\n")
    sys.stdout.write(f">> Final area: {area}\n")
    if not all(map(math.isfinite, env)) or area<=0 or (abs(env[1]-env[0])<1e-9 and abs(env[3]-env[2])<1e-9):
        raise RuntimeError("Invalid/zero envelope/area; refusing to write an empty shapefile.")

    # Ensure MultiPolygon type
    if "multipolygon" not in ogr.GeometryTypeToName(solid.GetGeometryType()).lower():
        mp=ogr.Geometry(ogr.wkbMultiPolygon)
        if "polygon" in ogr.GeometryTypeToName(solid.GetGeometryType()).lower():
            mp.AddGeometry(solid.Clone())
        else:
            mp.AddGeometry(solid.ConvexHull())
        solid=mp

    # Reproject to SHP SRS for output (so the .prj is consistent)
    out_srs = shp_srs if (srs_fp and shp_srs and not srs_fp.IsSame(shp_srs)) else (shp_srs or srs_fp)
    if srs_fp and shp_srs and not srs_fp.IsSame(shp_srs):
        _stage("Reprojecting footprint to SHP SRS")
        ct=osr.CoordinateTransformation(srs_fp, shp_srs); solid.Transform(ct)

    base,_=os.path.splitext(a.shp); out_path=base+"_cut.shp"
    _stage("Writing single-feature shapefile (solid, no holes)")
    _write_single_feature_shp(out_path, out_srs, solid)
    print("Done:", out_path)

except Exception as e:
    sys.stderr.write("ERROR: %s\n" % str(e)); sys.exit(2)

u.printRunTime(stime)
