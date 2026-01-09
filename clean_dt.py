#-------------------------------------------------------------------------------
# Name: clean_dt.py
# Purpose:
#   - Validate schemas for a .shp or a folder (report).
#   - --clean: create any missing required fields (e.g., ED_Comment),
#              ensure ID, delete extras, and (de forma segura) reescribir
#              ED_AOIName / ED_lazLoc solo si no coinciden.
#              Y/N solo para carpetas. Muestra POST-CLEAN REPORT.
#
# Author: Raul, rddgamboa@earthdefine.com
#-------------------------------------------------------------------------------

import sys, os, argparse
from osgeo import ogr
ogr.UseExceptions()

# ===== Configuration =====
KEEP_FIELDS_DEFAULT = [
    "ED_Date","ED_Year","ED_DateSt","ED_DateEnd","ED_PS",
    "ED_VertAcc","ED_HorAcc","ED_QL","ED_AOIName","ED_lazLoc",
    "ED_Drive","ED_Rank","ED_Comment"
]

FIELD_SPEC = {
    "ED_Date":    ("string", 20, None),
    "ED_Year":    ("integer", 10, None),
    "ED_DateSt":  ("string", 20, None),
    "ED_DateEnd": ("string", 20, None),
    "ED_PS":      ("real",   10, 3),
    "ED_VertAcc": ("real",   10, 3),
    "ED_HorAcc":  ("real",   10, 3),
    "ED_QL":      ("string", 8,  None),
    "ED_AOIName": ("string", 128, None),
    "ED_lazLoc":  ("string", 254, None),
    "ED_Drive":   ("string", 16,  None),
    "ED_Rank":    ("integer", 10, None),
    "ED_Comment": ("string", 254, None),
}

ID_NAME_DEFAULT  = "ID"
ID_WIDTH_DEFAULT = 10

# ===== Helpers =====
def list_fields(layer):
    defn = layer.GetLayerDefn()
    return [defn.GetFieldDefn(i).GetName() for i in range(defn.GetFieldCount())]

def get_field_def(name):
    kind, width, prec = FIELD_SPEC.get(name, ("string", 80, None))
    if kind == "integer":
        f = ogr.FieldDefn(name, ogr.OFTInteger)
        if width: f.SetWidth(width)
        return f
    if kind == "real":
        f = ogr.FieldDefn(name, ogr.OFTReal)
        if width: f.SetWidth(width)
        if prec:  f.SetPrecision(prec)
        return f
    f = ogr.FieldDefn(name, ogr.OFTString)
    if width: f.SetWidth(width)
    return f

def get_field_index_ci(layer, target_name):
    defn = layer.GetLayerDefn()
    tl = target_name.lower()
    for i in range(defn.GetFieldCount()):
        n = defn.GetFieldDefn(i).GetName()
        if n.lower() == tl:
            return i, n
    return -1, None

def ensure_field_exists(layer, name):
    """
    Ensure a field exists; create if missing (using FIELD_SPEC).
    Returns (idx, exact_name, created_bool). Revalida el índice tras crear.
    """
    idx, exact = get_field_index_ci(layer, name)
    if idx != -1:
        return idx, exact, False
    fdef = get_field_def(name)
    rc = layer.CreateField(fdef)
    if rc != 0:
        raise RuntimeError(f"Failed to create field '{name}' (rc={rc})")
    layer.SyncToDisk()
    # Re-adquirir índice: algunos drivers ajustan mayúsculas/minúsculas
    idx, exact = get_field_index_ci(layer, name)
    if idx == -1:
        names = list_fields(layer)
        for i, nm in enumerate(names):
            if nm.lower() == name.lower():
                return i, nm, True
        raise RuntimeError(f"Field '{name}' not found after creation")
    return idx, (exact if exact else name), True

def ensure_id_field(layer, id_name=ID_NAME_DEFAULT, width=ID_WIDTH_DEFAULT, autofill=True):
    existing = list_fields(layer)
    id_like = next((n for n in existing if n.lower() == "id"), None)
    if id_like:
        return id_like, False
    fdef = ogr.FieldDefn(id_name, ogr.OFTInteger)
    fdef.SetWidth(width)
    rc = layer.CreateField(fdef)
    if rc != 0:
        raise RuntimeError(f"Failed to create ID field (rc={rc})")
    if autofill:
        # Rebuscar índice por si el driver normaliza el nombre
        idx = layer.GetLayerDefn().GetFieldIndex(id_name)
        if idx == -1:
            idx, _ = get_field_index_ci(layer, id_name)
        layer.ResetReading()
        i = 1
        for feat in layer:
            feat.SetField(idx, i)
            if layer.SetFeature(feat) != 0:
                raise RuntimeError("Failed while filling ID")
            i += 1
    layer.SyncToDisk()
    return id_name, True

def ensure_required_fields(layer, keep_fields):
    """
    Create ANY missing required fields using FIELD_SPEC.
    Relee el schema después de cada creación.
    """
    created = []
    existing_lc = {n.lower() for n in list_fields(layer)}
    for k in keep_fields:
        if k.lower() not in existing_lc:
            fdef = get_field_def(k)
            rc = layer.CreateField(fdef)
            if rc != 0:
                raise RuntimeError(f"Failed to create required field '{k}' (rc={rc})")
            created.append(k)
            layer.SyncToDisk()
            # refrescar para evitar índices obsoletos
            existing_lc = {n.lower() for n in list_fields(layer)}
    return created

def delete_all_except(layer, keep_fields_ci):
    """
    Delete all fields except those listed (case-insensitive).
    """
    existing_exact = list_fields(layer)
    keep_lc = {k.lower() for k in keep_fields_ci}
    to_keep_exact = [n for n in existing_exact if n.lower() in keep_lc]
    defn = layer.GetLayerDefn()
    delete_idxs = []
    for i in range(defn.GetFieldCount()):
        name = defn.GetFieldDefn(i).GetName()
        if name not in to_keep_exact:
            delete_idxs.append(i)
    for idx in sorted(delete_idxs, reverse=True):
        if layer.DeleteField(idx) != 0:
            raise RuntimeError(f"Failed to delete field at index {idx}")
    layer.SyncToDisk()
    return len(delete_idxs)

def split_prefix_and_tail(path_str):
    """Devuelve (prefijo_con_sep, cola, sep) para una ruta-like; seguro con / o \\"""
    if path_str is None:
        return "", "", "\\"
    s = str(path_str).strip()
    b = s.rfind("\\"); f = s.rfind("/")
    if b == -1 and f == -1:
        return "", s, "\\"
    if b >= f:
        sep = "\\"
        i = b
    else:
        sep = "/"
        i = f
    return s[:i+1], s[i+1:], sep

def dataset_name_for_lazloc(base: str) -> str:
    """
    Para ED_lazLoc, quitar prefijo de estado (p.ej. 'CT_', 'MP_') SOLO si
    lo que sigue empieza con 'Retiled' (insensible a may/min).
    """
    if not base:
        return base
    i = base.find("_")
    if i > 0:
        rest = base[i + 1 :]
        if rest.lower().startswith("retiled"):
            return rest
    return base

def safe_rewrite_aoi_laz(layer, shp_base):
    """
    Reescribe ED_AOIName y ED_lazLoc si están desalineados. Nunca lanza excepción
    hacia arriba: si algo falla, continúa con el cleaning (no bloquea --clean).
    Devuelve dict con contadores.
    """
    stats = {"aoi_updates": 0, "lazloc_updates": 0, "error": None}
    try:
        aoi_idx, _, _  = ensure_field_exists(layer, "ED_AOIName")
        laz_idx, _, _  = ensure_field_exists(layer, "ED_lazLoc")
        laz_dataset     = dataset_name_for_lazloc(shp_base)

        layer.ResetReading()
        for feat in layer:
            # AOI
            if feat.GetField(aoi_idx) != shp_base:
                feat.SetField(aoi_idx, shp_base)
                if layer.SetFeature(feat) != 0:
                    raise RuntimeError("Failed to update ED_AOIName")
                stats["aoi_updates"] += 1
            # lazLoc
            cur_loc = feat.GetField(laz_idx)
            prefix, tail, _ = split_prefix_and_tail(cur_loc)
            new_loc = prefix + laz_dataset
            if cur_loc != new_loc:
                feat.SetField(laz_idx, new_loc)
                if layer.SetFeature(feat) != 0:
                    raise RuntimeError("Failed to update ED_lazLoc")
                stats["lazloc_updates"] += 1
    except Exception as e:
        stats["error"] = str(e)
    return stats

def analyze_shapefile(shp_path, required_fields):
    drv = ogr.GetDriverByName("ESRI Shapefile")
    ds = drv.Open(shp_path, 0)  # read-only
    if ds is None:
        return {"path": shp_path, "error": "Could not open shapefile"}
    layer = ds.GetLayer(0)
    fields_exact = list_fields(layer)
    fields_lc_map = {f.lower(): f for f in fields_exact}
    req_lc = [f.lower() for f in required_fields]
    missing = [rf for rf in required_fields if rf.lower() not in fields_lc_map]
    has_id = ("id" in fields_lc_map)
    extras = [f for f in fields_exact if f.lower() not in set(req_lc) | {"id"}]
    meets_required = has_id and (len(missing) == 0)
    return {
        "path": shp_path,
        "has_id": has_id,
        "missing": missing,
        "extras": extras,
        "total_fields": len(fields_exact),
        "meets_required": meets_required,
        "id_field_exact": fields_lc_map.get("id")
    }

def clean_shapefile(shp_path, keep_fields, id_name, id_width, autofill_id, do_rewrite=True):
    """
    Limpia un shapefile:
      - Reescribe AOI/laz (seguro) si do_rewrite=True.
      - Asegura ID.
      - Crea TODOS los campos requeridos faltantes.
      - Borra extras.
    """
    drv = ogr.GetDriverByName("ESRI Shapefile")
    ds = drv.Open(shp_path, 1)  # update
    if ds is None:
        raise RuntimeError(f"Could not open shapefile for update: {shp_path}")
    layer = ds.GetLayer(0)

    base = os.path.splitext(os.path.basename(shp_path))[0]
    rw = safe_rewrite_aoi_laz(layer, base) if do_rewrite else {"aoi_updates":0,"lazloc_updates":0,"error":None}

    actual_id, id_created = ensure_id_field(layer, id_name=id_name, width=id_width, autofill=autofill_id)
    created_fields = ensure_required_fields(layer, keep_fields)
    deleted = delete_all_except(layer, list(keep_fields) + [actual_id, "id"])

    layer.SyncToDisk(); ds.SyncToDisk(); ds = None
    return id_created, created_fields, deleted, rw

def find_shapefiles(target_path, recursive=True):
    if os.path.isfile(target_path) and target_path.lower().endswith(".shp"):
        yield target_path; return
    if os.path.isdir(target_path):
        if recursive:
            for root, _, files in os.walk(target_path):
                for f in files:
                    if f.lower().endswith(".shp"):
                        yield os.path.join(root, f)
        else:
            for f in os.listdir(target_path):
                if f.lower().endswith(".shp"):
                    yield os.path.join(target_path, f)
        return
    raise FileNotFoundError(f"Not a valid .shp or directory: {target_path}")

def print_report(results, title="SCHEMA VALIDATION REPORT"):
    ok_count = 0; fail_count = 0
    print(f"\n===== {title} =====\n", flush=True)
    for r in results:
        if "error" in r:
            print(f"[ERROR] {r['path']}: {r['error']}", flush=True)
            fail_count += 1; continue
        status = "OK" if r["meets_required"] else "FAIL"
        if r["meets_required"]: ok_count += 1
        else: fail_count += 1
        id_info = f"present as '{r['id_field_exact']}'" if r["has_id"] else "MISSING"
        extras_info = "none" if not r["extras"] else ", ".join(r["extras"])
        missing_info = "none" if not r["missing"] else ", ".join(r["missing"])
        print(f"{status} | {r['path']}", flush=True)
        print(f"  - ID column: {id_info}", flush=True)
        print(f"  - Missing required: {missing_info}", flush=True)
        print(f"  - Extra columns: {extras_info}", flush=True)
        print(f"  - Total fields: {r['total_fields']}\n", flush=True)
    print("===== SUMMARY =====", flush=True)
    print(f"  Meets required (+ID): {ok_count}", flush=True)
    print(f"  Does NOT meet required: {fail_count}\n", flush=True)

# ===== Main =====
def main():
    ap = argparse.ArgumentParser(
        description=("Validate Shapefile schemas (file or folder). "
                     "--clean: create missing required fields, ensure ID, delete extras, "
                     "and safely rewrite ED_AOIName/ED_lazLoc if mismatched (Y/N only for folders)."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    ap.add_argument("input", help="Path to a .shp file OR a directory")
    ap.add_argument("--keep", default=None,
                    help="Comma-separated list of fields to KEEP/REQUIRE (override defaults).")
    ap.add_argument("--non-recursive", action="store_true",
                    help="For a directory, only scan the top level.")
    ap.add_argument("--clean", action="store_true",
                    help="Clean the target: for a folder asks Y/N first; for a single .shp proceeds immediately.")
    ap.add_argument("--id-name", default=ID_NAME_DEFAULT,
                    help="ID name to use when creating a new ID field.")
    ap.add_argument("--id-width", type=int, default=ID_WIDTH_DEFAULT,
                    help="DBF width for the new integer ID field.")
    ap.add_argument("--no-autofill", action="store_true",
                    help="Do NOT auto-fill the new ID field (leave nulls).")
    ap.add_argument("--no-rw", action="store_true",
                    help="Disable the automatic ED_AOIName/ED_lazLoc rewrite inside --clean.")

    args = ap.parse_args()
    keep_fields = KEEP_FIELDS_DEFAULT if args.keep is None else [s.strip() for s in args.keep.split(",") if s.strip()]
    recursive = not args.non_recursive

    # Gather targets
    try:
        shp_list = list(find_shapefiles(args.input, recursive=recursive))
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr); sys.exit(1)

    # 1) Analyze always
    results = []
    for shp in shp_list:
        try:
            results.append(analyze_shapefile(shp, keep_fields))
        except Exception as e:
            results.append({"path": shp, "error": str(e)})

    is_file_input = os.path.isfile(args.input) and args.input.lower().endswith(".shp")
    print_report(results)

    # Single .shp behavior
    if is_file_input:
        r = results[0]
        if "error" in r:
            print(f"[ERROR] {r['path']}: {r['error']}", flush=True); sys.exit(1)

        if args.clean:
            try:
                id_created, created_fields, deleted, rw = clean_shapefile(
                    args.input, keep_fields, args.id_name, args.id_width,
                    autofill_id=not args.no_autofill, do_rewrite=(not args.no_rw)
                )
                cf = f"Created fields: {', '.join(created_fields)}" if created_fields else "Created fields: none"
                extra_rw = f" | AOI updates: {rw['aoi_updates']} | lazLoc updates: {rw['lazloc_updates']}"
                if rw.get("error"): extra_rw += f" | rewrite warning: {rw['error']}"
                print(f"[CLEAN] {args.input} | ID created: {id_created} | {cf} | Deleted extras: {deleted}{extra_rw}",
                      flush=True)
            except Exception as e:
                print(f"[ERROR] Cleaning failed: {e}", flush=True); sys.exit(2)

            # Post-clean check
            post = analyze_shapefile(args.input, keep_fields)
            print_report([post], title="POST-CLEAN REPORT")
            return
        else:
            # summarize
            if r["meets_required"]:
                extra_txt = "" if not r["extras"] else f" | Extra: {', '.join(r['extras'])}"
                print(f"[OK] Schema valid and ID present ({r['id_field_exact']}).{extra_txt}", flush=True)
                sys.exit(0)
            else:
                miss = "none" if not r["missing"] else ", ".join(r["missing"])
                idtxt = "present" if r["has_id"] else "MISSING"
                print(f"[FAIL] Missing: {miss} | ID: {idtxt}", flush=True)
                sys.exit(2)

    # Folder without --clean → report only
    if not args.clean:
        return

    # Folder + --clean → warning & confirmation
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("  WARNING: You are about to CLEAN ALL listed shapefiles:", flush=True)
    print("    • Create any missing required fields.", flush=True)
    print("    • Ensure an ID field exists.", flush=True)
    print("    • Delete all non-required fields.", flush=True)
    print("    • Safely rewrite ED_AOIName / ED_lazLoc if mismatched.", flush=True)
    print("  Review the report above and confirm to continue.", flush=True)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    resp = input("Proceed to clean all shapefiles? (Y/N): ").strip().lower()
    if resp not in ("y", "yes"):
        print("Aborted by user. No changes were made.", flush=True); return

    # Execute cleaning for folder
    total = len(shp_list); done = 0; errors = 0
    print("\n===== CLEANING START =====\n", flush=True)
    for shp in shp_list:
        try:
            id_created, created_fields, deleted, rw = clean_shapefile(
                shp, keep_fields, args.id_name, args.id_width,
                autofill_id=not args.no_autofill, do_rewrite=(not args.no_rw)
            )
            done += 1
            cf = f"Created fields: {', '.join(created_fields)}" if created_fields else "Created fields: none"
            extra_rw = f" | AOI updates: {rw['aoi_updates']} | lazLoc updates: {rw['lazloc_updates']}"
            if rw.get("error"): extra_rw += f" | rewrite warning: {rw['error']}"
            print(f"[OK] {shp} | ID created: {id_created} | {cf} | Deleted extras: {deleted}{extra_rw}", flush=True)
        except Exception as e:
            errors += 1
            print(f"[ERROR] {shp} | {e}", flush=True)

    print("\n===== CLEANING SUMMARY =====", flush=True)
    print(f"  Processed: {done}/{total}", flush=True)
    print(f"  Errors:    {errors}", flush=True)

    # Post-clean validation
    post_results = []
    for shp in shp_list:
        try: post_results.append(analyze_shapefile(shp, keep_fields))
        except Exception as e: post_results.append({"path": shp, "error": str(e)})
    print_report(post_results, title="POST-CLEAN REPORT")
    print("===== DONE =====", flush=True)

if __name__ == "__main__":
    main()
