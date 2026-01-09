#-------------------------------------------------------------------------------
# Name: check_dataset.py
# Purpose:
#
# Author: Ana,ahaguilar@earthdefine.com
#
# Created: 08/20/2025
# Copyright:   (c) EarthDefine 2025
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#!/usr/bin/env python
import ufunctions as u
import sfunctions as s
import uconstants as uc
import os,sys,glob,argparse,time,random,csv,shutil,re
from osgeo import ogr,gdal,osr
ogr.UseExceptions()

#Constants
################################################################################
script_name = os.path.basename(os.path.splitext(__file__)[0])
################################################################################

#Functions
################################################################################

################################################################################

#Parse arguments
################################################################################
parser = argparse.ArgumentParser(description='',formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('st',help='input state')
parser.add_argument('aoi',help='name of aoi')


if len(sys.argv)==1:
    parser.print_help()
    sys.exit(1)

args = parser.parse_args()

aoi = args.aoi
st = args.st
# jobdir = os.path.join(outdir,'jobs')

#Start
################################################################################
stime = u.printStartTime()

elevation_folder = r'\\DS3\Data\elevation'
state_folder=os.path.join(elevation_folder,st)
subfolders = [
    "AOI_Cutlines",
    "DEM_Float32",
    "DEM_footprints",
    "first_nDSM_UInt16",
    "first_nDSM_footprints",
    "last_nDSM_Float32",
    "last_nDSM_footprints"        
]

#write your code
def folder_has_contents(path: str) -> bool:
    """Return True if folder contains at least one item (file or subfolder)."""
    try:
        with os.scandir(path) as it:
            for _ in it:
                return True
        return False
    except PermissionError:
        print(f"[!] No permission to read: {path}")
        return False

def check_folders(state_folder: str, aoi: str, subfolders: list[str]) -> tuple[list[str], list[str]]:
    print(f"\nChecking inside state folder: {state_folder}\n")
    missing, empty = [], []

    for folder in subfolders:
        if folder == "AOI_Cutlines":
            shp_path = os.path.join(state_folder, folder, f"{aoi}.shp")
            archive_shp_path = os.path.join(state_folder, folder, "archive", f"{aoi}.shp")
            if os.path.isfile(shp_path):
                print(f"[✔] AOI shapefile exists: {shp_path}")
            elif os.path.isfile(archive_shp_path):
                print(f"[✔] AOI shapefile found in ARCHIVE: {archive_shp_path}")
            else:
                print(f"[X] AOI shapefile missing")
                missing.append(f"{aoi}.shp (AOI_Cutlines)")
            continue
        
        folder_path = os.path.join(state_folder, folder, aoi)
        
        if not os.path.isdir(folder_path):
            print(f"\n[X] Folder does NOT exist: {folder_path}")
            missing.append(folder_path)
            continue

        if folder_has_contents(folder_path):
            print(f"\n[✔] Exists and is NOT empty: {folder_path}")
            if folder in ['DEM_Float32','first_nDSM_UInt16','last_nDSM_Float32']:
                tif_files = glob.glob(os.path.join(folder_path, '*.tif'))
                print(f'Number of tif files: {len(tif_files)}')
        else:
            print(f"\n[!] Exists but is EMPTY: {folder_path}")
            empty.append(folder_path)

    print() 
    if missing:
        print("Summary: Missing folders:")
        for p in missing:
            print(f"  - {p}")
    if empty:
        print("\nSummary: Empty folders:")
        for p in empty:
            print(f"  - {p}")
    if not missing and not empty:
        print("\nSummary: All required folders exist and are non-empty.")

    return missing, empty

# Run the checks before cleanup/lock close
missing, empty = check_folders(state_folder, aoi, subfolders)


u.printRunTime(stime)
