#!/usr/bin/env python

'''
Builds s1-gunw-aoi-track and s1-gunw-aoi-track-merged products
'''

from __future__ import print_function
import os
import json
import pytz
import shutil
import pickle
import hashlib
import dateutil
import dateutil.parser
from shapely.geometry import shape, Polygon, MultiPolygon, mapping
from shapely.ops import cascaded_union
from hysds.celery import app
from hysds.dataset_ingest import ingest
#from osgeo import ogr, osr

def get_area(coords):
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    print("get_area : coords : %s" %coords)
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        #print("i : %s j: %s, coords[i][1] : %s coords[j][0] : %s coords[j][1] : %s coords[i][0] : %s"  %(i, j, coords[i][1], coords[j][0], coords[j][1], coords[i][0]))
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    #area = abs(area) / 2.0
    return area / 2

def change_coordinate_direction(cord):
    print("change_coordinate_direction 1 cord: %s\n" %cord)
    cord_area = util.get_area(cord)
    if not cord_area>0:
        print("change_coordinate_direction : coordinates are not clockwise, reversing it")
        cord = [cord[::-1]]
        print("change_coordinate_direction 2 : cord : %s" %cord)
        try:
            cord_area = util.get_area(cord)
        except:
            cord = cord[0]
            print("change_coordinate_direction 3 : cord : %s" %cord)
            cord_area = util.get_area(cord)
        if not cord_area>0:
            print("change_coordinate_direction. coordinates are STILL NOT  clockwise")
    else:
        print("change_coordinate_direction: coordinates are already clockwise")

    print("change_coordinate_direction 4 : cord : %s" %cord)
    return cord


def build(ifg_list, version, product_prefix, aoi, track, orbit):
    '''Builds and submits a aoi-track product.'''
    ds = build_dataset(ifg_list, version, product_prefix, aoi, track, orbit)
    met = build_met(ifg_list, version, product_prefix, aoi, track, orbit)
    print('Publishing Product: {0}'.format(ds['label']))
    print('    version:        {0}'.format(ds['version']))
    print('    starttime:      {0}'.format(ds['starttime']))
    print('    endtime:        {0}'.format(ds['endtime']))
    print('    location:       {0}'.format(ds['location']))
    #print('    master_scenes:  {0}'.format(met['master_scenes']))
    #print('    slave_scenes:   {0}'.format(met['slave_scenes']))
    build_product_dir(ds, met)
    #submit_product(ds)

def build_id(version, product_prefix, aoi, track, orbit, date_pair):
    '''builds the product uid'''
    uid = '{}-{}-T{}-{}-{}'.format(product_prefix, aoi.get('_id', 'AOI'), str(track).zfill(3), date_pair, version)
    return uid

def get_hash(es_obj):
    '''retrieves the full_id_hash. if it doesn't exists, it
        attempts to generate one'''
    full_id_hash = es_obj.get('_source', {}).get('metadata', {}).get('full_id_hash', False)
    if full_id_hash:
        return full_id_hash
    return gen_hash(es_obj)

def gen_hash(es_obj):
    '''copy of hash used in the enumerator'''
    met = es_obj.get('_source', {}).get('metadata', {})
    master_slcs = met.get('master_scenes', met.get('reference_scenes', False))
    slave_slcs = met.get('slave_scenes', met.get('secondary_scenes', False))
    master_ids_str = ""
    slave_ids_str = ""
    for slc in sorted(master_slcs):
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]
        if master_ids_str == "":
            master_ids_str = slc
        else:
            master_ids_str += " "+slc
    for slc in sorted(slave_slcs):
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]
        if slave_ids_str == "":
            slave_ids_str = slc
        else:
            slave_ids_str += " "+slc
    id_hash = hashlib.md5(json.dumps([master_ids_str, slave_ids_str]).encode("utf8")).hexdigest()
    return id_hash

def get_times(ifg_list, minimum=True):
    '''returns the minimum or the maximum start/end time'''
    times = [dateutil.parser.parse(get_secondary_time(x)).replace(tzinfo=pytz.UTC) for x in ifg_list] + [dateutil.parser.parse(get_reference_time(x)).replace(tzinfo=pytz.UTC) for x in ifg_list]
    if minimum:
        time = min(times)
    else:
        time = max(times)
    return time.strftime('%Y-%m-%dT00:00:00.000Z')

def get_secondary_time(obj):
    '''attempts to return proper dates for an object'''
    date = obj.get('_source', {}).get('metadata', {}).get('secondary_date', False)
    if date:
        return date
    # secondary doesn't exist
    date = obj.get('_source', {}).get('metadata', {}).get('sensing_start', False)
    if date:
        return date
    return obj.get('_source', {}).get('starttime', False)

def get_reference_time(obj):
    '''attempts to return proper dates for an object'''
    date = obj.get('_source', {}).get('metadata', {}).get('reference_date', [])
    if date:
        return date
    # secondary doesn't exist
    date = obj.get('_source', {}).get('metadata', {}).get('sensing_stop', False)
    if date:
        return date
    return obj.get('_source', {}).get('endtime', False)

def build_dataset(ifg_list, version, product_prefix, aoi, track, orbit):
    '''Generates the ds dict'''
    starttime = get_times(ifg_list, minimum = True)
    endtime = get_times(ifg_list, minimum = False)
    date_pair = '{}_{}'.format(starttime[:10].replace('-', ''), endtime[:10].replace('-',''))
    if starttime == endtime:
        date_pair = orbit
    uid = build_id(version, product_prefix, aoi, track, orbit, date_pair)
    #print('uid: {}'.format(uid))
    location = get_location(ifg_list)
    location = shape(location)
    #location = get_union_geojson_ifgs(ifg_list)
    print("location : {}".format(location))
    location = change_coordinate_direction(location)
    print("location : {}".format(location))
    ds = {'label':uid, 'starttime':starttime, 'endtime':endtime, 'location':location, 'version':version}
    return ds

def build_met(ifg_list, version, product_prefix, aoi, track, orbit):
    '''Generates the met dict'''
    starttime = get_times(ifg_list, minimum = True)
    endtime = get_times(ifg_list, minimum = False)
    date_pair = '{}_{}'.format(starttime[:10].replace('-', ''), endtime[:10].replace('-',''))    
    gunw_list = [x.get('_id') for x in ifg_list]
    orbits = []
    for x in ifg_list:
        orbits.extend(x.get('_source', {}).get('metadata',{}).get('orbit_number'))
    orbits = list(set(orbits))
    s1_gunw_ids = []
    s1_gunws = []
    s1_gunw_urls = []
    hashes = [get_hash(x) for x in ifg_list]
    for ifg in ifg_list:
        ifg_id = ifg.get('_id')
        s1_gunw_ids.append(ifg_id)
        ifg_met = ifg.get('_source').get('metadata')
        url = ifg.get('_source').get('urls', [])[-1]
        s1_gunw_urls.append(url)
        ctx = ifg_met.get('context', {})
        master_slcs = ifg_met.get('master_scenes')
        slave_slcs = ifg_met.get('slave_scenes')
        input_met = ctx.get('input_metadata', {})
        slave_orbit_file = input_met.get('slave_orbit_file', False)
        master_orbit_file = input_met.get('master_orbit_file', False)
        master_scenes = input_met.get('master_scenes', False)
        slave_scenes = input_met.get('slave_scenes', False)
        dct = {'id': ifg_id, 'master_slcs':master_slcs, 'slave_slcs':slave_slcs, 'master_scenes': master_scenes, 'url': url,
               'slave_scenes':slave_scenes, 'master_orbit_file':master_orbit_file, 'slave_orbit_file': slave_orbit_file}
        s1_gunws.append(dct)
    met = {'track_number': track, 'aoi': aoi.get('_id'), 'date_pair': date_pair, 'orbit': orbits,
           's1-gunw-ids': s1_gunw_ids, 's1-gunws': s1_gunws, 's1-gunw_urls': s1_gunw_urls, 'full_id_hash': hashes}
    return met

def get_location(ifg_list):
    '''generates the union of the ifg_list extent'''
    polygons = []
    for ifg in ifg_list:
        polygons.append(Polygon(ifg['_source']['location']['coordinates'][0]))
    multi = MultiPolygon(polygons)
    return mapping(cascaded_union(multi))

'''
def get_union_geojson_ifgs(ifg_list):
    geoms = list()
    union = None
    for ifg in ifg_list:
        geom = ogr.CreateGeometryFromJson(json.dumps(ifg['_source']['location']))
        geoms.append(geom)
        union = geom if union is None else union.Union(geom)
    union_geojson =  json.loads(union.ExportToJson())
    return union_geojson
'''

def build_product_dir(ds, met):
    label = ds['label']
    ds_dir = os.path.join(os.getcwd(), label)
    ds_path = os.path.join(ds_dir, '{0}.dataset.json'.format(label))
    met_path = os.path.join(ds_dir, '{0}.met.json'.format(label))
    if not os.path.exists(ds_dir):
        os.mkdir(ds_dir)
    with open(ds_path, 'w') as outfile:
        json.dump(ds, outfile)
    with open(met_path, 'w') as outfile:
        json.dump(met, outfile)

def submit_product(ds):
    uid = ds['label']
    ds_dir = os.path.join(os.getcwd(), uid)
    try:
        ingest(uid, './datasets.json', app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE, ds_dir, None)
        if os.path.exists(uid):
            shutil.rmtree(uid)
    except:
        print('failed on submission of {0} with {1}'.format(uid))
