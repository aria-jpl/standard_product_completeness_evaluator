#!/usr/bin/env python

'''
Builds s1-gunw-aoi-track and s1-gunw-aoi-track-merged products
'''

from __future__ import print_function
import os
import json
import shutil
import pickle
import hashlib
import dateutil.parser
from shapely.geometry import Polygon, MultiPolygon, mapping
from shapely.ops import cascaded_union
from hysds.celery import app
from hysds.dataset_ingest import ingest


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

def gen_hash(es_object):
    '''Generates a hash from the master and slave scene list'''
    master = pickle.dumps(sorted(es_object['_source']['metadata']['master_scenes']))
    slave = pickle.dumps(sorted(es_object['_source']['metadata']['slave_scenes']))
    return '{}_{}'.format(hashlib.md5(master).hexdigest(), hashlib.md5(slave).hexdigest())

def get_times(ifg_list, minimum=True):
    '''returns the minimum or the maximum start/end time'''
    times = [dateutil.parser.parse(x.get('_source').get('metadata').get('secondary_date')) for x in ifg_list] + [dateutil.parser.parse(x.get('_source').get('metadata').get('reference_date')) for x in ifg_list]
    if minimum:
        time = min(times)
    else:
        time = max(times)
    return time.strftime('%Y-%m-%dT00:00:00.000Z')

def build_dataset(ifg_list, version, product_prefix, aoi, track, orbit):
    '''Generates the ds dict'''
    starttime = get_times(ifg_list, minimum = True)
    endtime = get_times(ifg_list, minimum = False)
    date_pair = '{}_{}'.format(starttime[:10].replace('-', ''), endtime[:10].replace('-',''))    
    uid = build_id(version, product_prefix, aoi, track, orbit, date_pair)
    print('uid: {}'.format(uid))
    location = get_location(ifg_list)
    ds = {'label':uid, 'starttime':starttime, 'endtime':endtime, 'location':location, 'version':version}
    return ds

def build_met(ifg_list, version, product_prefix, aoi, track, orbit):
    '''Generates the met dict'''
    starttime = get_times(ifg_list, minimum = True)
    endtime = get_times(ifg_list, minimum = False)
    date_pair = '{}_{}'.format(starttime[:10].replace('-', ''), endtime[:10].replace('-',''))    
    gunw_list = [x.get('_id') for x in ifg_list]
    orbits = set()
    for x in ifg_list:    
        orbits.update(x.get('_source').get('metadata').get('orbit_number'))
    orbits = list(orbits)
    s1_gunw_ids = []
    s1_gunws = []
    for ifg in ifg_list:
        ifg_id = ifg.get('_id')
        s1_gunw_ids.append(ifg_id)
        ifg_met = ifg.get('_source').get('metadata')
        ctx = ifg_met.get('context', {})
        master_slcs = ifg_met.get('master_scenes')
        slave_slcs = ifg_met.get('slave_scenes')
        input_met = ctx.get('input_metadata', {})
        slave_orbit_file = input_met.get('slave_orbit_file', False)
        master_orbit_file = input_met.get('master_orbit_file', False)
        master_scenes = input_met.get('master_scenes', False)
        slave_scenes = input_met.get('slave_scenes', False)
        dct = {'id': ifg_id, 'master_slcs':master_slcs, 'slave_slcs':slave_slcs, 'master_scenes': master_scenes,
               'slave_scenes':slave_scenes, 'master_orbit_file':master_orbit_file, 'slave_orbit_file': slave_orbit_file}
        s1_gunws.append(dct)
    met = {'track_number': track, 'aoi': aoi.get('_id'), 'date_pair': date_pair, 'orbit': orbits, 's1-gunw-ids': s1_gunw_ids, 's1-gunws': s1_gunws}
    return met

def get_location(ifg_list):
    '''generates the union of the ifg_list extent'''
    polygons = []
    for ifg in ifg_list:
        polygons.append(Polygon(ifg['_source']['location']['coordinates'][0]))
    multi = MultiPolygon(polygons)
    return mapping(cascaded_union(multi))

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
    except Exception, err:
        print('failed on submission of {0} with {1}'.format(uid, err))
