#!/usr/bin/env python

'''
Input are either an AOI or a GUNW/GUNW-merged. For a given input product, determines which GUNWs/GUNW merged
are complete along track over the AOI (or, if a GUNW, any AOIs). If there are complete
products, it tags the product with <aoi_name> tag, and creates an AOI_TRACK product
for all GUNWs along that track/orbit pairing. 
'''

from __future__ import print_function
import re
import json
import hashlib
import requests
import warnings
from hysds.celery import app
import tagger
import build_validated_product

AOI_TRACK_PREFIX = 'S1-GUNW-AOI_TRACK'
AOI_TRACK_VERSION = 'v2.0'
ALLOWED_PROD_TYPES = ['S1-GUNW', "S1-GUNW-MERGED", "area_of_interest"]
INDEX_MAPPING = {'S1-GUNW-acq-list': 'grq_*_s1-gunw-acq-list',
                 'S1-GUNW':'grq_*_s1-gunw',
                 'S1-GUNW-MERGED': 'grq_*_s1-*-merged',
                 'S1-GUNW-acqlist-audit_trail': 'grq_*_s1-gunw-acqlist-audit_trail',
                 'S1-GUNW-completed': 'grq_*_s1-gunw-aoi_track',
                 'S1-GUNW-merged-completed': 'grq_*_s1-gunw-merged-aoi_track',
                 'area_of_interest': 'grq_*_area_of_interest'}

class evaluate():
    '''evaluates input product for completeness. Tags GUNWs/GUNW-merged & publishes AOI_TRACK products'''
    def __init__(self):
        '''fill values from context, error if invalid inputs, then kickoff evaluation'''
        self.ctx = load_context()
        self.prod_type = self.ctx.get('prod_type', False)
        self.track_number = self.ctx.get('track_number', False)
        self.full_id_hash = self.ctx.get('full_id_hash', False)
        self.uid = self.ctx.get('uid', False)
        self.location = self.ctx.get('location', False)
        self.starttime = self.ctx.get('starttime', False)
        self.endtime = self.ctx.get('endtime', False)
        self.version = self.ctx.get('version', False)
        self.orbit_number = self.ctx.get('orbit_number', False)
        # exit if invalid input product type
        if self.prod_type not in ALLOWED_PROD_TYPES:
            raise Exception('input product type: {} not in allowed product types for PGE'.format(self.prod_type))
        if not self.prod_type is 'area_of_interest' and not self.full_id_hash:
            warnings.warn('Warning: full_id_hash not found in metadata. Will attempt to generate')
        if not self.prod_type is 'area_of_interest' and self.track_number is False:
            raise Exception('metadata.track_number not filled. Cannot evaluate.')
        # run evaluation & publishing by job type
        if self.prod_type is 'area_of_interest':
            self.run_aoi_evaluation()
        else:
            self.run_gunw_evaluation()

    def run_aoi_evaluation(self):
        '''runs the evaluation & publishing for an aoi'''
        # retrieve all gunws, & gunw-merged products over an aoi
        s1_gunw = get_objects('S1-GUNW', location=self.location, starttime=self.starttime, endtime=self.endtime)
        s1_gunw_merged = get_objects('S1-GUNW-MERGED', location=self.location, starttime=self.starttime, endtime=self.endtime)
        # get all acq-list products over the aoi
        acq_list = get_objects('S1-GUNW-acqlist-audit_trail', aoi=self.uid)
        # get the full aoi product
        aoi = get_objects('area_of_interest', uid=self.uid, version=self.version)
        for gunw_list in [s1_gunw, s1_gunw_merged]:
            # evaluate to see which products are complete, tagging and publishing complete products
            self.gen_completed(gunw_list, acq_list, aoi)

    def run_gunw_evaluation(self):
        '''runs the evaluation and publishing for a gunw or gunw-merged'''
        # determine which AOI(s) the gunw corresponds to
        all_acq_lists = get_objects('S1-GUNW-acq-list', full_id_hash=self.full_id_hash) 
        acq_by_aoi = sort_by_aoi(all_acq_lists)
        for aoi_id in acq_by_aoi.keys():
            print('Evaluating associated GUNWs over AOI: {}'.format(aoi_id))
            # get all acq-list products
            acq_lists = acq_by_aoi.get(aoi_id, [])
            # get the aoi product
            aois = get_objects('area_of_interest', uid=aoi_id)
            if len(aois) > 1:
                raise Exception('unable to distinguish between multiple AOIs with same uid but different version: {}}'.format(aoi_id))
            if len(aois) == 0:
                warnings.warn('unable to find referenced AOI: {}'.format(aoi_id))
                continue
            aoi = aois[0]
            # get all associated gunw or gunw-merged products
            gunws = get_objects(self.prod_type, track_number=self.track_number, orbit_numbers=self.orbit_number, version=self.version)
            # evaluate to determine which products are complete, tagging & publishing complete products
            self.gen_completed(gunws, acq_lists, aoi)

    def gen_completed(self, gunws, acq_lists, aoi):
        '''determines which gunws (or gunw-merged) products are complete along track & orbit,
        tags and publishes TRACK_AOI products for those that are complete'''
        complete = []
        hashed_gunw_dct = sort_by_hash(gunws)
        for track_list in sort_by_track(acq_lists).items():
            for orbit_list in sort_by_orbit(track_list).items():
                # get all full_id_hashes in the acquisition list
                all_hashes = [get_hash(x) for x in orbit_list]
                # if all of them are in the list of gunw hashes, they are complete
                complete = True
                for full_id_hash in all_hashes:
                    if not hashed_gunw_dct.get(full_id_hash, False):
                        complete = False
                        break
                # they are complete. tag & generate products
                if complete:
                    self.tag_and_publish(orbit_list, aoi)

    def tag_and_publish(self, gunws, aoi):
        '''tags each object in the input list, then publishes an appropriate
           aoi-track product'''
        if len(gunws) < 1:
            return
        for obj in gunws:
            tag = aoi.get('_id')
            uid = obj.get('_id')
            prod_type = obj.get('_type')
            index = obj.get('_index')
            tagger.add_tag(index, uid, prod_type, tag)
        build_validated_product.build(gunws, AOI_TRACK_VERSION, AOI_TRACK_PREFIX, aoi, get_track(gunws[0]), get_orbit(gunws[0]))

def get_objects(prod_type, location=False, starttime=False, endtime=False, full_id_hash=False, track_number=False, orbit_numbers=False, version=False, uid=False, aoi=False):
    '''returns all objects of the object type that intersect both
    temporally and spatially with the aoi'''
    idx = INDEX_MAPPING.get(prod_type) # mapping of the product type to the index
    print_query(prod_type, location=False, starttime=False, endtime=False, full_id_hash=False, track_number=False, orbit_numbers=False, version=False, uid=False, aoi=False)
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, idx)
    filtered = {}
    if location:
        filtered["query"] = {"geo_shape": {"location": {"shape": location}}}
    if starttime or endtime or full_id_hash or track_number or version:
        must = []
        if starttime:
            must.append({"range": {"endtime": {"from": starttime}}})
        if endtime:
            must.append({"range": {"starttime": {"from": endtime}}})
        if full_id_hash:
            must.append({"term": {"metadata.full_id_hash": full_id_hash}})
        if track_number:
            must.append({"term": {"metadata.track_number": full_id_hash}})
        if version:
            must.append({"term": {"version": version}})
        if uid:
            must.append({"term": {"id": uid}})
        if aoi:
            must.append({"term": {"metadata.aoi": aoi}})
        filtered["filter"] = {"bool":{"must":must}}
    grq_query = {"query": {"filtered": filtered}, "from": 0, "size": 1000}
    results = query_es(grq_url, grq_query)
    print('found {} {} products matching query.'.format(len(results), prod_type))
    # if it's an orbit, filter out the bad orbits client-side
    if orbit_numbers:
        orbit_key = stringify_orbit(orbit_numbers)
        results = sort_by_orbit(results).get(orbit_key, [])
    return results

def print_query(prod_type, location=False, starttime=False, endtime=False, full_id_hash=False, track_number=False, orbit_numbers=False, version=False, uid=False, aoi=False):
    '''print statement describing grq query'''
    statement = 'Querying for products of type: {}'.format(prod_type)
    if location:
        statement += '\nwith location:     {}'.format(location)
    if starttime:
        statement += '\nwith starttime:    {}'.format(starttime)
    if endtime:
        statement += '\nwith endtime  :    {}'.format(endtime)
    if full_id_hash:
        statement += '\nwith full_id_hash: {}'.format(full_id_hash)
    if track_number:
        statement += '\nwith track_number: {}'.format(track_number)
    if orbit_numbers:
        statement += '\nwith orbits:       {}'.format(', '.join(orbit_numbers))
    if version:
        statement += '\nwith version:      {}'.format(version)
    if uid:
        statement += '\nwith uid:          {}'.format(uid)
    if uid:
        statement += '\nwith metadata.aoi: {}'.format(aoi)
    print(statement)

def load_context():
    '''loads the context file into a dict'''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse _context.json from work directory')

def query_es(grq_url, es_query):
    '''
    Runs the query through Elasticsearch, iterates until
    all results are generated, & returns the compiled result
    '''
    if 'size' in es_query.keys():
        iterator_size = es_query['size']
    else:
        iterator_size = 10
        es_query['size'] = iterator_size
    if 'from' in es_query.keys():
        from_position = es_query['from']
    else:
        from_position = 0
        es_query['from'] = from_position
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    response.raise_for_status()
    results = json.loads(response.text, encoding='ascii')
    results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    for i in range(iterator_size, total_count, iterator_size):
        es_query['from'] = i
        response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
        response.raise_for_status()
        results = json.loads(response.text, encoding='ascii')
        results_list.extend(results.get('hits', {}).get('hits', []))
    return results_list

def sort_by_orbit(es_result_list):
    '''
    Goes through the objects in the result list, and places them in an dict where key is orbit
    '''
    sorted_dict = {}
    for result in es_result_list:
        orbit = get_orbit(result)
        if orbit in sorted_dict.keys():
            sorted_dict.get(orbit, []).append(result)
        else:
            sorted_dict[orbit] = [result]
    return sorted_dict

def sort_by_hash(es_results_list):
    '''
    Goes through the objects in the result list, and places them in an dict where key is full_id_hash (or generated version of hash)
    '''
    sorted_dict = {}
    for result in es_results_list:
        idhash = get_hash(result)
        if idhash in sorted_dict.keys():
            sorted_dict.get(idhash, []).append(result)
        else:
            sorted_dict[idhash] = [result]
    return sorted_dict

def sort_by_track(es_result_list):
    '''
    Goes through the objects in the result list, and places them in an dict where key is track
    '''
    #print('found {} results'.format(len(es_result_list)))
    sorted_dict = {}
    for result in es_result_list:
        track = get_track(result)
        if track in sorted_dict.keys():
            sorted_dict.get(track, []).append(result)
        else:
            sorted_dict[track] = [result]
    return sorted_dict

def sort_by_aoi(es_result_list):
    '''
    Goes through the objects in the result list, and places them in an dict where key is aoi_id
    '''
    #print('found {} results'.format(len(es_result_list)))
    sorted_dict = {}
    for result in es_result_list:
        aoi_id = result.get('_source', {}).get('metadata', {}).get('aoi', False)
        if not aoi_id:
            continue
        if aoi_id in sorted_dict.keys():
            sorted_dict.get(aoi_id, []).append(result)
        else:
            sorted_dict[aoi_id] = [result]
    return sorted_dict

def get_track(es_obj):
    '''returns the track from the elasticsearch object'''
    es_ds = es_obj.get('_source', {})
    #iterate through ds
    track_met_options = ['track_number', 'track', 'trackNumber', 'track_Number']
    for tkey in track_met_options:
        track = es_ds.get(tkey, False)
        if track:
            return track
    #if that doesn't work try metadata
    es_met = es_ds.get('metadata', {})
    for tkey in track_met_options:
        track = es_met.get(tkey, False)
        if track:
            return track
    raise Exception('unable to find track for: {}'.format(es_obj.get('_id', '')))

def get_orbit(es_obj):
    '''returns the orbit as a string from the elasticsearch object'''
    es_ds = es_obj.get('_source', {})
    #iterate through ds
    options = ['orbit', 'orbitNumber', 'orbit_number']
    for tkey in options:
        orbit = es_ds.get(tkey, False)
        if orbit:
            return stringify_orbit(orbit)
    #if that doesn't work try metadata
    es_met = es_ds.get('metadata', {})
    for tkey in options:
        orbit = es_met.get(tkey, False)
        if orbit:
            return stringify_orbit(orbit)
    raise Exception('unable to find orbit for: {}'.format(es_obj.get('_id', '')))

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
    master_ids_str=""
    slave_ids_str=""
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

def stringify_orbit(orbit_list):
    '''converts the list into a string'''
    return '_'.join([str(x).zfill(3) for x in sorted(orbit_list)])

def get_version(es_obj):
    '''returns the version of the index. Since we are ignoring the subversions, only returns the main version.
    eg, v2.0.1 returns v2.0'''
    match = re.search(r'^([v]*?)([0-9])*\.([0-9])*[\.]{0,1}([0-9]){0,1}', es_obj.get('_source', {}).get('version', False))
    version = '{}{}.{}'.format(match.group(1), match.group(2), match.group(3))
    return version

if __name__ == '__main__':
    evaluate()
