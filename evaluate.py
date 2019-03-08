#!/usr/bin/env python

'''
For a given input AOI and optional track, looks for completed s1-gunw date pairs
 along track. If all s1-gunws are complete, it publishes a validated product.
'''

from __future__ import print_function
import re
import json
import pickle
import hashlib
import requests
from hysds.celery import app
import build_validated_product

def main():
    '''
    Main Loop
    '''
    ctx = load_context()
    track_number = ctx.get('track_number', False)
    if track_number is not False:
        try:
            track_number = int(track_number)
        except:
            track_number = False
    aoi_index = ctx.get('aoi_index', False)
    if not aoi_index:
        aoi_index = 'grq_*_area_of_interest'
    aoi_id = ctx['aoi_id']
    # get the metadata for the aoi
    aoi = get_aoi(aoi_id, aoi_index)

    # get all acq-lists that are covered by the aoi & track
    acq_list_dct = sort_by_track(get_objects('acq-list', aoi, track_number))

    # get all s1-gunw products covered
    ifg_dct = sort_by_track_and_version(get_objects('ifg', aoi, track_number))

    # get all s1-gunw merged products covered
    ifg_merged_dct = sort_by_track_and_version(get_objects('ifg-merged', aoi, track_number))
    
    # get all s1-gunw-completed products covered
    ifg_completed_dct = sort_by_track_and_version(get_objects('ifg-completed', aoi, track_number))

    # get all s1-gunw-merged completed products covered
    ifg_merged_completed_dct = sort_by_track_and_version(get_objects('ifg-merged-completed', aoi, track_number))

    # get a list of all possible tracks
    all_tracks = get_all_tracks([ifg_dct, ifg_merged_dct])
    print('all tracks: {}'.format(all_tracks))    

    for track in all_tracks:
        print('for track {}'.format(track))
        acq_list = acq_list_dct.get(track, [])
        print('found {} acquisition lists'.format(len(acq_list)))
        acq_dct = sort_by_orbit(acq_list) #sorts the acquisition list by orbit pair
        ifgs = ifg_dct.get(track, {})
        ifg_merged = ifg_merged_dct.get(track, {})
        ifg_completed = ifg_completed_dct.get(track, {})
        ifg_merged_completed = ifg_merged_completed_dct.get(track, {})

        print('total acqs: {}'.format(len(acq_list)))
        # for each version
        versions = ifgs.keys()
        print('versions found for track {}: {}'.format(track, versions))
        for version in versions:
            print('for version {}...'.format(version))
            print('ifgs: {}'.format(len(ifgs.get(version, []))))
            # determine which s1-gunws are complete and are not published as completed
            completed_gunws = determine_complete_orbits(acq_dct, ifgs.get(version, []), ifg_completed.get(version, []))
            print('found {} completed ifgs'.format(len(completed_gunws)))
            # determine which s1-gunw-merged are complete and not published as completed
            completed_merged_gunws = determine_complete_orbits(acq_dct, ifg_merged.get(version, []), ifg_merged_completed.get(version, []))

            # for each complete s1-gunw orbit, generate a product
            for orbit in completed_gunws.keys():

                build_validated_product.build(completed_gunws.get(orbit), version, 'S1-GUNW-AOI_TRACK', aoi, track, orbit)

            # for each complete s1-gunw-merged, generate a product
            for orbit in completed_merged_gunws.keys():
                build_validated_product.build(completed_merged_gunws.get(orbit), version, 'S1-GUNW-MERGED-AOI_TRACK', aoi, track, orbit)

def determine_complete_orbits(acq_list_dict, gunw_list, completed_list):
    '''determine which orbit date pair gunw objects are complete, and not in the completed list. Returns those gunw objects as a dict of lists where the key is orbit pair'''
    completed_gunws = {}
    orbit_gunw_list = sort_by_orbit(gunw_list)
    completed_hashed_dict = build_hashed_dict(completed_list)
    print('found {} hashes from the completed s1-gunw dict'.format(len(completed_hashed_dict)))
    for orbit in orbit_gunw_list.keys():
        print('for orbit {}...'.format(orbit))
        acq_list_objects = acq_list_dict.get(orbit, []) # get the acquisition lists for the given orbit pair
        print('found {} acq-lists for orbit {}'.format(len(acq_list_objects), orbit))
        acq_list_hashed_dict = build_hashed_dict(acq_list_objects) # generates a hashed dict of the naster/slave set for the given orbit pair
        gunw_list = orbit_gunw_list.get(orbit, [])
        print('found {} gunw products for orbit {}'.format(len(gunw_list), orbit))
        gunw_hashed_dict = build_hashed_dict(gunw_list) # generates a hashed dict of the known ifgs
        # determine if the gunws are complete
        complete = True
        for key in acq_list_hashed_dict.keys():
            if gunw_hashed_dict.get(key, False) is False:
                print('acquisition: {} is missing.'.format(acq_list_hashed_dict[key]['_id']))
                complete = False
        
        if complete is True:
            print('orbit {} is complete.'.format(orbit))
            for key in acq_list_hashed_dict.keys():
                if completed_hashed_dict.get(key, False) is False: # pass over any objects that have already been published
                    if completed_gunws.get(orbit, False) is False:
                        completed_gunws[orbit] = [gunw_hashed_dict.get(key, False)]
                    else:
                        completed_gunws[orbit].append(gunw_hashed_dict.get(key, False))
        else:
            print('orbit {} is incomplete'.format(orbit))
        print('all completed gunws: {}'.format(len(completed_gunws)))
    return completed_gunws


def build_hashed_dict(object_list):
    '''
    Builds a dict of the object list where the keys are a hashed object of each objects
    master and slave list. Returns the dict.
    '''
    hashed_dict = {}
    for obj in object_list:
        hashed_dict.update({gen_hash(obj):obj})
    return hashed_dict

def gen_hash(es_object):
    '''Generates a hash from the master and slave scene list'''
    master = get_master_slave_scenes(es_object, master=True)
    slave = get_master_slave_scenes(es_object, master=False)
    master = pickle.dumps(sorted(master))
    slave = pickle.dumps(sorted(slave))
    hsh = '{}_{}'.format(hashlib.md5(master).hexdigest(), hashlib.md5(slave).hexdigest())
    #print('{} hash : {}'.format(es_object.get('_id'), hsh))
    return hsh


def get_master_slave_scenes(es_object, master=True):
    '''gets the master/slave acquisition id list from the es object, returns the list'''
    key = 'slave_scenes'
    if master:
        key = 'master_scenes'
    met = es_object.get('_source', {}).get('metadata', {})
    lst = met.get(key, False)
    if is_acq(lst):
        return lst
    lst = met.get('context', {}).get('input_metadata', {}).get(key, False)
    if is_acq(lst):
        return lst
    print('unable to find master/slave scenes from {}'.format(es_object.get('_id')))
    return []

def is_acq(obj):
    '''returns True/False if the object contains acquisitions'''
    if not isinstance(obj, list):
       return False
    for element in obj:
        if not element.startswith('acquisition'):
            return False
    return True


def get_all_tracks(obj_dcts_list):
    '''returns all possible tracks from object list containing keys by track'''
    all_tracks = []
    for obj in obj_dcts_list:
        tracks = obj.keys()
        all_tracks = list(set(all_tracks + tracks))
    return all_tracks

def get_aoi(aoi_id, aoi_index):
    '''
    retrieves the AOI from ES
    '''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, aoi_index)
    es_query = {"query":{"bool":{"must":[{"term":{"id.raw":aoi_id}}]}}}
    result = query_es(grq_url, es_query)
    if len(result) < 1:
        raise Exception('Found no results for AOI: {}'.format(aoi_id))
    return result[0]

def get_objects(object_type, aoi, track_number):
    '''returns all objects of the object type that intersect both
    temporally and spatially with the aoi'''
    #determine index
    idx_dct = {'acq-list': 'grq_*_acq-list', 'ifg':'grq_*_s1-gunw', 'ifg-cfg':'grq_*_s1-gunw-ifg-cfg', 'ifg-merged': 'grq_*_s1-gunw-merged', 'ifg-completed': 'grq_*_s1-gunw-aoi-track', 'ifg-merged-completed': 'grq_*_s1-gunw-merged-aoi-track'}
    idx = idx_dct.get(object_type)
    starttime = aoi.get('_source', {}).get('starttime')
    endtime = aoi.get('_source', {}).get('endtime')
    location = aoi.get('_source', {}).get('location')
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, idx)
    if track_number is not False:
        grq_query = {"query":{"filtered":{"query":{"geo_shape":{"location": {"shape":location}}},"filter":{"bool":{"must":[{"term":{"metadata.track_number":track_number}},{"range":{"endtime":{"from":starttime}}},{"range":{"starttime":{"to":endtime}}}]}}}},"from":0,"size":1000}
    else:
        grq_query = {"query":{"filtered":{"query":{"geo_shape":{"location": {"shape":location}}},"filter":{"bool":{"must":[{"range":{"endtime":{"from":starttime}}},{"range":{"starttime":{"to":endtime}}}]}}}},"from":0,"size":1000}
    results = query_es(grq_url, grq_query)
    print('found {} {} products'.format(len(results), object_type))
    return results

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
        orbit = '_'.join([str(x) for x in get_orbit(result)])
        if orbit in sorted_dict.keys():
            sorted_dict.get(orbit, []).append(result)
        else:
            sorted_dict[orbit] = [result]
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

def sort_by_track_and_version(es_result_list):
    '''
    Goes through the objects in the result list, and places them in a dict where keys are track, then version
    '''
    print('found {} results'.format(len(es_result_list)))
    sorted_dict = {}
    #get all versions
    #versions = list(set([get_version(x) for x in es_result_list]))
    tracks = list(set([get_track(x) for x in es_result_list]))
    for track in tracks:
        sorted_dict[track] = {}
    for result in es_result_list:
        track = get_track(result)
        vers = get_version(result)
        if vers in sorted_dict[track].keys():
            sorted_dict[track].get(vers, []).append(result)
        else:
            sorted_dict[track][vers] = [result]
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
    '''returns the orbit from the elasticsearch object'''
    es_ds = es_obj.get('_source', {})
    #iterate through ds
    track_met_options = ['orbit', 'orbitNumber', 'orbit_number']
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
    raise Exception('unable to find orbit for: {}'.format(es_obj.get('_id', '')))

def get_version(es_obj):
    '''returns the version of the index. Since we are ignoring the subversions, only returns the main version.
    eg, v2.0.1 returns v2.0'''
    match = re.search(r'^([v]*?)([0-9])*\.([0-9])*[\.]{0,1}([0-9]){0,1}', es_obj.get('_source', {}).get('version', False))
    version = '{}{}.{}'.format(match.group(1), match.group(2), match.group(3))
    return version

if __name__ == '__main__':
    main()
