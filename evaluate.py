#!/usr/bin/env python

'''
Input are either an AOI or a GUNW/GUNW-merged. For a given input product, determines which GUNWs/GUNW merged
are complete along track over the AOI (or, if a GUNW, any AOIs). If there are complete
products, it tags the product with <aoi_name> tag, and creates an AOI_TRACK product
for all GUNWs along that track/orbit pairing.
'''

from __future__ import print_function
from builtins import str
from builtins import range
from builtins import object
import re, sys, os
import json
import hashlib
import urllib3
import requests
import warnings
import dateutil
import dateutil.parser
from hysds.celery import app
import tagger
import traceback
import build_validated_product

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

AOI_TRACK_PREFIX = 'S1-GUNW-AOI_TRACK'
AOI_TRACK_MERGED_PREFIX = 'S1-GUNW-MERGED-AOI_TRACK'
AOI_TRACK_VERSION = 'v2.0'
S1_GUNW_VERSION = "v2.0.2"
S1_GUNW_MERGED_VERSION = "v2.0.2"

ALLOWED_PROD_TYPES = ['S1-GUNW', "S1-GUNW-MERGED", "area_of_interest", "S1-GUNW-GREYLIST"]
INDEX_MAPPING = {'S1-GUNW-acq-list': 'grq_*_s1-gunw-acq-list',
                 'S1-GUNW':'grq_*_s1-gunw',
                 'S1-GUNW-MERGED': 'grq_*_s1-*-merged',
                 'S1-GUNW-acqlist-audit_trail': 'grq_*_s1-gunw-acqlist-audit_trail',
                 'S1-GUNW-AOI_TRACK': 'grq_*_s1-gunw-aoi_track',
                 'S1-GUNW-MERGED-AOI_TRACK': 'grq_*_s1-gunw-merged-aoi_track',
                 'S1-GUNW-GREYLIST': 'grq_*_s1-gunw-greylist',
                 'area_of_interest': 'grq_*_area_of_interest'}

class evaluate(object):
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
        self.s1_gunw_version = self.ctx.get("S1-GUNW-version", S1_GUNW_VERSION)
        self.s1_gunw_merged_version = self.ctx.get("S1-GUNW-MERGED-version", S1_GUNW_MERGED_VERSION)

        # exit if invalid input product type
        if not self.prod_type in ALLOWED_PROD_TYPES:
            raise Exception('input product type: {} not in allowed product types for PGE'.format(self.prod_type))
        if not self.prod_type == 'area_of_interest' and not self.full_id_hash:
            warnings.warn('Warning: full_id_hash not found in metadata. Will attempt to generate')
        #if not self.prod_type is 'area_of_interest' and self.track_number is False:
        #    raise Exception('metadata.track_number not filled. Cannot evaluate.')
        # run evaluation & publishing by job type
        if self.prod_type == 'area_of_interest':
            self.run_aoi_evaluation()
        elif self.prod_type == 'S1-GUNW-GREYLIST':
            self.run_greylist_evaluation()
        else:
            self.run_gunw_evaluation()

    def run_aoi_evaluation(self):
        '''runs the evaluation & publishing for an aoi'''
        # get all audit_trail products over the aoi
        audit_trail_list = get_objects('S1-GUNW-acqlist-audit_trail', aoi=self.uid)
        # determine all full_id_hashes from all audit_trail products
        full_id_hashes = list(sort_by_hash(audit_trail_list).keys())
        # retrieve associated gunws from the full_id_hash list
        s1_gunw = filter_hashes(get_objects('S1-GUNW', location=self.location, starttime=self.starttime, endtime=self.endtime), full_id_hashes)
        s1_gunw_merged = filter_hashes(get_objects('S1-GUNW-MERGED', location=self.location, starttime=self.starttime, endtime=self.endtime), full_id_hashes)
        # get all greylist hashes
        greylist_hashes = list(sort_by_hash(get_objects('S1-GUNW-GREYLIST', location=self.location)).keys()) 
        # get the full aoi product
        aois = get_objects('area_of_interest', uid=self.uid, version=self.version)
        if len(aois) > 1:
            raise Exception('unable to distinguish between multiple AOIs with same uid but different version: {}}'.format(self.uid))
        if len(aois) == 0:
            raise Exception('unable to find referenced AOI: {}'.format(self.uid))
        aoi = aois[0]
        # get the matching acquisition list products
        acq_list = self.get_matching_acq_lists(aoi, audit_trail_list, greylist_hashes)
        for gunw_list in [s1_gunw, s1_gunw_merged]:
            # evaluate to see which products are complete, tagging and publishing complete products
            self.gen_completed(gunw_list, acq_list, aoi)

    def run_greylist_evaluation(self):
        '''runs the evaluation and publishing for a greylist'''
        # fill the hash if it doesn't exist
        if self.full_id_hash is False:
            print('attempting to fill hash for submitted product...')
            self.full_id_hash = gen_hash(get_objects(self.prod_type, uid=self.uid)[0])
            print('Found hash {}'.format(self.full_id_hash))
        # get all the greylists
        greylist_hashes = list(sort_by_hash(get_objects('S1-GUNW-GREYLIST')).keys())
        # determine which AOI(s) the gunw corresponds to
        all_audit_trail = get_objects('S1-GUNW-acqlist-audit_trail', full_id_hash=self.full_id_hash)
        audit_by_aoi = sort_by_aoi(all_audit_trail)
        for aoi_id in list(audit_by_aoi.keys()):
            print('Evaluating associated GUNWs over AOI: {}'.format(aoi_id))
            aois = get_objects('area_of_interest', uid=aoi_id)
            if len(aois) > 1:
                raise Exception('unable to distinguish between multiple AOIs with same uid but different version: {}}'.format(aoi_id))
            if len(aois) == 0:
                warnings.warn('unable to find referenced AOI: {}'.format(aoi_id))
                continue
            aoi = aois[0]
            # get all audit-trail products that match orbit and track
            matching_audit_trail_list = get_objects('S1-GUNW-acqlist-audit_trail', track_number=self.track_number, aoi=aoi_id)
            print('Found {} audit trail products matching track: {}'.format(len(matching_audit_trail_list), self.track_number))
            if len(matching_audit_trail_list) < 1:
                continue
            #get all acq-list products that match the audit trail
            acq_lists = self.get_matching_acq_lists(aoi, matching_audit_trail_list, greylist_hashes)
            if len(acq_lists) < 1:
                print('Found {} acq-lists.'.format(len(acq_lists)))
                continue
            #filter invalid orbits
            acq_lists = sort_by_orbit(acq_lists).get(stringify_orbit(self.orbit_number))
            # get all associated gunw or gunw-merged products
            gunws = get_objects('S1-GUNW', track_number=self.track_number, orbit_numbers=self.orbit_number, version=self.s1_gunw_version)
            if len(gunws) < 1:
                print("No S1-GUNW FOUND for track_number={}, orbit_numbers={}, s1-gunw-version={}".format(self.track_number, self.orbit_number, self.s1_gunw_version))
            else:
                # evaluate to determine which products are complete, tagging & publishing complete products
                self.gen_completed(gunws, acq_lists, aoi)

            gunws_merged = get_objects('S1-GUNW-MERGED', track_number=self.track_number, orbit_numbers=self.orbit_number, version=self.s1_gunw_merged_version)
            if len(gunws_merged) < 1:
                print("No S1-GUNW-MERGED FOUND for track_number={}, orbit_numbers={}, s1-gunw-version={}".format(self.track_number, self.orbit_number, self.s1_gunw_merged_version))
            else:
                # evaluate to determine which products are complete, tagging & publishing complete products
                self.gen_completed(gunws_merged, acq_lists, aoi)

    def run_gunw_evaluation(self):
        '''runs the evaluation and publishing for a gunw or gunw-merged'''
        # fill the hash if it doesn't exist
        if self.full_id_hash is False:
            print('attempting to fill hash for submitted product...')
            self.full_id_hash = gen_hash(get_objects(self.prod_type, uid=self.uid)[0])
            print('Found hash {}'.format(self.full_id_hash))
        # get all the greylists
        greylist_hashes = list(sort_by_hash(get_objects('S1-GUNW-GREYLIST')).keys())
        # determine which AOI(s) the gunw corresponds to
        all_audit_trail = get_objects('S1-GUNW-acqlist-audit_trail', full_id_hash=self.full_id_hash)
        audit_by_aoi = sort_by_aoi(all_audit_trail)
        for aoi_id in list(audit_by_aoi.keys()):
            print('Evaluating associated GUNWs over AOI: {}'.format(aoi_id))
            aois = get_objects('area_of_interest', uid=aoi_id)
            if len(aois) > 1:
                raise Exception('unable to distinguish between multiple AOIs with same uid but different version: {}}'.format(aoi_id))
            if len(aois) == 0:
                warnings.warn('unable to find referenced AOI: {}'.format(aoi_id))
                continue
            aoi = aois[0]
            # get all audit-trail products that match orbit and track
            matching_audit_trail_list = get_objects('S1-GUNW-acqlist-audit_trail', track_number=self.track_number, aoi=aoi_id)
            print('Found {} audit trail products matching track: {}'.format(len(matching_audit_trail_list), self.track_number))
            if len(matching_audit_trail_list) < 1:
                continue
            #get all acq-list products that match the audit trail
            acq_lists = self.get_matching_acq_lists(aoi, matching_audit_trail_list, greylist_hashes)
            if len(acq_lists) < 1:
                print('Found {} acq-lists.'.format(len(acq_lists)))
                continue
            #filter invalid orbits
            print("self.orbit_number : {}".format(self.orbit_number))
            acq_lists = sort_by_orbit(acq_lists).get(stringify_orbit(self.orbit_number))
            # get all associated gunw or gunw-merged products
            gunws = get_objects(self.prod_type, track_number=self.track_number, orbit_numbers=self.orbit_number, version=self.version)
            # evaluate to determine which products are complete, tagging & publishing complete products
            completed = self.gen_completed(gunws, acq_lists, aoi)
            if not completed:
                raise RuntimeError("Not Completed : {}".format(self.uid))

    def gen_completed(self, gunws, acq_lists, aoi):
        '''determines which gunws (or gunw-merged) products are complete along track & orbit,
        tags and publishes TRACK_AOI products for those that are complete'''
        complete = []
        hashed_acq_dct = sort_duplicates_by_hash(acq_lists)
        hashed_gunw_dct = sort_duplicates_by_hash(gunws) # iterates through the list & removes older gunws with duplicate full_id_hash
        track_dct = sort_by_track(acq_lists)
        for track in list(track_dct.keys()):
            track_list = track_dct.get(track, [])
            orbit_dct = sort_by_orbit(track_list)
            for orbit in list(orbit_dct.keys()):
                print('------------------------------')
                orbit_list = orbit_dct.get(orbit, [])
                print('Found {} ACQ-lists over aoi: {} & track: {} & orbit: {}'.format(len(orbit_list), aoi.get('_source').get('id'), track, orbit))
                print('Evaluating GUNWs and Acquisitions over track: {} and orbit: {}'.format(track, orbit))
                # get all full_id_hashes in the acquisition list
                all_hashes = [get_hash(x) for x in orbit_list]
                print('all relevant ids over AOI: {}'.format(', '.join([x.get('_source').get('id') for x in orbit_list])))
                print('all relevant hashes over AOI: {}'.format(', '.join(all_hashes)))
                # if all of them are in the list of gunw hashes, they are complete
                complete = True
                complete_acq_lists = []
                incomplete_acq_lists = []
                missing_hashes = []
                for full_id_hash in all_hashes:
                    if hashed_gunw_dct.get(full_id_hash, False) is False:
                        complete = False
                        missing_hashes.append(full_id_hash)
                        print('hash: {} is missing... products are incomplete.'.format(full_id_hash))
                        incomplete_acq_lists.append(hashed_acq_dct.get(full_id_hash))
                    else:
                        complete_acq_lists.append(hashed_acq_dct.get(full_id_hash))
                print('found {} complete and {} missing hashes.'.format(len(complete_acq_lists), len(incomplete_acq_lists)))
                if not complete:
                    print('missing hashes: {}'.format(', '.join(missing_hashes)))
                # tag acq-lists if iterating over gunws (not gunw merged')
                if gunws[0].get('_type', False) == 'S1-GUNW':
                    print('tagging acq-lists appropriately')
                    for obj in complete_acq_lists:
                        tags = obj.get('_source', {}).get('metadata', {}).get('tags', [])
                        uid = obj.get('_source', {}).get('id', False)
                        if 'gunw_missing' in tags:
                            print('removing tag: "gunw_missing" from: {}'.format(uid))
                            self.remove_obj_tag(obj, 'gunw_missing')
                        if not 'gunw_generated' in tags:
                            print('adding tag: "gunw_generated" to: {}'.format(uid))
                            self.tag_obj(obj, 'gunw_generated')
                    for obj in incomplete_acq_lists:
                        tags = obj.get('_source', {}).get('metadata', {}).get('tags', [])
                        uid = obj.get('_source', {}).get('id', False)
                        if 'gunw_generated' in tags:
                            print('removing tag: "gunw_generated" from: {}'.format(uid))
                            self.remove_obj_tag(obj, 'gunw_generated')
                        if not 'gunw_missing' in tags:
                            print('adding tag: "gunw_missing" to: {}'.format(uid))
                            self.tag_obj(obj, 'gunw_missing')
                # they are complete. tag & generate products
                if complete:
                    gunw_list = []
                    for hsh in all_hashes:
                        gunw_list.append(hashed_gunw_dct.get(hsh))
                    print('found {} products complete over aoi: {} for track: {} and orbit: {}'.format(len(gunw_list), aoi.get('_id'), track, orbit))
                    self.tag_and_publish(gunw_list, aoi)
                    return True
                else:
                    return False

    def tag_and_publish(self, gunws, aoi):
        '''tags each object in the input list, then publishes an appropriate
           aoi-track product'''
        if len(gunws) < 1:
            return
        if self.aoi_track_is_published(gunws, aoi.get('_source').get('id')):
            print('AOI_TRACK product is already published... skipping.')
            return
        print('AOI_TRACK product has not been published. Publishing product...')
        for obj in gunws:
            tag = aoi.get('_source').get('id')
            self.tag_obj(obj, tag)
        prefix = AOI_TRACK_PREFIX
        if gunws[0].get('_type') == 'S1-GUNW-MERGED':
            prefix = AOI_TRACK_MERGED_PREFIX
        build_validated_product.build(gunws, AOI_TRACK_VERSION, prefix, aoi, get_track(gunws[0]), get_orbit(gunws[0]))

    def tag_obj(self, obj, tag):
        '''tags the object with the given tag'''
        uid = obj.get('_source').get('id')
        prod_type = obj.get('_type')
        index = obj.get('_index')
        tagger.add_tag(index, uid, prod_type, tag)

    def remove_obj_tag(self, obj, tag):
        '''removes the tag from the given object'''
        uid = obj.get('_source').get('id')
        prod_type = obj.get('_type')
        index = obj.get('_index')
        tagger.remove_tag(index, uid, prod_type, tag)

    def get_matching_acq_lists(self, aoi, audit_trail_list, greylist_hashes):
        '''returns all acquisition lists matching the audit trail products under the given aoi'''
        aoi_met = aoi.get('_source', {}).get('metadata', {})
        start = aoi_met.get('starttime', False)
        end = aoi_met.get('endtime', False)
        location = aoi.get('_source', {}).get('location', False)
        audit_dct = sort_by_hash(audit_trail_list)
        matching = []
        all_acq_lists = get_objects('S1-GUNW-acq-list', starttime=start, endtime=end, location=location)
        for acq_list in all_acq_lists:
            hsh = get_hash(acq_list)
            if audit_dct.get(hsh, False) and hsh not in greylist_hashes:
                matching.append(acq_list)
        return matching

    def aoi_track_is_published(self, gunws, aoi_id):
        '''determines if the aoi_track product is published already. Returns True/False'''
        gunw = gunws[-1]
        gunw_type = gunw.get('_type')
        prod_type = 'S1-GUNW-AOI_TRACK'
        if gunw_type == 'S1-GUNW-MERGED':
            prod_type = 'S1-GUNW-MERGED-AOI_TRACK'
        matches = get_objects(prod_type, track_number=get_track(gunw), orbit_numbers=gunw.get('_source').get('metadata').get('orbit_number'), aoi=aoi_id)
        if matches:
            return True
        return False

def get_objects(prod_type, location=False, starttime=False, endtime=False, full_id_hash=False, track_number=False, orbit_numbers=False, version=False, uid=False, aoi=False):
    '''returns all objects of the object type that intersect both
    temporally and spatially with the aoi'''
    idx = INDEX_MAPPING.get(prod_type) # mapping of the product type to the index
    print_query(prod_type, location, starttime, endtime, full_id_hash, track_number, orbit_numbers, version, uid, aoi)
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, idx)
    filtered = {}
    must = []
    if location:
        filtered["query"] = {"geo_shape": {"location": {"shape": location}}}
    if starttime or endtime or full_id_hash or track_number or version or uid or aoi:
        must = []
        if starttime:
            must.append({"range": {"endtime": {"from": starttime}}})
        if endtime:
            must.append({"range": {"starttime": {"to": endtime}}})
        if full_id_hash:
            if isinstance(full_id_hash, list):
                must.append({"terms": {"metadata.full_id_hash.raw": full_id_hash}})
            else: 
                must.append({"term": {"metadata.full_id_hash.raw": full_id_hash}})
        if track_number:
            must.append({"term": {"metadata.track_number": track_number}})
        if version:
            must.append({"term": {"version.raw": version}})
        if uid:
            must.append({"term": {"id.raw": uid}})
        if orbit_numbers:
            #determine the proper field type & append all orbits
            orbit_term = resolve_orbit_field(prod_type)
            if isinstance(orbit_term, list):
                # reference/secondary orbits which need to be specified separately
                must.append({"term":{"metadata.{}".format(orbit_term[0]): sorted(orbit_numbers)[0]}})
                must.append({"term":{"metadata.{}".format(orbit_term[1]): sorted(orbit_numbers)[1]}})
            else:
                for orbit in orbit_numbers:
                    must.append({"term":{"metadata.{}".format(orbit_term): orbit}})
        if aoi:
            must.append({"term": {"metadata.aoi.raw": aoi}})
        filtered["filter"] = {"bool":{"must":must}}
    if location:
        grq_query = {"query": {"filtered": filtered}, "from": 0, "size": 1000}
    else:
        grq_query = {"query": {"bool":{"must": must}}}
    #print(grq_url)
    #print(grq_query)
    results = query_es(grq_url, grq_query)
    # if it's an orbit, filter out the bad orbits client-side
    #if orbit_numbers:
    #    orbit_key = stringify_orbit(orbit_numbers)
    #    results = sort_by_orbit(results).get(orbit_key, [])
    print('found {} {} products matching query.'.format(len(results), prod_type))
    if prod_type in ["S1-GUNW-acqlist-audit_trail", "S1-GUNW-acq-list"]  and len(results) == 0:
        raise RuntimeError("0 matching found for {} with full_id_hash {} in {} with query :\n{}".format(prod_type, full_id_hash, grq_url, json.dumps(grq_query)))

    #print(results)
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
        statement += '\nwith orbits:       {}'.format(', '.join([str(x) for x in orbit_numbers]))
    if version:
        statement += '\nwith version:      {}'.format(version)
    if uid:
        statement += '\nwith uid:          {}'.format(uid)
    if aoi:
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
    print("query_es query: \n{}".format(json.dumps(es_query)))

    if 'size' in list(es_query.keys()):
        iterator_size = es_query['size']
    else:
        iterator_size = 10
        es_query['size'] = iterator_size
    if 'from' in list(es_query.keys()):
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
        if orbit in list(sorted_dict.keys()):
            sorted_dict[orbit].append(result)
        else:
            sorted_dict[orbit] = [result]

    print("sort_by_orbit : orbits found : {}".format(list(sorted_dict.keys())))
    return sorted_dict

def sort_by_hash(es_results_list):
    '''
    Goes through the objects in the result list, and places them in an dict where key is full_id_hash (or generated version of hash)
    '''
    sorted_dict = {}
    for result in es_results_list:
        idhash = get_hash(result)
        if idhash in list(sorted_dict.keys()):
            sorted_dict.get(idhash, []).append(result)
        else:
            sorted_dict[idhash] = [result]

    print("sort_by_hash : hash found : {}".format(list(sorted_dict.keys())))
    return sorted_dict

def sort_by_track(es_result_list):
    '''
    Goes through the objects in the result list, and places them in an dict where key is track
    '''
    #print('found {} results'.format(len(es_result_list)))
    sorted_dict = {}
    for result in es_result_list:
        track = get_track(result)
        if track in list(sorted_dict.keys()):
            sorted_dict.get(track, []).append(result)
        else:
            sorted_dict[track] = [result]

    print("sort_by_track : tracks found : {}".format(list(sorted_dict.keys())))
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
        if aoi_id in list(sorted_dict.keys()):
            sorted_dict.get(aoi_id, []).append(result)
        else:
            sorted_dict[aoi_id] = [result]
    print("sort_by_aoi : aois found : {}".format(list(sorted_dict.keys())))
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
    options = ['orbit_number', 'orbitNumber', 'orbit']
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

def sort_duplicates_by_hash(es_results_list):
    '''
    iterates through the list of products and removes products with duplicate full_id_hash
    (using creation time) and returns a dict sorted by full_id_hash.
    '''
    sorted_dict = {}
    for result in es_results_list:
        idhash = get_hash(result)
        if idhash in list(sorted_dict.keys()):
            latest_result = get_most_recent(result, sorted_dict.get(idhash))
            print('found duplicate gunws: {}, {}'.format(result.get('_source').get('id'), sorted_dict.get(idhash).get('_source').get('id')))
            sorted_dict[idhash] = latest_result
        else:
            sorted_dict[idhash] = result
    return sorted_dict

def filter_hashes(es_results_list, full_id_hash_list):
    '''
    filters out objects in the es_results_list that don't contain a 
    full_id_hash from full_id_hash_list. Returns the filtered list.
    '''
    filtered_list = []
    for es_result in es_results_list:
        hsh = get_hash(es_result)
        if hsh in full_id_hash_list:
            filtered_list.append(es_result)
    return filtered_list

def get_most_recent(obj1, obj2):
    '''returns the object with the most recent ingest time'''
    ctime1 = dateutil.parser.parse(obj1.get('_source', {}).get('creation_timestamp', False))
    ctime2 = dateutil.parser.parse(obj2.get('_source', {}).get('creation_timestamp', False))
    if ctime1 > ctime2:
        return obj1
    return obj2

def resolve_orbit_field(prod_type):
    '''resolves the orbit metadata field by product type'''
    orbit_mapping = {'S1-GUNW-acq-list': 'orbitNumber',
                 'S1-GUNW':'orbit_number',
                 'S1-GUNW-MERGED': 'orbit_number',
                 'S1-GUNW-acqlist-audit_trail': ['secondary_orbit.raw', 'reference_orbit.raw'],
                 'S1-GUNW-AOI_TRACK': 'orbit',
                 'S1-GUNW-MERGED-AOI_TRACK': 'orbit'}
    return orbit_mapping.get(prod_type, False)

def stringify_orbit(orbit_list):
    '''converts the list into a string'''
    if len(orbit_list) == 0:
        raise RuntimeError("Orbit List is EMPTY")
    return '_'.join([str(x).zfill(3) for x in sorted(orbit_list)])

def get_version(es_obj):
    '''returns the version of the index. Since we are ignoring the subversions, only returns the main version.
    eg, v2.0.1 returns v2.0'''
    match = re.search(r'^([v]*?)([0-9])*\.([0-9])*[\.]{0,1}([0-9]){0,1}', es_obj.get('_source', {}).get('version', False))
    version = '{}{}.{}'.format(match.group(1), match.group(2), match.group(3))
    return version

if __name__ == '__main__':
    try:
        evaluate()
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(0)

