#!/usr/bin/env python

'''
updates product grq metadata with the machine tag
'''
from __future__ import print_function
import json
import requests
from hysds.celery import app

def add_tag(index, uid, prod_type, tag):
    '''updates the product with the given tag'''
    if tag is None:
        tag_list = [] #tag is empty, remova ll tags
    else:
        existing_tags = get_current_tags(uid, prod_type, index)
        if not tag is False and tag in existing_tags:
            print('tag: {} already in tags for: {}'.format(tag, uid))
            return
        tag_list = tag.split(',')
        if not type(existing_tags) is list:
            existing_tags = []
        tag_list = list(set(existing_tags + tag_list))
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/{2}/{3}/_update'.format(grq_ip, index, prod_type, uid)
    es_query = {"doc" : {"metadata": {"tags" : tag_list}}}
    response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
    response.raise_for_status()
    print('successfully updated {} with tag {}'.format(uid, tag))

def remove_tag(index, uid, prod_type, tag):
    '''removes the tag from the product'''
    if tag is False:
        return
    else:
        existing_tags = get_current_tags(uid, prod_type, index)
        if not tag in existing_tags:
            print('tag: {} does not exist in tags for: {}'.format(tag, uid))
            return
        tag_list = tag.split(',')
        if not type(existing_tags) is list:
            existing_tags = []
        existing_tags.remove(tag)
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/{2}/{3}/_update'.format(grq_ip, index, prod_type, uid)
    es_query = {"doc" : {"metadata": {"tags" : existing_tags}}}
    response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
    response.raise_for_status()
    print('successfully removed tag {} from {}'.format(tag, uid))

def get_current_tags(uid, prod_type, index):
    '''gets the current tags of the object'''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/{2}/_search'.format(grq_ip, index, prod_type)
    grq_query = {"query": {"bool": {"must": {"match": {"_id": uid}}}}}
    results = query_es(grq_url, grq_query)
    tags = results[0].get('_source', {}).get('metadata', {}).get('tags', [])
    print('{} has current tags: {}'.format(uid, tags))
    return tags

def query_es(grq_url, es_query):
    '''
    Runs the query through Elasticsearch, iterates until
    all results are generated, & returns the compiled result
    '''
    # make sure the fields from & size are in the es_query
    if 'size' in es_query.keys():
        iterator_size = es_query['size']
    else:
        iterator_size = 1000
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
