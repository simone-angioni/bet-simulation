from elasticsearch import Elasticsearch
import math

class Connection:
    def __init__(self, period):
        self.es = Elasticsearch(["rexplore2.kmi.open.ac.uk"], http_auth=("simoneangioni", "egg8-enjoying-Headgear"), scheme="https", port="9200")
        self.period = period

    def get_num_partitions(self):
        query ={
            "query":{
                "bool":{
                    "must":[
                        {"range": {"publication_date": {"gte": self.period['start'],"lt": self.period['end']}}}
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "total": {
                    "cardinality": {
                        "field": "dbpedia_entities.uri.keyword"
                    }
                }
            }
        }
        raw_data = self.es.search(index="news_microsoft", body=query)
        total_entities = raw_data['aggregations']["total"]["value"]
        return math.ceil(int(total_entities)/1000)

    def get_entities_query(self, min_doc_count, num_partition):
        query = {
        "track_total_hits": True,
        "query":{
            "bool":{
                "must":[
                    {"range": {"publication_date": {"gte": self.period['start'],"lt": self.period['end']}}}
                ]
            }
        },
        "size": 0,
        "aggs": {
            "entities": {
            "terms": {
                "field": "dbpedia_entities.uri.keyword",
                "size": 1000,
                "min_doc_count":min_doc_count,
                "include":{
                    "partition":0,
                    "num_partitions": num_partition
                }
            }
            }
        }
        }
        return query

    def get_daily_entity_query(self, entity):
        query = {
          "track_total_hits": True,
          "query": {
            "bool": {
              "must": [
                {"match_phrase": {"dbpedia_entities.uri.keyword": entity}},
                {"range": {
                  "publication_date": {
                    "gte": self.period['start'],
                    "lt": self.period['end']
                  }
                }}
              ]
            }
          },
          "size": 0, 
          "aggs": {
            "daily": {
              "date_histogram": {
                "field": "publication_date",
                "interval": "day"
              }
            }
          }
        }
        return query

    def get_all_news_daily_query(self):
        query = {
        "track_total_hits": True,
        "query": {
            "bool": {
            "must": [
                {"range": {
                "publication_date": {
                    "gte": self.period['start'],
                    "lt": self.period['end']
                }
                }}
            ]
            }
        },
        "size": 0,
        "aggs": {
            "daily": {
            "date_histogram": {
                "field": "publication_date",
                "interval": "day"
            }
            }
        }
        }
        return query

    def get_aggregation(self, entity):
        query = self.get_daily_entity_query(entity)
        res = self.es.search(index="news_microsoft", body=query)
        return [{'key': e['key_as_string'].split("T")[0], 'doc_count':e['doc_count']} for e in res['aggregations']['daily']['buckets'] if not e['doc_count'] == 0], res['hits']['total']['value']

    def get_all_news_daily_aggregations(self, entity):
        query = self.get_daily_entity_query(entity)
        res = self.es.search(index="news_microsoft", body=query)
        return {e['key_as_string'].split("T")[0]: e['doc_count'] for e in res['aggregations']['daily']['buckets'] if not e['doc_count'] == 0}

    def _get_entities(self, es_aggregation):
        entities = [entity['key'] for entity in es_aggregation]
        return entities

    def get_entities_in_period(self, min_doc_count):
        num_partition = self.get_num_partitions()
        query = self.get_entities_query(min_doc_count, num_partition)
        raw_entities = []
        for i in range(0, num_partition):
            query['aggs']['entities']['terms']['include']['partition'] = i
            res = self.es.search(index="news_microsoft", body=query)
            raw_entities.extend(res['aggregations']['entities']['buckets'])
        entities = self._get_entities(raw_entities)
        return entities