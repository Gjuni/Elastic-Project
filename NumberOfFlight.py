from elasticsearch import Elasticsearch
import requests
import json


def lambda_handler(event, context):
    CLOUD_ID="ID of Cloud"
    ELASTIC_PASSWORD="Elastic Password"

    es =Elasticsearch(
        cloud_id=CLOUD_ID,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )

    url = 'http://apis.data.go.kr/B551177/AviationStatsByCountry/getTotalNumberOfFlight'

    params ={'serviceKey' : 'EncodeKey',
             'from_month' : '202212', 'to_month' : '202212', 'periodicity' : None, 'pax_cargo' : None,
             'type' : 'json' }

    response = requests.get(url, params=params)


    content = response.content

    json_object = json.loads(content)
    array_len = len(json_object["response"]["body"]["items"])
    #print(len(json_object["response"]["body"]["items"]))

    for i in range(0, array_len):
        document = {
            'region' : json_object["response"]["body"]["items"][i]['region'],
            'country' : json_object["response"]["body"]["items"][i]['country'],
            'arrFlight' : json_object["response"]["body"]["items"][i]['arrFlight'],
            'depFlight' : json_object["response"]["body"]["items"][i]['depFlight'],
            'flights' : json_object["response"]["body"]["items"][i]['flights'],
        }
        index_name = "flight_mapping3"
        es.index(index=index_name,  document =json_object)