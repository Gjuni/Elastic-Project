from elasticsearch import Elasticsearch
import requests
import json


def lambda_handler(event, context):
    CLOUD_ID="===="
    ELASTIC_PASSWORD="password"

    es =Elasticsearch(
        cloud_id=CLOUD_ID,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )
    

    url = 'http://apis.data.go.kr/B551177/AviationStatsByCountry/getTotalNumberOfFlight'

    year_dic = [201301,	201401,	201501,	201601,	201701,	201801,	201901,	202001,	202101, 202201]
    
    for date in year_dic:
        for j in range(0, 12):
            params ={'serviceKey' : 'keyValue',
             'from_month' : date+j, 'to_month' : date+j, 'periodicity' : None, 'pax_cargo' : None,
             'type' : 'json' }
            response = requests.get(url, params=params)
            content = response.content
            json_object = json.loads(content)

            array_len = len(json_object["response"]["body"]["items"])
    
            for i in range(0, array_len):
                new_object = {
                    'region' : json_object["response"]["body"]["items"][i]['region'],
                    'country' : json_object["response"]["body"]["items"][i]['country'],
                    'arrFlight' : json_object["response"]["body"]["items"][i]['arrFlight'],
                    'depFlight' : json_object["response"]["body"]["items"][i]['depFlight'],
                    'flights' : json_object["response"]["body"]["items"][i]['flights'],
                    'year' : date +j,
                }
                index_name = "flight_mapping03"
                es.index(index=index_name,  body =new_object)