import time
from elasticsearch import Elasticsearch
import requests
import json


CLOUD_ID="Need Cloud_ID"
ELASTIC_PASSWORD="Need Password"

es =Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)
    
url = 'http://apis.data.go.kr/B551177/AviationStatsByCountry/getTotalNumberOfFlight'

year_dic = [201301,	201401,	201501,	201601,	201701,	201801,	201901,	202001,	202101, 202201, 202301]
endyear_dic = [201312,	201412,	201512,	201612,	201712,	201812,	201912,	202012,	202112, 202212, 202303]
    
for t in range(0,11):
    time.sleep(1)
    params ={'serviceKey' : 'Need Service Key',
        'from_month' : year_dic[t], 'to_month' : endyear_dic[t], 'periodicity' : None, 'pax_cargo' : None,
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
            'year' : year_dic[t],
        }
                
        new_object['arrFlight'] = (new_object['arrFlight']).replace(',','')
        new_object['depFlight'] = (new_object['depFlight']).replace(',','')
        new_object['flights'] = (new_object['flights']).replace(',','')
                
                
        new_object['arrFlight'] = int(new_object['arrFlight'])
        new_object['depFlight'] = int(new_object['depFlight'])
        new_object['flights'] = int(new_object['flights'])
        new_object['year'] = str(new_object['year'])
                
        index_name = "flight_mappingfinal"
        es.index(index=index_name,  document=new_object)