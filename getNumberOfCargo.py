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
    
url = 'http://apis.data.go.kr/B551177/AviationStatsByCountry/getTotalTonsOfCargo'

year_dic = [201301,	201401,	201501,	201601,	201701,	201801,	201901,	202001,	202101, 202201, 202301]
endyear_dic = [201312,	201412,	201512,	201612,	201712,	201812,	201912,	202012,	202112, 202212, 202302]
    
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
            'arrBaggage' : json_object["response"]["body"]["items"][i]['arrBaggage'],
            'depBaggage' : json_object["response"]["body"]["items"][i]['depBaggage'],
            'baggage' : json_object["response"]["body"]["items"][i]['baggage'],
            'year' : year_dic[t],
            }
                
        new_object['arrBaggage'] = (new_object['arrBaggage']).replace(',','')
        new_object['depBaggage'] = (new_object['depBaggage']).replace(',','')
        new_object['baggage'] = (new_object['baggage']).replace(',','')
                
        new_object['arrBaggage'] = int(new_object['arrBaggage'])
        new_object['baggage'] = int(new_object['baggage'])
        new_object['depBaggage'] = int(new_object['depBaggage'])
        new_object['year'] = str(new_object['year'])
                
        index_name = "cargo_mappingfinal"
        es.index(index=index_name,  document=new_object)