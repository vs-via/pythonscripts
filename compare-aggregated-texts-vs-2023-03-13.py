from elasticsearch import Elasticsearch
import requests
from pprint import pprint
from datetime import datetime
from prettytable import PrettyTable
import urllib3
import traceback
import sys
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



elastic_client = Elasticsearch(hosts=["https://trumpf.viamedici.cloud:80/datastore"],  verify_certs=False)
FMT = '%d-%b-%YT%H:%M:%S.%f'


def getProductsWithPricelistversionViaIc1(lang, date):

    query = {
        "from": 0,
        "size": 9999,
        "query": {
            "bool": {

             
                "must": [

                    {
                    "terms": {
                        "pricelistVersion": [

                            "P64"
                            #"Forecast Go Live >> Create Version", "P63.1"
                       
                        ]
                    }
                },
                    
                
                {
                    "has_child": {
                        "type": "additional",
                        "query": {
                            "terms": {
                                "identifier": [
                               
                                    "ic_28_1"
                               
                                ]
                            }
                        },
                        "inner_hits": {
                            "size":99
                        }
                    }
                }
                ]
            }
        }
    }

    
    result = search(lang, query)
    return result




def findDifferencesInLanguages(pricelistVersion, lang):

    myLanguages = {
        "deu_deu",
        lang
    }

    indicators = {}
    differences = {
        'liCountTechDesc': {}
    }
    for lang in myLanguages:
        print("fetching "+lang)
        #result = getProductsWithPricelistversion(lang, pricelistVersion, 'ic_28_1')
        result = getProductsWithPricelistversionViaIc1(lang, "2023-03-21T00:00:00.00000Z")
        #result2 = getProductsWithPricelistversionViaIc1(lang, "2032-10-25T00:00:00.00000Z")
        try:
            hits = result.json()['hits']['hits']
            #hits2 = result2.json()['hits']['hits']
            #hits = hits + hits2
        except:
            print("no hits")
            continue
        for hitIc1 in hits:

            hit = hitIc1['inner_hits']['additional']['hits']['hits'][0]

            if lang not in indicators:
                indicators[lang] = {}
            if hit['_id'] not in indicators[lang]:
                indicators[lang][hit['_id']] = {}

            for attribute in hit['_source']['content'][0]:
                if 'identifier' in attribute:
                    if attribute['identifier'] == 'technical-description-offer-str':
                        if attribute['values'][0]['rawValue']:
                            indicators[lang][hit['_id']]['liCountTechDesc'] = attribute['values'][0]['rawValue'].count('<li>')
                            indicators[lang][hit['_id']]['liCountTechDescContent'] = attribute['values'][0]['rawValue']
                            
                        else:
                            indicators[lang][hit['_id']]['liCountTechDesc'] = 0
                            indicators[lang][hit['_id']]['liCountTechDescContent'] = "" 
                            
                        indicators[lang][hit['_id']]['timestamp'] = hit['_source']['timestamp']
                        indicators[lang][hit['_id']]['parentUniqueId'] = hit['_source']['parentUniqueId']


    f = open("techinfo-monitor-"+lang+".html", "w")
    f.write("<!DOCTYPE html><HTML><HEAD><style>table, th, td: {border: 1px solid black;border-collapse: collapse;}</style></HEAD><BODY><table>")
    f.write('<tr><td></td>')

    for lang in myLanguages:
        try: 
            for productKey, product in indicators[lang].items():
                if 'liCountTechDesc' in product:
                    
                    try:
                        if product['liCountTechDesc'] != indicators['deu_deu'][productKey]['liCountTechDesc']:
                            if productKey not in differences['liCountTechDesc']:
                                #print('"'+indicators['deu_deu'][productKey]['parentUniqueId'].split("-")[0]+'",')
                                print(''+indicators['deu_deu'][productKey]['parentUniqueId'].split("-")[0]+',')
                                f.write('<tr>')
                                differences['liCountTechDesc'][productKey] = {}
                                differences['liCountTechDesc'][productKey]['deu_deu'] = indicators['deu_deu'][productKey]['liCountTechDescContent']
                                differences['liCountTechDesc'][productKey][lang] = indicators[lang][productKey]['liCountTechDescContent']
                                f.write('<td style="font:arial;border: 1px solid black;border-collapse: collapse;">'+"deu_deu"+" "+indicators['deu_deu'][productKey]['parentUniqueId']+"<br/>"+indicators['deu_deu'][productKey]['timestamp']+"<br/>"+indicators['deu_deu'][productKey]['liCountTechDescContent']+'</td>')
                                f.write('<td style="font:arial;border: 1px solid black;border-collapse: collapse;">'+lang+" "+productKey+"<br/>"+indicators['deu_deu'][productKey]['timestamp']+"<br/>"+indicators[lang][productKey]['liCountTechDescContent']+'</td>')
                                f.write('</tr>')
                            differences['liCountTechDesc'][productKey]['message'] = (str(productKey)+' LI count '+lang+" "+str(product['liCountTechDesc']-indicators['deu_deu'][productKey]['liCountTechDesc']))
                    except:
                        True
                        #print("missung: "+productKey)
                        #print(traceback.format_exc())
        except:
            print("no lang related hits?")
    f.write("</table></BODY></HTML>")
    f.close()

    #pprint(differences)
    print("count liCountTechDesc: "+str(len(differences['liCountTechDesc'])))


def search(lang, query):
    result = requests.post('https://trumpf.viamedici.cloud/datastore/trumpf_ds_infoobjects_'+lang+'_1/_search', json=query, verify=False)
    return result


languages = {
    "bul_bgr",
    "ces_cze",
    "eng_gbr",
    "eng_glo",,
    "fra_fra",
    "hun_hun",
    "ita_ita",
    "kor_kor",
    "nld_nld",
    "pol_pol",
    "por_bra",
    "por_prt",
    "rus_rus",
    "spa_esp",
    "slk_svk",
    "swe_swe",
    "tur_tur",
    "zho_glo",
    "zho_chn",
    "zho_twn"
}



for lang in languages:
    #findDifferencesInLanguages(["P63.1", "Forecast Go Live >> Create Version"], lang)
    findDifferencesInLanguages(["P64"], lang)
exit()
