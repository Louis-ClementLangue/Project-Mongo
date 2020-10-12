import pymongo as pymongo
import requests
import json
from src import dbi


# Question 1
from src import dbi


def get_vlille():
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


def get_vparis():
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


def get_vlyon():
    url = "https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json?maxfeatures=500&start=1"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("values", [])


def get_vrennes():
    url = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-des-stations-le-velo-star-en-temps-reel&q=&facet=nom&facet=etat&facet=nombreemplacementsactuels&facet=nombreemplacementsdisponibles&facet=nombrevelosdisponibles"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


def prepare_data(lille_data, paris_data, lyon_data, rennes_data):
    data = []
    for i in lille_data:
        data.append({
            "name": i["fields"]["nom"],
            "city": "Lille",
            "size": i["fields"]["nbvelosdispo"] + i["fields"]["nbplacesdispo"],
            "geo": i["geometry"],
            "TPE": i["fields"]["type"] == "AVEC TPE",
            "status": i["fields"]["etat"] == "EN SERVICE",
            "last update": i["fields"]["datemiseajour"]
        })

    for j in paris_data:
        data.append({
            "name": j["fields"]["name"],
            "city": "Paris",
            "size": j["fields"]["capacity"],
            "geo": j["geometry"],
            "TPE": j["fields"]["is_renting"] == "OUI",
            "status": j["fields"]["is_installed"] == "OUI",
            "last update": j["fields"]["duedate"]
        })

    for k in lyon_data:
        data.append({
            "name": k["name"],
            "city": "Lyon",
            "size": k["bike_stands"],
            "geo": {
                "type": "Point",
                "coordinates": [
                    k["lng"],
                    k["lat"]
                ]
            },
            "TPE": k["banking"],
            "status": k["status"] == "OPEN",
            "last update": k["last_update_fme"]
        })

    for l in rennes_data:
        data.append({
            "name": l["fields"]["nom"],
            "city": "Rennes",
            "size": l["fields"]["nombreemplacementsactuels"],
            "geo": l["geometry"],
            "TPE": False,
            "status": l["fields"]["etat"] == "En fonctionnement",
            "last update": l["fields"]["lastupdate"]
        })

    return data

# Question 2

def update_db():
    client = pymongo.MongoClient("mongodb+srv://db_User:djshbfcvedv@cluster0.xkzk9.gcp.mongodb.net/bikeAvailable?retryWrites=true&w=majority")

    db = client.bikeAvailable

    print("Connection succeed")


if __name__ == '__main__':
    vlille_data = get_vlille()
    vparis_data = get_vparis()
    vlyon_data = get_vlyon()
    vrennes_data = get_vrennes()
    stations_data = prepare_data(vlille_data, vparis_data, vlyon_data, vrennes_data)
    print(stations_data)
    update_db()
