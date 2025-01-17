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
            "geometry": {
                "type": "Point",
                "coordinates": [
                    i["fields"]["localisation"][1],
                    i["fields"]["localisation"][0]
                ]
            },
            "TPE": i["fields"]["type"] == "AVEC TPE",
            "status": i["fields"]["etat"] == "EN SERVICE",
            "last update": i["fields"]["datemiseajour"]
        })

    for j in paris_data:
        data.append({
            "name": j["fields"]["name"],
            "city": "Paris",
            "size": j["fields"]["capacity"],
            "geometry": {
                "type": "Point",
                "coordinates": [
                    j["fields"]["coordonnees_geo"][1],
                    j["fields"]["coordonnees_geo"][0]
                ]
            },
            "TPE": j["fields"]["is_renting"] == "OUI",
            "status": j["fields"]["is_installed"] == "OUI",
            "last update": j["fields"]["duedate"]
        })

    for k in lyon_data:
        data.append({
            "name": k["name"],
            "city": "Lyon",
            "size": k["bike_stands"],
            "geometry": {
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
            "geometry": {
                "type": "Point",
                "coordinates": [
                    l["fields"]["coordonnees"][1],
                    l["fields"]["coordonnees"][0]
                ]
            },
            "TPE": False,
            "status": l["fields"]["etat"] == "En fonctionnement",
            "last update": l["fields"]["lastupdate"]
        })

    return data

# Question 2

def update_db():
    client = pymongo.MongoClient("mongodb+srv://dbUser:djshbfcvedv@cluster0.xkzk9.gcp.mongodb.net/bikeAvailable?retryWrites=true&w=majority")

    db = client.bikeAvailable
    bikeAvailable = db.bikeAvailable
    bikeAvailable.create_index([("geometry", "2dsphere")])
    print("Connection succeed")
    bikeAvailable.insert_many(stations_data)
    print("data stored successfully")
    client.bikeAvailable.stations.create_index([("geometry", "2dsphere")])
    client.bikeAvailable.stations.insert_many(stations_data)


#Question 3



def closest_station(lat, lon):
    client = pymongo.MongoClient(
        "mongodb+srv://dbUser:djshbfcvedv@cluster0.xkzk9.gcp.mongodb.net/bikeAvailable?retryWrites=true&w=majority")
    db = client.bikeAvailable
    bikeAvailable = db.bikeAvailable
    close_stations = bikeAvailable.find({
        'geometry': {
            '$near': {
                '$geometry': {
                    'type': "Point",
                    'coordinates': [lon, lat]},
                    '$minDistance': 0,
                    '$maxDistance': 500
                }
            }
        })
    last_stations = []
    for s in close_stations:
        last_stations.append(s['name'])
        print(s)




if __name__ == '__main__':
    vlille_data = get_vlille()
    vparis_data = get_vparis()
    vlyon_data = get_vlyon()
    vrennes_data = get_vrennes()
    stations_data = prepare_data(vlille_data, vparis_data, vlyon_data, vrennes_data)
    print(stations_data)
    update_db()
    lat = 45.77589951121384
    lon = 4.82537896207112
    closest_station(lat, lon)

