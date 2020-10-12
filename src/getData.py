import pymongo as pymongo
import requests
import json

# Question 1


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
    return response_json

# Question 2

def update_db():
    client = pymongo.MongoClient(
        "mongodb+srv://" + dbi.db_user + ":" + dbi.db_password +
        "@cluster0.yxfmb.gcp.mongodb.net/" + dbi.db_name + "?retryWrites=true&w=majority")

    db = client.vls
    refresh_stations = p1.get_stations(city)

    for sta in refresh_stations:
        db.stations.update_one({
            "name": sta["name"]
        }, {
            "$set": sta
        }, upsert=True)  # Add the data if not found


if __name__ == '__main__':
    vlille_data = get_vlille()
    vparis_data = get_vparis()
    vlyon_data = get_vlyon()
    vrennes_data = get_vrennes()
    print(vlille_data)
    print(vparis_data)
    print(vlyon_data)
    print(vrennes_data)
