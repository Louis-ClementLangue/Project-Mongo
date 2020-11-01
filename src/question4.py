from pprint import pprint

import requests
import json
import re
from pymongo import MongoClient

client = MongoClient('mongodb+srv://dbUser:djshbfcvedv@cluster0.xkzk9.gcp.mongodb.net/bikeAvailable?retryWrites=true&w=majority')

db = client.bikeAvailable

db.datas.create_index([('station_id', 1), ('date', -1)], unique=True)

stations = db.stations


def get_vlille():
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=300&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"

    response = requests.request("GET", url, headers={}, data={})
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


def get_station_id(id_ext):
    tps = db.stations.find_one({'source.id_ext': id_ext}, {'_id': 1})
    return tps['_id']


def rechercher_station(nom):
    regex_nom = re.compile(nom, re.IGNORECASE)

    recherche = {"name": regex_nom}
    nb_resultats = stations.count_documents(recherche)
    resultats = stations.find(recherche)

    if nb_resultats == 0:
        return 0
    else:
        return [nb_resultats, resultats]


def moditifer_station(id, champ, valeur):
    stations.update_one({"_id": id}, {"$set": {champ: valeur}})


def supprimer_station(id, nom_ville):
    stations.delete_one({"_id": id})

    if nom_ville == "Lille":
        stations.delete_many({"station_id": id})


def gestion_zone(status):
    liste_stations = []

    with open("zone.geojson", 'r') as geojson:
        geojson = geojson.read()
        donnees_json = json.loads(geojson)

        recherche = {"geometry": {"$geoWithin": {"$geometry": donnees_json["features"][0]["geometry"]}}}
        resultats = stations.find(recherche)
        for resultat in resultats:
            liste_stations.append(resultat['name'])

        stations.update_many(recherche, {"$set": {"available": status}})
    return liste_stations


def ratio_ville():
    liste = stations.aggregate([
        {"$addFields" : {"time":{"$toDate" : "$date"}}},
        {"$addFields" : {"dayOfWeek":{"$isoDayOfWeek" : "$time"}}},
        {"$addFields" : {"heure":{"$hour" : "$time"}}},
        {"$match" : {"heure" : {"$in" : [18]}}},
        {"$match" : {"dayOfWeek" : {"$in" : [1,2,3,4,5]}}},
        {"$group" : {"_id": "$_id",
            "totalv" : {"$sum" : "$bike_available"},
            "totalp" : {"$sum" : "$stand_available"}
        } },
        {"$addFields" : {"total" : {"$sum" : ["$totalv", "$totalp"] } } },
        {"$match" : {"total" : {"$gt" : 0} } },
        {"$addFields" : {"ratio" : {"$avg" : {"$divide" : ["$totalv", "$total"] } } } },
    ])
    for i in liste:
        if i['ratio'] <= 0.2:
            print('la stration: ', str(i['_id']), ' a un ratio de : ', i['ratio'])


if __name__ == '__main__':
    try:
        choix = input(
            "recherche [R] ou manipuler une zone [Z] ou voir stations un ratio < 20% [X] : ")
        while choix != 'R' and choix != 'r' and choix != 'Z' and choix != 'z' and choix != 'X' and choix != 'x':
            choix = input(
                "recherche [R] ou manipuler une zone [Z] ou voir stations un ratio < 20% [X] : ")

        # Recherche de stations
        if choix == 'R' or choix == 'r':
            nom = input("nom : ")
            resultats_recherche = rechercher_station(nom)
            while resultats_recherche == 0:
                nom = input("Pas de résultat, reesayez :")
                resultats_recherche = rechercher_station(nom)

            print("\n%d station(s) ont été trouvée(s)\n" % resultats_recherche[0])
            liste = list(resultats_recherche[1])
            for elem in liste:
                pprint(elem)
                print('\n')

            choix = int(
                input("Sélectionnez l'index de la station :"))
            elem_choisi = liste[choix]
            choix = input("modifier [M] ou supprimer [S] : ")
            while choix != 'M' and choix != 'm' and choix != 'S' and choix != 's':
                choix = input("modifier [M] ou supprimer [S] : ")


            if choix == 'M' or choix == 'm':
                print("\nEdition de la station : " + elem_choisi["name"])
                choix = input("Voulez-vous modifier le name [N] ou la size [S] : ")
                while choix != 'N' and choix != 'n' and choix != 'S' and choix != 's':
                    choix = input("Voulez-vous modifier le name [N] ou la size [S] : ")

                if choix == 'N' or choix == 'n':
                    nouveau_nom = input("Choississez le nouveau nom : ")
                    moditifer_station(elem_choisi["_id"], "name", nouveau_nom)
                    print("\nLa station " + elem_choisi["name"] + " a désormais le nom " + nouveau_nom)
                elif choix == 'S' or choix == 's':
                    nouvelle_taille = int(input("Choississez la nouvelle taille : "))
                    moditifer_station(elem_choisi["_id"], "size", nouvelle_taille)
                    print("\nLa station " + elem_choisi["name"] + " accueille désormais " + str(
                        nouvelle_taille) + " vélos")

            elif choix == 'S' or choix == 's':
                supprimer_station(elem_choisi["_id"], elem_choisi["source"]["dataset"])
                print("\nLa station " + elem_choisi["name"] + " a bien été supprimée")

        # Activation/Désactivation des stations dans une zone
        elif choix == 'Z' or choix == 'z':
            choix = input('activer [A] ou désactiver [D] les stations : ')
            while choix != 'A' and choix != 'a' and choix != 'D' and choix != 'd':
                choix = input('activer [A] ou désactiver [D] les stations : ')

            if choix == 'A' or choix == 'a':
                status = True
                status_texte = "activées"
            elif choix == 'D' or choix == 'd':
                status = False
                status_texte = "désactivées"

            stations_changees = gestion_zone(status)
            print("\n Les stations ont été " + status_texte + " : ")
            print(stations_changees)
        elif choix == 'X' or choix == 'x':
            ratio_ville()
    except Exception as e:
        print('\nErreur !\n')
        pprint(e)

