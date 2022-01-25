import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


DB_LOCATION = "sqlite:///Chansons_jouées.sqlite"
UTILISATEUR_ID = "" # Insérer votre ID 
TOKEN = "" # votre Spotify TOKEN

# Vous pouvez genérer votre TOKEN sur ce lien  https://developer.spotify.com/console/get-recently-played/

def check_if_valid_data(df: pd.DataFrame) :
    # Verifier si la dataframe est vide
    if df.empty:
        print("Aucune chanson téléchargée")
        return False 

    # Verification de la clé primaire
    if pd.Series(df['Horaire']).is_unique:
        pass
    else:
        raise Exception("Probléme du clé primaire")

    # Vérification des valeurs nulles
    if df.isnull().values.any():
        raise Exception("Valeurs nulles trouvées")

    # Vérifiez que tous les timestamps sont de la date d'hier.
    Hier = datetime.datetime.now() - datetime.timedelta(days=1)
    Hier = Hier.replace(hour=0, minute=0, second=0, microsecond=0)

    Dates = df["Date"].tolist()
    for D in Dates:
        if datetime.datetime.strptime(D, '%Y-%m-%d') != Hier:
            raise Exception("Il existe au moins une chanson retournée qui n'était pas lancée hier")

    return True

if __name__ == "__main__":

    # La partie extract du processus ETL
 
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    
    # Convertir le temps en timestamp Unix en milisecondes      
    Aujourdhui = datetime.datetime.now()
    Hier = Aujourdhui - datetime.timedelta(days=1)
    Hier_unix_timestamp = int(Hier.timestamp()) * 1000

    # Selectionner toutes les chansons ecoutées durant les derniers 24 heures     
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=Hier_unix_timestamp), headers = headers)

    data = r.json()

    Titres_des_chansons = []
    Nom_des_artists = []
    Horaire = []
    Date = []

    # Extraire les données qui nous intéressent du Json     
    for song in data["items"]:
        Titres_des_chansons.append(song["track"]["name"])
        Nom_des_artists.append(song["track"]["album"]["artists"][0]["name"])
        Horaire.append(song["played_at"])
        Date.append(song["played_at"][0:10])
        
    # Création du dictionnaire qui va contenir nos data et l'utiliser pour créer une dataframe      
    song_dict = {
        "Titres_des_chansons" : Titres_des_chansons,
        "Nom_des_artists": Nom_des_artists,
        "Horaire" : Horaire,
        "Date" : Date
    }

    song_df = pd.DataFrame(song_dict, columns = ["Titres_des_chansons", "Nom_des_artists", "Horaire", "Date"])
    
    # Validation
    if check_if_valid_data(song_df):
        print("Données valides")

    # Partie Load du processus ETL

    engine = sqlalchemy.create_engine(DB_LOCATION)
    conn = sqlite3.connect('Chansons_jouées.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS Chansons_jouées(
        Titres_des_chansons VARCHAR(200),
        Nom_des_artists VARCHAR(200),
        Horaire VARCHAR(200),
        Date VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (Horaire)
    )
    """

    cursor.execute(sql_query)
    print("La connexion est établie avec succès")

    try:
        song_df.to_sql("Chansons_jouées", engine, index=False, if_exists='append')
    except:
        print("La base de données existe déja ")

    conn.close()
    print("Terminé")