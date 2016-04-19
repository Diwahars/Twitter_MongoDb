# -*- coding: utf-8 -*-
"""
Created on Thu Mar 03 16:15:32 2016

@author: b.amoussou-djangban
"""

##############################################################################
##############################################################################

    # MongoDb Python
    # Récupérartion et analyse de Tweets
    # Auteur : Baruch AMOUSSOU-DJANGBAN

##############################################################################
##############################################################################

##########                   Import packages                       ##########  
import pandas as pd
import numpy as np
#For Data importation  
import csv
import urllib
import os
import time
## For Web crawling
import requests
from bs4 import BeautifulSoup
import html5lib
import urllib3
from oauth2client.client import SignedJwtAssertionCredentials
## To transform json to dataframe
from pandas.io.json import json_normalize
#For Vizualisation
import matplotlib.pyplot as plt
# For statistic
import scipy.stats as scs
# For modeling (Machine Learning)
## USE REQUEST
import json

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

## Path use
#path = r'C:\Users\b.amoussou-djangban\Documents\DATAIKU\BenchDataiku\Data_Tweeter'
##########################
# Twitter Parameters
##########################
consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

file_data = r'C:\Users\b.amoussou-djangban\Documents\POC Group One Point\Life_Science\Tweet\test_abdoul.json'

class StdOutListener(StreamListener):
    
#    def on_data(self, data):
#        try:
#            with open('python.json', 'a') as f:
#                f.write(data)
#                return True
#        except BaseException as e:
#            print("Error on_data: %s" % str(e))
#        return True

    def on_data(self, data):
        print data
        with open(file_data,'a') as tf:
            tf.write(data)
        return True

    def on_error(self, status):
        print status

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, StdOutListener())
file_final = stream.filter(track=['Champion','League','Real Madrid', 'Barcelone'])

#if __name__ == '__main__':
#
#    #This handles Twitter authetification and the connection to Twitter Streaming API
#    l = StdOutListener()
#    auth = OAuthHandler(consumer_key, consumer_secret)
#    auth.set_access_token(access_token, access_token_secret)
#    stream = Stream(auth, l)
#    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
#    file_final = stream.filter(track=['Plateforme', 'big data', 'analytics'])
    
########################################
### Test traitement
########################################

import pymongo
from pymongo import MongoClient
#from pymongo import Connection


## Connexion à mongo db en local
client = MongoClient()
## Liste de toutes les bases de données
print(client.database_names())

################################################################################"
# Analyse de tweets pour GOP
#############################################################################

## Charger la base de données GOP
GOP = client['bench']          

## Afficher le nom de toute les collections de GOP
print(GOP.collection_names())       

## Vérifier si la collection posts existe dans GOP
print("posts" in GOP.collection_names())    

## Selection de la collectio Tweets
tweets = GOP['Champion_Abdoul']
## Vérifier si la collection est vide
print(tweets.count() == 0)    

## Nombre d'élément dans la collection
tweets.count()

## remier element de la collection
tweets.find_one()

## Afficher tous les text
text = tweets.distinct('text')

text_final = ''

for i in range(0,len(text)):
    a = text[i].encode('ascii','ignore')
    text_final = text_final + ' ' + a
    
text_file = open(r"C:\Users\b.amoussou-djangban\Documents\DATAIKU\BenchDataiku\Data_Tweeter\Abdoul.txt", "w")
text_file.write(text_final)
text_file.close()
## Liste de tous les mots
list_mot = []

for i in range(0,len(text)):
    a = text[i].encode('ascii','ignore').split(' ')
    list_mot.extend(a)

list_mot1 = [x.lower() for x in list_mot]

## count occurence
#from collections import Counter
z = ['DataIku','Dato','Sense','Domino','Studio','Rapidminer','Datameer','Creativedata',
'Alteryx','KNIME','Microstrategy','wakari.io','Databricks','Civisanalytics',
'Flyelephant','Imathresearch','Bigml','Trifacta','Aunalytics','Microsoft',
'Revelotution','Alpinedata','SAS','IBM','H2O','Skytree']

z1 = [x.lower() for x in z]

#Counter(z)
nbre_mot = []
for el in z1:
    nbre_mot.append(list_mot1.count(el))

## Dataframe
Name_Columns = ['mot','nbre']
mot = pd.DataFrame(columns=Name_Columns)
mot['mot'] = z
mot['nbre'] = nbre_mot

mot.shape

mot.to_csv("Pushnew.csv",sep=",", index=False)


tweets = pd.read_json(r"C:\Users\b.amoussou-djangban\Documents\POC Group One Point\Life_Science\Tweet\fetched_tweets.json")





