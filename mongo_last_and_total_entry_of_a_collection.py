import os
import re
import ast
import csv
import time
import pymongo
import datetime
import threading
import pandas as pd
import plotly.express as px
from threading import Thread
from pymongo import MongoClient
from subprocess import Popen,PIPE
import plotly.graph_objects as go
from itertools import zip_longest

def mongo_stream_last_entry_and_total_doc_count(p_list):

        client = MongoClient("mongodb://localhost", 27017)
        db = client.mydb
        print("Connected")

        PId = []
        total_doc_count = []
        Last_entry = []


        for stream_col in p_list:

            try:
                print(f'Getting last entry for {stream_col}')

                collection = db[f'{stream_col}_stream']
                # PId.append(str(stream_col.split("_")[0]))
                PId.append(stream_col)
                mydoc_count = collection.estimated_document_count() 
                total_doc_count.append(mydoc_count)

                last_document = collection.find_one(sort=[("_id", pymongo.DESCENDING)])
                Last_entry.append(last_document["DOC"])


            except:
                print("Not Found")

        header = ["PId", "Last_Entry", "Total_Entries"]
        output_file = "./doc_stream.csv"


        with open(output_file, "a") as out:
            writer = csv.writer(out)
            writer.writerow(header)
            writer.writerows(zip_longest(PId, Last_entry, total_doc_count))

        PId.clear()
        Last_entry.clear()
        total_doc_count.clear()



p_list = ['ABC00','ABC01']

mongo_stream_last_entry_and_total_doc_count(p_list)
