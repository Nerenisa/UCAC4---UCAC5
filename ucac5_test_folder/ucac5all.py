#!/usr/bin/env python3
# coding: utf-8

import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import struct
import os


# items to read from input files 
       # I | unsigned int   | integer 4 | 
       # H | unsigned short | integer 2 | 
       # h | short          | integer 2 |  
       # B | unsigned char  | integer 1 | 
       #   |                |           | 
       # b | char           | integer 1 | 

z_catalog =  r'C:\Users\User\Documents\UCAC4---UCAC5\ucac5_test_folder'    # '/home/source_cat/UCAC5/u5z'   # folder with directories
binary_unpack = '=QIiHHBBHIihhHHHHHHHH'             # format characters module struct (52 bytes)
col = ['srcid', 'rag', 'dcg', 'erg', 'edg', 'flg', 'nu1', 'epu', 'ira', 'idc', 'pmur', 'pmud', 'pmer', 'pmed', 'gmag', 'umag', 'rmag', 'jmag', 'hmag', 'kmag']
#pg_engine = create_engine('postgresql+psycopg2://user@localhost:5433/test2')
#psql = 'select * from "ucac5" limit 300;'


counter_w=0

# walk through files in a directory z_catalog
zfiles = [f for f in os.listdir(z_catalog) if f.startswith("z")]
for z_files in zfiles:
    full_name_path = os.path.join(z_catalog, z_files)
    with open(full_name_path, 'rb') as fin:
        din = True
        all_catalog = []         # creating a temporary list of decoded binary file strings z001....z900
        while din:
            din = fin.read(52)   # 52 bytes one row
            if len(din) == 52:
                list_row = list(struct.unpack(binary_unpack, din)) 
                print(list_row)
                #all_catalog.append(list_row)
        #df = pd.DataFrame(all_catalog, columns = col)
        #print(df)
        #if counter_w == 0:
            #df.to_sql('ucac5', index=False, con=pg_engine)
           # counter_w = 1
        #else:
            #df.to_sql('ucac5', con=pg_engine, index=False, if_exists='append')

#pg_df = pd.read_sql_query(psql, con=pg_engine)
#print(pg_df)