#!/usr/bin/env python3
# coding: utf-8

import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import struct
import os


# items to read from input files 
       # I | unsigned int   | integer 4 | ra, spd, pts_key, tsf, rnm, rn
       # H | unsigned short | integer 2 | momag, apmag, cepra, cepdc, gma, rma, ima, zn2
       # h | short          | integer 2 | pmrac, pmdc, jm, hm, km, bma, vma,  
       # B | unsigned char  | integer 1 | sigmag, objt, dsf, sigra, sigdc, nt, ns, nc, sigpmr, sigpmd 
       #   |                |           | fj, fh, fks, ejm, ehm, ekm, gcflg, leda, x2m
       # b | char           | integer 1 | ebma, evma, egma, erma, eima

z_catalog = '/home/source_cat/UCAC4/u4b'      #r'R:\Git_hub\UCAC4---UCAC5\ucac4_test_folder'       #'/home/source_cat/UCAC4/u4b'     #r'C:\Users\User\Documents\UCAC4---UCAC5\ucac4_test_folder'       # folder with directories
binary_unpack = '=IIHHBBBBBBBBHHhhBBIhhhBBBBBBhhHHHbbbbbBIBBIHI'             # format characters module struct (78 bytes)
col = ['ra', 'spd', 'momag', 'apmag', 'sigmag', 'objt', 'dsf', 'sigra', 'sigdc', 'nt', 'ns', 'nc', 'cepra', 'cepdc', 'pmrac', 'pmdc', 'sigpmr', 'sigpmd', 'pts_key', 'jm', 'hm', 'km', 'fj', 'fh', 'fks', 'ejm', 'ehm', 'ekm', 'bma', 'vma', 'gma', 'rma', 'ima', 'ebma', 'evma', 'egma', 'erma', 'eima', 'gcflg', 'tsf', 'leda', 'x2m', 'rnm', 'zn2', 'rn2']
pg_engine = create_engine('postgresql+psycopg2://user@localhost:5433/test2')
psql = 'select * from "ucac4" limit 300;'


counter_w=0

# walk through files in a directory z_catalog
zfiles = [f for f in os.listdir(z_catalog) if f.startswith("z")]
for z_files in zfiles:
    full_name_path = os.path.join(z_catalog, z_files)
    with open(full_name_path, 'rb') as fin:
        din = True
        all_catalog = []         # creating a temporary list of decoded binary file strings z001....z900
        while din:
            din = fin.read(78)   # 78 bytes one row
            if len(din) == 78:
                list_row = list(struct.unpack(binary_unpack, din)) 
                #print(list_row)
                all_catalog.append(list_row)
        df = pd.DataFrame(all_catalog, columns = col)
        #print(df)
        if counter_w == 0:
            df.to_sql('ucac4', index=False, con=pg_engine)
            counter_w = 1
        else:
            df.to_sql('ucac4', con=pg_engine, index=False, if_exists='append')

pg_df = pd.read_sql_query(psql, con=pg_engine)
print(pg_df)


