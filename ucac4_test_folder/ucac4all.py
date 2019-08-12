#!/usr/bin/env python3
# coding: utf-8

import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import struct
import os


z_catalog = r'C:\Users\User\Documents\UCAC4---UCAC5\ucac4_test_folder'     # folder with directories
binary_unpack = 'iihhbbbBBbbbhhhhBB'  # format characters module struct (78 bytes)
col = ['ra', 'spd', 'momag', 'apmag', 'sigmag', 'objt', 'dsf', 'sigra', 'sigdc', 'nt', 'ns', 'nc', 'cepra', 'cepdc', 'pmrac', 'pmdc', 'sigpmr', 'sigpmd', 'pts_key', 'jm', 'hm', 'km', 'fj', 'fh', 'fks', 'ejm', 'ehm', 'ekm', 'bma', 'vma', 'gma', 'rma', 'ima', 'ebma', 'evma', 'egma', 'erma', 'eima', 'gcflg', 'tsf', 'acmf', 'bcmf', 'hcmf', 'zcmf', 'bbcmf', 'lcmf', 'ncmf', 'scmf', 'leda', 'x2m', 'rnm', 'zn2', 'rn2']
#pg_engine = create_engine('postgresql+psycopg2://user@localhost:5433/test2')
#psql = 'select * from "ucac4" limit 300;'


counter_w=0

# walk through files in a directory z_catalog
zfiles = [f for f in os.listdir(z_catalog) if f.startswith("z001")]
for z_files in zfiles:
    full_name_path = os.path.join(z_catalog, z_files)
    with open(full_name_path, 'rb') as fin:
        din = True
        n = 0
        all_catalog = []   # creating a temporary list of decoded binary directory strings z326....z900
        s = []    # creation of a temporary list from binary directory line numbering z326....z900
        while din:
            din = fin.read(30)   # 78 bytes one row
            if len(din) == 30:
                list_row = list(struct.unpack(binary_unpack, din)) 
                print(list_row)
                #all_catalog.append(list_row)
                #n = n + 1
                #s.append(str(n).zfill(6))
        #zn = ''.join((z_files.lstrip('z'), '-')).split() * all_catalog.__len__() 
        #idn = [zn[i] + s[i] for i in range(len(s))]  
        #df = pd.DataFrame(all_catalog, index = idn, columns = col)
        #print(df)
        #if counter_w == 0:
            #df.to_sql('ucac4', con=pg_engine)
            #counter_w = 1
        #else:
            #df.to_sql('ucac4', con=pg_engine, if_exists='append')

#pg_df = pd.read_sql_query(psql, con=pg_engine)
#print(pg_df)


#  https://irsa.ipac.caltech.edu/data/UCAC4/ucac4.html
# https://irsa.ipac.caltech.edu/data/UCAC4/readme_u4.txt
# https://docs.python.org/3/library/struct.html