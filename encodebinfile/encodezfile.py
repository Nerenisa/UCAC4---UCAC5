#!/usr/bin/env python3
# coding: utf-8

import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import struct
import os
import sys


binary_pack = '=IIHHBBBBBBBBHHhhBBIhhhBBBBBBhhHHHbbbbbBIBBIHI'
pg_engine = create_engine('postgresql+psycopg2://user@localhost:5433/test2')
s = list(range(0, 648000000, 720000))       # 
d = list(range(720000, 648720000, 720000))
z = 0
for i, x in zip(s, d):
    files = []
    psql = ' '.join(('select * from "ucac4" where spd >=', str(i), 'and', 'spd <', str(x), 'order by ra;'))
    pg_df = pd.read_sql_query(psql, con=pg_engine)
    #print(pg_df)
    z = z + 1
    files.append(''.join(('z', str(z).zfill(3))))
    for row in pg_df.itertuples(index=False, name=False):
        #print(row)
        binary = struct.pack(binary_pack, *row)
        for file in files:
            with open(file, 'ab') as fout:
                fout.write(binary)
            