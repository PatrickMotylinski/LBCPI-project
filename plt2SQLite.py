#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 22 16:06:53 2019

@author: patrickm
"""

import os
import pandas as pd
from sqlalchemy import create_engine


#### Connect to SQLite. NB!!! Change path accordingly!
wdir = '/home/patrickm/projects/GeoLife/'
database = 'GeoLifeDB'
suffix = ".db"

engine = create_engine('sqlite:///'+wdir+database+suffix, echo=False)
con = engine.connect()  # establish connection
trans = con.begin()     # enable transaction

#### Environment
rootdir = '/home/patrickm/projects/GeoLife/Geolife Trajectories 1.3/Data' 

#### Extract all the user IDs from the names of directories
dirslst    = []
for _, dirs, _ in os.walk(rootdir):
    dirslst.append(dirs)
dirslst = dirslst[0] 

#### List holding all dataframes, each corresponding to a trajectory
dfs = []

#### Loop over users. Remove the dummy column (only containing '0'), add columns for user IDs and track Ids
for uID in dirslst:
    workdir = rootdir + "/" + uID + "/" + "Trajectory/"
    for _, _, files in os.walk(workdir):
        for file in files:
            if os.path.splitext(file)[1] == '.plt':
                trkID = os.path.splitext(file)[0]  
                df = pd.read_csv(workdir+file, skiprows=[i for i in range(6)], header=None)
                df = df.rename(columns={0: 'Lat', 1: 'Long', 2: 'dummy', 3: 'Alt', 4: 'no days', 5: 'date str', 6: 'time str'})
                df.drop('dummy', axis=1, inplace=True)
                df['usrID'] = int(uID)
                df['trkID'] = trkID
                #print (df)
                #dfs.append(df)
                df.to_sql(database, con=engine, if_exists = 'append', index=False )

#### Merge all the dataframes into one
#one_big_table = pd.concat(dfs)

#### Commit to the database and close the file.
trans.commit()
