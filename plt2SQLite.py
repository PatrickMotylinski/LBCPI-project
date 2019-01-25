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
database = 'GeoLifeDBtmp'
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

#### Lists holding trajectories, indexing traject. IDs, dataframes
traj = []
usrdct = {}
dfs = []
####
for uID in dirslst:
    traj = []
    dct = {}
    workdir = rootdir + "/" + uID + "/" + "Trajectory/"
    for _, _, files in os.walk(workdir):
        for file in files:
            if os.path.splitext(file)[1] == '.plt':
                traj.append(os.path.splitext(file)[0])
    traj.sort()
    dct = { trk : traj.index(trk) for trk in traj }       
    usrdct.update( {uID : dct} )
    #print("usrID ", uID, " dict : ", usrdct)
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
                df['usrID']   = int(uID)
                df['trkID']   = trkID
                df['trk no']  = usrdct[str(uID)][str(trkID)]
                if int(uID) < int(len(dirslst)/20.0):
                    df.to_sql(database, con=engine, if_exists = 'append', index=False )
                #dfs.append(df)
                #trklst = df['trkID'].unique().tolist()
                #trklst.sort()
                #dct2 = { trk : trklst.index(trk) for trk in trklst }
                #print (trklst)
                #print(dct2)
                #df['trk no'] = df['trkID'].map(usrdct[uID])
                #df['trk no'] = df['trkID'].
                #print (df)
                #dfs.append(df)
#        df = pd.concat(dfs)
#        trklst = df['trkID'].unique().tolist()
#        trklst.sort()
#        dct2 = { trk : trklst.index(trk) for trk in trklst }
#        print (trklst)
#        print(dct2)
#        df['trk no'] = df['trkID'].map(usrdct[uID])
#        df.to_sql(database, con=engine, if_exists = 'append', index=False )
    
#### Merge all the dataframes into one
#one_big_table = pd.concat(dfs)

#### Commit to the database and close the file.
trans.commit()
