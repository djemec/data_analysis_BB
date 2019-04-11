#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 13:19:49 2019

@author: domenjemec
"""
import pandas as pd
import os
import re

out_dir = './PROCESSED_DATA'
raw_data_bb ='./RAW_DATA/ex_bb_new.csv'
raw_data_sprint = './RAW_DATA/ex_bb_sprint.csv'
raw_data = pd.DataFrame()
date_cols = ['snapshotStartDate','snapshotEndDate']
cols_to_keep = ['bbid','bbRefCategory','bbRefName','snapshotId','snapshotName',
                'snapshotStatus','snapshotStartDate','snapshotEndDate',
                'engagementId','engagementName','solutionId','solutionName',
                'personHours']

def sprint_parse(text):
    sprint = re.search('sprint \d*\.?\d*', text.lower())
    if sprint is None:
        return 0
    else:
        return pd.to_numeric(re.sub("[a-z]", "", sprint.group(0)), downcast='integer', errors='coerce')

def data_clean(rdf):
    rdf = rdf.dropna(subset = ['personHours', 'snapshotEndDate', 'snapshotStartDate'])
    #rdf['snapshotNum'] = pd.to_numeric(rdf['snapshotName'].str.replace('[A-Za-z ()\-]','', regex=True), errors='coerce')
    rdf['snapshotNum'] = rdf['snapshotName'].apply(sprint_parse)

    rdf = rdf[rdf['personHours'] > 0]
    return rdf

def data_extend(rdf):
    ext_df = rdf.apply(lambda x: pd.Series(
            {
            'expPersonHours':x['personHours']/((x['snapshotEndDate']-x['snapshotStartDate']).days +1),
            'bbid':x['bbid'],
            'date_range':pd.date_range(x['snapshotStartDate'],x['snapshotEndDate'])
            }), 
        axis=1).set_index(['bbid','expPersonHours'])['date_range'].apply(pd.Series).stack().reset_index().drop('level_2',axis=1)
    
    # to remove 0 if needed
    ext_df = ext_df[(ext_df.expPersonHours != 0)]
    
    ext_df.reset_index(inplace=True)
    rdf = pd.merge(pd.merge(rdf, ext_df, how='inner', on='bbid'),
        pd.read_csv(raw_data_sprint).rename(columns={'pkid':'snapshotId'}),
        how='inner', on='snapshotId').drop(['index'],axis=1)
    rdf.rename(columns={0: 'date'}, index=str, inplace=True)
    return rdf

def main():
    global raw_data
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    raw_data = pd.read_csv(raw_data_bb, parse_dates=date_cols, usecols= cols_to_keep)
    
    processed_data = data_clean(raw_data)
    processed_data = data_extend(processed_data)
    processed_data.to_csv(out_dir + '/processed.csv', sep=',', index = False)
    print("Success")

if __name__ == "__main__":
    main()