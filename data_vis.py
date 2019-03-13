#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 13:19:49 2019

@author: domenjemec
"""
import pandas as pd
import os

out_dir = './PROCESSED_DATA'
raw_data = pd.DataFrame()
date_cols = ['snapshotStartDate','snapshotEndDate']
cols_to_keep = ['bbid','bbRefCategory','bbRefName','snapshotId','snapshotName',
                'snapshotStatus','snapshotStartDate','snapshotEndDate',
                'engagementId','engagementName','solutionId','solutionName',
                'personHours']


def data_clean(rdf):
    rdf = rdf.dropna(subset = ['personHours', 'snapshotEndDate', 'snapshotStartDate'])
    rdf = rdf[rdf['personHours'] > 0]
    return rdf

def data_extend(rdf):
    bbid_set = list(set(rdf['bbid']))
    rdf['dayAvgPersonHrs'] = rdf.apply(lambda avg: avg['personHours']/
       ((avg['snapshotEndDate']-avg['snapshotStartDate']).days +1),axis=1)
    
    index = pd.MultiIndex.from_product([bbid_set,pd.date_range(
            start = rdf.snapshotStartDate.min(), 
            end = rdf.snapshotEndDate.max())], 
            names=['bbid', 'date'])
    ext_df = pd.DataFrame(index = index, data = {'expPersonHours':0})

    dates_list = ext_df.index.get_level_values(1)
    bbID_list = ext_df.index.get_level_values(0) 
    
    for row in rdf.itertuples():
        ext_df.expPersonHours[(bbID_list == row.bbid) & (dates_list>=row.snapshotStartDate) & (dates_list<= row.snapshotEndDate)] += row[14]
        
    # to remove 0 if needed
    ext_df = ext_df[(ext_df.expPersonHours != 0)]
    
    ext_df.reset_index(inplace=True)
    rdf = pd.merge(rdf, ext_df, how='inner', on='bbid')

    return rdf

def main():
    global raw_data
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    raw_data = pd.read_csv('./RAW_DATA/ex_bb_new.csv', parse_dates=date_cols, usecols= cols_to_keep)
    
    processed_data = data_clean(raw_data)
    processed_data = data_extend(processed_data)
    processed_data.to_csv(out_dir + '/processed.csv',sep=',')
    print("Success")

if __name__ == "__main__":
    main()