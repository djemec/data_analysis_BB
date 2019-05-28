#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 13:19:49 2019

@author: domenjemec
"""
import pandas as pd
import numpy as np
import os
import re

out_dir = './PROCESSED_DATA'
raw_data_bb = './RAW_DATA/app_feature_raw.xlsx'
raw_data_sheet = 'Sheet1'
raw_data = pd.DataFrame()
date_cols = ['Start Date', 'End Date']
processed_template = 'Processed_Template_v2.xlsx'


def data_clean(rdf):
    rdf = rdf.dropna(subset = ['Sprint Id','Sprint Num','Num Weeks','Num Developers','Time Spent Value'])
    rdf = rdf[rdf['Time Spent Value'] > 0]
    rdf['Total Time Spent Value'] = rdf.groupby('Sprint Id')['Time Spent Value'].transform(np.sum)
    rdf['Total Time Spent Hrs'] = rdf['Time Spent Value']/rdf['Total Time Spent Value']*40*rdf['Num Developers']

    
    return rdf

def data_extend(rdf):
    ext_df = rdf.apply(lambda x: pd.Series(
            {
            'Daily Time Spent Hrs':x['Total Time Spent Hrs']/((x['End Date']-x['Start Date']).days +1),
            'Pk Id':x['Pk Id'],
            'date_range':pd.date_range(x['Start Date'],x['End Date'])
            }), 
        axis=1).set_index(['Pk Id','Daily Time Spent Hrs'])['date_range'].apply(pd.Series).stack().reset_index().drop('level_2',axis=1)

    # to remove 0 if needed
    ext_df = ext_df[(ext_df['Daily Time Spent Hrs'] != 0)]    
    ext_df.reset_index(inplace=True)
    rdf = pd.merge(rdf, ext_df, how='inner', on='Pk Id').drop(['index'],axis=1)
    rdf.rename(columns={0: 'date'}, index=str, inplace=True)
    return rdf

def main():
    global raw_data
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    raw_data = pd.read_excel(raw_data_bb, sheet_name=raw_data_sheet, parse_dates= date_cols)
    processed_data = data_clean(raw_data)
    processed_data = data_extend(processed_data)
    processed_data.to_csv(out_dir + '/processed_v2.csv', sep=',', index = False)

    print("Success")

if __name__ == "__main__":
    main()