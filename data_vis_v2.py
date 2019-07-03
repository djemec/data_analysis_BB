#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 13:19:49 2019

@author: domenjemec
"""
import pandas as pd
import numpy as np
import os

import matplotlib.pyplot as plt 

plt.style.use('seaborn-whitegrid')
out_dir = './PROCESSED_DATA'
raw_data_bb = './RAW_DATA/app_feature_raw.xlsx'
raw_dev_data_bb = './RAW_DATA/dev_tasks_raw.xlsx'
raw_data_sheet = 'Sheet1'
raw_data = pd.DataFrame()
date_cols = ['Start Date', 'End Date']
processed_template = 'Processed_Template_v2.xlsx'
chart_label_font = dict({'fontsize':14})
chart_title_font = dict({'fontweight':'bold', 'fontsize':18})


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

def cal_kpi_value(rdf, value_field, div_by_time):
    avg_df = rdf[['Category Id','Category Name',value_field,'Daily Time Spent Hrs']]
    avg_df['weight_raw'] = \
    (avg_df['Daily Time Spent Hrs']/avg_df.groupby('Category Name')['Daily Time Spent Hrs'].transform("sum") \
     * avg_df[value_field]/avg_df['Daily Time Spent Hrs']) \
     if div_by_time else \
     (avg_df['Daily Time Spent Hrs']/avg_df.groupby('Category Name')['Daily Time Spent Hrs'].transform("sum") * avg_df[value_field])
    avg_df_value = avg_df.groupby('Category Name')['weight_raw','Daily Time Spent Hrs'].apply(sum).reset_index()
        
    return avg_df_value

def plot_business_value(rdf, value_field, div_by_time):
    x = rdf['Daily Time Spent Hrs']
    y = rdf['weight_raw']
    f = plt.figure()
    plt.scatter(x,y , marker='o')
    plt.xlabel('Total Time [hrs]')
    if div_by_time:
        plt.ylabel( value_field + ' Density [avg value/hr ]')
    else:
        plt.ylabel(value_field + ' [avg value]')
    
    for i, txt in enumerate(rdf['Category Name']):
        plt.annotate(txt, (rdf['Daily Time Spent Hrs'][i], rdf['weight_raw'][i]))
    
    fileEnd = '_density_plot.pdf' if div_by_time else '_avg_plot.pdf'
    f.savefig(out_dir + '/' + value_field.replace(' ','_') + fileEnd,  bbox_inches='tight')
    
def plot_graphs(plt, rdf, value_field, title, div_by_time):
    
    x = rdf['Daily Time Spent Hrs']
    y = rdf['weight_raw']
    plt.scatter(x,y , marker='o')
    
    title_update =  title + (' (Normalized)' if div_by_time else '')
    plt.set_title(title_update, chart_title_font)
    plt.set_xlabel('Total Time [hrs]', chart_label_font)
    ylabel = value_field + (' Density [avg value/hr ]' if div_by_time else ' [avg value]')
    plt.set_ylabel(ylabel, chart_label_font)    
    for i, txt in enumerate(rdf['Category Name']):
        plt.annotate(txt, (rdf['Daily Time Spent Hrs'][i], rdf['weight_raw'][i]))
    
    return plt

def main():
    global raw_data
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    
    # app features
    raw_data = pd.read_excel(raw_data_bb, sheet_name=raw_data_sheet, parse_dates= date_cols)
    processed_data = data_clean(raw_data)
    processed_data = data_extend(processed_data)
    processed_data.to_csv(out_dir + '/processed_app_feature_v2.csv', sep=',', index = False)
    
    print("Features Complete")
    
    # dev tools
    dev_data = pd.read_excel(raw_dev_data_bb, sheet_name=raw_data_sheet, parse_dates= date_cols)
    processed_dev_data = data_clean(dev_data)
    processed_dev_data = data_extend(processed_dev_data)
    processed_dev_data.to_csv(out_dir + '/processed_dev_v2.csv', sep=',', index = False)
    
    print("Dev Task")
        
    avg_density_df = cal_kpi_value(processed_data, 'Business Value', True)
    avg_density_df.to_csv(out_dir + '/processed_business_value_density_v2.csv', sep=',', index = False)
    avg_df = cal_kpi_value(processed_data, 'Business Value', False)
    avg_df.to_csv(out_dir + '/processed_business_value_v2.csv', sep=',', index = False)    
    avg_density_dev_df = cal_kpi_value(processed_dev_data, 'Complexityvalue', True)
    avg_density_dev_df.to_csv(out_dir + '/processed_complexity_value_density_v2.csv', sep=',', index = False)
    avg_dev_df = cal_kpi_value(processed_dev_data, 'Complexityvalue', False)
    avg_dev_df.to_csv(out_dir + '/processed_complexity_value_v2.csv', sep=',', index = False)
    
    fig, axes = plt.subplots(2, 2)
    axes[0, 0] = plot_graphs(axes[0, 0], avg_df, 'Business Value', 'App Feature', False)
    axes[0, 1] = plot_graphs(axes[0, 1], avg_density_df, 'Business Value', 'App Feature',True)
    axes[1, 0] = plot_graphs(axes[1, 0], avg_dev_df, 'Complexity Value', 'Dev Task', False)
    axes[1, 1] = plot_graphs(axes[1, 1], avg_density_dev_df, 'Complexity Value', 'Dev Task', True)
    fig.set_size_inches(22, 16)
    # print(type(fig))
    fig.savefig(out_dir + '/' + 'kpi_plot.pdf',  bbox_inches='tight')
    
    print("Plotting Complete Task")

if __name__ == "__main__":
    main()