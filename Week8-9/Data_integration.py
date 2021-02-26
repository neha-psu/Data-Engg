import json 
import csv 
import pandas as pd
import numpy as np
import sys
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta
import seaborn as sns


def acs_group_by_county(df):
    d = {"State":"state","County":"county","TotalPop":"Population","totalIncome":"PerCapitaIncome","totalPoverty":"Poverty"}
    df['totalIncome'] = df.TotalPop * df.IncomePerCap
    df['totalPoverty'] = df.TotalPop * df.Poverty * 0.01        
    df.drop(columns=['IncomePerCap','Poverty'],inplace=True, axis=1)
    df = df.groupby(["State", "County"]).agg({'TotalPop':'sum', 'Men':'sum','Women':'sum',\
                                              'Hispanic':'mean', 'White':'mean', 'Black':'mean', \
                                              'Native':'mean', 'Asian':'mean', 'Pacific':'mean',  \
                                              'VotingAgeCitizen':'sum', 'Income':'mean', 'IncomeErr':'mean', \
                                              'totalIncome':'sum', 'IncomePerCapErr':'mean', 'totalPoverty':'sum',  \
                                              'ChildPoverty':'mean', 'Professional':'mean', 'Service':'mean',   \
                                              'Office':'mean', 'Construction':'mean', 'Production':'mean',   \
                                              'Drive':'mean', 'Carpool':'mean', 'Transit':'mean', \
                                              'Walk':'mean', 'OtherTransp':'mean', 'WorkAtHome':'mean',   \
                                              'MeanCommute':'mean', 'Employed':'sum', 'PrivateWork':'mean',  \
                                              'PublicWork':'mean', 'SelfEmployed':'mean', 'FamilyWork':'mean', \
                                              'Unemployment':'mean'}).reset_index()
    df['totalIncome'] = df.totalIncome / df.TotalPop
    df['totalPoverty'] = 100*(df.totalPoverty / df.TotalPop)  
    df = df.rename(columns=d)
    out = df.loc[(df['county'] == 'Washington County') & (df['state'] == 'Oregon')]
    #print(out)
    return df



def simplify_covid(df):
    df1 = df.groupby(["county", "state"]).agg({'cases':'sum', 'deaths':'sum'}).reset_index()
    d = {"cases":"TotalCases","deaths":"TotalDeaths"}
    df1 = df1.rename(columns=d)
    df1.filter(['county','state','TotalCases','TotalDeaths'])
    #print(df1.head())
    
    df2 = df.loc[(df['date'] >= '2020-12-01') & (df['date'] <= '2020-12-31')]
    df2 = df2.groupby(["county", "state"]).agg({'cases':'sum', 'deaths':'sum'}).reset_index()
    d = {"cases":"Dec2020Cases","deaths":"Dec2020Deaths"}
    df2 = df2.rename(columns=d)
    df2.filter(['county','state','Dec2020Cases','Dec2020Deaths'])
    #print(df2.head())
    
    coviddf=df1.merge(df2, on=['county','state'], how='inner')
    coviddf["TotalDeaths"] = coviddf["TotalDeaths"].astype('Int32')
    coviddf["Dec2020Deaths"] = coviddf["Dec2020Deaths"].astype('Int32')

    
    coviddf['county'] = coviddf['county'].astype(str) + ' County'
    #print(coviddf.head())
    
    out = coviddf.loc[(coviddf['county'] == 'Loudoun ') & (coviddf['state'] == 'Oregon')]
    #print(out)
    
    return coviddf



def integrate(acs_df, covid_df):
    '''
    out = covid_df.loc[(covid_df['state'] == 'Oregon')]
    print(out)
    out1 = acs_df.loc[(acs_df['state'] == 'Oregon')]
    print(out1)
    '''
    acs_df1 = acs_df.filter(['county','state','Population','Poverty','PerCapitaIncome'])
    integrated_df=covid_df.merge(acs_df1, on=['county','state'], how='inner')

    out1 = integrated_df.loc[(integrated_df['state'] == 'Oregon')].reset_index()
    out1.drop(columns =['index'],inplace=True, axis=1)
    out1.to_csv('out1.csv',index=False,na_rep='None')

    #print(integrated_df.head())
    integrated_df['norm_TotalCases'] = (integrated_df.TotalCases * 100000)/integrated_df.Population
    integrated_df['norm_TotalDeaths'] = (integrated_df.TotalDeaths * 100000)/integrated_df.Population
    integrated_df['norm_Dec2020Cases'] = (integrated_df.Dec2020Cases * 100000)/integrated_df.Population
    integrated_df['norm_Dec2020Deaths'] = (integrated_df.Dec2020Deaths * 100000)/integrated_df.Population
    #print(integrated_df)
    out = integrated_df.loc[(integrated_df['state'] == 'Oregon')].reset_index()
    out.drop(columns =['index'],inplace=True, axis=1)
    out.to_csv('out.csv',index=False,na_rep='None')
    return integrated_df, out


def main(acs2017, covid19):
    # show all the rows and columns
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    pd.set_option('display.expand_frame_repr', False)
    #Read the json file into the dataframe
    acs_df = pd.read_csv(acs2017)
    covid_df = pd.read_csv(covid19)
    #print(acs_df.head())
    #print(covid_df.head())
    
    # ========= A. Aggregate Census Data to County Level============== 
    acs_df = acs_group_by_county(acs_df)
    #print(acs_df.head())
    
    
    # ========= B. Simplify the COVID Data ==========================
    covid_df = simplify_covid(covid_df)
    
    # ========= C. Integrate COVID Data with ACS Data ===============
    df, oregon_df = integrate(acs_df, covid_df)
    #df.dtypes()
    
    # ========= D. Analysis ========================================
    R = df['norm_TotalCases'].corr(df['Poverty'])
    R1 = df['norm_TotalDeaths'].corr(df['Poverty'])
    R2 = df['norm_TotalCases'].corr(df['PerCapitaIncome'])
    R3 = df['norm_TotalDeaths'].corr(df['PerCapitaIncome'])
    R4 = df['norm_Dec2020Cases'].corr(df['Poverty'])
    R5 = df['norm_Dec2020Deaths'].corr(df['Poverty'])
    R6 = df['norm_Dec2020Cases'].corr(df['PerCapitaIncome'])
    R7 = df['norm_Dec2020Deaths'].corr(df['PerCapitaIncome'])
    print(R,"\n",R1,"\n",R2,"\n",R3,"\n",R4,"\n",R5,"\n",R6,"\n",R7)

    R = oregon_df['norm_TotalCases'].corr(oregon_df['Poverty'])
    R1 = oregon_df['norm_TotalDeaths'].corr(oregon_df['Poverty'])
    R2 = oregon_df['norm_TotalCases'].corr(oregon_df['PerCapitaIncome'])
    R3 = oregon_df['norm_TotalDeaths'].corr(oregon_df['PerCapitaIncome'])
    R4 = oregon_df['norm_Dec2020Cases'].corr(oregon_df['Poverty'])
    R5 = oregon_df['norm_Dec2020Deaths'].corr(oregon_df['Poverty'])
    R6 = oregon_df['norm_Dec2020Cases'].corr(oregon_df['PerCapitaIncome'])
    R7 = oregon_df['norm_Dec2020Deaths'].corr(oregon_df['PerCapitaIncome'])
    print("\n\n",R,"\n",R1,"\n",R2,"\n",R3,"\n",R4,"\n",R5,"\n",R6,"\n",R7)
    
    sns_plot = sns.scatterplot(data=oregon_df, x="norm_TotalDeaths", y="PerCapitaIncome")
    sns_plot.figure.savefig("output.png")  
    
if __name__ == "__main__":
    main('acs2017_census_tract_data.csv', 'COVID_county_data.csv')