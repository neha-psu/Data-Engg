#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import math


# In[2]:


def existence_assertion(df):
    '''
    CASE 1: Check if any record has NULL crash ID
    CASE 2: Check if any record has NULL record type
    '''
    print("\n=====================EXISTENCE VALIDATIONS================================")
    print("\n----- CASE 1: Check if any record has NULL crash ID--------")
    invalid_record_count = 0
    for item, data in enumerate(df['Crash ID']):
        if( math.isnan(data)):
            print("--------------Invalid!! record with NULL Crash ID-------------")
            df.drop(df.index[item])
            invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the records passed Case 1 check!")
    
 
    print("\n----- CASE 2: Check if any record has NULL record type--------")
    invalid_record_count = 0
    for item, data in enumerate(df['Record Type']):
        if( math.isnan(data)):
            print("--------------Invalid!! record with NULL Record Type-------------")
            df.drop(df.index[item])    
            invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the records passed Case 2 check!")
    
    return df


# In[3]:


def limit_assertion(df):
    '''
    Case 3: The date field should be between 1/1/2019 and 12/31/2019 inclusive
    Case 4: The crash hour should be between 00 and 23 inclusive and 99 for unknown time
    Case 5: The County Code that identifies the county in which the crash occurred should be in between 01 and 36 both inclusive.
    '''
    print("\n=====================LIMIT VALIDATIONS================================")
    print("\n----- CASE 3: Check the date field should be between 1/1/2019 and 12/31/2019 inclusive--------")
    invalid_record_count = 0
    for i, row in df.iterrows():
        if(row['Record Type'] == 1):
            mm = int(row['Crash Month'])
            dd = int(row['Crash Day'])
            yyyy = int(row['Crash Year'])
            if(mm > 0 and mm < 13 and dd > 0 and dd < 32 and yyyy == 2019):
                pass
            else:
                print("--------------Invalid date!-----------")
                #df.drop(df.index[i])
                invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the CRASH records passed Case 3 check!")
    
    print("\n----- CASE 4: Check the crash hour should be between 00 and 23 inclusive and 99 for unknown time")
    invalid_record_count = 0
    unknown_time_count = 0
    for i, row in df.iterrows():
        if(row['Record Type'] == 1):
            hr = row['Crash Hour']
            if((hr >= 0 and hr < 24) or hr ==99):
                if(hr ==99):
                    #print("Time at which crash occured is unknown")
                    unknown_time_count += 1
                else:
                    pass
            else:
                print("--------------ASSERTION VOILATION!!Invalid Crash Hour!-----------")
                #df.drop(df.index[i])
                invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the CRASH records passed Case 4 check!")
    if(unknown_time_count>0):
        print("Time at which crash occured is unknown for {} records".format(unknown_time_count))
        
    print("\n----- CASE 5: Check the County Code that identifies the county in which the crash occurred should be in between 01 and 36 both inclusive.")
    invalid_record_count = 0
    for i, row in df.iterrows():
        if(row['Record Type'] == 1):
            county = row['County Code']
            if(county >= 1 and county < 37):
                pass
            else:
                print("--------------ASSERTION VOILATION!!Invalid County Code!-----------")
                #df.drop(df.index[i])
                invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the CRASH records passed Case 5 check!")
    return df


# In[4]:


def referential_integrity_assertions(df):
    '''
    CASE 6: Every record with record type = 1 has a serial#
    CASE 7: Every record with record type = 2 has a vehicle ID field
    CASE 8: Every record with record type = 3 should have both Vehicle ID and Participant ID field.
    '''
    print("\n=====================REFRENTIAL INTEGRITY VALIDATIONS================================")
    print("\n----- CASE 6: Every record with record type = 1 has a serial#")
    invalid_record_count = 0
    for i, row in df.iterrows():
        if(row['Record Type'] == 1):
            serial = row['Serial #']
            if( math.isnan(serial)):
                print("--------------ASSERTION VOILATION!! Serial Number should be NOT NULL for record type 1-----------")
                df.drop(df.index[i])
                invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the CRASH records passed Case 6 check!")
        
    print("\n----- CASE 7: Every record with record type = 2 has a vehicle ID field")
    invalid_record_count = 0
    for i, row in df.iterrows():
        if(row['Record Type'] == 2):
            veh_id = row['Vehicle ID']
            if( math.isnan(veh_id)):
                print("--------------ASSERTION VOILATION!! Vehicle ID should be NOT NULL value fro record type 2----------")
                df.drop(df.index[i])
                invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the records passed Case 7 check!")
        
    print("\n------CASE 8: Every record with record type = 3 should have both Vehicle ID and Participant ID field.-------")
    invalid_record_count = 0
    for i, row in df.iterrows():
        if(row['Record Type'] == 3):
            veh_id = row['Vehicle ID']
            part_id = row['Participant ID']
            if( math.isnan(veh_id) and math.isnan(part_id)):
                print("--------------ASSERTION VOILATION!! Vehicle ID and Particpant_ID should be NOT NULL values for record type 3-----------")
                df.drop(df.index[i])
                invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the records passed Case 8 check!")    
    
    return df


# In[5]:


def main():
    # show all the rows and columns
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    df = pd.read_csv ('oregon-crash2019.csv')

    float_col = df.select_dtypes(include=['float64'])
    float_col_list = list(float_col.columns.values)
    ignore = ['Distance from Intersection', 'Latitude Seconds', 'Longitude Seconds']
    for col in float_col_list:
        if(col in ignore):
            pass
        else:
            df[col] = df[col].astype('Int64')
    #print(df.dtypes)
    first5 = df.head()
    #print(first5)
    
    df = existence_assertion(df)
    df = limit_assertion(df)
    df = referential_integrity_assertions(df)


# In[6]:


if __name__ == "__main__":
    main()

