#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import math
import sys
import matplotlib.pyplot as plt


def existence_assertion(df, case_num, flag = None):
    '''
    CASE 1: Every record in Crash dataframe should have a valid not null crash id and serial number.
    CASE 2: Every record in Vehicle dataframe should have a valid not null vehicle id 
    CASE 3: Every record in Participant dataframe should have a valid not null participant id

    '''
    print("\n=====================EXISTENCE VALIDATIONS================================")
    if(flag == 0):
        case_num = case_num + 1
        print("\n----- CASE {}: Every CRASH record should have not Null crash ID And Serial #".format(case_num))
        invalid_record_count = 0
        for i, row in df.iterrows():
            serial = row['Serial #']
            crash_id = row['Crash ID']
            if( math.isnan(serial)):
                print("--------------EXISTENCE ASSERTION VOILATION!! Serial Number and Vehicle ID should not be NULL for CRASH Records-----------")
                #df.drop(df.index[i])
                invalid_record_count += 1
        if(invalid_record_count == 0):
            print("All the CRASH records passed Case {} check!".format(case_num))
    
    if(flag == 1):
        case_num = case_num + 1
        print("\n----- CASE {}: Every record of VEHICLE DF has a vehicle ID field".format(case_num))
        invalid_record_count = 0
        for i, row in df.iterrows():
            veh_id = row['Vehicle ID']
            if( math.isnan(veh_id)):
                print("--------------EXISTENCE ASSERTION VOILATION!! Vehicle ID should be NOT NULL value for VEHICLE records----------")
                #df.drop(df.index[i])
                invalid_record_count += 1
        if(invalid_record_count == 0):
            print("All the records passed Case {} check!".format(case_num))
    if(flag == 2): 
        case_num = case_num + 1
        print("\n------CASE {}: Every record of participant DF should have a NOT NULL Participant ID field.-------".format(case_num))
        invalid_record_count = 0
        for i, row in df.iterrows():
            part_id = row['Participant ID']
            if(math.isnan(part_id)):
                print("--------------EXISTENCE ASSERTION VOILATION!! Particpant_ID should be NOT NULL values for PARTICIPANT records-----------")
                #df.drop(df.index[i])
                invalid_record_count += 1
        if(invalid_record_count == 0):
            print("All the records passed Case {} check!".format(case_num))    
    
    return case_num


def limit_assertion(df, case_num):
    '''
    Case 4: The date field should be between 1/1/2019 and 12/31/2019 inclusive
    Case 5: The crash hour should be between 00 and 23 inclusive and 99 for unknown time
    Case 6: The County Code that identifies the county in which the crash occurred should be in between 01 and 36 both inclusive.
    '''
    print("\n=====================LIMIT VALIDATIONS================================")
    case_num = case_num + 1
    print("\n----- CASE {}: Check the date field should be between 1/1/2019 and 12/31/2019 inclusive--------".format(case_num))
    
    invalid_record_count = 0
    for i, row in df.iterrows():
        if(row['Record Type'] == 1):
            mm = int(row['Crash Month'])
            dd = int(row['Crash Day'])
            yyyy = int(row['Crash Year'])
            if(mm > 0 and mm < 13 and dd > 0 and dd < 32 and yyyy == 2019):
                pass
            else:
                print("--------------LIMIT ASSERTION VOILATION!!Invalid date!-----------")
                #df.drop(df.index[i])
                invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the CRASH records passed Case {} check!".format(case_num))
    
    case_num = case_num + 1
    print("\n----- CASE {}: Check the crash hour should be between 00 and 23 inclusive and 99 for unknown time".format(case_num))
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
                print("--------------LIMIT ASSERTION VOILATION!!Invalid Crash Hour!-----------")
                #df.drop(df.index[i])
                invalid_record_count += 1
    if(invalid_record_count == 0):
        print("All the CRASH records passed Case {} check!".format(case_num))
    if(unknown_time_count>0):
        print("Time at which crash occured is unknown for {} records".format(unknown_time_count))
    
    case_num = case_num + 1
    print("\n----- CASE {}: Check the County Code that identifies the county in which the crash occurred should be in between 01 and 36 both inclusive.".format(case_num))
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
        print("All the CRASH records passed Case {} check!".format(case_num))
    return case_num


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
                print("--------------ASSERTION VOILATION!! Vehicle ID should be NOT NULL value for record type 2----------")
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



def statistical_distribution(df, case_num):
    case_num = case_num + 1
    print("\n------CASE {}: Crashes are evenly/uniformly distributed throughout the year-------".format(case_num))
    df['Crash Month'].value_counts().sort_index().plot(kind = 'bar')
    plt.xlabel('Crash Month')
    plt.ylabel('Number of crashes')

    plt.show()
    return case_num


# In[21]:


def main():
    # show all the rows and columns
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    df = pd.read_csv('oregon-crash2019.csv')


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
    CrashesDF = df[df['Record Type'] == 1]
    VehiclesDF = df[df['Record Type'] == 2]
    ParticipantsDF = df[df['Record Type'] == 3]

    CrashesDF = CrashesDF.dropna(axis=1,how='all')
    VehiclesDF = VehiclesDF.dropna(axis=1,how='all')
    ParticipantsDF = ParticipantsDF.dropna(axis=1,how='all')
    
    case_num = 0
    print("--------------Crash DATAFRAME-------------\n")
    case_num = existence_assertion(CrashesDF, case_num, flag = 0)
    case_num = limit_assertion(CrashesDF, case_num)
    case_num = statistical_distribution(CrashesDF, case_num)
    
    print("\n------------Vehicle DATAFRAME-----------\n")
    case_num = existence_assertion(VehiclesDF, case_num, flag = 1)
    
    print("\n------------Participant DATAFRAME--------\n")
    case_num = existence_assertion(ParticipantsDF, case_num, flag = 2)


# In[22]:


if __name__ == "__main__":
    main()






