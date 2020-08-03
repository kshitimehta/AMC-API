# Preprocess pipeline for AMC
# Authors: Augusto Espin, Kshiti Mehta
# DS4CG 2020
# UMass

import pandas as pd
import numpy as np
from geopy import Point
from geo_amc import GeoOperations
from os import listdir
from os.path import isfile, join
import re
import hashlib
import random
import string
from mimesis import Person
from mimesis import Address
from mimesis import Generic
from sys import exit



class preprocess:
    def __init__(self, geo):
        '''
        Initialize lists of data that needs to be validated and object for geographic operations
        '''
        self.geo = geo

    def join_files(self, path, year):
        '''
        Join quarter csv files that are in path over the year specified. 
        The expected filename has the following structure: Q1_year_*.csv
        '''
        # Find all files in path
        file_list = [f for f in listdir(path) if isfile(join(path, f)) and re.match(f'^Q\\d_{year}\\w*.csv$', f)]
        # Sort them in order of Q
        file_list.sort()
        # Read files in pandas list
        file_Q = []
        for f in file_list:
            print(f'Reading file {path}/{f}...')
            df = pd.read_csv(join(path,f))
            file_Q.append(df)
        
        amc_raw = pd.concat(file_Q)

        return amc_raw

    def filter_rate_category(self, raw_df):
        '''
        Filter by rate_category. Only value of 'room' are valid data. Return both dataframes
        '''
        # Get everything different than room (Should be discarded)
        df_wr = raw_df[raw_df['rate_category'] != 'room']
        # Get everyhing with room (equivalent to dropna in building_code)
        df = raw_df[raw_df['rate_category'] == 'room']

        return (df,df_wr)

    def validate_data(self, filtered_df):
        '''
        Validate data using geographic information. Input should be filtered data.
        Output is gonna be two dataframes, one with validated data and one with empty 
        guest address information
        '''
        
        prev_reservation = None
        zip_lst = []
        st_lst = []
        co_lst = []
        city_lst = []
        loc = {}

        def preprocess_guest_location(row):
            '''
            This function will be used with apply to validate all data.
            Private function
            '''
            nonlocal prev_reservation, zip_lst, st_lst, co_lst, city_lst, loc 
            reservation_n = row['reservation_number']
            country = row['country_code']
            zip_c = row['zip_postal_code']
            
            # This is the case where previous record is part of the same party
            if prev_reservation == reservation_n:
                zip_lst.append(loc['zip'])
                st_lst.append(loc['state'])
                co_lst.append(loc['country'])
                city_lst.append(loc['city'])
            else:
            # Different record, do the lookup
                loc = self.geo.get_coordinates_from_address(f'{zip_c}, {country}')
                if loc['country'] == 'CA':
                    if loc['zip'] != None:
                        loc['zip'] += " 1X1"
                zip_lst.append(loc['zip'])
                st_lst.append(loc['state'])
                co_lst.append(loc['country'])
                city_lst.append(loc['city'])
                
            # Store previous reservation    
            prev_reservation = reservation_n
            # -- End of function --

        # Sort the data by reservation number
        sorted_df = filtered_df.sort_values(by=['reservation_number'])

        # Modify arrival and departure as dates
        valid_df = sorted_df.dropna(subset=['country_code'])
        invalid_df = sorted_df[sorted_df['country_code'].isnull()]

        # Verify all data
        _ = valid_df.apply(lambda row: preprocess_guest_location(row), axis=1)

        # Update fields with validated data
        _valid_df = valid_df.copy()
        _valid_df['zip_postal_code'] = zip_lst
        _valid_df['state_province_code'] = st_lst
        _valid_df['country_code'] = co_lst
        _valid_df['city_code'] = city_lst

        # Create Stay_Date field
        _valid_df.insert(0,'Stay_Date', value = pd.to_datetime(dict(year=sorted_df.Stay_Year, 
                                                                    month=sorted_df.Stay_Month, 
                                                                    day=sorted_df.Stay_Day)))
        
        # Correct fields with dates 
        _valid_df['arrival_date'] = pd.to_datetime(valid_df['arrival_date'] )
        _valid_df['departure_date'] = pd.to_datetime(valid_df['departure_date'] )

        # Drop NA in zip_postal_code and update invalids
        valid_df = _valid_df.dropna(subset = ['zip_postal_code'])
        invalid_df1 = _valid_df[_valid_df['zip_postal_code'].isnull()]
        invalid_df = pd.concat([invalid_df,invalid_df1])

        return (valid_df, invalid_df)

    def generate_UID(self, valid_df, generate_fake = False):
        '''
        Generate UIDs using PII fields in the data. Input should be validated data.
        Output is gonna be two dataframes, one with mapping data and one with updated validated data attached with a UID column in the  
        guest reservation data.       
        '''

        guest_data = valid_df
  
        #Taking as input the PII columns used to generate the UID 
        column_names = ["first_name","last_name", "address_1", "address_2","city_code","state_province_code","zip_postal_code", "phone_number", "home_phone_number", "cell_phone_number", "email_address", "internet_address"]

        counter = 0

        #creating a new column UID
        guest_data["UID"] = guest_data[column_names].apply(lambda x: '$'.join(x.astype(str)), axis = 1).str.replace(r'[^$@\w\s]','')


        new_column = "UID"
        #Generating mapping data for the PII fields and UID associated with it
        mapping_data = [column_names]
        mapping_data[counter].append(str(new_column))

        #Generating the hash values
        for i,row in guest_data.iterrows():
            if (i%1000==0 and i!=0):
                #print("Successfully ran ",i," number of rows to generate UIDs.....")
                pass
            value = str(row["UID"]).lower()
            counter+=1
            guest_data.at[i,new_column] = value
            uid = hashlib.sha256(value.encode()).hexdigest()
            mapping_data.append(value.split("$"))
            mapping_data[counter].append(uid)
            guest_data.at[i,new_column] = uid 

        #print("Created UIDs for all PII columns ")  


        #Generation of fake data
        if generate_fake == True:
            print("Running generate fake PIIS......")
            guest_data = self.generate_fake_PIIs(guest_data)
            #print("Done running....")


        #print("Created a new dataframe with the UIDs for each unique PII for the data as ", guest_data)
        print("Loading Mapping data into a new dataframe..... ")
        mapping_df = self.map_data(mapping_data,"")
        print("Done mapping.")

        return (guest_data, mapping_df)
    
    def map_data(self, mapping_data, method):
        '''
        Creates a dataframe with unique IDs for each unique PII combination 
        '''
    
        lookup_data = set(map(tuple,mapping_data))  #need to convert the inner lists to tuples so they are hashable
        #print("Removed Duplicates from Mapping data list....")

        d = list(map(list,lookup_data))
        #print("Converted back to list....")
        #print("Converting list of mapping data into a Dataframe....")

        try:
            data = pd.DataFrame(d)
            df1 = data.iloc[:,0:13]
            df1.columns = ["first_name","last_name", "address_1", "address_2","city_code","state_province_code","zip_postal_code", "phone_number", "home_phone_number", "cell_phone_number", "email_address", "internet_address","UID"]
            df1 = df1[(df1 != df1.columns).all(axis=1)]
            df1.replace('nan', np.nan, inplace=True)
            df1.replace('null', np.nan, inplace=True)
            return df1
        except:
            data = pd.DataFrame(d)
            print(data)
            print("Error while generating dataframe for mapping data..... ")
            exit(1)

    def generate_fake_PIIs(self, f):
        '''Generates fake data in place of original PIIs'''
    
        person = Person('en')
        address = Address('en')
        generic = Generic('en')

        f.replace('', np.nan, inplace=True)
        f.replace('nan', np.nan, inplace=True)
        first_name_replacements = {first_name: "fn_"+person.full_name().split()[0]+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits)for i in range(5))) for first_name in f['first_name'].unique() if first_name is not np.nan}
        # apply replacement
        f["first_name"]=f.first_name.map(first_name_replacements)
        #print("Done replacements for first_name column.....")

        last_name_replacements = {last_name: "ln_"+person.full_name().split()[1]+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(5))) for last_name in f['last_name'].unique() if last_name is not np.nan}
        f["last_name"]=f.last_name.map(last_name_replacements)
        #print("Done replacements for last_name column.....")

        address1_replacements = {add1: "a1_"+address.address()+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(5))) for add1 in f['address_1'].unique() if add1 is not np.nan}
        f["address_1"]=f.address_1.map(address1_replacements)
        #print("Done replacements for address_1 column.....")

        address2_replacements = {add2: "a2_"+address.address()+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(5))) for add2 in f['address_2'].unique() if add2 is not np.nan}
        f['address_2']=f.address_2.map(address2_replacements)
        #print("Done replacements for address_2 column.....")

        phone_replacements = {ph: "ph_"+person.telephone()+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(5))) for ph in f['phone_number'].unique() if ph is not np.nan}
        f['phone_number'] = f.phone_number.map(phone_replacements)
        #print("Done replacements for phone_number column.....")

        home_replacements = {hph: "home_"+person.telephone()+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(5)))  for hph in f['home_phone_number'].unique() if hph is not np.nan}
        f["home_phone_number"]=f.home_phone_number.map(home_replacements)
        #print("Done replacements for home_phone_number column.....")

        cell_replacements = {cell: "cell_"+person.telephone()+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(5)))  for cell in f['cell_phone_number'].unique() if cell is not np.nan}
        f["cell_phone_number"]=f.cell_phone_number.map(cell_replacements)
        #print("Done replacements for cell_phone_number column.....")

        email_replacements = {email: "em_"+person.email()+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(5)))  for email in f['email_address'].unique() if email is not np.nan}
        f["email_address"]=f.email_address.map(email_replacements)
        #print("Done replacements for email_address column.....")

        internet_replacements = {internet: "ia_"+person.email()+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(5)))  for internet in f['internet_address'].unique() if internet is not np.nan}
        f["internet_address"]=f.internet_address.map(internet_replacements)
        #print("Done replacements for internet_address column.....")

        group_name_replacements = {gname: "group_"+generic.text.word()+"_"+(''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(5)))  for gname in f['group_name'].unique() if gname is not np.nan}
        f["group_name"]=f.group_name.map(group_name_replacements)
        #print("Done replacements for group_name column.....")

        return f              

    def execute(self,df,generate_fake = False, message = None):
        '''
        Execute all the preprocessing pipeline. Returns preprocessed dataframe, rows with errors and invalid data
        '''

        if message != None:
            snd = message['send']
            jb = message['job']
            snd(jb,"Filtering non-room records...", 2)

        # Filter all non-room data
        print('Filtering non-room data...')
        
        df1, df_err = self.filter_rate_category(df)

        if message != None:
            snd = message['send']
            jb = message['job']
            snd(jb,"Validating data...", 6)

        # Validate all possible fields
        print('Validating data...')
        df1, df_invalid = self.validate_data(df1)

        if message != None:
            snd = message['send']
            jb = message['job']
            if generate_fake:
                snd(jb,"De-identifying data and generating UIDs...", 12)
            else:
                snd(jb,"Generating UIDs", 16)

        # Generate UIDs
        print(f'De-identification: {generate_fake}')
        print('Generating UIDs...')
        df1, _ = self.generate_UID(df1,generate_fake)

        # Return preprocessed, errors and invalid data
        return df1, df_err, df_invalid