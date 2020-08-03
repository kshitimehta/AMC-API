# Processing pipeline for AMC
# Authors: Augusto Espin, Kshiti Mehta
# DS4CG 2020
# UMass
from geo_amc import GeoOperations
import pandas as pd
import datetime
from amcdb import amcdb
from emissions import ghg_calc

class process:
    def __init__(self, geo ,dbstring):
        '''
        Initialize the object. Only geo object required at this time
        ''' 
        self.geo = geo
        self.dbstring = dbstring


    def process_geo_distance(self, df, year):
        '''
        Create a table with all unique distance pairs (building_code - zip, country). The
        data will be marked with the year variable for reference in that database
        '''
        geo_data_d = self.geo.compute_unique_block_geo_distance(df, year)

        # Create separate fields for point of origin
        lat = []
        lon = []
        for x in geo_data_d.point:
            if x != None:
                lat.append(x.latitude)
                lon.append(x.longitude)
            else:
                lat.append(0)
                lon.append(0)

        # Append separate columns and drop point
        geo_data_d['lat_p'] = lat
        geo_data_d['lon_p'] = lon
        geo_data_d = geo_data_d.drop(columns=['point'])

        return geo_data_d.reset_index()


    def process_drv_distance(self, df, use_api = False, cutoff = 600):
        ''' 
        Process an entire block of data, appending distance to the record
        If not found use the API to gather the driving distance from Bing or estimate
        '''

        # Compute distance for every pair
        d_geo = []
        d_drv = []
        t_drv = []
        annotate = []

        # Iterate over all items in block
        for _, row in df.iterrows():
            # Get origin zip code
            if row['country_code'] != 'CA':
                zip1 = row['zip_postal_code'] + ', ' + row['country_code']
            else:
                zip1 = row['zip_postal_code'] + ' 1X1, ' + row['country_code']
            # Set building code    
            bldg = row['building_code']
            
            # Lookup for distances
            dgeo, ddrv, tdrv, loc, _ = self.geo.get_distances(zip1, bldg, use_api, cutoff)
        
            # Update vector
            d_geo.append(dgeo)
            d_drv.append(ddrv)
            t_drv.append(tdrv)

            # Check for correct state or province in field
            ann = "ok"
            if loc['state'] != row['state_province_code'].upper():
                ann = f"State typo {loc['state']}"
            annotate.append(ann)

        # Append columns to block
        df['geodesic_distance'] = d_geo
        df['driving_distance'] = d_drv
        df['driving_time'] = t_drv
        df['annotation'] = annotate

        return df

    def reservation_info(self, guest_data):
        '''
        Computing group sizes for each reservation number and information of the origin and destination
        locations 
        '''
        
        #computing group count by number of bednights
        group_size_res = guest_data.groupby(['UID','reservation_number','arrival_date','departure_date','Stay_Date'], as_index=False)["NumberofBednights"].agg(sum)
        group_size_res = group_size_res.groupby(["reservation_number"], as_index=False)["NumberofBednights"].max()
        group_size_res.columns=["reservation_number","guest_count"]
        
        #including destination locations to the group_size_res df
        guest_data["building_code"].replace({"nan":""})
        replace_data= guest_data.groupby(["reservation_number"])["building_code"].unique().apply(lambda x: ", ".join(x.astype(str)))
        group_size_res = (group_size_res.merge(replace_data, left_on='reservation_number', right_on='reservation_number'))

        #including zips and country codes to group_size_res df
        #    guest_data["zip_postal_code"].replace({"nan":""})
        replace_data= guest_data.groupby(["reservation_number"])["zip_postal_code"].unique().apply(lambda x: ", ".join(x.astype(str)))
        group_size_res = (group_size_res.merge(replace_data, left_on='reservation_number', right_on='reservation_number'))

        replace_data= guest_data.groupby(["reservation_number"])["country_code"].unique().apply(lambda x: ", ".join(x.astype(str)))
        group_size_res = (group_size_res.merge(replace_data, left_on='reservation_number', right_on='reservation_number'))
        
        #including UID to group_size_res df
        replace_data= guest_data.groupby(["reservation_number"])["UID"].unique().apply(lambda x: ", ".join(x.astype(str)))
        group_size_res = (group_size_res.merge(replace_data, left_on='reservation_number', right_on='reservation_number'))

        return group_size_res

    def visits_info(self, guest_data,group_size_res):
        

        # Grouping
        UID_adrb = guest_data.groupby(['UID','reservation_number','arrival_date','departure_date','Stay_Date']).size().reset_index(name="Group_size")
        
        # Use the UID_adrb dataframe to compute the difference between arrival and departure for the same individual
        puid = None
        pdeparture = None
        pres = None
        diff_days = []
        
        for _, row in UID_adrb.iterrows():
            uid = row['UID']
            arrival = row['arrival_date']
            departure = row['departure_date']
            res = row['reservation_number']
            # Check that puid is assigned and compute difference days
            if pres!=None:
                if puid != None:
                    if pdeparture != None:
                        if puid == uid and pres!=res:
                            diff_days.append((arrival - pdeparture)/datetime.timedelta(days=1))
                        if pres==res:
                            diff_days.append(0.1)
                        if puid!=uid:
                            diff_days.append(0.01)
            else:
                diff_days.append(-1)
            
            puid = uid
            pdeparture = departure 
            pres = res
        
        UID_adrb_days = UID_adrb.copy()
        UID_adrb_days['days'] = diff_days
        # Included a new field for days apart 
        visits = UID_adrb_days.groupby(["UID","reservation_number"])["days"].nth(0).reset_index()
        
        visits['reservation_number'] = visits.reservation_number.astype(str)
        
        pres = None
        puid = None
        to_drop_list = []
        # clubbed visits that are less than 6 days apart
        for index,rows in visits.iterrows():
    #             if index%1000==0:
    #                 print(index) 
                uid = rows['UID']
                res = str(rows['reservation_number'])
                days = rows['days']
                if puid!=None:
                    if pres!=None:
                        if puid==uid and days in range(-6,6):
                            i = int(index)-1
                            visits.at[index,'reservation_number']= " , ".join([pres,res])
                            to_drop_list.append(i)
                            res = " , ".join([str(pres),str(res)])
                pres = res
                puid = uid
                
        rows = visits.index[to_drop_list]

        visits.drop(rows, inplace=True)
    
        # including group count by visits
            
        grouped_visits = visits.groupby(['reservation_number'])['UID'].unique().transform(lambda x: ", ".join(x.astype(str))).reset_index()
        # grouped_visits = grouped_visits.to_frame()    

        
        group_size_res["reservation_number"].astype('str')
        group_count = []
        for index,row in grouped_visits.iterrows():
            list_of_group_sizes = []
            nums = row["reservation_number"].split(",")
            for each_res in nums:
                list_of_group_sizes.append(group_size_res.loc[group_size_res["reservation_number"]==int(each_res) , ["guest_count"]].values[0])
            group_count.append(max(list_of_group_sizes)[0])
        grouped_visits["group_count"] = group_count
            
    
        # grouped by reservation number and clubbed visits to get total number of visits
        
        group2 = grouped_visits.groupby(['reservation_number'])['UID'].unique().transform(lambda x: ", ".join(x.astype(str))).reset_index()
        
        # merging to get visits with uid and guest count
        
        group3 = grouped_visits.groupby(['reservation_number'])['group_count'].max()
        group3 = group3.to_frame()
        grouped_visits=pd.merge(group3, group2, on='reservation_number', how="left")
        return grouped_visits

    def join_on_ItID(self, guest_data, guest_size_res, grouped_visits):
            
        # adding the column itinerary ID in both files and merging them
        grouped_visits["itinerary_ID"] = grouped_visits.index
        grouped_visits_explode=grouped_visits.assign(reservation_number=grouped_visits['reservation_number'].str.split(',')).explode('reservation_number')
            
        guest_size_res["reservation_number"] = guest_size_res["reservation_number"].astype(int)
        grouped_visits_explode["reservation_number"] = grouped_visits_explode["reservation_number"].astype(int)
        reservation_with_ID = pd.merge(guest_size_res, grouped_visits_explode , on='reservation_number', how="left")
        # Select colums in reservation_with_ID
        reservation_with_ID = reservation_with_ID.drop(columns=['UID_y','group_count'])
        reservation_with_ID = reservation_with_ID.rename(columns={'UID_x':'UID'})

        # reservation_with_ID = reservation_with_ID.drop("Group_count",axis=1)
        itinerary_df = reservation_with_ID.merge(grouped_visits_explode, left_on='itinerary_ID', right_on='itinerary_ID')       
        itinerary_df = itinerary_df.groupby('itinerary_ID').agg(lambda x: ','.join(set(x.astype(str)))).reset_index()
        # select columns in itinerary_df
        itinerary_df = itinerary_df.drop(columns=['reservation_number_y','group_count','UID_y'])
        itinerary_df = itinerary_df.rename(columns={'reservation_number_x':'reservation_number','UID_x':'UID'})

        return reservation_with_ID, grouped_visits, itinerary_df

    def process_group(self, df, year):
        '''
        Compute group sizes from itineraries
        '''
        group_size_res = self.reservation_info(df)
        grouped_visits = self.visits_info(df,group_size_res)
        _, grouped_visits, itinerary_df = self.join_on_ItID(df,group_size_res,grouped_visits)

        last_it = 0
        try:
            # Create connector to database
            amc_db = amcdb(self.dbstring)
            last_it = amc_db.itinerary_number(year) 
        except:
            print("Error: Couldn't connect with the database")

        # Udpdate itinerary number with last number on database
        itinerary_df.itinerary_ID = itinerary_df.itinerary_ID + last_it

        return itinerary_df


    def update_db(self, itinerary, df1):
        ''' 
        Create the required tables for database and update the database. Return all tables produced
        '''

        # This are the tables to be created
        r_tbl = pd.DataFrame(columns=['itinerary_id','reservation'])
        i_tbl = pd.DataFrame(columns=['itinerary_id','guest_uid','max_group_size','arrival_date','departure_date','in_geo_d','in_drv_d','in_drv_time','out_geo_d','out_drv_d','out_drv_time'])
        b_visited_tbl = pd.DataFrame(columns=['itinerary_id','building_code','arrival','departure'])
        g_tbl = pd.DataFrame(columns=['guest_id','zipcode','city','state_province','country'])

        def upload_database(row, df):
            '''
            Process the itineraries with the reservation dataframe to generate all required
            tables. The function should be applied over the itinerary dataFrame
            '''
            nonlocal r_tbl, i_tbl, b_visited_tbl, g_tbl

            itID = row['itinerary_ID']
            uid = row['UID']
            zip_code = row['zip_postal_code']
            country = row['country_code']
            #bldg_code = row['building_code'].split(',')
            grp_size = max(row['guest_count'].split(','))
            reserv_n = row['reservation_number'].split(',')
            
            # Process arrival and departure from building
            arrival_date = df[df.reservation_number == int(reserv_n[0])]['arrival_date'].mean()
            departure_date = df[df.reservation_number == int(reserv_n[-1])]['departure_date'].mean()
            
            # Get the arrival dates for all buildings in list
            arr_date = []
            bld_cd = []
            for _ in reserv_n:
                res = df[df.reservation_number == int(reserv_n[0])]
                res = res.groupby(['building_code','Stay_Date']).mean().reset_index().groupby(['building_code']).min().reset_index()
                for _,r in res.iterrows():
                    arr_date.append(r['Stay_Date'])
                    bld_cd.append(r['building_code'])
            
            # Buildings table entries for this record
            arr_date.sort()
            dep_date = arr_date[1:]
            dep_date.append(departure_date)
            bldg_visited_tbl = pd.DataFrame( data = { 'itinerary_id': itID,
                                                    'building_code': bld_cd,
                                                    'arrival': arr_date,
                                                    'departure': dep_date })
            
            # Get distances
            origin = zip_code + ", " + country
            # Distance calculation use estimation here, because in the pipeline all new addresses should be already 
            # loaded in the lookup table for now
            in_geo, in_drv, in_time, loc, _ = self.geo.get_distances(origin, bldg_visited_tbl.iloc[0]['building_code'])
            out_geo, out_drv, out_time, loc, _ = self.geo.get_distances(origin, bldg_visited_tbl.iloc[-1]['building_code'])
            
            # Complete data of city and state
            city = loc['city']
            state = loc['state']
            
            # Create guest table
            guest_tbl = pd.DataFrame({'guest_id':[uid],
                                    'zipcode':[zip_code],
                                    'city':[city],
                                    'state_province':[state],
                                    'country':[country]})
            
            # Create reservation table
            reserv_tbl = pd.DataFrame( {'itinerary_id': [itID]*len(reserv_n),'reservation': reserv_n})
            
            # Create itinerary
            group_type_code = df[df.reservation_number == int(reserv_n[0])]['group_type_code'].iloc[0]
            if group_type_code == 'nan':
                group_type_code = ''
            itinerary_tbl = pd.DataFrame( {'itinerary_id': [itID],
                                        'guest_uid': [uid],
                                        'max_group_size': [grp_size],
                                        'arrival_date': [arrival_date],
                                        'departure_date': [departure_date],
                                        'in_geo_d': [in_geo],
                                        'in_drv_d': [in_drv],
                                        'in_drv_time': [in_time],
                                        'out_geo_d': [out_geo],
                                        'out_drv_d': [out_drv],
                                        'out_drv_time': [out_time],
                                        'group_type_code': [group_type_code] })
            
            # Update globals
            r_tbl = r_tbl.append(reserv_tbl)
            i_tbl = i_tbl.append(itinerary_tbl)
            b_visited_tbl = b_visited_tbl.append(bldg_visited_tbl)
            g_tbl = g_tbl.append(guest_tbl)
            
            # Print itinerary
            if itID % 1000 == 0:
                print(itID)
            
            return itID

        # Find all tables for data
        print('Creating Tables...')
        _ = itinerary.apply(lambda r: upload_database(row=r,df=df1), axis=1)

        try:
            # Create connector to database
            amc_db = amcdb(self.dbstring)

            print("Loading data to database...")
            # Upload all found tables to the database
            g_tbl.apply(lambda r: amc_db.guest_insert(r), axis=1)
            i_tbl.apply(lambda r: amc_db.itinerary_insert(r), axis=1)
            r_tbl.apply(lambda r: amc_db.reservation_insert(r), axis=1)
            b_visited_tbl.apply(lambda r: amc_db.building_visited_insert(r), axis=1)
        except:
            print('Error: Could not update database. Connection error.')
        
            print('Done.')

        return i_tbl,g_tbl,r_tbl,b_visited_tbl

    def compute_emissions(self, i_tbl, emission_data):
        '''
        Implement the emission computation here. Emission_data is used for parameters. In this case,
        the emission_data contains the configuration for any desired ratio and could include calculation
        with a bus
        '''

        # Function used for calculation
        def calc_emissions(row, emissions_data):
            in_drv_d = row['in_drv_distance']
            out_drv_d = row['out_drv_distance']
            group_size = row['max_group_size']
            group_type = row['group_type_code']
            ghg = []
            for em in emissions_data:
                ghg.append(ghg_calc(group_size,in_drv_d,out_drv_d,group_type,em['parameters'],em['ratio'],em['bus']))
                
            return ghg
        
        # Compute emissions
        ghg = i_tbl.apply(lambda r: calc_emissions(r,emission_data), axis=1)

        # Create the dataframe
        d = {'itinerary_id':i_tbl.itinerary_id}
        for em, i in zip(emission_data,range(len(emission_data))):
            d.update({str(em['name']):[x[i] for x in ghg]})

        ghg_tbl = pd.DataFrame(d)
        # Update database
        # Create connector to database
        amc_db = amcdb(self.dbstring)
        # Delete data on table
        size = amc_db.ghg_delete()
        # Insert calculated data
        ghg_tbl.apply(lambda r: amc_db.ghg_insert(r), axis=1)

        return ghg_tbl


    def execute(self, df, year, use_api = False, message = None):
        '''
        Execute the whole processing pipeline
        '''

        # If using API update the database with correct new data gathered
        if use_api == True:
            print('Bing API used. Updating database...')
            # Process distance information (geodesic_distance)
            print('Processing geodesic distances...')
            lookup_table = self.process_geo_distance(df,year)

            if message != None:
                snd = message['send']
                jb = message['job']
                snd(jb,"Geodesic distance processed...", 24)


            # Process driving distance
            print('Processing driving distances...')
            lookup_table_complete = proc.process_drv_distance(lookup_table, use_api)
            lookup_filtered = lookup_table_complete[~((lookup_table_complete.driving_distance > -1) & (lookup_table_complete.driving_distance < 0))]
            
            if message != None:
                snd = message['send']
                jb = message['job']
                snd(jb,"Driving distance processed...", 30)


            # Try to update the distance lookup database
            try:
                amc_db = amcdb(dbstring)
                lookup_filtered.apply(lambda r: amc_db.distance_lookup_insert(r), axis=1) 

                if message != None:
                    snd = message['send']
                    jb = message['job']
                    snd(jb,"Distance lookup table updated...", 35)

            except:
                print('Error: Could not access the database! Check connection strings and make sure the server is running')
        else:
            # In this case we just notify that we are going to estimate distances
            print('Using estimation for missing driving distances...')

        if message != None:
            snd = message['send']
            jb = message['job']
            snd(jb,"Finding itineraries and group sizes...", 50)

        # Processing for itineraries and group sizes
        print('Finding itineraries and group sizes...')
        itinerary = self.process_group(df,year)

        if message != None:
            snd = message['send']
            jb = message['job']
            snd(jb,"Tables for database created...", 80)

        # Create tables for database and update the database if possible
        print('Creating database tables and updating the database...')
        i_tbl, g_tbl, r_tbl, b_visited_tbl = self.update_db(itinerary, df)

        if message != None:
            snd = message['send']
            jb = message['job']
            snd(jb,"Tables uploaded to database...", 90)

        print('Processing finished...')

        return i_tbl, g_tbl, r_tbl, b_visited_tbl
        