# Distance and geographic calculations for AMC
# Author: Augusto Espin
# DS4CG 2020
# UMass

import pandas as pd
import numpy as np
from geopy import distance
from geopy.geocoders import Nominatim
from geopy.point import Point
import re, pgeocode 
import requests, json
import sys
import sqlalchemy as db

class GeoOperations:
    ''' Geopraphic operations for AMC: Computes point coordinates, computes distances, 
        either geodesic or driving over single points or sets 
    '''
    def __init__(self, uszipfile, key, dbstring, sep = ';', dbschema = ""):
        """ Initialize all required information to operate"""
        try:
            # Load data file with US zip data
            self.uszip = pd.read_csv(uszipfile, sep=sep)
        except:
            print('Error: Could not load zip code data',file=sys.stderr)
            sys.exit(1)
        # Initialize geolocator
        self.geolocator = Nominatim(user_agent='amc')

        # Initialize data for Bing API
        self.bing = 'https://dev.virtualearth.net/REST/v1/Routes/DistanceMatrix'
        self.key = key
        self.km_mile = 1.60934 #Km per mile
        self.beta = 1.27714323 #Regressor value computed 
        self.speed = 60 # Average speed used to estimate time
        
        # Initialize the database connection
        if dbschema == "":
            self.pgsql = db.create_engine(dbstring)
        else:
            self.pgsql = db.create_engine(dbstring,
                     connect_args={'options':'-csearch_path={}'.format(dbschema)})
        
        # Initialize the structure of the database of distances
        meta = db.MetaData()
        self.facilities_distance = db.Table(
            'distance_lookup', meta,
            db.Column('building_code', db.String, primary_key = True),
            db.Column('zipcode', db.String),
            db.Column('city', db.String),
            db.Column('state_province', db.String),
            db.Column('country_code', db.String),
            db.Column('lat', db.Float),
            db.Column('lon', db.Float),
            db.Column('geodesic_distance', db.Float),
            db.Column('driving_distance', db.Float),
            db.Column('driving_time', db.Float),
        )
        
        # Get building data from database
        query_bldgs = "Select building_code, building_name, latitude, longitude, geo_dst_near_airport, geo_dst_intl_airport, drv_dst_near_airport, drv_time_near_airport, drv_dst_intl_airport, drv_time_intl_airport, nearest_airport, international_airport from amc_building"
        try:
            conn = self.pgsql.connect()
            amc_bldgs = conn.execute(query_bldgs)
        except:
            print("Could not query database for AMC buildings")
            sys.exit(1)
        
        # Put everything on memory
        df = []
        for bld_code, name, lat, lon, gd_na, dd_na, dt_na, gd_ia, dd_ia, dt_ia, na, ia in amc_bldgs:
            df.append([bld_code,name,lat,lon, gd_na, dd_na, dt_na, gd_ia, dd_ia, dt_ia, na, ia])
        
        # Manage AMC building data as a pandas dataframe
        self.amc_buildings = pd.DataFrame(df, columns = ['building_code','name','lat','lon','gd_na','dd_na','dt_na','gd_ia','dd_ia','dt_ia','na','ia'])
        
        
    def get_zip_state_from_address(self, addrs):
        ''' Return the zip code and the country from an addrs string. This only
            works for US and Canada, otherwise the returned will be an empty object
        '''
        # Returns a structure with data found
        loc_data = {'zip':None, 
                    'state':None, 
                    'city': None,
                    'country':None, 
                    'point':None}
        
        # Find if address is in the US or Canada
        s = addrs.split(',')
        co = s[len(s)-1].strip().upper()
        # Check for country of origin
        if co == 'US':
            loc_data['country'] = 'US'
            # Find zip code
            zipus = re.search(r'\d{4,5}', addrs)
            
            if zipus != None:
                # Zip code found in text
                zipus = zipus.group()
                if len(zipus) != 5:
                    # Fix codes that do not start with zero
                    zipus = '0' + zipus
                if zipus != '':
                    # zip code found
                    loc_data['zip'] = zipus
                    
                
        elif co == 'CA':
            # Lookup for Canadian zip-code
            loc_data['country'] = 'CA'
            zipca = re.search(r'\w\d\w\s{0,1}\d\w\d',addrs)
            
            if zipca != None:
                # Zip code found 
                zipca = zipca.group()
                # Fix zip codes that don't have a space in between
                if len(zipca) == 6:
                    zipca = zipca[0:3] + ' ' + zipca[3:6]
                # Fill up the information
                loc_data['zip'] = zipca[0:3].upper()

        else:
            # International travel
            loc_data['country'] = co.upper()
            loc_data['zip'] = 'INTL'

        return loc_data
        
    def get_coordinates_from_address(self, addrs):
        ''' Return coordinates from address from Canada and US. Will use the same
            structure as the function get_zip_state_from_address and complete it
        '''
        # Fill up the loc structure
        loc_data = self.get_zip_state_from_address(addrs)
        
        # If zip is none, then address is wrong
        if loc_data['zip'] != None:
            # Find coordinates for US address
            if loc_data['country'] == 'US':
                zipc = int(loc_data['zip'])
                reg = self.uszip[self.uszip['Zip'] == zipc].reset_index()
                # Test if zip code was found
                if reg.empty == False:
                    # It is not empty, update
                    loc_data['point'] = Point(reg.geopoint[0])
                    loc_data['state'] = reg.State[0]
                    loc_data['city'] = reg.City[0]
                else:
                    # Try to find the place by other means
                    nomi = pgeocode.Nominatim('us')
                    a = nomi.query_postal_code(loc_data['zip'])
                    # Verify if it was found
                    if not np.isnan(a.latitude) and not np.isnan(a.longitude):
                        loc_data['point'] = Point(a.latitude, a.longitude)
                        loc_data['state'] = a.state_code
                        loc_data['city'] = a.place_name
        
            elif loc_data['country'] == 'CA':
                # Find Canadian address
                nomi = pgeocode.Nominatim('ca')
                a = nomi.query_postal_code(loc_data['zip'])
                # Verify that was found
                if not np.isnan(a.latitude) and not np.isnan(a.longitude):
                    loc_data['point'] = Point(a.latitude, a.longitude)
                    loc_data['state'] = a.state_code
                    loc_data['city'] = a.place_name
                    loc_data['zip'] = a.postal_code

            else:
                # Find international location
                geolocator = Nominatim(user_agent="AMC")
                try:
                    loc = geolocator.geocode(loc_data['country'])
                except:
                    loc = None
                    print(f'Error trying to locate: {loc_data["country"]}')

                if loc != None:
                    loc_data['point'] = Point(loc[1])

        return loc_data
    
    def compute_geo_distance(self, **kwargs):
        ''' Returns geodesic distance in Km '''
        p1 = p2 = None
        loc1 = loc2 = None

        # Initialize coordinates from params, the first valid params will be used
        for key,value in kwargs.items():
            if key == "zip1":
                if p1 == None:
                    loc1 = self.get_coordinates_from_address(value)
                    p1 = loc1['point']
            elif key == "zip2":
                if p2 == None:
                    loc2 = self.get_coordinates_from_address(value)
                    p2 = loc2['point']
            elif key == "p1":
                if p1 == None:
                    p1 = value
            elif key == "p2":
                if p2 == None:
                    p2 = value
        
        # Check that params are valid and return distance or error
        if p1 == None or p2 == None:
            dist = -1
        else:
            dist =  distance.geodesic(p1,p2,ellipsoid='WGS-84').miles
            
        if loc1 != None and loc2 != None:
            return (dist,loc1,loc2)
        elif loc1 != None:
            return (dist,loc1,p2)
        elif loc2 != None:
            return (dist,p1,loc2)
        else:
            return (dist,p1,p2)

    def _driving_distance(self, p1,p2):
        ''' Returns driving distance from Bing and time as well'''
        params =  { "origins":'%f,%f'%(p1.latitude,p1.longitude),
                    "destinations":'%f,%f'%(p2.latitude,p2.longitude),
                    "travelMode":'driving',
                    "key":self.key}
        payload_str = '&'.join('%s=%s' % (k,v) for k,v in params.items())
        response = requests.get(self.bing, params = payload_str)
        if response.status_code == 200:
            resp = response.json()
            results = resp['resourceSets'][0]['resources'][0]['results'][0]
            return(results['travelDistance']/self.km_mile, results['travelDuration'])
        else:
            print('Error when using Bing API', file=sys.stderr)
            return (-1,-1)


    def compute_driving_distance(self, **kwargs):
        ''' Return driving distance in miles '''
        p1 = p2 = None
        loc1 = loc2 = None
        # Initialize coordinates from params, the first valid params will be used
        for key,value in kwargs.items():
            if key == "zip1":
                if p1 == None:
                    loc1 = self.get_coordinates_from_address(value)
                    p1 = loc1['point']
            elif key == "zip2":
                if p2 == None:
                    loc2 = self.get_coordinates_from_address(value)
                    p2 = loc2['point']
            elif key == "p1":
                if p1 == None:
                    p1 = value
            elif key == "p2":
                if p2 == None:
                    p2 = value
        
        # Check that params are valid and return distance or error
        if p1 == None or p2 == None:
            dist = -1
        else:
            dist = self._driving_distance(p1,p2)
            
        if loc1 != None and loc2 != None:
            return (dist,loc1,loc2)
        elif loc1 != None:
            return (dist,loc1,p2)
        elif loc2 != None:
            return (dist,p1,loc2)
        else:
            return (dist,p1,p2)
        
    def lookup_distances(self, address, building_code, verbose = False):
        ''' Will lookup for geodesic and driving distances in lookup table
        '''
        # Fill up the loc structure
        dist = None
        drv_dst = None
        drv_time = None
        loc_data = self.get_zip_state_from_address(address)
        
        # Lookup for nearest airport
        if loc_data['country'] != 'US' and loc_data['country'] != 'CA':
            airport = self.amc_buildings[self.amc_buildings.building_code == building_code]['ia'].values[0] 
            dist = self.amc_buildings[self.amc_buildings.building_code == building_code]['gd_ia'].values[0]
            drv_dst = self.amc_buildings[self.amc_buildings.building_code == building_code]['dd_ia'].values[0]
            drv_time = self.amc_buildings[self.amc_buildings.building_code == building_code]['dt_ia'].values[0]
            if verbose == True:
                print(f'Intl: [{building_code} <- {address}], assigning: {airport}') 
        else:
            # Query the database
            s = self.facilities_distance.select().where(db
                    .and_(self.facilities_distance.c.building_code == building_code, 
                            self.facilities_distance.c.zipcode == loc_data['zip']))
            conn = self.pgsql.connect()
            result = conn.execute(s)
            dist_list = [r for r in result]
            # Check that size is just 1
            if len(dist_list) == 0:
                print(f'Pair [{building_code} <- {address}] not found in lookup table', file=sys.stderr)     
            else:
                for building_code, zip_code, city, state, country, lat, lon, geo_dist, drv_dist, drv_tm in dist_list:
                    if (loc_data['state'] != state):
                        loc_data['state'] = state
                    if (loc_data['country'] != country):
                        loc_data['country'] = country
                    if (loc_data['city'] != city):
                        loc_data['city'] = city
                    p = Point(lat,lon)
                    if (loc_data['point'] != p):
                        loc_data['point'] = p
                        
                    if len(dist_list) > 1:
                        print(f'Error: More than one pair found in {building_code} <- {zip_code}', file=sys.stderr)
                    else: 
                        if geo_dist >= 600:
                            # If distance is -1 (>600 miles), get airport data
                            airport = self.amc_buildings[self.amc_buildings.building_code == building_code]['na'].values[0]
                            #dist = self.amc_buildings[self.amc_buildings.building_code == building_code]['gd_na'].values[0]
                            dist = geo_dist
                            drv_dst = self.amc_buildings[self.amc_buildings.building_code == building_code]['dd_na'].values[0]
                            drv_time = self.amc_buildings[self.amc_buildings.building_code == building_code]['dt_na'].values[0]
                            if verbose == True:
                                print(f'Regional: [{building_code} <- {address}], assigning: {airport}') 
                        else:
                            # Distance found for <600 miles
                            dist = geo_dist
                            drv_dst = drv_dist
                            drv_time = drv_tm
                     
        return (dist, drv_dst, drv_time, loc_data, building_code)

    def compute_unique_block_geo_distance(self, block, year):
        ''' Compute distances over a block on pandas which requires to have the following:
            building_code, zip_postal_code, state_code and country_code. 
            The output will be annotated with errors found 
        '''
        
        # Create unique origin - destination pairs
        unique_travel = block.groupby(['building_code',
                                       'state_province_code',
                                       'zip_postal_code',
                                       'country_code']).size().reset_index(name='Freq')
        
        d_to_find = unique_travel.set_index('building_code').join(self.amc_buildings.set_index('building_code'))
        
        # Compute distance for every pair
        dist_r = []
        state = []
        city = []
        zipc = []
        country = []
        point = []
        
        # Iterate over all items in block
        for _, row in d_to_find.iterrows():
            zip1 = row['zip_postal_code'] + ', ' + row['country_code']
            p2 = Point(float(row['lat']),float(row['lon']))
            # Get geodesic distance only
            dist, loc, _ = self.compute_geo_distance(zip1 = zip1, p2 = p2)    
            # Append columns
            dist_r.append(dist)
            state.append(loc['state'])
            city.append(loc['city'])
            zipc.append(loc['zip'])
            country.append(loc['country'])
            point.append(loc['point'])
        
        assert(len(dist_r) == d_to_find.shape[0])
        d_to_find['distance'] = dist_r
        d_to_find['state_province_code'] = state
        d_to_find['city'] = city
        d_to_find['zip_postal_code'] = zipc
        d_to_find['country_code'] = country
        d_to_find['point'] = point
        d_to_find['year'] = year
        
        return d_to_find
        
    def get_distances(self, address, building_code, use_api = False, cutoff = 600):
        ''' Get both driving and geodesic distances between AMC facility and a guest
            by looking up on the database or trying to find through the available resources
        '''
        geo_d, drv_d, drv_t, loc_data, amc_bldg = self.lookup_distances(address,building_code)
        
        if geo_d == None:
            # Try lo lookup for data, this address is new
            # First lookup for building coordinates
            qry_bldg = self.amc_buildings[self.amc_buildings['building_code'] == amc_bldg]
            build_coord = Point(float(qry_bldg['lat']),float(qry_bldg['lon']))
            
            # Compute geo_distance
            geo_d, loc_data, _ = self.compute_geo_distance(zip1 = address, p2 = build_coord)
            # Compute driving_distance if less than cutoff
            if geo_d == -1:
                print(f'{address} -> {build_coord}: {geo_d}')
            if geo_d < cutoff:
                if use_api == True:
                    drv_dist, loc_data, _ = self.compute_driving_distance(zip1 = address, p2 = build_coord)
                    drv_d = drv_dist[0]
                    drv_t = drv_dist[1]
                else:
                    # Use linear estimation
                    drv_d = geo_d*self.beta
                    drv_t = drv_d/self.speed
            else:
                    drv_d = -1
                    drv_t = -1
        
            #if geo_d != -1:
                # Update database
            #    p = loc_data['point']
            #    ins = self.facilities_distance.insert().values(building_code = building_code,
            #                                                   zipcode = loc_data['zip'],
            #                                                   geodesic_distance = geo_d,
            #                                                   driving_distance = drv_d,
            #                                                   driving_time = drv_t,
            #                                                   state_province = loc_data['state'],
            #                                                   country_code = loc_data['country'],
            #                                                   city = loc_data['city'],
            #                                                   lat = p.latitude,
            #                                                   lon = p.longitude
            #                                                  )
            #    conn = self.pgsql.connect()
            #    _ = conn.execute(ins)
        
        # Return the data as is
        return (geo_d, drv_d, drv_t, loc_data, amc_bldg)
                
    def process_distance(self, block, use_api = False, cutoff = 600):
        ''' 
        Process an entire block of data, appending distance to the record
        If not found use the API to gather the driving distance from Bing or use a regression
        '''
        # Compute distance for every pair
        d_geo = []
        d_drv = []
        t_drv = []
        annotate = []

        # Iterate over all items in block
        for _, row in block.iterrows():
            # Get origin zip code
            zip1 = row['zip_postal_code'] + ', ' + row['country_code']
            bldg = row['building_code']
            
            # Lookup for distances
            dgeo, ddrv, tdrv, loc, _ = self.get_distances(zip1, bldg, use_api, cutoff)
        
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
        block['geodesic_distance'] = d_geo
        block['driving_distance'] = d_drv
        block['driving_time'] = t_drv
        block['annotation'] = annotate

        return block
            