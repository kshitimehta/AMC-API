# AMC database definitions
# Authors: Augusto Espin, Kshiti Mehta
# DS4CG 2020
# UMass

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float, Date
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_, or_, not_
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy import func
from datetime import datetime
import sys
import pandas as pd

@compiles(Insert)
def compile_upsert(insert_stmt, compiler, **kwargs):
    """
    converts every SQL insert to an upsert  i.e;
    INSERT INTO test (foo, bar) VALUES (1, 'a')
    becomes:
    INSERT INTO test (foo, bar) VALUES (1, 'a') ON CONFLICT(foo) DO UPDATE SET (bar = EXCLUDED.bar)
    (assuming foo is a primary key)
    :param insert_stmt: Original insert statement
    :param compiler: SQL Compiler
    :param kwargs: optional arguments
    :return: upsert statement
    """
    pk = insert_stmt.table.primary_key
    insert = compiler.visit_insert(insert_stmt, **kwargs)
    pk_str = ",".join(c.name for c in pk)
    if pk_str == '':
        ondup = ''
    else:
        ondup = f'ON CONFLICT ({pk_str}) DO NOTHING'
        
    #updates = ', '.join(f"{c.name}=EXCLUDED.{c.name}" for c in insert_stmt.table.columns)
    upsert = ' '.join((insert, ondup))
    return upsert

class amcdb:
    def __init__(self, dbstring):

        pgsql = create_engine(dbstring)
        try:
            self.conn = pgsql.connect() 
        except:
            print("Fatal: Couldn't connect to database")
            sys.exit(1)

        metadata = MetaData()

        self.db_distance_lookup = Table('distance_lookup', metadata,
                 Column('building_code',String, primary_key = True),
                 Column('zipcode',String, primary_key = True),
                 Column('city',String),
                 Column('state_province',String),
                 Column('country_code',String),
                 Column('lat',Float),
                 Column('lon',Float),
                 Column('geodesic_distance',Float),
                 Column('driving_distance',Float),
                 Column('driving_time',Float),
                )

        self.db_guest = Table('guest', metadata,
                 Column('guest_uid',String,primary_key=True),
                 Column('zipcode',String),
                 Column('city',String),
                 Column('state_province',String),
                 Column('country_code',String),
                )

        self.db_itinerary = Table('itinerary', metadata,
                 Column('itinerary_id',Integer,primary_key=True),
                 Column('guest_uid',ForeignKey('guest.guest_uid')),
                 Column('max_group_size',Integer),
                 Column('arrival_date',Date),
                 Column('departure_date',Date),
                 Column('in_geodesic_distance',Float),
                 Column('in_drv_distance',Float),
                 Column('in_drv_time',Float),
                 Column('out_geodesic_distance',Float),
                 Column('out_drv_distance',Float),
                 Column('out_drv_time',Float),
                 Column('group_type_code',String),
                )

        self.db_building_visited = Table('building_visited', metadata,
                 Column('itinerary_id',Integer,ForeignKey('itinerary.itinerary_id')),
                 Column('building_code',String),
                 Column('arrival_date',Date),
                 Column('departure_date',Date),
                )

        self.db_reservation = Table('reservation', metadata,
                 Column('itinerary_id',Integer,ForeignKey('itinerary.itinerary_id')),
                 Column('reservation',Integer),
                )

        self.db_ghg = Table('ghg', metadata,
                 Column('itinerary_id',Integer,ForeignKey('itinerary.itinerary_id')),
                 Column('ghg30',Float),
                 Column('ghg50',Float),
                 Column('bus',Float),
                 Column('grp',Float),
                )

        self.view_building_emissions = Table('building_emissions', metadata,
                                 Column('building_name',String),
                                 Column('building_class',String),
                                 Column('arrival_date',Date),
                                 Column('building_code',String),
                                 Column('departure_date',Date),
                                 Column('ghg30',Float),
                                 Column('ghg50',Float),
                                 Column('bus',Float),
                                 Column('grp',Float),        
                               )

        self.view_building_origin = Table('building_origin', metadata,
                             Column('month',String),
                             Column('year',Integer),
                             Column('building_code',String),
                             Column('building_name',String),
                             Column('building_class',String),
                             Column('zipcode',String),
                             Column('ghg30',Float),
                             Column('ghg50',Float),
                             Column('bus',Float),
                             Column('grp',Float),        
                           )

    def guest_insert(self, row):
        '''
        Insert a guest on that table. Input is a pandas row
        '''
        ins = self.db_guest.insert(None).values(guest_uid = row.guest_id,
                                    zipcode = row.zipcode,
                                    city = row.city, 
                                    state_province = row.state_province,
                                    country_code = row.country,
                                    )

        result = self.conn.execute(ins)
        return result.is_insert

    def distance_lookup_insert(self, row):
        '''
        Insert a row in the distance lookup table
        '''
        ins = self.db_distance_lookup.insert(None).values(building_code=row.building_code,
                                    zipcode = row.zip_postal_code,
                                    city = row.city, 
                                    state_province = row.state_province_code,
                                    country_code = row.country_code,
                                    lat = row.lat_p,
                                    lon = row.lon_p,
                                    geodesic_distance = row.geodesic_distance,
                                    driving_distance = row.driving_distance,
                                    driving_time = row.driving_time,
                                    )
        
        result = self.conn.execute(ins)
        return result.is_insert

    def itinerary_insert(self, row):
        '''
        Insert a row in the itinerary table
        '''
        ins = self.db_itinerary.insert(None).values(itinerary_id = row.itinerary_id,
                                    guest_uid = row.guest_uid,
                                    max_group_size = row.max_group_size, 
                                    arrival_date = row.arrival_date,
                                    departure_date = row.departure_date,
                                    in_geodesic_distance = row.in_geo_d,
                                    in_drv_distance = row.in_drv_d,
                                    in_drv_time = row.in_drv_time,
                                    out_geodesic_distance = row.out_geo_d,
                                    out_drv_distance = row.out_drv_d,
                                    out_drv_time = row.out_drv_time,
                                    group_type_code = row.group_type_code,
                                    )

        result = self.conn.execute(ins)
        return result.is_insert

    def itinerary_number(self, year):
        '''
        Get the initial itinerary number for this year
        '''
        # Find itinerary number for previous year
        prev_year_s = f'{year}-1-1'
        prev_year = datetime.strptime(prev_year_s, '%Y-%m-%d')
        # Generate the query
        s = select([func.max(self.db_itinerary.c.itinerary_id)]).where(self.db_itinerary.c.arrival_date < prev_year)
        result = self.conn.execute(s)
        # Get the number of itinerary
        n = result.fetchall()[0][0]
        if n == None:
            n = 0

        return n 

    def building_visited_insert(self, row):
        '''
        Insert a row in the building_visited table
        '''
        ins = self.db_building_visited.insert(None).values(itinerary_id = row.itinerary_id,
                                    building_code = row.building_code,
                                    arrival_date = row.arrival, 
                                    departure_date = row.departure,
                                    )

        result = self.conn.execute(ins)
        return result.is_insert

    def reservation_insert(self, row):
        '''
        Insert a row in the reservation table
        '''
        ins = self.db_reservation.insert(None).values(itinerary_id = row.itinerary_id,
                                    reservation = row.reservation,
                                    )

        result = self.conn.execute(ins)
        return result.is_insert

    def ghg_insert(self, row):
        '''
        Insert a row in the ghg table
        '''
        ins = self.db_ghg.insert(None).values(itinerary_id = row.itinerary_id,
                                    ghg30 = row.ghg30,
                                    ghg50 = row.ghg50, 
                                    bus = row.bus,
                                    grp = row.grp,
                                    )
        
        result = self.conn.execute(ins)
        return result.is_insert
    
    def ghg_delete(self):
        '''
        Delete all emissions calculation
        '''
        ghg_del = self.db_ghg.delete(None)
        result = self.conn.execute(ghg_del)
        return result.rowcount

    def emissions_by_day(self, start_date, end_date):
        s = select([func.extract('dow',self.view_building_emissions.c.arrival_date).label("dow"), 
            self.view_building_emissions.c.building_code,
            self.view_building_emissions.c.building_name,
            self.view_building_emissions.c.building_class,
            func.count(self.view_building_emissions.c.ghg30).label('trips'),
            func.sum(self.view_building_emissions.c.ghg30).label('ghg30'),
            func.sum(self.view_building_emissions.c.ghg50).label('ghg50'),
            func.sum(self.view_building_emissions.c.bus).label('bus'),
           func.sum(self.view_building_emissions.c.grp).label('grp')]).\
            where(and_(self.view_building_emissions.c.arrival_date > start_date,
                       self.view_building_emissions.c.arrival_date < end_date)).\
            group_by("dow",
                     "building_code",
                     "building_name",
                     "building_class")

        return pd.read_sql(s, self.conn)


    def emissions_by_building_origin(self, building_code, zipcode):
        s = select([self.view_building_origin.c.month,
            self.view_building_origin.c.year,
            self.view_building_origin.c.building_code,
            self.view_building_origin.c.building_name,
            self.view_building_origin.c.building_class,
            self.view_building_origin.c.zipcode,
            func.count(self.view_building_origin.c.ghg30).label('trips'),
            func.sum(self.view_building_origin.c.ghg30).label('ghg30'),
            func.sum(self.view_building_origin.c.ghg50).label('ghg50'),
            func.sum(self.view_building_origin.c.bus).label('bus'),
           func.sum(self.view_building_origin.c.grp).label('grp')]).\
            where(and_(self.view_building_origin.c.building_code.in_(building_code),
                       self.view_building_origin.c.zipcode == zipcode)).\
            group_by("month",
                     "year",
                     "building_code",
                     "building_name",
                     "building_class",
                     "zipcode")

        return pd.read_sql(s, self.conn)