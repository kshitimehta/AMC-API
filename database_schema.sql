CREATE TABLE airport (
	airport_code varchar NOT NULL,
	airport_name varchar NOT NULL,
    latitude float4 NOT NULL,
    longitude float4 NOT NULL,
	CONSTRAINT airport_pk PRIMARY KEY (airport_code)
);

CREATE TABLE AMC_building (
	building_code varchar NOT NULL,
    building_name varchar NOT NULL,
	lodging_type varchar NULL,
	access_type varchar NULL,
	building_class varchar NULL,
    latitude_site float4 NULL,
    longitude_site float4 NULL,
    altitude_site float4 NULL,
	latitude float4 NOT NULL,
	longitude float4 NOT NULL,
	zip_code varchar NULL,
    street_address varchar NULL,
    city varchar NULL,
    state_code varchar NULL,
	number_of_rooms int4 NULL,
    nearest_airport varchar NULL,
    international_airport varchar NULL,
    geo_dst_near_airport float4 NOT NULL,
    geo_dst_intl_airport float4 NOT NULL,
    drv_dst_near_airport float4 NOT NULL,
    drv_time_near_airport float4 NOT NULL,
    drv_dst_intl_airport float4 NOT NULL,
    drv_time_intl_airport float4 NOT NULL,
	CONSTRAINT AMC_building_pk PRIMARY KEY (building_code),
    CONSTRAINT AMC_building_nearest FOREIGN KEY (nearest_airport) REFERENCES airport(airport_code),
    CONSTRAINT AMC_building_intl FOREIGN KEY (international_airport) REFERENCES airport(airport_code)
);


CREATE TABLE distance_lookup (
	building_code varchar NOT NULL,
	zipcode varchar NOT NULL,
    city varchar NULL,
    state_province varchar NULL,
    country_code varchar NOT NULL,
    lat float4 NULL,
	lon float4 NULL,
	geodesic_distance float4 NOT NULL,
	driving_distance float4 NULL,
    driving_time float4 NULL,
	CONSTRAINT distance_lookup_pk PRIMARY KEY (building_code, zipcode),
	CONSTRAINT distance_lookup_fk FOREIGN KEY (building_code) REFERENCES AMC_building(building_code)
);

CREATE TABLE guest (
	guest_UID varchar NOT NULL,
	zipcode varchar NOT NULL,
    city varchar NULL,
    state_province varchar NULL,
    country_code varchar NOT NULL,
	CONSTRAINT guest_pk PRIMARY KEY (guest_UID)
);

CREATE TABLE itinerary (
	itinerary_id int4 NOT NULL,
	guest_uid varchar NOT NULL,
	max_group_size int4 NOT NULL,
	arrival_date date NOT NULL,
	departure_date date NOT NULL,
	in_geodesic_distance float4 NULL,
	in_drv_distance float4 NULL,
	in_drv_time float4 NULL,
	out_geodesic_distance float4 NULL,
	out_drv_distance float4 NULL,
	out_drv_time float4 NULL,
	group_type_code VARCHAR NULL,
	CONSTRAINT itinerary_pk PRIMARY KEY (itinerary_id),
    CONSTRAINT itinerary_fk FOREIGN KEY (guest_uid) REFERENCES guest(guest_uid)
);

CREATE TABLE reservation (
	itinerary_id int4 NOT NULL,
    reservation int4 NOT NULL,
	CONSTRAINT reservation_fk FOREIGN KEY (itinerary_id) REFERENCES itinerary(itinerary_id)
);

CREATE TABLE building_visited (
	itinerary_id int4 NOT NULL,
    building_code varchar NOT NULL,
    arrival_date date NOT NULL,
	departure_date date NOT NULL,
	CONSTRAINT bldg_visited_fk1 FOREIGN KEY (itinerary_id) REFERENCES itinerary(itinerary_id),
    CONSTRAINT bldg_visited_fk2 FOREIGN KEY (building_code) REFERENCES AMC_building(building_code)
);

CREATE TABLE ghg (
	itinerary_id int4 NOT NULL,
    ghg30 float4 NOT NULL,
	ghg50 float4 NOT NULL,
	bus float4 NOT NULL,
	grp float4 NOT NULL,
	CONSTRAINT ghg_fk FOREIGN KEY (itinerary_id) REFERENCES itinerary(itinerary_id)
);

CREATE TABLE ghg_err (
	year_in int4 NOT NULL,
	invalid int4 NOT NULL,
	errors int4 NOT NULL,
	total int4 NOT NULL,
	CONSTRAINT year_pk PRIMARY KEY (year_in)
);

-- Views
CREATE VIEW emissions AS
SELECT 	i.itinerary_id, 
		i.arrival_date,
		i.departure_date, 
		i.max_group_size, 
		i.in_drv_distance, 
		i.out_drv_distance, 
		i.guest_uid,
		g.ghg30, 
		g.ghg50,
		bus,
		grp
FROM itinerary i
INNER JOIN ghg g 
ON g.itinerary_id = i.itinerary_id;

create view building_emissions as 
select 
	a.building_name,
	a.building_class,
	b.arrival_date,
	b.building_code,
	b.departure_date,
	e.ghg30 as GHG30, 
	e.ghg50 as GHG50,
	e.bus as bus,
	e.grp as grp,
	e.guest_uid as guest_uid
	from emissions e 
	right join building_visited b 
	on e.itinerary_id = b.itinerary_id
	inner join amc_building a
	on b.building_code = a.building_code;

CREATE VIEW building_emissions_per_month as
select
	to_char(arrival,'Mon') as month,
	extract(year from arrival) as year,
	building_name,
	building_class,
	ghg30,
	ghg50,
	bus,
	grp
from
(select
	date_trunc('month', arrival_date) as arrival, -- or hour, day, week, month, year
	building_name,
	building_class,
	sum(ghg30) as GHG30, 
	sum(ghg50) as GHG50,
	sum(bus) as bus,
	sum(grp) as grp
	from building_emissions
	group by arrival, building_name, building_class
	order by 1) as be;

CREATE VIEW building_origin AS
SELECT
	to_char(arrival,'Mon') as month,
	extract(year from arrival) as year,
	building_code,
	building_name,
	building_class,
	zipcode,
	ghg30,
	ghg50,
	bus,
	grp
FROM
(select
	date_trunc('month', b.arrival_date) as arrival,
	b.building_code as building_code,
	b.building_name as building_name,
	b.building_class as building_class,
	left(g.zipcode, 3) as zipcode,
	sum(b.ghg30) as GHG30, 
	sum(b.ghg50) as GHG50,
	sum(b.bus) as bus,
	sum(b.grp) as grp
from building_emissions b
left join guest g
on b.guest_uid = g.guest_uid
GROUP BY arrival, building_code, building_name, building_class, zipcode) as bo;

-- Queries 
select extract(year from arrival_date) as year, 
sum(ghg30) as GHG30, 
sum(ghg30) as GHG50 
from emissions 
group by 1;

select to_char(arrival_date,'Mon') as month, 
extract(year from arrival_date) as year, 
sum(ghg30) as GHG30, 
sum(ghg30) as GHG50 
from emissions 
group by 1,2
order by 1;

select 
	to_char(arrival,'Mon') as month,
	extract(year from arrival) as year,
	ghg30,
	ghg50,
	bus
from 
	(select
	date_trunc('month', arrival_date) as arrival, -- or hour, day, week, month, year
	sum(ghg30) as GHG30, 
	sum(ghg50) as GHG50,
	sum(bus) as bus
	from emissions
	group by 1
	order by 1) as ghg_monthly;

SELECT
	extract(year from arrival_date) as year,
	avg(in_drv_distance) as in_drv_dist,
	avg(out_drv_distance) as out_drv_dist,
	avg(max_group_size) as guests,
	count(itinerary_id) as trips
FROM
	emissions
group by 1
order by 1;

SELECT
	extract(year from arrival_date) as year,
	sum(ghg30) as ghg30,
	sum(ghg50) as ghg50,
	sum(bus) as bus,
	sum(grp) as grp,
	count(itinerary_id) as trips
FROM
	emissions
group by 1
order by 1;

SELECT
	extract(year from arrival_date) as year,
	count(in_geodesic_distance) as in_geo,
	count(out_geodesic_distance) as out_geo
FROM
	itinerary
WHERE
	in_geodesic_distance < 600
group by 1
order by 1;

-- Queries for AMC

select
	to_char(arrival,'Mon') as month,
	extract(year from arrival) as year,
	building_name,
	building_class,
	ghg30,
	ghg50,
	bus,
	grp
from
(select
	date_trunc('month', arrival_date) as arrival, -- or hour, day, week, month, year
	building_name,
	building_class,
	sum(ghg30) as GHG30, 
	sum(ghg50) as GHG50,
	sum(bus) as bus,
	sum(grp) as grp
	from building_emissions
	group by arrival, building_name, building_class
	order by 1) as be

SELECT
	to_char(arrival,'Mon') as month,
	extract(year from arrival) as year,
	building_name,
	building_class,
	zipcode,
	ghg30,
	ghg50,
	bus,
	grp
FROM
(select
	date_trunc('month', b.arrival_date) as arrival,
	b.building_name as building_name,
	b.building_class as building_class,
	left(g.zipcode, 3) as zipcode,
	sum(b.ghg30) as GHG30, 
	sum(b.ghg50) as GHG50,
	sum(b.bus) as bus,
	sum(b.grp) as grp
from building_emissions b
left join guest g
on b.guest_uid = g.guest_uid
GROUP BY arrival, building_name, building_class, zipcode) as bo

-- Emissions from Origin
select 
	month,
	year,
	building_code,
	building_name,
	building_class,
	zipcode,
	count(ghg30) as trips,
	sum(ghg30) as ghg30,
	sum(ghg50) as ghg50,
	sum(bus) as bus,
	sum(b.grp) as grp
from building_origin
where building_code = '1ca' AND zipcode='021'
GROUP BY month, year, building_code, building_name, building_class, zipcode;

-- Emissions by day of the week
SELECT
	extract(dow from arrival_date) as dow,
	building_code,
	building_name,
	building_class,
	count(ghg30) as trips,
	sum(ghg30) as ghg30,
	sum(ghg50) as ghg50,
	sum(bus) as bus,
	sum(grp) as grp
from building_emissions
WHERE arrival_date > '2016-06-01' and arrival_date < '2016-09-01'
GROUP BY dow, building_code, building_name, building_class;

-- Test of data
select 
	i.itinerary_id, 
	i.arrival_date, 
	i.departure_date, 
	r.reservation,
	i.max_group_size
from itinerary i
right join reservation r
on i.itinerary_id = r.itinerary_id
where i.itinerary_id = 20;