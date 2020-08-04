# -*- coding: utf-8 -*-
"""
API for AMC
Author: Kshiti Mehta, Augusto Espin
UMASS
DS4CG
"""    
from flask import Flask, render_template, make_response, request, flash, jsonify
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, DateField, FieldList, BooleanField, FormField, SelectField, SelectMultipleField
from wtforms.validators import DataRequired, InputRequired
from flask_wtf.file import FileRequired, FileField
import psycopg2
from pandas import DataFrame
import pandas as pd
import matplotlib.pyplot as plt
import csv
import io
from waitress import serve
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
# Required for upload
from redis import Redis
import rq
from upload import upload_and_process
from amcdb import amcdb

app = Flask(__name__, static_url_path='/static')
# Initialize secret key and bootstrap

bootstrap = Bootstrap(app)

# Definitions of classes for forms

# Class for form for upload
class UploadForm(FlaskForm):
    year = DateField('Year of the data:', format='%Y', validators=[DataRequired()])
    csv = FileField(label='CSV File:', validators=[FileRequired()])
    submit = SubmitField('Upload')

# Class for most analysis
class AnalysisForm(FlaskForm):
    year = SelectMultipleField('Years:', validators=[InputRequired()], coerce=int)
    facilities = SelectMultipleField('Facilities:', validators=[InputRequired()])
    submit = SubmitField('Choose')

# Class for Yearly emissions
class AnalysisYearlyForm(FlaskForm):
    facilities = SelectMultipleField('Facilities:', validators=[InputRequired()])
    submit = SubmitField('Choose')

# Class for Area and and facility
class AnalysisAreaForm(FlaskForm):
    year = SelectMultipleField('Years:', validators=[InputRequired()], coerce=int)
    area = SelectField('Area:', choices=[('010','Springfield, MA'),('015','Worceter, MA'),('017','Framingham, MA'),('018','Woburn, MA'),('019','Lynn, MA'),('020','Brockton, MA'),('021','Boston, MA'),('024','Lexington, MA'),('028','Providence,RI'),('030','Manchester, NH'),('032','Waterville Valley, NH'),('038','Portsmouth, NH'),('040','Portland, ME'),('054','Burlington, VT'),('060','Hartford, CT'),('064','New Haven, CT'),('070','Newark, NJ'),('100','New York, NY'),('112','Brooklyn, NY'),('190','Philadelphia, PA'), ('200','Washington DC')], validators=[InputRequired()])
    facilities = SelectMultipleField('Facilities:', validators=[InputRequired()])
    submit = SubmitField('Choose')

# Class for Days of the week over a certain dates
class AnalysisDOWFacilities(FlaskForm):
    start_date = DateField('Start of Period (mm/dd/yyyy):', format=r'%m/%d/%Y', validators=[DataRequired()])
    end_date = DateField('End of Period (mm/dd/yyyy):', format=r'%m/%d/%Y', validators=[DataRequired()])
    facilities = SelectMultipleField('Facilities:', validators=[InputRequired()])
    submit = SubmitField('Choose')

########## We should check the use of psycopg2 since it will prevent migration to other databases ########
con = psycopg2.connect(database="amcdb", user="aespin", password="amc2020", host="128.119.246.25", port="5432")
queue = rq.Queue('amc-tasks', connection=Redis.from_url('redis://'))
job = None

@app.route("/")
def main():   
    return render_template("home.html")

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    global job
    # Find out if job is still running
    try: 
        if job != None:
            job.refresh()
            p = job.meta['progress']
            year = job.meta['year']
            if p < 100:
                return render_template('loading.html', year = year)
    except:
        job = None
    # This works with GET
    form = UploadForm()
    # This works with POST
    if form.validate_on_submit():
        # Process data here
        year = form.year.data.year
        csv = form.csv.data
        # Load the data and put in the server 
        df = pd.read_csv(csv) 
        # Run the process
        job = queue.enqueue(upload_and_process, dbstring=dbstring, apikey=key, year=year, data=df, job_timeout=1500)
        return render_template('loading.html', year = year)
    # If get then return the template with the form
    return render_template('upload_reservations.html', form = form)

@app.route('/progress')
def task_progress():
    #job = rq.get_current_job()
    #if job:
    job.refresh()
    return jsonify(job.meta)

@app.route('/q_emissions_facility', methods=['GET','POST'])
def q_emissions_facility():
    '''
    Emissions per facility per year
    '''
    
    # Select the data from database
    years = pd.read_sql("select distinct year from building_emissions_per_month WHERE building_class='AMC'", dbstring)
    years = years['year'].astype(int).sort_values().tolist()
    facilities = pd.read_sql("select distinct building_name from building_emissions_per_month WHERE building_class='AMC'", dbstring)
    facilities = facilities['building_name'].sort_values().tolist()

    # Create form and load the possible choices
    form = AnalysisForm()
    form.year.choices = [(x,x) for x in years]
    form.facilities.choices = [(x,x) for x in facilities]
    
    # Validate data from form
    if form.validate_on_submit():
        # Initialize objects needed for querying information and creating 
        # response
        si = io.StringIO()
        # Initialize required fields
        years = form.year.data
        buildings = form.facilities.data
        selectYear = ",".join(str(x) for x in years)
        selectFacility = ",".join(repr(x) for x in buildings)
        # Create the query and the file
        qry_str = f"SELECT year, building_name, building_class, sum(ghg30) as ghg30, sum(ghg50) as ghg50, sum(bus) as bus, sum(grp) as grp FROM building_emissions_per_month WHERE year IN ({selectYear}) AND building_name IN ({selectFacility}) AND building_class='AMC' GROUP BY year, building_name, building_class ORDER BY year"
        # Query on database
        df = pd.read_sql(qry_str, dbstring)
        # Make a response as a CSV attachment
        df.to_csv(si, index = False)
        response = make_response(si.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=emissions_for_facilities.csv'
        response.headers["Content-type"] = "text/csv"

        return response
        

    return render_template("q_emissions_facilities.html", form = form)

@app.route('/analysis/q_emissions_facility', methods=['GET','POST'])
def analysis_q_emissions_facility():
    '''
    Emissions per facility per year
    '''
    
    # Select the data from database
    years = pd.read_sql("select distinct year from building_emissions_per_month WHERE building_class='AMC'", dbstring)
    years = years['year'].astype(int).sort_values().tolist()
    facilities = pd.read_sql("select distinct building_name from building_emissions_per_month WHERE building_class='AMC'", dbstring)
    facilities = facilities['building_name'].sort_values().tolist()

    # Create form and load the possible choices
    form = AnalysisForm()
    form.year.choices = [(x,x) for x in years]
    form.facilities.choices = [(x,x) for x in facilities]
    
    # Validate data from form
    if form.validate_on_submit():
#       # Get fields from the form
        years = form.year.data
        buildings = form.facilities.data
        selectYear = ",".join(str(x) for x in years)
        selectFacility = ",".join(repr(x) for x in buildings)

        # Create the query
        qry_str = f"SELECT * FROM building_emissions_per_month WHERE year IN ({selectYear}) AND building_name IN ({selectFacility}) AND building_class = 'AMC'"
        # Query on database
        df = pd.read_sql(qry_str, dbstring)
        
        #Call function to generate the graph and return it
        fig = monthly(df)
        pngImage = io.BytesIO()
        FigureCanvas(fig).print_png(pngImage)
        response = make_response(pngImage.getvalue())
        response.mimetype = 'image/png'
        return response 
        print("Nothing")

    return render_template("/analysis/q_emissions_facilities.html", form = form)

@app.route('/q_emissions_monthly', methods=['GET','POST'])
def q_emissions_monthly():
    '''
    Query emissions per month for given years and facilities 
    '''
    # Initialize years and facilities from database
    years = pd.read_sql("select distinct year from building_emissions_per_month", dbstring)
    years = years['year'].astype(int).sort_values().tolist()
    facilities = pd.read_sql("select distinct building_name from building_emissions_per_month", dbstring)
    facilities = facilities['building_name'].sort_values().tolist()

    # Initialize the form
    form = AnalysisForm()
    form.year.choices = [(x,x) for x in years]
    form.facilities.choices = [(x,x) for x in facilities]
    
    # Validate the form 
    if form.validate_on_submit():
        # Initialize objects needed for querying information and creating 
        # response
        si = io.StringIO()
        # Get fields from the form
        years = form.year.data
        buildings = form.facilities.data
        selectYear = ",".join(str(x) for x in years)
        selectFacility = ",".join(repr(x) for x in buildings)

        # Create the query
        qry_str = f"SELECT * FROM building_emissions_per_month WHERE year IN ({selectYear}) AND building_name IN ({selectFacility})"
        # Query on database
        df = pd.read_sql(qry_str, dbstring)
        # Make a response as a CSV attachment
        df.to_csv(si, index = False)
        response = make_response(si.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=emissions_per_month.csv'
        response.headers["Content-type"] = "text/csv"

        return response

    return render_template("q_emissions_monthly.html", form = form)

@app.route('/analysis/q_emissions_monthly', methods=['GET','POST'])
def analysis_q_emissions_monthly():
    '''
    Query emissions per month for given years and facilities 
    '''
    # Initialize years and facilities from database
    years = pd.read_sql("select distinct year from building_emissions_per_month", dbstring)
    years = years['year'].astype(int).sort_values().tolist()
    facilities = pd.read_sql("select distinct building_name from building_emissions_per_month", dbstring)
    facilities = facilities['building_name'].sort_values().tolist()

    # Initialize the form
    form = AnalysisForm()
    form.year.choices = [(x,x) for x in years]
    form.facilities.choices = [(x,x) for x in facilities]
    
    # Validate the form 
    if form.validate_on_submit():
        # Get fields from the form
        years = form.year.data
        buildings = form.facilities.data
        selectYear = ",".join(str(x) for x in years)
        selectFacility = ",".join(repr(x) for x in buildings)

        # Create the query
        qry_str = f"SELECT * FROM building_emissions_per_month WHERE year IN ({selectYear}) AND building_name IN ({selectFacility})"
        # Query on database
        df = pd.read_sql(qry_str, dbstring)
        
        #Call function to generate the graph and return it
        fig = monthly(df)
        pngImage = io.BytesIO()
        FigureCanvas(fig).print_png(pngImage)
        response = make_response(pngImage.getvalue())
        response.mimetype = 'image/png'
        return response

    return render_template("q_emissions_monthly.html", form = form)


@app.route('/q_emissions_yearly', methods=['GET','POST'])
def q_emissions_yearly():
    '''
    Emissions per year per facility
    '''
    # Select available facilities from database
    facilities = pd.read_sql("select distinct building_name from building_emissions_per_month", dbstring)
    bldgs = facilities['building_name'].sort_values().tolist()

    # Create the form
    form = AnalysisYearlyForm()
    form.facilities.choices = [(x,x) for x in bldgs]
    
    # POST was submitted, validate the form
    if form.validate_on_submit():
        # Initialize objects needed for querying information and creating 
        # response
        si = io.StringIO()
        # Validate fields
        buildings = form.facilities.data
        selectFacility = ",".join(repr(x) for x in buildings)

        # Create the query
        qry_str = f"SELECT year, building_name, building_class, sum(ghg30) as ghg30, sum(ghg50) as ghg50, sum(bus) as bus, sum(grp) as grp FROM building_emissions_per_month WHERE building_name IN ({selectFacility}) GROUP BY year, building_name, building_class ORDER BY year"
        # Query on database
        df = pd.read_sql(qry_str, dbstring)
        # Make a response as a CSV attachment
        df.to_csv(si, index = False)
        response = make_response(si.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=emissions_per_years.csv'
        response.headers["Content-type"] = "text/csv"

        return response

    return render_template("q_emissions_yearly.html", form = form)

@app.route('/analysis/q_emissions_yearly', methods=['GET','POST'])
def analysis_q_emissions_yearly():
    '''
    Emissions per year per facility
    '''
    # Select available facilities from database
    facilities = pd.read_sql("select distinct building_name from building_emissions_per_month", dbstring)
    bldgs = facilities['building_name'].sort_values().tolist()

    # Create the form
    form = AnalysisYearlyForm()
    form.facilities.choices = [(x,x) for x in bldgs]
    
    # POST was submitted, validate the form
    if form.validate_on_submit():
        # Initialize objects needed for querying information and creating 
        # Validate fields
        buildings = form.facilities.data
        selectFacility = ",".join(repr(x) for x in buildings)

        # Create the query
        qry_str = f"SELECT year, building_name, building_class, sum(ghg30) as ghg30, sum(ghg50) as ghg50, sum(bus) as bus, sum(grp) as grp FROM building_emissions_per_month WHERE building_name IN ({selectFacility}) GROUP BY year, building_name, building_class ORDER BY year"
        # Query on database
        df = pd.read_sql(qry_str, dbstring)
        # Make a response as a CSV attachment
        fig = yearly(df)
        pngImage = io.BytesIO()
        FigureCanvas(fig).print_png(pngImage)
        response = make_response(pngImage.getvalue())
        response.mimetype = 'image/png'
        return response

    return render_template("/analysis/q_emissions_yearly.html", form = form)

@app.route('/q_emissions_facility_boston', methods=['GET','POST'])
def q_emissions_facility_boston():
    '''
    Emissions by area from selected years and facilities
    '''
    
    # Query values available for the select controls
    years = pd.read_sql("select distinct year from building_origin", dbstring)
    years = years['year'].astype(int).sort_values().tolist()
    facilities = pd.read_sql("select distinct building_code, building_name from building_origin", dbstring)
    facilities.sort_values(by=['building_name'])
    bldg_name = facilities['building_name'].tolist()
    bldg_code = facilities['building_code'].tolist()

    # Create a form and add choices found
    form = AnalysisAreaForm()
    form.year.choices = [(x,x) for x in years]
    form.facilities.choices = [(x,y) for x,y in zip(bldg_code,bldg_name)]
      
    if form.validate_on_submit():
        # Initialize objects needed for querying information and creating 
        # response
        si = io.StringIO()
        amc = amcdb(dbstring)
        # After validated and POST create the variables from user response 
        years = form.year.data
        buildings = form.facilities.data
        area = form.area.data

        # Query data from database and filter for years
        df = amc.emissions_by_building_origin(buildings,area)
        resp = df[df['year'].isin(years)]

        # Make a response as a CSV attachment
        resp.to_csv(si, index = False)
        response = make_response(si.getvalue())
        response.headers['Content-Disposition'] = f'attachment; filename=emissions_for_{area}.csv'
        response.headers["Content-type"] = "text/csv"

        return response

    return render_template("q_emissions_area.html", form = form)

@app.route('/analysis/q_emissions_facility_boston', methods=['GET','POST'])
def analysis_q_emissions_facility_boston():
    '''
    Emissions by area from selected years and facilities
    '''
    
    # Query values available for the select controls
    years = pd.read_sql("select distinct year from building_origin", dbstring)
    years = years['year'].astype(int).sort_values().tolist()
    facilities = pd.read_sql("select distinct building_code, building_name from building_origin", dbstring)
    facilities.sort_values(by=['building_name'])
    bldg_name = facilities['building_name'].tolist()
    bldg_code = facilities['building_code'].tolist()

    # Create a form and add choices found
    form = AnalysisAreaForm()
    form.year.choices = [(x,x) for x in years]
    form.facilities.choices = [(x,y) for x,y in zip(bldg_code,bldg_name)]
      
    if form.validate_on_submit():
        # Initialize objects needed for querying information and creating 
        amc = amcdb(dbstring)
        # After validated and POST create the variables from user response 
        years = form.year.data
        buildings = form.facilities.data
        area = form.area.data

        # Query data from database and filter for years
        df = amc.emissions_by_building_origin(buildings,area)
        resp = df[df['year'].isin(years)]

        #Call function to generate the graph and return it
        fig = zipcode(resp)
        pngImage = io.BytesIO()
        FigureCanvas(fig).print_png(pngImage)
        response = make_response(pngImage.getvalue())
        response.mimetype = 'image/png'
        return response
    
    return render_template("/analysis/q_emissions_area.html", form = form)

@app.route('/q_emissions_dow', methods=['GET','POST'])
def q_emissions_dow():
    '''
    Query emissions per month for given years and facilities 
    '''
    # Select available facilities from database
    years = pd.read_sql("select distinct year from building_origin", dbstring)
    years = years['year'].astype(int).sort_values().tolist()
    facilities = pd.read_sql("select distinct building_code, building_name from building_origin", dbstring)
    facilities.sort_values(by=['building_name'])
    bldg_name = facilities['building_name'].tolist()
    bldg_code = facilities['building_code'].tolist()

    # Create the form
    form = AnalysisDOWFacilities()
    form.facilities.choices = [(x,y) for x,y in zip(bldg_code,bldg_name)]
    
    # POST was submitted, validate the form
    if form.validate_on_submit():
        # Initialize objects needed for querying information and creating 
        # response
        si = io.StringIO()
        amc = amcdb(dbstring)
        # After validated and POST create the variables from user response 
        start_date = form.start_date.data
        end_date = form.end_date.data
        buildings = form.facilities.data

        print(start_date,end_date)

        # Query data from database and filter for years
        df = amc.emissions_by_day(start_date,end_date)
        resp = df[df['building_code'].isin(buildings)].groupby(['dow','building_name']).sum().reset_index()
        resp['dow'] = resp['dow'].astype(int)
        resp['dow'] = resp['dow'].map({0:'Mon',1:'Tue',2:'Wed',3:'Thu',4:'Fri',5:'Sat',6:'Sun'})
        
        # Make a response as a CSV attachment
        resp.to_csv(si, index = False)
        response = make_response(si.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=emissions_per_years.csv'
        response.headers["Content-type"] = "text/csv"

        return response

    return render_template("q_emissions_dow.html", form = form)

@app.route('/analysis/q_emissions_dow', methods=['GET','POST'])
def analysis_q_emissions_dow():
    '''
    Query emissions per month for given years and facilities 
    '''
    # Select available facilities from database
    years = pd.read_sql("select distinct year from building_origin", dbstring)
    years = years['year'].astype(int).sort_values().tolist()
    facilities = pd.read_sql("select distinct building_code, building_name from building_origin", dbstring)
    facilities.sort_values(by=['building_name'])
    bldg_name = facilities['building_name'].tolist()
    bldg_code = facilities['building_code'].tolist()

    # Create the form
    form = AnalysisDOWFacilities()
    form.facilities.choices = [(x,y) for x,y in zip(bldg_code,bldg_name)]
    
    # POST was submitted, validate the form
    if form.validate_on_submit():
        # Initialize objects needed for querying information and creating 
        # response
        amc = amcdb(dbstring)
        # After validated and POST create the variables from user response 
        start_date = form.start_date.data
        end_date = form.end_date.data
        buildings = form.facilities.data

        # Query data from database and filter for years
        df = amc.emissions_by_day(start_date,end_date)
        resp = df[df['building_code'].isin(buildings)].groupby(['dow','building_name']).sum().reset_index()
        resp['dow'] = resp['dow'].astype(int)
        resp['dow'] = resp['dow'].map({0:'Mon',1:'Tue',2:'Wed',3:'Thu',4:'Fri',5:'Sat',6:'Sun'})
        resp = resp.groupby('dow').sum().reset_index()
        fig = plt.figure(figsize=(20,20))
        ax = fig.add_subplot(1, 1, 1)
        resp.plot(ax=ax, x='dow', y=["ghg30", "ghg50", "bus", "grp"], kind="bar")
        ax.set_xlabel("Day of the week")
        ax.set_ylabel("GHG Emissions in metric tonnes")
        ax.set_title("Distribution of GHG Emissions by day of the week as per facility chosen and dates")
        pngImage = io.BytesIO()
        FigureCanvas(fig).print_png(pngImage)
        response = make_response(pngImage.getvalue())
        response.mimetype = 'image/png'
        return response
    return render_template("/analysis/q_emissions_dow.html", form = form)

@app.route('/analysis/visualisation')
def visualisation():
    fig = vis()
    pngImage = io.BytesIO()
    FigureCanvas(fig).print_png(pngImage)
    response = make_response(pngImage.getvalue())
    response.mimetype = 'image/png'
    return response

def monthly(result):
    fig = Figure(figsize=(20,40))
    df = DataFrame(result,columns=['month','year','building_code','building_name','building_class','zipcode','ghg30','ghg50','bus','grp'])
    years=df["year"].unique().tolist()
    years = list(map(str, years))
    x = len(years)
    i = 0
    for year in years:
        i += 1
        new = df.loc[df['year']==float(year)]
        
        ghg30=new.groupby(['month'])['ghg30'].sum().reset_index(name="Total GHG30 per month")
        month_ghg30 = dict(zip(ghg30['month'], ghg30['Total GHG30 per month'])) 
        ghg50=new.groupby(['month'])['ghg50'].sum().reset_index(name="Total GHG50 per month")
        month_ghg50 = dict(zip(ghg50['month'], ghg50['Total GHG50 per month']))
    
        keylist30 = month_ghg30.keys()
        keylist50 = month_ghg50.keys()
        sorted_d = {}
        for key in keylist30:
            sorted_d.update({key: month_ghg30[key]})
        axis1 = fig.add_subplot(x,2,i)
        axis1.bar(list(sorted_d.keys()), sorted_d.values(), color='black')
        axis1.set_xlabel("Month")
        axis1.set_ylabel("GHG30 in metric tonnes")
        axis1.set_title("Distribution of GHG30 Monthly of year "+year)
        
        for key in keylist50:
            sorted_d.update({key: month_ghg50[key]})
        axis = fig.add_subplot(x,2,i+1, sharey=axis1)
        axis.bar(list(sorted_d.keys()), sorted_d.values(), color='blue')
        axis.set_xlabel("Month")
        axis.set_ylabel("GHG50 in metric tonnes")
        axis.set_title("Distribution of GHG50 Monthly of year "+year)
        i+=1
    return fig

def yearly(result):
    fig = Figure(figsize=(40,20))
    df = DataFrame(result,columns=['month','year','building_code','building_name','building_class','zipcode','ghg30','ghg50','bus','grp'])
    
    
    ghg30=df.groupby(['year'])['ghg30'].sum().reset_index(name="Total GHG30 per year")
    year_ghg30 = dict(zip(ghg30['year'], ghg30['Total GHG30 per year'])) 
    ghg50=df.groupby(['year'])['ghg50'].sum().reset_index(name="Total GHG50 per year")
    year_ghg50 = dict(zip(ghg50['year'], ghg50['Total GHG50 per year']))

    keylist30 = sorted(year_ghg30.keys())
    keylist50 = sorted(year_ghg50.keys())
    sorted_d = {}
    for key in keylist30:
        sorted_d.update({key: year_ghg30[key]})
    axis1 = fig.add_subplot(1,2,1)
    axis1.bar(list(sorted_d.keys()), sorted_d.values(), color='black')
    axis1.set_xlabel("Year")
    axis1.set_ylabel("GHG30 in metric tonnes")
    axis1.set_title("Distribution of GHG30 Yearly")
    
    for key in keylist50:
        sorted_d.update({key: year_ghg50[key]})
    axis = fig.add_subplot(1,2,2)
    axis.bar(list(sorted_d.keys()), sorted_d.values(), color='blue')
    axis.set_xlabel("Month")
    axis.set_ylabel("GHG50 in metric tonnes")
    axis.set_title("Distribution of GHG50 Yearly")
    return fig    

def zipcode(result):
    fig = Figure(figsize=(20,40))
    df = DataFrame(result,columns=['month','year','building_code','building_name','building_class','zipcode','ghg30','ghg50','bus','grp'])
    
    years=df["year"].unique().tolist()
    years = list(map(str, years))
    x = len(years)
    i = 0
    for year in years:
        i += 1
        new = df.loc[df['year']==float(year)]
        
        ghg30=new.groupby(['month'])['ghg30'].sum().reset_index(name="Total GHG30 per month")
        month_ghg30 = dict(zip(ghg30['month'], ghg30['Total GHG30 per month'])) 
        ghg50=new.groupby(['month'])['ghg50'].sum().reset_index(name="Total GHG50 per month")
        month_ghg50 = dict(zip(ghg50['month'], ghg50['Total GHG50 per month']))
    
        keylist30 = month_ghg30.keys()
        keylist50 = month_ghg50.keys()
        sorted_d = {}
        for key in keylist30:
            sorted_d.update({key: month_ghg30[key]})
        axis1 = fig.add_subplot(x,2,i)
        axis1.bar(list(sorted_d.keys()), sorted_d.values(), color='black')
        axis1.set_xlabel("Month")
        axis1.set_ylabel("GHG30 in metric tonnes")
        axis1.set_title("Distribution of GHG30 Monthly of year "+year)
        
        for key in keylist50:
            sorted_d.update({key: month_ghg50[key]})
        axis = fig.add_subplot(x,2,i+1, sharey=axis1)
        axis.bar(list(sorted_d.keys()), sorted_d.values(), color='blue')
        axis.set_xlabel("Month")
        axis.set_ylabel("GHG50 in metric tonnes")
        axis.set_title("Distribution of GHG50 Monthly of year "+year)
        i+=1
    return fig

def vis():
    fig = Figure(figsize=(10,10))

    qry_str = f"SELECT * FROM building_emissions_per_month"
    # Query on database
    df = pd.read_sql(qry_str, dbstring)   
    ghg30=df.groupby(['year'])['ghg30'].sum().reset_index(name="Total GHG30 per year")
    year_ghg30 = dict(zip(ghg30['year'], ghg30['Total GHG30 per year'])) 
    ghg50=df.groupby(['year'])['ghg50'].sum().reset_index(name="Total GHG50 per year")
    year_ghg50 = dict(zip(ghg50['year'], ghg50['Total GHG50 per year'])) 
    bus=df.groupby(['year'])['bus'].sum().reset_index(name="Total emissions with bus per year")
    year_bus = dict(zip(bus['year'], bus['Total emissions with bus per year'])) 
    group=df.groupby(['year'])['grp'].sum().reset_index(name="Total emissions with group per year")
    year_group = dict(zip(group['year'], group['Total emissions with group per year'])) 
    list(year_ghg30)
    list(year_ghg50)
    list(year_bus)
    list(year_group)
    keylist = []
    value30list = []
    value50list = []
    valuebuslist = []
    valuegrouplist = []
    for key, value in year_ghg30.items():
        keylist.append(key)
        value30list.append(value)
    for key, value in year_ghg50.items():
        value50list.append(value)
    for key, value in year_bus.items():
        valuebuslist.append(value)
    for key, value in year_group.items():
        valuegrouplist.append(value)

    axis = fig.add_subplot(1,1,1)
    axis.plot(keylist, value30list, color='g', marker = 'o')
    axis.plot(keylist, value50list, color='orange', marker = 'o')
    axis.plot(keylist, valuebuslist, color='blue', marker = 'o')
    axis.plot(keylist, valuegrouplist, color='pink', marker = 'o')
    axis.set_xlabel('Year')
    axis.set_ylabel('GHG Emissions per year in metric tonnes')
    axis.legend(['GHG with 30%', 'GHG with 50%', 'GHG with bus','GHG with groups'], loc='upper right')
    axis.set_title('GHG emission comparison with 30%, 50% light duty trucks, considering bus and groups travel per year')
    return fig
    
@app.route("/export")
def csv_export():
    table = request.args.get('type')
    si = io.StringIO()
    cw = csv.writer(si)
    c = con.cursor()
    if table == "table1":
        c.execute('SELECT * FROM itinerary FULL OUTER JOIN reservation ON itinerary.itinerary_id=reservation.itinerary_id ORDER BY itinerary.itinerary_id ASC')
        rows = c.fetchall()
        cw.writerow([i[0] for i in c.description])
        cw.writerows(rows)
        response = make_response(si.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=itinerary.csv'
        response.headers["Content-type"] = "text/csv"
        return response    
    if table == "table2":
        c.execute('SELECT * FROM building_origin')
        rows = c.fetchall()
        cw.writerow([i[0] for i in c.description])
        cw.writerows(rows)
        response = make_response(si.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=emissions.csv'
        response.headers["Content-type"] = "text/csv"
        return response    
        print()
    if table == "table3":
        c.execute('SELECT * FROM distance_lookup')
        rows = c.fetchall()
        cw.writerow([i[0] for i in c.description])
        cw.writerows(rows)
        response = make_response(si.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=emissions.csv'
        response.headers["Content-type"] = "text/csv"
        return response  
        print()
    if table == "table4":
        # insert function here
        print()
    else:
        return render_template('export.html')
    
app.run()
#serve(app, host='127.0.0.1', port=8000)

