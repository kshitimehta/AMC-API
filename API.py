# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""    
from flask import Flask, render_template, make_response, request, send_file, Response
import psycopg2
from pandas import DataFrame
import pandas as pd
#import matplotlib.pyplot as plt
import io
from waitress import serve
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
#from mpl_toolkits.basemap import Basemap
import csv
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import numpy as np
from base64 import urlsafe_b64encode

#PEOPLE_FOLDER = os.path.join('static', 'images')
app = Flask(__name__, static_url_path='/static')
#app.debug=True

#@app.route('/user/<username>')
#def show_user_profile(username):
#    # show the user profile for that user
#    return 'User %s' % escape(username)
#
#@app.route('/post/<int:post_id>')
#def show_post(post_id):
#    # show the post with the given id, the id is an integer
#    return 'Post %d' % post_id
#
#app.config['UPLOAD_FOLDER'] = PEOPLE_FOLDER

con = psycopg2.connect(database="amcdb", user="aespin", password="amc2020", host="128.119.246.25", port="5432")
cursor = con.cursor()

@app.route("/")
def main():  
#    fig = guestdemo()
#    pngImage = io.BytesIO()
#    FigureCanvas(fig).print_png(pngImage)
#    response = make_response(pngImage.getvalue())
#    response.mimetype = 'image/png'
#    full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'dot_plot.jpg')
    return render_template("test.html")
#    return response

#def guestdemo():
#    cursor.execute("Select * from map_data")
#    org_df = cursor.fetchall()
#    org_df['year'] = pd.DatetimeIndex(org_df['arrival_date']).year
#    df = org_df.dropna(subset=['lat','lon'])
#    df_u = df.groupby(['lat','lon','year']).size().reset_index(name='freq')
#
#    # Compute coordinates for map
#    lat_ = [x for x in df_u.lat_p if x != 0]
#    lon_ = [x for x in df_u.lon_p if x != 0]
#    
##    fig = plt.figure(figsize=(8,8))
#    m = Basemap(llcrnrlon=-119,llcrnrlat=22,urcrnrlon=-64,urcrnrlat=49,
#            projection='lcc',lat_1=33,lat_2=45,lon_0=-95)
#    
#    # Setup map
#    m.drawcoastlines(color = 'gray')
#    m.drawcountries(color = 'red')
#    m.drawstates(color ='black')
#    
#    # Coordinates of origin
#    lon, lat = m(lon_, lat_)
#    # Frequency of that point
#    sz = df_u['freq']
#    
#    m.scatter(lon,lat, s=sz, c=df_u['year'], cmap='Blues', alpha = 0.5)
#    plt.colorbar(label=r'Years')
#    plt.clim(2013, 2019)
##    plt.savefig("/static/images/dot_plot.png", dpi=300, bbox_inches='tight')
#    return plt

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    df = pd.DataFrame()
    type_field = ""
    if request.method == 'POST':
        df = pd.read_csv(request.files.get('file'))
    type_field = request.args.get('type')
    if type_field=="guestload":
        #run pipeline for guest data here  
        print("")
    return render_template('upload.html', shape=df.shape, data=type_field) 

@app.route('/analysis', methods=['GET','POST'])
def analysis():
    viz = request.args.get('type')
    cursor.execute("SELECT * from building_origin")
    result = cursor.fetchall()
    df = DataFrame(result,columns=['month','year','building_code','building_name','building_class','zipcode','ghg30','ghg50','bus','grp'])
    
    years=df["year"].unique().tolist()
    years = list(map(float, years))
    years.append("ALL")
    
    facilities = df["building_name"].unique().tolist()
    facilities = list(map(str, facilities))
    facilities.append("None")
    
    if request.method == "POST":
        global selectYear, selectFacility
        listselectYear = request.form.getlist('year')
        selectYear = ",".join(x for x in listselectYear)
        listselectFacility = request.form.getlist('fac')
        print(type(listselectFacility[0]))
        selectFacility = ",".join(repr(x) for x in listselectFacility)
        print(selectFacility)
    if viz == "graph1":
        fig = monthly(selectYear, selectFacility)
        pngImage = io.BytesIO()
        FigureCanvas(fig).print_png(pngImage)
        response = make_response(pngImage.getvalue())
        response.mimetype = 'image/png'
        return response
    
    if viz == "graph2":
        fig = yearly(selectFacility)
        pngImage = io.BytesIO()
        FigureCanvas(fig).print_png(pngImage)
        response = make_response(pngImage.getvalue())
        response.mimetype = 'image/png'
        return response
        
    if viz == "graph3":
        fig = zipcode(selectYear,selectFacility)
        pngImage = io.BytesIO()
        FigureCanvas(fig).print_png(pngImage)
        response = make_response(pngImage.getvalue())
        response.mimetype = 'image/png'
        return response
        
    if viz == "graph4":
        # insert function here
        print()
    
    link = request.args.get('type')
    
    if link == "clicked":
        si = io.StringIO()
        cw = csv.writer(si)
        if selectFacility!="'None'" and selectYear!='ALL':
            s = "SELECT * from building_origin Where year IN ("+selectYear+") and building_name IN ("+selectFacility+")"
            cursor.execute(s)
        elif selectYear!='ALL':
            cursor.execute("SELECT * from building_origin Where year IN (",selectYear,")")
        else:
            cursor.execute('SELECT * from building_origin')
        rows = cursor.fetchall()
        cw.writerow([i[0] for i in cursor.description])
        cw.writerows(rows)
        response = make_response(si.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=emissions.csv'
        response.headers["Content-type"] = "text/csv"
        return response
    else:
        return render_template('analysis.html', years=years, facilities=facilities)
  
def monthly(selectYear, selectFacility):
    fig = Figure(figsize=(20,40))
    if selectFacility!="'None'" and selectYear!='ALL':
        s = "SELECT * from building_origin Where year IN ("+selectYear+") and building_name IN ("+selectFacility+")"
        cursor.execute(s)
        fig = Figure(figsize=(40,20))
    elif selectYear!='ALL':
        cursor.execute("SELECT * from building_origin Where year IN ("+selectYear+")")
    else:
        cursor.execute('SELECT * from building_origin')
    result = cursor.fetchall()
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

def yearly(selectFacility):
    fig = Figure(figsize=(40,20))
    if selectFacility!="'None'":
        cursor.execute("SELECT * from building_origin Where building_name IN ("+selectFacility+")")
    else:
        print("Im in")
        cursor.execute("SELECT * from building_origin")
    result = cursor.fetchall()
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

def zipcode(selectYear, selectFacility):
    fig = Figure(figsize=(20,40))
    if selectFacility!="'None'" and selectYear!='ALL':
        s = "SELECT * from building_origin Where year IN ("+selectYear+") and zipcode= '021' and building_name IN ("+selectFacility+")"
        cursor.execute(s)
        fig = Figure(figsize=(40,20))
    elif selectYear!='ALL':
        cursor.execute("SELECT * from building_origin Where zipcode='021' and year IN ("+selectYear+")")
    else:
        cursor.execute("SELECT * from building_origin Where zipcode='021'")
    result = cursor.fetchall()
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

@app.route("/export")
def csv_export():
    table = request.args.get('type')
    si = io.StringIO()
    cw = csv.writer(si)
    c = con.cursor()
    if table == "table1":
#        s = "SELECT * FROM itinerary FULL OUTER JOIN reservation ON itinerary.itinerary_id=reservation.itinerary_id ORDER BY itinerary.itinerary_id ASC"
#        SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(s)
        # Set up a variable to store our file path and name.
#        t_path_n_file = ".\itinerary_data.csv"
        # Trap errors for opening the file
#        try:
#            f_output = open(t_path_n_file, 'w')  
#            cursor.copy_expert(SQL_for_file_output, f_output)
#        except psycopg2.Error as e:
#            t_message = "Error: " + e + "/n query we ran: " + s + "/n t_path_n_file: " + t_path_n_file
#            print(t_message)
#        return render_template(t_path_n_file, as_attchment=True)
        # Success!
#        f_output.close()
        # Clean up: Close the database cursor and connection
    #    cursor.close()
    #    con.close()
        
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
    
@app.route('/visualisation')
def visualisation():
    viz = request.args.get('type')
    if viz == "graph1":
        fig = vis()
        pngImage = io.BytesIO()
        FigureCanvas(fig).print_png(pngImage)
        response = make_response(pngImage.getvalue())
        response.mimetype = 'image/png'
        return response
    if viz == "graph2":
        # insert function here
        print()
    if viz == "graph3":
        # insert function here
        print()
    if viz == "graph4":
        # insert function here
        print()
    else:
        return render_template('visualisation.html')


def vis():
    fig = Figure(figsize=(10,10))
    cursor.execute("SELECT itinerary.itinerary_id, ghg.itinerary_id, itinerary.arrival_date, ghg30, ghg50, bus, grp FROM ghg INNER JOIN itinerary ON itinerary.itinerary_id=ghg.itinerary_id")
    result = cursor.fetchall()
    df = DataFrame(result,columns=['itinerary_id1','itinerary_id2','arrival_date','ghg30','ghg50', 'bus','grp'])
    df['year'] = pd.DatetimeIndex(df['arrival_date']).year
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
#    x1 = [val-0.2 for val in keylist]
#    x2 = [val+0.2 for val in keylist]
    axis = fig.add_subplot(1,1,1)
    axis.plot(keylist, value30list, color='g', marker = 'o')
    axis.plot(keylist, value50list, color='orange', marker = 'o')
    axis.plot(keylist, valuebuslist, color='blue', marker = 'o')
    axis.plot(keylist, valuegrouplist, color='pink', marker = 'o')
    axis.set_xlabel('Year')
    axis.set_ylabel('GHG Emissions per year in metric tonnes')
    axis.legend(['GHG with 30%', 'GHG with 50%', 'GHG with bus','GHG with groups'], loc='lower right')
    axis.set_title('GHG emission comparison with 30%, 50% light duty trucks, considering bus and groups travel per year')
#    plt.savefig("/static/images/Line CHart for comparison_1.png", dpi=300, bbox_inches='tight')
#    
    axis.set_autoscaley_on(b=True)
    return fig
    

app.run()
#serve(app, host='127.0.0.1', port=8000)

