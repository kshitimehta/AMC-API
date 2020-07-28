# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""    
from flask import Flask, render_template, make_response, request, flash, jsonify
import psycopg2
from pandas import DataFrame
import pandas as pd
#import matplotlib.pyplot as plt
import io
from waitress import serve
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from redis import Redis
import rq


app = Flask(__name__, static_url_path='/static')
app.secret_key = 'very secret key'
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
 
con = psycopg2.connect(database="amcdb", user="postgres", password="amcdb", host="localhost", port="5432")
cursor = con.cursor()
queue = rq.Queue('amc-tasks', connection=Redis.from_url('redis://'))
job = None

@app.route("/")
def main():   
    return render_template("home.html")

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    global job
    df = pd.DataFrame()
    type_field = "guestload"
    progress = 0
    loading = False

    if request.method == 'POST':
        year = request.form['year']
        ufile = request.files['file']
    
        if ufile.filename == '':
            flash('No file selected')
            return render_template('upload.html', shape=df.shape, data=type_field) 
        else:
            df = pd.read_csv(ufile)
    
        type_field = request.args.get('type')
        if type_field=="guestload":
            # Here we execute the code to preprocess and upload data to database
            #df.to_csv(f'tmp/{ufile.filename}')
            #progress = 20
            job = queue.enqueue("upload.upload_and_process", seconds=30, year=year)
            loading = True

            print(f"Running pipeline on {year}")
    return render_template('upload.html', data=type_field, progress = progress, loading = loading) 

@app.route('/progress')
def task_progress():
    #job = rq.get_current_job()
    #if job:
    job.refresh()
    return jsonify(job.meta)

    #return("Nothing")

@app.route('/Extracting')
def extract():
    query = request.args.get('type')
    result = pd.DataFrame()
    if query == "q1":
        cursor.execute("Select * from itinerary")
        result = cursor.fetchall()
    if query == "q2":
        # insert sql query here
        print()
    if query == "q3":
        # insert sql query here
        print()
    if query == "q4":
        # insert sql query here
        print()
    return render_template('Extracting.html', data = result)


def monthly():
    fig = Figure(figsize=(20,40))
    cursor.execute("SELECT itinerary.itinerary_id, ghg.itinerary_id, itinerary.arrival_date, ghg30, ghg50 FROM ghg INNER JOIN itinerary ON itinerary.itinerary_id=ghg.itinerary_id")
    result = cursor.fetchall()
    df = DataFrame(result,columns=['itinerary_id1','itinerary_id2','arrival_date','ghg30','ghg50'])
    df['month'] = pd.DatetimeIndex(df['arrival_date']).month
    df['Stay_Year'] = pd.DatetimeIndex(df['arrival_date']).year
    years=df["Stay_Year"].unique().tolist()
    years = list(map(str, years))
    x = len(years)
    i = 0
    for year in years:
        i += 1
        new = df.loc[df['Stay_Year']==int(year)]
        ghg30=new.groupby(['month'])['ghg30'].sum().reset_index(name="Total GHG30 per month")
        month_ghg30 = dict(zip(ghg30['month'], ghg30['Total GHG30 per month'])) 
        ghg50=new.groupby(['month'])['ghg50'].sum().reset_index(name="Total GHG50 per month")
        month_ghg50 = dict(zip(ghg50['month'], ghg50['Total GHG50 per month'])) 
        
        keylist30 = sorted(month_ghg30.keys())
        keylist50 = sorted(month_ghg50.keys())
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

@app.route('/analysis')
def analysis():
    viz = request.args.get('type')
    if viz == "graph1":
        fig = monthly()
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
        return render_template('analysis.html')

@app.route("/export")
def csv_export():
    table = request.args.get('type')
    if table == "table1":
        s = "SELECT * FROM itinerary FULL OUTER JOIN reservation ON itinerary.itinerary_id=reservation.itinerary_id ORDER BY itinerary.itinerary_id ASC"
        SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(s)
        # Set up a variable to store our file path and name.
        t_path_n_file = ".\itinerary_data.csv"
        # Trap errors for opening the file
        try:
            f_output = open(t_path_n_file, 'w')  
            cursor.copy_expert(SQL_for_file_output, f_output)
        except psycopg2.Error as e:
            t_message = "Error: " + e + "/n query we ran: " + s + "/n t_path_n_file: " + t_path_n_file
            print(t_message)
#        return render_template("error.html", t_message = t_message)
        # Success!
        f_output.close()
        # Clean up: Close the database cursor and connection
    #    cursor.close()
    #    con.close()
        return render_template('export.html')    

    if table == "table2":
        # insert function here
        print()
    if table == "table3":
        # insert function here
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
    cursor.execute("SELECT itinerary.itinerary_id, ghg.itinerary_id, itinerary.arrival_date, ghg30, ghg50, bus FROM ghg INNER JOIN itinerary ON itinerary.itinerary_id=ghg.itinerary_id")
    result = cursor.fetchall()
    df = DataFrame(result,columns=['itinerary_id1','itinerary_id2','arrival_date','ghg30','ghg50', 'bus'])
    df['year'] = pd.DatetimeIndex(df['arrival_date']).year
    ghg30=df.groupby(['year'])['ghg30'].sum().reset_index(name="Total GHG30 per year")
    year_ghg30 = dict(zip(ghg30['year'], ghg30['Total GHG30 per year'])) 
    ghg50=df.groupby(['year'])['ghg50'].sum().reset_index(name="Total GHG50 per year")
    year_ghg50 = dict(zip(ghg50['year'], ghg50['Total GHG50 per year'])) 
    bus=df.groupby(['year'])['bus'].sum().reset_index(name="Total emissions with bus per year")
    year_bus = dict(zip(bus['year'], bus['Total emissions with bus per year'])) 
    list(year_ghg30)
    list(year_ghg50)
    list(year_bus)
    keylist = []
    value30list = []
    value50list = []
    valuebuslist = []
    for key, value in year_ghg30.items():
        keylist.append(key)
        value30list.append(value)
    for key, value in year_ghg50.items():
        value50list.append(value)
    for key, value in year_bus.items():
        valuebuslist.append(value)
    x1 = [val-0.2 for val in keylist]
    x2 = [val+0.2 for val in keylist]
    axis = fig.add_subplot(1,1,1)
    axis.bar(x1, value30list, width = 0.2, color='g')
    axis.bar(keylist, value50list, width = 0.2, color='orange')
    axis.bar(x2, valuebuslist, width = 0.2, color='blue')
    axis.set_xlabel('Year')
    axis.set_ylabel('GHG Emissions per year in metric tonnes')
    axis.legend(['GHG with 30%', 'GHG with 50%', 'GHG with bus'], loc='lower right')
    axis.set_title('GHG emission comparison with 30%, 50% light duty trucks and considering bus travel per year')
#    plt.savefig("/static/images/Line CHart for comparison_1.png", dpi=300, bbox_inches='tight')
#    
    return fig
    

app.run()

