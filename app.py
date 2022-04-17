#Importing libraries
from flask import Flask, request, render_template, redirect, url_for
#from flask_ngrok import run_with_ngrok

import pandas as pd

import json

import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.figure_factory as ff

from visualizations import visualize_dashboard
from ml_models import fit_and_evaluate

#Instantiating app
app = Flask(__name__)
#run_with_ngrok(app)

#Setting landing page with input form
@app.route('/', methods = ['GET', 'POST'])
def dropdown():
    df = pd.read_csv("output_data/data_vf.csv", index_col = 0)
    postcodes = df["postcode"].value_counts()
    postcodes = postcodes[postcodes > 5] 
    postcodes = list(postcodes.index)
    
    if request.method == "POST":
        if "postcode" in request.form:
            postcode = request.form['postcode']
            return redirect(url_for('dashboard', postcode = postcode))
        else:
            prop_details = request.form['prop_details']
            return redirect(url_for('proposed_price', prop_details = prop_details))
    
    return render_template('form.html', postcodes = postcodes)

#Setting output page with dashboard
@app.route('/dashboard/<postcode>', methods = ['GET', 'POST'])
def dashboard(postcode):
    fig = visualize_dashboard(postcode)
    
    graphJSON = json.dumps(fig, cls = plotly.utils.PlotlyJSONEncoder)
    
    return render_template('plotly.html', graphJSON = graphJSON)

#Setting output page with proposed price
@app.route('/proposed_price/<prop_details>', methods = ['GET', 'POST'])
def proposed_price(prop_details):
    price = fit_and_evaluate(prop_details)
    
    return render_template('price.html', prop_details = prop_details, price = price)

if __name__ == '__main__':
    app.run()