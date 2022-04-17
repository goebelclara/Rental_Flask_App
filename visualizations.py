#Importing libraries
import pandas as pd
import numpy as np

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.figure_factory as ff
from plotly import io

import os

def visualize_dashboard(pcd):
    #os.remove("templates/dashboard.html")

    #Filtering data
    df = pd.read_csv("output_data/data_vf.csv", index_col = 0)
    df_filtered = df[df["postcode"] == pcd].reset_index(drop = True)
    df_filtered = df_filtered[df_filtered["price"] < 20000].reset_index(drop = True)
    
    #Preparing data for listing-specific visualizations
    types = df_filtered["type"].value_counts()
    bedrooms = df_filtered["bedrooms"].value_counts()
    bathrooms = df_filtered["bathrooms"].value_counts()
    weights = pd.crosstab(df_filtered["bedrooms"], df_filtered["bathrooms"]).stack(dropna = True).reset_index(name = "weight")

    #Preparing data for postcode brief
    geographic_coordinates = str(round(df_filtered.loc[[0]]["lat"].values[0],1)) \
        + "° latitude, " \
        + str(round(df_filtered.loc[[0]]["long"].values[0],1)) \
        + "° longitude"
    pop_density = str(int(df_filtered.loc[[0]]["pop_density"].values[0])) \
        + " persons per hectar"
    public_transport_accessibility = str(int(df_filtered.loc[[0]]["public_transport_accessibility"].values[0])) \
        + " / 8"
    mean_income = str(int(df_filtered.loc[[0]]["mean_income"].values[0] / 1000)) \
        + "k£"

    #Generating count weights for for visualization of bathrooms and bedrooms
    df_filtered["weight"] = 0
    for index in range(len(df_filtered)):
        bedrooms = df_filtered.loc[index, "bedrooms"]
        bathrooms = df_filtered.loc[index, "bathrooms"]
        weight = weights[(weights["bedrooms"] == bedrooms) & (weights["bathrooms"] == bathrooms)]["weight"]
        df_filtered.loc[index, "weight"] = weight.values[0]

    #Defining function to plot stacked bar
    def stacked_bar(columns, labels, row, col):
        df_filtered_pcd = df_filtered[columns].loc[[0]]
        
        colors = ["#326196", "#6B8FCE", "#85A9D6", "#A6C9E1", "#A3B1E0", "#A19DDE", "#818BD5", "#636ECB",
                "#8763CB", "#A573D0", "#BBA0DF", "#D3BEE9", "#E8C7EC", "#E2A8CB", "#DA8FCB", "#D277B7",
                "#AB397F"]
        colors = colors*int(len(df_filtered_pcd.columns)-1)
        
        fig.add_trace(go.Bar(x = df_filtered_pcd["postcode"], 
                            y = round(df_filtered_pcd[columns[1]]*100,2),
                            text = labels[1],
                            textposition = "inside",
                            marker = dict(color = colors[0]),
                            customdata = [labels[1]],
                            hovertemplate = '%{customdata}: %{y}%'),
                    row = row,
                    col = col)
        
        for col_name, label, color in zip(columns[2:], labels[2:], colors[1:]):
            fig.add_bar(x = df_filtered_pcd["postcode"],
                        y = round(df_filtered_pcd[col_name]*100,2),
                        text = label,
                        textposition = "inside",
                        marker = dict(color = color),
                        customdata = [label],
                        hovertemplate = '%{customdata}: %{y}%',
                        row = row,
                        col = col)
        
        fig.update_layout(barmode = 'stack', 
                        uniformtext = dict(minsize = 10, mode = 'hide'))

    #Creating plot architecture
    fig = make_subplots(rows = 2,
                        cols = 4,
                        vertical_spacing = 0.2,
                        horizontal_spacing = 0.1,
                        subplot_titles = ["Housing<br>types", 
                                        "Number of bathrooms and bedrooms<br>per listing by count", 
                                        "Distance to next station<br>(in miles)", 
                                        "Rent<br>(in £ per month)", 
                                        "Age<br>distribution",
                                        "Dwelling ownership<br>distribution",
                                        "Economic activity<br>distribution",
                                        "Qualification level<br>distribution"], 
                        specs = [[{"type": "domain"}, {"type": "xy"}, {"type": "xy"}, {"type": "xy"}],
                                [{"type": "xy"}, {"type": "xy"}, {"type": "xy"}, {"type": "xy"}]
                                ])

    #Setting colors
    colors = ["#326196", "#6B8FCE", "#85A9D6", "#A6C9E1", "#A3B1E0", "#A19DDE", "#818BD5", "#636ECB",
            "#8763CB", "#A573D0", "#BBA0DF", "#D3BEE9", "#E8C7EC", "#E2A8CB", "#DA8FCB", "#D277B7",
            "#AB397F"]
    colors = colors*len(df_filtered)

    #Plotting dwelling types
    fig.add_trace(go.Pie(labels = [x.capitalize() for x in types.index],
                        values = types),
                row = 1,
                col = 1)

    fig.update_traces(hoverinfo = 'label',
                    textinfo = 'value',
                    marker = dict(colors = colors),
                    row = 1,
                    col = 1)

    #Plotting bathrooms vs. bedrooms by count
    fig.add_trace(go.Scatter(x = df_filtered["bedrooms"],
                            y = df_filtered["bathrooms"],
                            customdata = df_filtered["weight"],
                            mode = 'markers',
                            marker_color = colors,
                            marker = dict(size = df_filtered["weight"].apply(lambda x: np.log1p(x)*10)),
                            hovertemplate = '%{customdata} listings with %{y} bathrooms and %{x} bedrooms'
                            ),
                        row = 1,
                        col = 2)

    fig.update_xaxes(title_text = "Bedrooms", row = 1, col = 2)
    fig.update_yaxes(title_text = "Bathrooms", row = 1, col = 2)

    #Plotting distance to nearest station
    fig.add_trace(go.Histogram(x = df_filtered["nearest_station"], 
                            nbinsx = 7, 
                            marker_color = colors, 
                            hovertemplate = '%{x} miles to next station'),
                row = 1,
                col = 3)

    fig.update_xaxes(title_text = "Miles", row = 1, col = 3)
    fig.update_yaxes(title_text = "Listing count", row = 1, col = 3)

    #Plotting price
    fig.add_trace(go.Histogram(x = df_filtered["price"], 
                            nbinsx = 6, 
                            marker_color = colors, 
                            hovertemplate = '%{x} £ per month'),
                row = 1,
                col = 4)

    fig.update_xaxes(title_text = "£ per month", row = 1, col = 4)
    fig.update_yaxes(title_text = "Listing count", row = 1, col = 4)

    #Plotting age distribution
    columns = ['postcode', 'ages_0_15', 'ages_16_29', 'ages_30_44', 'ages_45_65', 'ages_66_plus']
    labels = ['postcode', 'Ages 0-15', 'Ages 16-29', 'Ages 30-44', 'Ages 45-65', 'Ages 66+']
    stacked_bar(columns, labels, 2, 1)

    #Plotting ownership distribution
    columns = ['postcode', 'owned_outright', 'owned_mortgage', 'rented_social', 'rented_private']
    labels = ['postcode', 'Owned outright', 'Owned w/ mortgage', 'Rented under<br>social scheme', 'Rented privately']
    stacked_bar(columns, labels, 2, 2)

    #Plotting economic activity distribution
    columns = ['postcode', 'economic_inactive', 'employee', 'self-employed', 'unemployed', 'student']
    labels = ['postcode', 'Economically inactive', 'Employed', 'Self-employed', 'Unemployed', 'Student']
    stacked_bar(columns, labels, 2, 3)

    #Plotting qualification level distribution
    columns = ['postcode', 'unqualified', 'level_1', 'level_2', 'apprenticeship', 'level_3', 'level_4', 'other']
    labels = ['postcode', 'Unqualified', 'Level 1', 'Level 2', 'Apprenticeship', 'Level 3', 'Level 4', 'Other']
    stacked_bar(columns, labels, 2, 4)

    #Setting parameters
    fig.update_layout(title = go.layout.Title(text = '<span style = "font-size: 30px;">Postcode: ' 
                                            + pcd
                                            + '</span><br><span style = "font-size: 13px;"><b>Geographic coordinates</b>: '
                                            + geographic_coordinates
                                            + '<br><b>Population density</b>: '
                                            + pop_density
                                            + '<br><b>Public transport accessibility score</b>: '
                                            + public_transport_accessibility
                                            + '<br><b>Annual mean income</b>: '
                                            + mean_income
                                            + '</span>',
                                            x = 0, y = 0.97),
                    paper_bgcolor = 'white',
                    plot_bgcolor = 'white',
                    margin = dict(t = 200),
                    hoverlabel = dict(bgcolor = 'white'),
                    height = 900, 
                    width = 1400,
                    showlegend = False)
                    
    fig.write_html("templates/dashboard.html",
                    full_html = False,
                    include_plotlyjs = 'cdn')
    
    return fig