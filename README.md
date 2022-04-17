# Rental_Flask_App
App to visualize data for London's rental market and provide pricing estimates

## Authors: 
Clara G.

## Project description:
The rental market in London is subject to high demand for accommodation, frequently at short notice, and is tightly controlled by professional landlords or real estate companies with long-term experience surrounding the dynamics of the rental market. This causes a fundamental power imbalance between landlords and prospective tenants: Tenants frequently cannot reliably determine whether they are paying a fair amount for an accommodation.<br>
To address this challenge, I have developed an application providing prospective tenants with valuable information in just one click. Tenants can gain insights surrounding a given postcode and rental listings in this area through a visually appealing dashboard. Moreover, both tenants and landlords can enter the details of a given listing they are seeking to rent or let and receive a real-time estimate of its fair market value based on cutting-edge machine learning algorithms.<br>
This application was developed using Flask, AWS Lambda, AWS ECR, Docker, Spark, an AWS Postgres database, and Airflow.

## Included files: 
#### IPYNB files:
**1 rentals_data_webscraping**: Webscraping and cleaning listings data from Zoopla and Rightmove<br>
**2 lsoa_data**: Cleaning postcode data from London Statistics office<br>
**3 merged_data**: Merging listing and postcode data<br>
**4 database**: Writing data to database<br>
**5 ml_models**: Running machine learning models, using Spark ML, including visualizations surrounding feature importance and performance<br>
**6 visualizations**: Visualizing data, using Plotly<br>
**airflow_schedule**: Implementing Airflow schedule to scrape, clean, merge, and write data to database, relying on methods from "scraping_merging_data".py and "writing_to_database".py<br>

#### PY files:
**app**: Flask app, relying on methods from ml_models.py and visualizations.py<br>
**scraping_merging_data**: Library to scrape, clean, and merge listing and postcode data<br>
**writing_to_database**: Contains methods of "4 database".ipynb as a library <br>
**ml_models**: Contains methods of "5 ml_models".ipynb as a library<br>
**visualizations**: Contains methods of "6 visualizations".ipynb as a library <br>

#### Folders:
**templates**: Includes HTML frontend for website<br>
**static**: Includes CSS frontend for website<br>
**output_data**: Contains data generated through webscraping, cleaning, and merging<br>

#### Other:
**schema.sql**: Schema for database<br>
**requirements.txt**: Requirements for Flask app<br>

## User inputs required:
**ml_models.py**: Set Spark home and Java home<br>
**writing_to_database.py**: Set Spark home and Java home<br>