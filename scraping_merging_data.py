#Importing libraries
import requests
from bs4 import BeautifulSoup

import pandas as pd
import numpy as np

import time

# Rightmove

def scrape_rightmove(pages):
    #Generating URLS for search pages
    urls = []
    url_base = "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490&minBedrooms=1&index="
    url_end = "&propertyTypes=&includeLetAgreed=false&mustHave=&dontShow=houseShare%2Cstudent%2Cretirement&furnishTypes=&keywords="

    end_point = pages * 24

    for i in range(0,end_point+1,24):
        url = url_base + str(i) + url_end
        urls.append(url)

    #Defining function to get link for each listing from a given search page
    def get_links(soup):
        link_container = soup.find_all("div", class_ = "propertyCard-details")
        for link in link_container:
            link_end = link.find("a").get("href")
            link_base = "https://www.rightmove.co.uk"
            link_list.append(link_base + link_end)

    #Looping through search page URLs to get listing links 
    link_list = []

    for url in urls:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        
        get_links(soup)

    #Defining function to get property details 
    def get_prop_details(url_rental):
        page_rental = requests.get(url_rental)
        soup_rental = BeautifulSoup(page_rental.content, "html.parser")
        
        prop_specs_container = soup_rental.find_all("div", class_ = "tmJOVKTrHAB4bLpcMjzQ")
        prop_info_container = soup_rental.find_all("div", class_ = "_1fcftXUEbWfJOJzIUeIHKt")
        list_dict = {"PROPERTY TYPE": type_list, "BEDROOMS": bedrooms_list, "BATHROOMS": bathrooms_list, "SIZE": size_list}
        for i in range(len(prop_specs_container)):
            spec = prop_specs_container[i].text
            append_list = list_dict[spec]
            append_list.append(prop_info_container[i].text)
        for key in list_dict.keys():
            if key not in str(prop_specs_container):
                append_list = list_dict[key]
                append_list.append(np.nan)
        
        try:
            nearest_station_dist = soup_rental.find("span", class_ = "_1ZY603T1ryTT3dMgGkM7Lg").text
            nearest_station_dist_list.append(nearest_station_dist)
        except:
            nearest_station_dist_list.append(np.nan)
            
        try:
            price = soup_rental.find("div", class_ = "_1gfnqJ3Vtd1z40MlC0MzXu").text.split("£")[1]
            price_list.append(price)
        except:
            price_list.append(np.nan)
        
        try:
            address = soup_rental.find("h1", itemprop = "streetAddress").text
            address_list.append(address)
        except:
            address_list.append(np.nan)

    #Looping through listing links to get property details 
    type_list = []
    bedrooms_list = []
    bathrooms_list = []
    size_list = []
    nearest_station_dist_list = []
    price_list = []
    address_list = []

    for link in link_list:
        get_prop_details(link)

    #Storing data in dataframe 
    df = pd.DataFrame(data = {
        "type": type_list,
        "bedrooms": bedrooms_list,
        "bathrooms": bathrooms_list,
        "size": size_list,
        "nearest_station": nearest_station_dist_list,
        "address": address_list,
        "price": price_list})

    #Defining function to check whether address contains numeric postcode
    def has_numbers(string):
        return any(char.isdigit() for char in string)

    #Cleaning data
    df["bedrooms"] = df["bedrooms"].str.replace("×","").str.strip()

    df["bathrooms"] = df["bathrooms"].str.replace("×","").str.strip()

    df["size"] = df["size"].str.replace(" sq. ft.","").str.replace(",","").str.strip()
    df["size"] = df["size"].apply(lambda x: x.split("-")[0] if "-" in str(x) else x).str.strip()

    df["nearest_station"] = df["nearest_station"].str.replace(" miles","").str.strip()

    df["price"] = df["price"].str.replace(" pcm","").str.replace(",","").str.strip()

    df["postcode"] = df["address"].apply(lambda x: x.split(",")[-1])
    df["postcode"] = df["postcode"].apply(lambda x: x.split(" ")[-1] if len(x) > 8 else x)
    df["postcode"] = df["postcode"].apply(lambda x: x if has_numbers(x) else np.nan)
    df["postcode"] = df["postcode"].str.strip()

    #Changing datatypes
    df["bedrooms"] = df["bedrooms"].astype("Int64")
    df["bathrooms"] = df["bathrooms"].astype("Int64")
    df["size"] = df["size"].astype("Int64")
    df["nearest_station"] = df["nearest_station"].astype("float")
    df["price"] = df["price"].astype("Int64")

    #Imputing size of flat
    df["size_imputed"] = np.where(df["size"].isna(), df["size"].mean(), df["size"])

    #Dropping missing values
    df_final_rightmove = df.drop("size", axis = 1).dropna().reset_index(drop = True)

    return df_final_rightmove

# Zoopla

def scrape_zoopla(pages):
    #Generating URLS for search pages
    urls_zoopla = []
    url_base_zoopla = "https://www.zoopla.co.uk/to-rent/property/london/?beds_min=1&furnished_state=unfurnished&include_shared_accommodation=false&price_frequency=per_month&q=London&results_sort=newest_listings&search_source=to-rent&pn="

    for i in range(0,pages+1,1):
        url_zoopla = url_base_zoopla + str(i)
        urls_zoopla.append(url_zoopla)

    #Defining function to get link for each listing from a given search page
    def get_links_zoopla(soup):
        link_container = soup.find_all("a", class_ = "e1to43qx9 css-15xcaqt-StyledLink-Link-ImageLink e33dvwd0")
        for link in link_container:
            link_end = link.get("href")
            link_base = "https://www.zoopla.co.uk"
            link_list_zoopla.append(link_base + link_end)

    #Looping through search page URLs to get listing links 
    link_list_zoopla = []

    for url in urls_zoopla:
        page_zoopla = requests.get(url)
        soup_zoopla = BeautifulSoup(page_zoopla.content, "html.parser")
        get_links_zoopla(soup_zoopla)

    #Defining function to get property type from property title
    def find_type_zoopla(string, start, end):
        return (string.split(start))[1].split(end)[0].strip()

    #Defining function to get property details 
    def get_prop_details_zoopla(url_rental):
        page_rental = requests.get(url_rental)
        soup_rental = BeautifulSoup(page_rental.content, "html.parser")
        
        prop_info_container = soup_rental.find_all("span", class_ = "css-8rvu8h-AttributeLabel e19p681f0")
        list_dict = {"beds-label": bedrooms_list_zoopla, "baths-label": bathrooms_list_zoopla, "floorarea-label": size_list_zoopla}
        for i in range(len(prop_info_container)):
            spec = prop_info_container[i].get("data-testid")
            if spec == "receptions-label":
                continue
            append_list_zoopla = list_dict[spec]
            append_list_zoopla.append(prop_info_container[i].text)
        for key in list_dict.keys():
            if key not in str(prop_info_container):
                append_list_zoopla = list_dict[key]
                append_list_zoopla.append(np.nan)
                
        try:
            title = soup_rental.find("span", class_ = "css-jv46e5-DisplayTitleLabel e19p681f5").text
            type_list_zoopla.append(find_type_zoopla(title, "bed", "to"))
        except:
            type_list_zoopla.append(np.nan)
        
        try:
            nearest_station_dist = soup_rental.find("span", class_ = "css-ofdupv-AmenityDistance e1opb2ur2").text
            nearest_station_dist_list_zoopla.append(nearest_station_dist)
        except:
            nearest_station_dist_list_zoopla.append(np.nan)
            
        try:
            price = soup_rental.find("span", class_ = "css-dob1au-PricingLabel e19p681f13").text
            price_list_zoopla.append(price)
        except:
            price_list_zoopla.append(np.nan)
        
        try:
            address = soup_rental.find("span", class_ = "css-192hawr-DisplayAddressLabel e19p681f4").text
            address_list_zoopla.append(address)
        except:
            address_list_zoopla.append(np.nan)

    #Looping through listing links to get property details 
    type_list_zoopla = []
    bedrooms_list_zoopla = []
    bathrooms_list_zoopla = []
    size_list_zoopla = []
    nearest_station_dist_list_zoopla = []
    price_list_zoopla = []
    address_list_zoopla = []

    for link in link_list_zoopla:
        get_prop_details_zoopla(link)
        time.sleep(1)

    #Storing data in dataframe 
    df_zoopla = pd.DataFrame(data = {
        "type": type_list_zoopla,
        "bedrooms": bedrooms_list_zoopla,
        "bathrooms": bathrooms_list_zoopla,
        "size": size_list_zoopla,
        "nearest_station": nearest_station_dist_list_zoopla,
        "address": address_list_zoopla,
        "price": price_list_zoopla})

    #Cleaning data
    df_zoopla["bedrooms"] = df_zoopla["bedrooms"].str.replace("beds","").str.replace("bed","").str.strip()

    df_zoopla["bathrooms"] = df_zoopla["bathrooms"].str.replace("baths","").str.replace("bath","").str.strip()

    df_zoopla["size"] = df_zoopla["size"].str.replace(" sq. ft","").str.replace(",","").str.strip()

    df_zoopla["nearest_station"] = df_zoopla["nearest_station"].str.replace(" miles","").str.strip()

    df_zoopla["price"] = df_zoopla["price"].str.replace(" pcm","").str.replace(",","").str.replace("£","").str.strip()

    df_zoopla["postcode"] = df_zoopla["address"].apply(lambda x: str(x).split(",")[-1])
    df_zoopla["postcode"] = df_zoopla["postcode"].apply(lambda x: x.split(" ")[-1] if len(x) > 8 else x)
    df_zoopla["postcode"] = df_zoopla["postcode"].apply(lambda x: x if has_numbers(x) else np.nan)
    df_zoopla["postcode"] = df_zoopla["postcode"].str.strip()

    #Changing datatypes
    df_zoopla["bedrooms"] = df_zoopla["bedrooms"].astype("Int64")
    df_zoopla["bathrooms"] = df_zoopla["bathrooms"].astype("Int64")
    df_zoopla["size"] = df_zoopla["size"].astype("Int64")
    df_zoopla["nearest_station"] = df_zoopla["nearest_station"].astype("float")
    df_zoopla["price"] = df_zoopla["price"].astype("Int64")

    #Imputing size of flat
    df_zoopla["size_imputed"] = np.where(df_zoopla["size"].isna(), df_zoopla["size"].mean(), df_zoopla["size"])

    #Dropping missing values
    df_final_zoopla = df_zoopla.drop("size", axis = 1, inplace = True)

    return df_final_zoopla

# Merge listings data with postcode data

def merge_data(new_rightmove_data, new_zoopla_data):
    #Reading in existing data as CSV
    df = pd.read_csv("output_data/data_vf.csv", index_col = 0)
    pcd = pd.read_csv("output_data/pcd_info.csv", index_col = 0)

    #Saving source of data
    new_rightmove_data["source"] = "rightmove"
    new_zoopla_data["source"] = "zoopla"

    #Merging listings data
    rentals = pd.concat([df, new_zoopla_data, new_rightmove_data], axis = 0, ignore_index = True)

    #Mapping listings data to postcode information
    df = pd.merge_ordered(rentals, pcd, how = "left", left_on = "postcode", right_on = "pcd_adjusted")

    #Dropping listings where postcode cannot be mapped
    df.dropna(inplace = True)

    #Cleaning data
    df["type"] = df["type"].apply(lambda x: x.lower())
    type_dict = {'end of terrace': 'end terrace house', 
                'terraced': 'terraced house',
                'semi-detached': 'semi-detached house', 
                'mews': 'mews house', 
                'detached': 'detached house', 
                'ground flat': 'flat', 
                'equestrian property': 'property', 
                'town house': 'mews house'}
    df["type"] = df["type"].replace(type_dict)

    df["postcode"] = df["postcode"].apply(lambda x: x.strip())

    #Dropping one postcode column
    df.drop("pcd_adjusted", axis = 1, inplace = True)
    df = df.reset_index(drop = True)

    #Dropping economic activity, given more granular split in economically inactive, employee, self-employed, unemployed, student
    df.drop("economic_active", axis = 1, inplace = True)
    df = df.reset_index(drop = True)

    #Saving to CSV 
    df.to_csv("output_data/data_vf.csv")

    return df