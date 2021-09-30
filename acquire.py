import numpy as np
import pandas as pd
import requests
import os

def get_item_data():
    """
        This function will first check if 'items.csv' exists, and if it does, return it as a dataframe.
        If it does not exist, this function will gather the data via an API, cache the resulting dataframe
        as a .csv, and then return the dataframe.
    """
    file_name = 'items.csv'

    #Check for 'items.csv'
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)

    else:
        #Gather the data from the first page
        base_url = 'https://python.zgulde.net'
        response = requests.get(base_url + '/api/v1/items')

        #Verify the connection is good. If bad, print message and return nothing.
        if response.ok != True:
            print('Something went wrong! Error code:', response.status_code)
            return None

        else:
            data = response.json()

            #Write a loop that gathers the information from each page and adds it to a dataframe until there are no more pages left.
            #Create a df to store the items
            items = pd.DataFrame(data['payload']['items'])

            for page in range(1, data['payload']['max_page']):
                data = requests.get(base_url + data['payload']['next_page']).json()
                
                items = pd.concat([items, pd.DataFrame(data['payload']['items'])]).reset_index(drop = True)

            #Now that the data has been put together, cache it as a .csv
            items.to_csv('items.csv', index = False)

            return items

def get_store_data():
    """
        This function will first check if 'stores.csv' exists, and if it does, return it as a dataframe.
        If it does not exist, this function will gather the data via an API, cache the resulting dataframe
        as a .csv, and then return the dataframe.
    """
    file_name = 'stores.csv'

    #Check for 'stores.csv'
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)

    else:
        #Gather the data from the first page
        base_url = 'https://python.zgulde.net'
        response = requests.get(base_url + '/api/v1/stores')

        #Verify the connection is good. If bad, print message and return nothing.
        if response.ok != True:
            print('Something went wrong! Error code:', response.status_code)
            return None

        else:
            data = response.json()

            #Write a loop that gathers the information from each page and adds it to a dataframe until there are no more pages left.
            #Create a df to store the items
            stores = pd.DataFrame(data['payload']['stores'])

            for page in range(1, data['payload']['max_page']):
                data = requests.get(base_url + data['payload']['next_page']).json()
                
                stores = pd.concat([stores, pd.DataFrame(data['payload']['stores'])]).reset_index(drop = True)

            #Now that the data has been put together, cache it as a .csv
            stores.to_csv('stores.csv', index = False)

            return stores

def get_sales_data():
    """
        This function will first check if 'sales.csv' exists, and if it does, return it as a dataframe.
        If it does not exist, this function will gather the data via an API, cache the resulting dataframe
        as a .csv, and then return the dataframe.
    """
    file_name = 'sales.csv'

    #Check for 'sales.csv'
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)

    else:
        #Gather the data from the first page
        base_url = 'https://python.zgulde.net'
        response = requests.get(base_url + '/api/v1/sales')

        #Verify the connection is good. If bad, print message and return nothing.
        if response.ok != True:
            print('Something went wrong! Error code:', response.status_code)
            return None

        else:
            data = response.json()

            #Write a loop that gathers the information from each page and adds it to a dataframe until there are no more pages left.
            #Create a df to store the items
            sales = pd.DataFrame(data['payload']['sales'])

            for page in range(1, data['payload']['max_page']):
                data = requests.get(base_url + data['payload']['next_page']).json()
                
                sales = pd.concat([sales, pd.DataFrame(data['payload']['sales'])]).reset_index(drop = True)

            #Now that the data has been put together, cache it as a .csv
            sales.to_csv('sales.csv', index = False)

            return sales

def get_payload_data():
    """
        This function will first check if 'payload.csv' exists, and if it does, return it as a dataframe.
        If it does not, this function will retrieve all of the item, store, and sales data, merge it all into one dataframe, cache it, and then return it. 
    """
    file_name = 'payload.csv'

    #Check for 'payload.csv'
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)

    else:
        #Get items data
        items = get_item_data()

        #Get store data
        stores = get_store_data()

        #Get sales data
        sales = get_sales_data()

        #Merge into a single dataframe

        #Stores and sales first
        payload = pd.merge(stores, sales, left_on='store_id', right_on = 'store',  how='left').drop(columns = ['store'])

        #Now merge payload and items
        payload = pd.merge(payload, items, left_on = 'item', right_on = 'item_id', how = 'left').drop(columns = ['item'])

        #Cache the dataframe
        payload.to_csv('payload.csv', index = False)

        return payload

def get_german_power_data():
    """
        This function will first check if 'german_power_data.csv' exists, and if it does, return it as a dataframe.
        If it does not exist, this function will gather the data, cache the resulting dataframe
        as a .csv, and then return the dataframe.
    """
    filename = 'german_power_data.csv'

    if os.path.isfile(filename):
        return pd.read_csv(filename)

    else:
        #Gather the data
        df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')

        #Cache the above dataframe as a .csv for future use
        df.to_csv('germany_power_data.csv', index = False)

        return df





    


