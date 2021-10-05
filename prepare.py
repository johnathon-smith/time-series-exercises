import numpy as np
import pandas as pd
# datetime utilities
from datetime import timedelta, datetime
# visualization
import matplotlib.pyplot as plt
import seaborn as sns

#Plot the individual distributions
def get_dists(df):
    """
        This function will take in a dataframe and plot the distributions for all of its variables.
    """
    for col in df.columns:
        sns.histplot(x = col, data = df)
        plt.title(col)
        plt.show()


def prepare_sales_data(sales):
    """
        This funciton takes in the complete sales dataframe and returns a new prepared dataframe.
    """

    #Convert sales_date to datetime format
    sales.sale_date = pd.to_datetime(sales.sale_date)

    #plot the dists of sale_amount and item_price
    sales.sale_amount.hist()
    sales.item_price.hist()

    #Add a month and day of week column
    sales['month'] = sales.sale_date.dt.month
    sales['day_of_week'] = sales.sale_date.dt.day_name()

    #Remove the time portion of the date
    sales.sale_date = sales.sale_date.dt.date

    #Set the index to be the date and sort it
    sales = sales.set_index('sale_date').sort_index()

    #Add a sales_total column to the df
    sales['sales_total'] = sales.sale_amount * sales.item_price

    return sales

def prepare_energy_data(energy):
    """
        This function will take in the unprepared version of the german energy dataframe and return a new prepared dataframe.
    """

    #Convert the Date column to a datetime format
    energy.Date = pd.to_datetime(energy.Date)

    #Plot the distributions of the variables
    get_dists(energy)

    #Add a month and a year column to dataframe
    energy['month'] = energy.Date.dt.month
    energy['year'] = energy.Date.dt.year

    #Set the Date column as the index
    energy = energy.set_index('Date').sort_index()

    """
    The Wind, Solar, and Wind+Solar columns are the amount of energy produced in GWh by those types. 
    I'm assuming that the NaN values are there because Germany didn't have those types of power production available at the time. 
    So, I'm going to replace all NaNs in the Wind and Solar columns with 0. 
    Then, I will set Wind+Solar as the sum of the Wind and Solar columns (as it should be).
    """

    #Fill in any missing values
    energy.Wind.fillna(0, inplace = True)
    energy.Solar.fillna(0, inplace = True)
    energy['Wind+Solar'].fillna(energy.Wind + energy.Solar, inplace = True)

    return energy