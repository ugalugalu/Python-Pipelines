import logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO)
import pandas as pd
import csv
def extract_data():
    # Load the sample data sets
    dataset1 = pd.read_csv("dataset1.csv")
    dataset2 = pd.read_csv("dataset2.csv")
    dataset3 = pd.read_csv("dataset3.csv")
    # Merge the data sets
    df = pd.merge(dataset1,dataset2, how = 'inner',on = ['customer_id','payment_method'])
    data = pd.merge(df,dataset3, how = 'inner',on=['customer_id'])
    #Log information
    logger = logging.getLogger(__name__)
    logger.info("Data extraction completed.")
    print('Extraction successful')
    return data



def transform_data(data):
    # Data cleaning and handling missing values
    data.dropna(inplace = True)
    data.drop_duplicates(inplace = True)
    #Change the date formats for fun
    data['date_of_purchase'] = pd.to_datetime(data['date_of_purchase'])
    data['date_of_payment'] = pd.to_datetime(data['date_of_payment'])
    data['date_of_refund'] = pd.to_datetime(data['date_of_refund'])
    transformeddata = data
    logger = logging.getLogger(__name__)
    logger.info("Data transformation completed.")
    print('Transformation successful')
    return transformeddata


def load_data(transformeddata):
    #Save the final dataframe to a csv file
    transformeddata.to_csv('/Users/ubogalugalu/Documents/Moringa_School/Python Pipelines/final.csv',header = True)
    logger = logging.getLogger(__name__)
    logger.info("Data Loading succesful completed.")
    print('Loading successful')

if __name__ == '__main__':
     data = extract_data()
     transformeddata = transform_data(data)
     load_data
    