import sys
import os
import pandas as pd 
import numpy as np  

from data_ingestion import DataIngestion
from src.exception import CustomException
from src.logger import logging

class DataPreprocessing:

    def __init__(self):
        pass

    def initiate_data_preprocessing(self):
        logging.info("Data preprocessing started")
        try:
            logging.info("Reading the data from the DataIngestion")
            healthcare = DataIngestion().raw_data()

            text_columns = ['Name', 'Gender', 'Blood Type', 'Medical Condition', 'Doctor', 'Hospital', 
                'Insurance Provider', 'Admission Type', 'Medication', 'Test Results']

            for col in text_columns:
                healthcare[col] = healthcare[col].str.strip().str.lower().str.title()
            
            healthcare['Date of Admission'] = pd.to_datetime(healthcare['Date of Admission'])
            healthcare['Discharge Date'] = pd.to_datetime(healthcare['Discharge Date'])

            healthcare['Length of Stay'] = (healthcare['Discharge Date'] - healthcare['Date of Admission']).dt.days
            healthcare['Length of Stay'] = healthcare['Length of Stay'].astype(int)

            healthcare = healthcare.dropna(subset=['Date of Admission', 'Discharge Date'])


            categorical_columns = ['Doctor', 'Hospital', 'Insurance Provider']

            for col in categorical_columns: 
                healthcare[col] = healthcare[col].fillna(healthcare[col].mode()[0])

            healthcare.to_csv('data/processed/healthcare_preprocessed_data.csv', index=False)

            logging.info("Data preprocessing completed")
            return healthcare
            
        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    obj = DataPreprocessing()
    obj.initiate_data_preprocessing()


