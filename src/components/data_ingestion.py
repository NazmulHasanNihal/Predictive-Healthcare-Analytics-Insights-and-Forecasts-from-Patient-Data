import os
import pandas as pd
import sys

from src.exception import CustomException
from src.logger import logging


class DataIngestion:

    def __init__(self):
        pass

    def raw_data(self):
        logging.info("Data Ingestion Started")
        try:
            logging.info("Reading the data from the dataset")
            healthcare = pd.read_csv("data/raw/healthcare_dataset.csv")
            logging.info("Data Ingestion Completed")
            return healthcare

        except Exception as e:
            logging.error(f"Error during data ingestion: {e}")
            raise CustomException(e, sys)



if __name__ == "__main__":
    obj = DataIngestion()
    healthcare = obj.raw_data()

