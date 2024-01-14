# Importing necessary libraries and module
import pandas as pd
import numpy as np


# indicate the volume where the file is present into the the Docker Container in docker-airflow/dags
CSV_PATH = '/home/giambar/Git/docker-airflow/dags/movie_short.csv'

def retrieve_movie_df(path):

    """Fetches user data from the provided .csv movie_short file"""
    df = pd.read_csv(path)
    df_json = pd.read_json("df_movies.json")
    df_movie = df_json.to_dict(orient="records")
    return df_movie

if __name__ == "__main__":
    retrieve_movie_df(CSV_PATH)