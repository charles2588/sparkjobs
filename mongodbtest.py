from pymongo import MongoClient
import pandas as pd

from pyspark import SparkContext

if __name__ == "__main__":
        sc = SparkContext(appName="MongoDB CON TEST")
        print("Hello Spark Demo. Compute the mean and variance of a collection")
        print("Begining MongoCon")

        mongo_url = "mongodb://ds045252.mlab.com:45252"

        connection = MongoClient(mongo_url)

        db = connection["samplemongodb"]
        # MongoLab need user authentication
        db.authenticate("username", "password")
        db.Photos.insert({'m': 'Worked Spark Submit Test Real test'})
        print("Insert worked")
        sc.stop()
