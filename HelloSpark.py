import sys

from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":              # Main entry point of the pyspark program
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()                  # SparkSession created as spark. Each spark programme can have only one session

    logger = Log4J(spark)

    # Check for command line argument, if not present, we log ERROR and exit program
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    # logger.info("Starting HelloSpark")
    # Your process code
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    # Read data by colling the load_survey_df function defined in utils.py
    survey_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_df.repartition(2)

    # Apply some transformation
    count_df = count_by_country(survey_df)

    logger.info(count_df.collect())       # Send the output to logger with .count()

    # input("Press Enter")               # Remember to remove this line, which is to ensure the program does not finish.
    # logger.info("Finished HelloSpark")
    # spark.stop()

    count_df.show()
