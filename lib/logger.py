class Log4J:
    # constructor taking SparkSession as argument
    # create logger attribute & initialize with .LogManager.getLogger
    def __init__(self, spark):                      # spark is the SparkSession defined in HelloSpark.py
        log4j = spark._jvm.org.apache.log4j         # get log4j instance from sparksession JVM
        root_class = "guru.learningjournal.spark.examples"
        conf = spark.sparkContext.getConf()         # get configs from sparkContext to query the app name to set logger
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class + "." +app_name)  # create a logger

    # Create a method for warning
    def warn(self, message):
        self.logger.warn(message)

    # Create a method for info
    def info(self, message):
        self.logger.info(message)

    # Create method for error
    def error(self, message):
        self.logger.error(message)

    # Create method for debug
    def debug(self, message):
        self.logger.debug(message)
