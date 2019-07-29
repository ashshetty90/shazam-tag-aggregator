import unittest, logging, sys
from pyspark.sql import SQLContext
import driver
try:
    # Import PySpark modules here
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.context import SparkContext
except ImportError as e:
    print("Can not import Spark modules", e)
    sys.exit(1)

# change level for logger to suppress gibberish information
logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)


class BaseTestClass(unittest.TestCase):

    def setUp(self):
        """Create a single node Spark application."""
        conf = SparkConf()
        conf.set("spark.executor.memory", "1g")
        conf.set("spark.cores.max", "1")
        conf.set("spark.app.name", "shazam-unittest-spark")
        SparkSession._instantiatedContext = None
        self.spark_session = SparkSession.builder.config(conf=conf).getOrCreate()



    def test_validate_chart_params(self):
        self.assertRaises(ValueError, driver.input_validator, 'chart', 'a')
        self.assertRaises(ValueError, driver.input_validator, 'invalid_value', '3')


    def tearDown(self):
        """Stop the SparkSession."""
        self.spark_session.stop()


if __name__ == '__main__':
    unittest.main()
