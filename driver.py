from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.window import Window
from pyspark.sql.utils import *
import sys

VALID_CHART_TYPES = ('chart', 'state_chart')


def input_validator(chart_type, chart_length):
    """
    :param chart_type: the type of chart to be displayed possible values are `chart` and `state_chart`
    :param chart_length: the number of records that need to be displayed
    :return: void if all the conditions are met, exception in case if its not
    """
    try:
        chart_size = int(chart_length)
        if chart_size == 0:
            raise ValueError('Chart length should be greater than 0')
    except ValueError:
        raise ValueError('Please enter a valid integer value for chart length')

    if chart_type not in VALID_CHART_TYPES:
        raise ValueError('Please enter a valid chart type from the given set ', VALID_CHART_TYPES)


def get_tag_event_schema():
    """
    :return: the schema for the raw json data
    """
    return StructType(
        [
            StructField('client', StringType(), True),
            StructField("geolocation", StructType([
                StructField("altitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("region", StructType([
                    StructField("country", StringType(), True),
                    StructField("locality", StringType(), True)])
                            , True),
                StructField("zone", StringType(), True)])
                        , True),
            StructField("installation", StructType([
                StructField("accountId", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("application",
                            StructType([
                                StructField("flavour", StringType(), True),
                                StructField("version", StringType(), True)])
                            , True),
                StructField("country", StringType(), True),
                StructField("device",
                            StructType([
                                StructField("operatingSystem",
                                            StructType([
                                                StructField("name", StringType(), True),
                                                StructField("version", StringType(), True)])
                                            , True)
                            ]), True),
                StructField("language", StringType(), True),
                StructField("platform", StringType(), True)])
                        ),
            StructField("match", StructType([StructField("track",
                                                         StructType([
                                                             StructField("id", StringType(), True),
                                                             StructField("metadata",
                                                                         StructType([
                                                                             StructField("artistname", StringType(),
                                                                                         True),
                                                                             StructField("tracktitle", StringType(),
                                                                                         True)
                                                                         ])
                                                                         , True),
                                                             StructField("offset", StringType(), True)
                                                         ])
                                                         , True)])),
            StructField('tagid', StringType(), True),
            StructField('timestamp', StringType(), True),
            StructField('timezone', StringType(), True),
            StructField('type', StringType(), True)
        ])


def get_top_charts_globally(raw_df, chart_length):
    """

    :param raw_df: Raw data to be processed
    :param chart_length: the number of records to be displayed
    :return:  clean data frame containing top ranked combinaton of atrist to track globally
    """
    df_raw = raw_df.select('tagid', col('match.track.id').alias('track_id'),
                           col('match.track.metadata.tracktitle').alias('track_title'),
                           col('match.track.metadata.artistname').alias('artist_name'))
    # Getting distinct counts of tagids for each track
    df_agg = df_raw.groupBy("track_id").agg(countDistinct("tagid").alias('tag_id_count')).orderBy(desc("tag_id_count"))
    # Joining back to the raw dataframe as we need artist name and track name for display

    df_final = df_agg.join(df_raw, df_raw['track_id'] == df_agg['track_id'], how="inner")
    # Using dense_rank function to rank each record based on the tag_id_cout
    df_final = df_final.select('artist_name', 'track_title', 'tag_id_count').withColumn("chart_position",
                                                                                        dense_rank().over(
                                                                                            Window.orderBy(desc(
                                                                                                "tag_id_count")))).orderBy(
        desc("tag_id_count")).dropDuplicates()
    df_final = df_final.select('chart_position', 'track_title', 'artist_name')
    return df_final


def get_top_charts_by_state(raw_df, chart_length):
    """

    :param raw_df: Raw data to be processed
    :param chart_length: the number of records to be displayed for each state
    :return: clean data frame containing top ranked combinaton of atrist to track for each state in the US
    """
    df_raw = raw_df.select('tagid', col('geolocation.region.locality').alias('state'),
                           col('match.track.id').alias('track_id'),
                           col('match.track.metadata.tracktitle').alias('track_title'),
                           col('match.track.metadata.artistname').alias('artist_name')) \
        .filter("geolocation.region.country = 'United States'")

    # Getting distinct counts of tagids for each track
    df_agg = df_raw.groupBy("track_id").agg(countDistinct("tagid").alias('tag_id_count')).orderBy(desc("tag_id_count"))

    # Joining back to the raw dataframe as we need artist name and track name for display
    df_final = df_agg.alias('df_aggregate').join(df_raw.alias('df_raw'),
                                                 col('df_aggregate.track_id') == col('df_raw.track_id'), how="inner")

    # Using dense_rank function to partition the result by each state
    df_final = df_final.select('df_raw.artist_name', 'df_raw.track_title', 'df_aggregate.tag_id_count',
                               'df_raw.state').withColumn("chart_position", dense_rank().over(
        Window.partitionBy("state").orderBy(desc("tag_id_count")))).filter(
        col('chart_position') <= int(chart_length)).dropDuplicates()
    return df_final.select('chart_position', col('df_raw.track_title').alias('track_title'),
                           col('df_raw.artist_name').alias('artist_name'),
                           'state')


def display_top_charts(df_clean, chart_length):
    df_clean.show(chart_length, False)


def process_top_charts_globally(raw_df, display_length):
    df_clean = get_top_charts_globally(raw_df, display_length)
    display_top_charts(df_clean, int(display_length))


def process_top_charts_by_state(raw_df, display_length):
    df_clean = get_top_charts_by_state(raw_df, display_length)
    display_top_charts(df_clean, 500)


"""
Using a dictionary based approach to return the method to be called to avoid conditional statements.
Based on the key passed, method would be returned
"""
method_dispatch = {
    'chart': process_top_charts_globally,
    'state_chart': process_top_charts_by_state
}


def get_raw_df(sql_context, raw_path):
    """
    :param sql_context: SparkContext for the running application
    :param raw_path: The raw path of the json file to be fetched from
    :return: Returns a DataFrame built on the raw json data
    """
    return sql_context.read.schema(get_tag_event_schema()).json(raw_path)


def start():
    """
    Fetching command line arguments passed in spark-submit command

    """
    chart_type = sys.argv[1]
    chart_length = sys.argv[2]
    raw_json_path = sys.argv[3]
    try:
        """
        Validating the command line arguments passed
        
        """
        input_validator(chart_type, chart_length)
    except ValueError as e:
        print(e)
        sys.exit(1)
    """
    Initialize the Spark Context
    
    """
    conf = SparkConf().setAppName("shazam-tag-aggregator")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sql_context = SQLContext(sc)
    try:
        df_raw = get_raw_df(sql_context, raw_json_path)
        method_dispatch[chart_type](df_raw, chart_length)
    except AnalysisException as e:
        print(str(e))


if __name__ == '__main__':
    """
    Entry point of the Spark Application
    """
    start()
