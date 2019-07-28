import unittest,os
from pyspark.sql.types import *
from . import base_tests
import driver


class TestShazamData(base_tests.BaseTestClass):

    def get_raw_data(self):
        return driver.get_raw_df(self.spark_session,os.path.join(os.path.dirname(
            os.path.abspath(__file__)), "mock_data.json"))

    def test_data_count(self):
        self.assertEqual(self.get_raw_data().count(), 25)


    def test_process_top_charts_globally_columns(self):
        raw_df = self.get_raw_data()
        clean_df = driver.get_top_charts_globally(raw_df, 5)
        self.assertEqual(clean_df.columns,list(['chart_position', 'track_title', 'artist_name']))

    def test_process_top_charts_globally_count(self):
        raw_df = self.get_raw_data()
        clean_df = driver.get_top_charts_globally(raw_df, 5)
        self.assertEqual(clean_df.count(),22)


    def test_process_top_charts_by_state_columns(self):
        raw_df = self.get_raw_data()
        clean_df = driver.get_top_charts_by_state(raw_df, 5)
        self.assertEqual(clean_df.columns,list(['chart_position', 'track_title', 'artist_name','state']))


    def test_process_top_charts_by_state(self):
        raw_df = self.get_raw_data()
        clean_df = driver.get_top_charts_by_state(raw_df, 5)
        self.assertEqual(clean_df.count(),11)

