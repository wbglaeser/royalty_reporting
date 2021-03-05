from dags.scripts.custom_macros import *

class TestDateTimeConversions:

    def test_retrieve_start_date(self):
        new_date = "2021-03-05"
        required_start_date = "20210301" 
        assert required_start_date == retrieve_start_date(new_date)

    def test_retrieve_start_timestamp(self):
        new_date = "2021-03-05"
        required_start_date = "2021-03-01 00:00:00 UTC" 
        assert required_start_date == retrieve_start_timestamp(new_date)

    def test_retrieve_end_date(self):
        new_date = "2021-03-05"
        required_start_date = "20210307" 
        assert required_start_date == retrieve_end_date(new_date)
    
    def test_retrieve_end_timestamp(self):
        new_date = "2021-03-05"
        required_start_date = "2021-03-08 00:00:00 UTC" 
        assert required_start_date == retrieve_end_timestamp(new_date)

    def test_day_string(self):
        test_day = "monday"
        required_index = 0
        assert required_index == switch_daystring_to_index(test_day)

    def test_date_retrieval(self):
        test_date = "2021-03-05"
        test_day = "tuesday"
        target_day = "20210302"
        assert target_day == retrieve_date(test_date, test_day)
