from datetime import datetime, timedelta

def retrieve_start_date(ds) -> str:
    """ Retrieve start date of running week from input date"""
    ds = datetime.strptime(ds, '%Y-%m-%d')
    return (ds - timedelta(ds.weekday())).strftime("%Y%m%d")

def retrieve_start_timestamp(ds) -> str:
    """ Retrieve start timestamp of running week from input date"""
    ds = datetime.strptime(ds, '%Y-%m-%d')
    return (ds - timedelta(ds.weekday())).strftime("%Y-%m-%d %H:%M:%S UTC")

def retrieve_end_date(ds) -> str:
    """ Retrieve end date of running week from input date"""
    ds = datetime.strptime(ds, '%Y-%m-%d')
    return (ds - timedelta(ds.weekday()-6)).strftime("%Y%m%d")

def retrieve_end_timestamp(ds) -> str:
    """ Retrieve end timestamp of running week from input date"""
    ds = datetime.strptime(ds, '%Y-%m-%d')
    return (ds - timedelta(ds.weekday()-7)).strftime("%Y-%m-%d %H:%M:%S UTC")

def switch_daystring_to_index(day: str) -> int:
    """ Switch day of week to relevant day. This is simply for easier readibility"""
    day_to_ind = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    return day_to_ind[day]

def retrieve_date(ds, day: str) -> str:
    """ Retrieve date of weekday for selection listen tables"""
    ds = datetime.strptime(ds, '%Y-%m-%d')
    shifter = switch_daystring_to_index(day)
    return (ds - timedelta(ds.weekday()-shifter)).strftime("%Y%m%d")
