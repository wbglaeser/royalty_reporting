from datetime import datetime, timedelta

def retrieve_start_date(ds):
    ds = datetime.strptime(ds, '%Y-%m-%d')
    return (ds - timedelta(ds.weekday())).strftime("%Y%m%d")

def retrieve_start_timestamp(ds):
    ds = datetime.strptime(ds, '%Y-%m-%d')
    return (ds - timedelta(ds.weekday())).strftime("%Y-%m-%d %H:%M:%S UTC")

def retrieve_end_date(ds):
    ds = datetime.strptime(ds, '%Y-%m-%d')
    return (ds - timedelta(ds.weekday()-6)).strftime("%Y%m%d")

def retrieve_end_timestamp(ds):
    ds = datetime.strptime(ds, '%Y-%m-%d')
    return (ds - timedelta(ds.weekday()-7)).strftime("%Y-%m-%d %H:%M:%S UTC")
