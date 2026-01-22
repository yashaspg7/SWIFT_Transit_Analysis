import pandas as pd

def parse_timestamp(ts):
    """Standardizes MongoDB $numberLong and ISO strings to IST."""
    if ts is None:
        return None
    # Handle MongoDB format: {"$numberLong": "1584353640000"}
    if isinstance(ts, dict) and '$numberLong' in ts:
        dt = pd.to_datetime(int(ts['$numberLong']), unit='ms', utc=True)
        return dt.tz_convert('Asia/Kolkata')
    # Handle ISO strings
    try:
        dt = pd.to_datetime(ts, utc=True)
        return dt.tz_convert('Asia/Kolkata')
    except Exception:
        return None

def safe_get(data, keys, default=None):
    """Safely navigates nested dictionaries to handle missing fields."""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        else:
            return default
    return data