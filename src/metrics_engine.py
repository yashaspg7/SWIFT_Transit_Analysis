import pandas as pd
import numpy as np
from .utils import parse_timestamp, safe_get

def calculate_metrics(row):
    events = row.get('events', [])
    if not events:
        return pd.Series({"total_transit_hours": np.nan, "num_facilities_visited": 0})

    ev_df = pd.DataFrame(events)
    ev_df["parsed_time"] = ev_df["timestamp"].apply(parse_timestamp)
    ev_df = ev_df.sort_values("parsed_time").drop_duplicates(subset=["parsed_time", "eventType"])

    # 1. Facility Count (Unique City + Location Name)
    fac_ev = ev_df[ev_df["arrivalLocation"].str.contains("FACILITY", na=False, case=False)]
    num_facilities = fac_ev.apply(lambda x: f"{safe_get(x, ['address', 'city'])}_{x.get('arrivalLocation')}", axis=1).nunique()

    # 2. Transit Time Logic (Authoritative -> Event Fallback)
    pickup_ist = parse_timestamp(row.get("pu_raw"))
    delivery_ist = parse_timestamp(row.get("dl_raw"))
    
    if pickup_ist is None:
        pu_ev = ev_df[ev_df["eventType"] == "PU"]
        if not pu_ev.empty: pickup_ist = pu_ev["parsed_time"].min()
    if delivery_ist is None:
        dl_ev = ev_df[ev_df["eventType"] == "DL"]
        if not dl_ev.empty: delivery_ist = dl_ev["parsed_time"].max()

    total_hours = (delivery_ist - pickup_ist).total_seconds() / 3600 if pickup_ist and delivery_ist else np.nan

    # 3. Inter-facility Time
    inter_fac = (fac_ev["parsed_time"].max() - fac_ev["parsed_time"].min()).total_seconds() / 3600 if len(fac_ev) > 1 else 0.0

    return pd.Series({
        "pickup_datetime_ist": pickup_ist,
        "delivery_datetime_ist": delivery_ist,
        "total_transit_hours": total_hours,
        "num_facilities_visited": num_facilities,
        "num_in_transit_events": len(ev_df[ev_df["eventType"] == "IT"]),
        "time_in_inter_facility_transit_hours": inter_fac,
        "num_out_for_delivery_attempts": len(ev_df[ev_df["eventType"] == "OD"]),
        "first_attempt_delivery": len(ev_df[ev_df["eventType"] == "OD"]) == 1,
        "total_events_count": len(ev_df)
    })

def compute_network_summary(df):
    """Produces the comprehensive summary statistics required by Part 6."""
    
    # 1. Overall Metrics
    summary = {
        "total_shipments_analyzed": len(df),
        "avg_transit_hours": df["total_transit_hours"].mean(),
        "median_transit_hours": df["total_transit_hours"].median(),
        "std_dev_transit_hours": df["total_transit_hours"].std(),
        "min_transit_hours": df["total_transit_hours"].min(),
        "max_transit_hours": df["total_transit_hours"].max(),
        
        # 2. Facility Metrics
        "avg_facilities_per_shipment": df["num_facilities_visited"].mean(),
        "median_facilities_per_shipment": df["num_facilities_visited"].median(),
        "mode_facilities_per_shipment": df["num_facilities_visited"].mode()[0] if not df["num_facilities_visited"].mode().empty else 0,
        "avg_hours_per_facility": df["avg_hours_per_facility"].mean(),
        "median_hours_per_facility": df["avg_hours_per_facility"].median(),
        
        # 3. Delivery Performance
        "pct_first_attempt_delivery": (df["first_attempt_delivery"].mean() * 100),
        "avg_out_for_delivery_attempts": df["num_out_for_delivery_attempts"].mean()
    }
    
    # 4. Service Type Comparison (Grouped logic)
    service_grp = df.groupby("service_type").agg(
        avg_transit_hours_by_service_type=("total_transit_hours", "mean"),
        avg_facilities_by_service_type=("num_facilities_visited", "mean"),
        count_shipments_by_service_type=("tracking_number", "count")
    )
    
    # Flatten the grouped stats into additional columns for a single-row executive summary
    for svc, row in service_grp.iterrows():
        summary[f"{svc}_avg_transit_hours"] = row["avg_transit_hours_by_service_type"]
        summary[f"{svc}_avg_facilities"] = row["avg_facilities_by_service_type"]
        summary[f"{svc}_count"] = row["count_shipments_by_service_type"]

    return pd.DataFrame([summary])