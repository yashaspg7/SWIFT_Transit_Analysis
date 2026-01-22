import os
import pandas as pd
from src.data_loader import load_and_flatten
from src.metrics_engine import calculate_metrics, compute_network_summary

DATA_PATH = "data/rawdata.json"
OUTPUT_DIR = "output"

def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    
    # 1. Process Pipeline
    base_df = load_and_flatten(DATA_PATH)
    metrics_results = base_df.apply(calculate_metrics, axis=1)
    
    # 2. Integrate and Classify
    detailed_df = pd.concat([base_df, metrics_results], axis=1)
    detailed_df["is_express_service"] = detailed_df["service_type"].str.contains("EXPRESS|PRIORITY", case=False, na=False)
    detailed_df["avg_hours_per_facility"] = detailed_df["total_transit_hours"] / detailed_df["num_facilities_visited"].replace(0, float('nan'))

    # 3. Part 5: Required Column Order & Cleanup
    required_cols = [
        "tracking_number", "service_type", "carrier_code", "package_weight_kg", "packaging_type",
        "origin_city", "origin_state", "origin_pincode", "destination_city", "destination_state",
        "destination_pincode", "pickup_datetime_ist", "delivery_datetime_ist", "total_transit_hours",
        "num_facilities_visited", "num_in_transit_events", "time_in_inter_facility_transit_hours",
        "avg_hours_per_facility", "is_express_service", "delivery_location_type",
        "num_out_for_delivery_attempts", "first_attempt_delivery", "total_events_count"
    ]
    # Filter to only the requested columns
    final_detailed_df = detailed_df[required_cols]

    # 4. Save Final Files
    final_detailed_df.to_csv(os.path.join(OUTPUT_DIR, "transit_performance_detailed.csv"), index=False)
    
    summary_df = compute_network_summary(detailed_df)
    summary_df.to_csv(os.path.join(OUTPUT_DIR, "transit_performance_summary.csv"), index=False)
    
    print("Files successfully generated in /output.")

if __name__ == "__main__":
    main()