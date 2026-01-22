import json
import pandas as pd
from .utils import safe_get
DATA_PATH = "data/raw_data.json"


def load_and_flatten(DATA_PATH):
    """Corrected loader for root -> trackDetails[] structure."""
    with open(DATA_PATH, 'r') as f:
        data = json.load(f)
    
    flattened_data = []
    for root_entry in data:
        for shipment in root_entry.get('trackDetails', []):
            # Authoritative Dates search within the list
            dates = shipment.get('datesOrTimes', [])
            actual_pu = next((d.get('dateOrTimestamp') for d in dates if d.get('type') == 'ACTUAL_PICKUP'), None)
            actual_dl = next((d.get('dateOrTimestamp') for d in dates if d.get('type') == 'ACTUAL_DELIVERY'), None)
            
            record = {
                'tracking_number': shipment.get('trackingNumber'),
                'service_type': safe_get(shipment, ['service', 'type']),
                'carrier_code': shipment.get('carrierCode'),
                'weight_val': safe_get(shipment, ['packageWeight', 'value'], 0),
                'weight_unit': safe_get(shipment, ['packageWeight', 'units'], 'KG'),
                'packaging_type': safe_get(shipment, ['packaging', 'type']),
                'origin_city': safe_get(shipment, ['shipperAddress', 'city']),
                'origin_state': safe_get(shipment, ['shipperAddress', 'stateOrProvinceCode']),
                'origin_pincode': safe_get(shipment, ['shipperAddress', 'postalCode']),
                'destination_city': safe_get(shipment, ['destinationAddress', 'city']),
                'destination_state': safe_get(shipment, ['destinationAddress', 'stateOrProvinceCode']),
                'destination_pincode': safe_get(shipment, ['destinationAddress', 'postalCode']),
                'delivery_location_type': shipment.get('deliveryLocationType'),
                'pu_raw': actual_pu,
                'dl_raw': actual_dl,
                'events': shipment.get('events', [])
            }
            # Standardize weight to KG
            val = float(record['weight_val'])
            record['package_weight_kg'] = val * 0.453592 if str(record['weight_unit']).upper() == 'LB' else val
            flattened_data.append(record)
            
    return pd.DataFrame(flattened_data)