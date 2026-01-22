# SWIFT Transit Performance Analysis

A specialized logistics analytics pipeline designed to transform raw, nested courier tracking data into actionable transit efficiency metrics. This project identifies network bottlenecks, calculates touchpoint density, and evaluates delivery reliability.

## 🏗 Project Architecture

The project follows a modular design to ensure resilience against missing data and inconsistent courier logging formats.

* **`main.py`**: The central orchestrator that coordinates data ingestion, metric derivation, and report generation.
* **`src/data_loader.py`**: Handles the structural flattening of nested JSON hierarchies, including recursive searching for authoritative timestamps within list-based fields.
* **`src/metrics_engine.py`**: The core logic layer that performs spatiotemporal calculations, distinct facility counting, and service-level benchmarking.
* **`src/utils.py`**: A foundational layer for India Standard Time (IST) normalization and defensive data extraction using `safe_get` patterns.

## 🛠 Setup & Execution (Using `uv`)

This project uses `uv` for lightning-fast dependency management and reproducible environments.

### 1. Synchronize the environment

Ensure you have `uv` installed, then run:

```bash
uv sync

```

This will automatically create a virtual environment and install dependencies like `pandas` and `numpy`.

### 2. Run the Analysis

To execute the full pipeline and generate the required deliverables:

```bash
uv run main.py

```

## 📊 Analytics Methodology

This implementation goes beyond simple timestamp subtraction by incorporating the following "Analytical Wizardry":

* **Authoritative Date Hierarchy**: Prioritizes official "ACTUAL_PICKUP" and "ACTUAL_DELIVERY" status codes, falling back to earliest and latest event scans only when official records are absent.
* **Distinct Facility ID Logic**: Identifies unique facility touchpoints by generating composite keys based on `city`, `state`, and `arrivalLocation`. This prevents double-counting redundant arrival/departure scans at the same hub.
* **Middle-Mile Efficiency**: Calculates `time_in_inter_facility_transit_hours`, specifically isolating the time spent between sorting centers vs. the "last mile" delivery attempt.
* **IST Normalization**: Automatically detects MongoDB `$numberLong` (millisecond) formats and ISO strings, standardizing all reporting to India Standard Time (IST).

## 📂 Deliverables

The execution produces two optimized CSV files in the `output/` directory:

1. **`transit_performance_detailed.csv`**: A granular dataset with 23 specific columns covering every shipment touchpoint, transit velocity, and success flag.
2. **`transit_performance_summary.csv`**: An executive summary providing network-level KPIs (Mean, Median, Mode) and performance comparisons by service level (e.g., Express vs. Standard).

---

### Technical Notes

* **Python Version**: Developed and tested on Python 3.12+
* **Performance**: Utilizes vectorized Pandas operations for efficient processing of large tracking datasets.
* **Resilience**: The pipeline is designed to handle null addresses, empty event sequences, and missing packaging metadata without crashing.
