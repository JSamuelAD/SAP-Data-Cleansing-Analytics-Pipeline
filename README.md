# SAP Data Cleansing & Analytics Pipeline

## Project Overview

This project simulates a professional consulting engagement to build an end-to-end ETL (Extract, Transform, Load) pipeline. The process automatically extracts raw sales data from multiple CSV sources, cleans and validates it using Python, and loads it into a centralized SQLite database. The final, clean dataset is optimized for KPI analysis and visualization in BI tools like Power BI, creating a reliable Single Source of Truth.

---

## Tech Stack

- **Data Extraction & Transformation:** Python (Pandas)
- **Database:** SQLite
- **Version Control:** Git & GitHub
- **BI & Visualization:** Power BI

---

## ETL Process

1.  **Extract:** The Python script reads and consolidates all `.csv` files located in the `/data` directory.
2.  **Transform:**
    - Numeric columns (`Unit Selling Price`, `Quantity Sold`) are cleaned of non-numeric characters and converted to the correct data type.
    - Column names are standardized for consistency.
    - A professional logging system records the entire process in the `proceso_etl.log` file.
3.  **Load:** The cleaned and transformed DataFrame is loaded into the `proyecto.db` SQLite database, replacing the old table to ensure data freshness.

---

## How to Run

1.  Clone the repository.
2.  Set up a Python virtual environment and install the dependencies from `requirements.txt`:
    ```bash
    pip install -r requirements.txt
    ```
3.  Run the main ETL script:
    ```bash
    python scripts/limpieza_ventas.py
    ```
4.  The cleaned data will be available in the `proyecto.db` file, ready to be connected to a BI tool.