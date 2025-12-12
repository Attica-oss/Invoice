import duckdb
import polars as pl

"""
This script connects to a DuckDB database, creates a table, and inserts data from a Polars DataFrame.
"""

from dataframe.miscellaneous import cross_stuffing

# Filter historical data (exclude 2025)
historical_cross_stuffing = cross_stuffing.filter(pl.col("date").dt.year().ne(2025))

if __name__ == "__main__":
    # Connect to DuckDB and create/populate table
    with duckdb.connect(r"backup\miscellaneous.db") as con:
        # Create table with proper schema
        con.sql("""
            CREATE TABLE IF NOT EXISTS cross_stuffing (
                day VARCHAR,
                vessel_client VARCHAR,
                date DATE,
                origin VARCHAR,
                destination VARCHAR,
                start_time TIME,
                end_time TIME,
                total_tonnage DOUBLE,
                overtime_tonnage DOUBLE,
                is_origin_empty BOOLEAN,
                Service VARCHAR,
                invoiced VARCHAR,
                Price DOUBLE,
                total_price DOUBLE
            )
        """)


        
        # Insert data from Polars DataFrame
        # Method 1: Using DuckDB's direct Polars integration
        con.sql("INSERT INTO cross_stuffing SELECT * FROM historical_cross_stuffing")
        
        # Alternative Method 2: Convert to pandas first (if Method 1 doesn't work)
        # historical_cross_stuffing_pandas = historical_cross_stuffing.to_pandas()
        # con.sql("INSERT INTO cross_stuffing SELECT * FROM historical_cross_stuffing_pandas")
        
        # Verify data was inserted
        result = con.sql("SELECT COUNT(*) as row_count FROM cross_stuffing").fetchone()
        print(f"Rows inserted: {result[0]}")
        
        # Show sample data
        print("\nSample data:")
        con.sql("SELECT * FROM cross_stuffing LIMIT 5").show()
        
        # Show table info
        print("\nTable schema:")
        con.sql("DESCRIBE cross_stuffing").show()
    