# Databricks Retail Sales Capstone

This project uses **PySpark** and **Spark SQL** in **Azure Databricks** to turn two raw retail CSVs into sales summaries. The notebook cleans sales transactions and store records, joins them by store ID, and answers two questions. Which products sold the most units, and which stores generated the most sales?

## Course and certificate

I completed this capstone as part of **Complete Guide to Databricks for Data Engineering** by **Deepak Goyal** on **LinkedIn Learning**. The course completion certificate is included in this directory. [View the certificate on LinkedIn Learning](https://www.linkedin.com/learning/certificates/81f0f5810ebeab97384e2f9a08ab6cbff453a13081e645014e1bb38cf8cf5eb3?trk=share_certificate)

| File | Contents |
| --- | --- |
| [RetailProject-DataCleaning.ipynb](./RetailProject-DataCleaning.ipynb) | Databricks notebook with the cleaning, transformations, SQL queries, and saved previews |
| [Course completion certificate (PDF)](./CertificateOfCompletion_Complete%20Guide%20to%20Databricks%20for%20Data%20Engineering.pdf) | LinkedIn Learning certificate of completion |

## What the notebook does

1. **Load the data.** Read `sales_data.csv` and `store_data.csv` as Spark DataFrames using the CSV headers and inferred schemas.
2. **Clean the sales and store records.** Remove duplicate sales rows and sales with missing IDs or dates, non-positive quantities, or non-positive amounts. For the store data, drop rows with missing store IDs or opening dates and fill missing store sizes with the average store size.
3. **Enrich the sales.** Inner-join the cleaned datasets on `store_id`. Derive the sale year and calculate `sales_per_sqft` as the transaction amount divided by the store size, rounded to two decimal places.
4. **Summarize and rank with Spark SQL.** Calculate total sales and units sold by store and region. Rank the top five products by units sold and the top five stores by sales amount.
5. **Write the rankings.** Save the two ranking datasets as Parquet under `top_products/` and `top_stores/` in a Databricks volume.

## Run in Databricks

1. Import the notebook into a Databricks workspace and attach compute that can run Python and Spark.
2. Place `sales_data.csv` and `store_data.csv` in a location the compute can read. The CSVs are required separately. Update both paths in the first code cell to match your workspace.
3. In the final write cell, update the two `/Volumes/...` paths to a Unity Catalog volume you can write to. The cell uses overwrite mode for both output directories.
4. Run the notebook from top to bottom to preview the cleaned data, inspect the SQL results, and write the rankings.
