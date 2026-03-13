# infrabel-kpi-punctuality-analysis

• Sources: Infrabel Open Data + complementary public datasets (Statbel, SNCB)
• Objective: analyze punctuality trends across the Belgian railway network and challenge the official metric by proposing alternative metrics
• Stack: Python (pandas, GeoPandas, SQLAlchemy) • SQL Server / T-SQL • Power BI (DAX)
• Architecture:
  - Data pipeline orchestrated with Jupyter notebooks
  - Custom Python package for ingestion, transformation and geospatial preparation
  - Structured data flow (raw → intermediate → processed)
  - SQL Server data warehouse (star schema)
  - Power BI dashboards

**Documentation is currently being expanded**
🚧 This project is currently being refactored to adopt professional data engineering standards.

## Project Architecture

```mermaid
flowchart TD

A[Open Data Sources<br>Infrabel / Statbel / SNCB]

B[Data Collection<br><br>Jupyter Notebooks<br>Custom Python package]

C[Raw Data<br>]

D[Data Cleaning & Transformation<br><br>Jupyter Notebooks<br>Custom Python package]

E[Intermediate Data<br>]

F[Feature Engineering & Geospatial Enrichment<br><br>Jupyter Notebooks<br>Custom Python package]

G[Processed Data<br>]

H[SQL Server Data Warehouse<br>Star schema]

I[Power BI Dashboards]

A --> B
B --> C
C --> D
D --> E
E --> F
F --> G
G --> H
H --> I

