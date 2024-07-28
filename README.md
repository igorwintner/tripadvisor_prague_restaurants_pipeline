# Tripadvisor Prague Restaurants End-to-End Project on Google Cloud 

## Project Summary
This data engineering project implements a comprehensive ETL (Extract, Transform, Load) data pipeline designed to manage restaurant data from Tripadvisor in Prague. The project utilizes **Google Cloud Engine** to run the **MAGE** pipeline, which orchestrates the ETL process. The pipeline leverages **Google Cloud Storage** for storing raw data, **Pandas** and **Jupyter Notebook** for data transformation, and **Google BigQuery** for storing and querying the processed data with **SQL**. The final transformed data is visualized using **Looker Studio**. This automated pipeline efficiently collects, transforms, and stores Tripadvisor restaurant data, providing valuable insights for analysis.

## Overview
**Pipeline Workflow:**
1. **Extract Data:**
   - **Load Raw Data:** Raw data files are stored in **Google Cloud Storage**.
   
    [View Extract Code](/mage_pipeline/1_extract_raw_data_gs.py)

2. **Transform Data:**
   - **Clean and Process Data:** The collected data is cleaned and transformed using **Pandas** to ensure accuracy and consistency. There is also a [link](/transform_notebook.ipynb) to the **Jupyter Notebook**, which contains the data transformation code and where the code was originally written. 

    [View Transform Code](/mage_pipeline/2_transform_raw_data.py)

3. **Load Data:**
   - **Store in BigQuery:** The transformed data is uploaded to **BigQuery** for efficient querying and analysis.

    [View Load Code](/mage_pipeline/3_load_to_bq.py)

The overall workflow is managed by a **MAGE** pipeline that orchestrates the entire process. [Here](https://github.com/mage-ai/mage-ai) is a link to their GitHub. There's a detailed description of how to install and launch MAGE. See below for a graph after a successful pipeline trigger in **MAGE**. 

![Alt Text](/images/mage_pipeline_etl.png)
*Complete data pipeline in MAGE*

## Architecture
![Alt Text](/images/architecture.png)
*Architecture graph of the data pipeline*

### Key Components and Tech Stack:
- **Tripadvisor**: Source of the Prague restaurants data.
- **Python**: Programming language used for data processing.
  - **Jupyter Notebook**: Used for data transformation and interactive analysis during the development process.
  - **Pandas**: Library used for data transformation and filtering.
- **SQL**: Utilized for querying and managing the **BigQuery** tables.
- **MAGE**: Orchestrates the ETL workflow and builds real-time pipelines to transform data using Python (or other languages). 
- **Google Cloud Platform**:
  - **Google Cloud Storage**: Stores raw data.
  - **Google Cloud Engine**: Provides computational resources to deploy and run the ETL pipeline.
  - **Google BigQuery**: Serves as the data warehouse for transformed data. SQL analysis is also performed here.
  - **Looker Studio**: Used for building and visualizing the final dashboard.

## Data Model
The original data was just a CSV flat file that was transformed into 6 tables (5 dimension tables and 1 fact table). This transformation was done in Python using Mage and is included in the code above. Here is a diagram showing the new data model. 

![Alt Text](/images/data_model.png)
*Data model diagram*

### Raw Data on Google Storage
Raw Tripadvisor data was stored on **Google Storage**. See image below. The original file can be found [here](/raw_data/tripadvisor_prague_restaurants.csv). 

![Alt Text](/images/raw_data_bucket.png)
*Raw data bucket*

### BigQuery Tables and SQL Analysis
The transformed data is loaded into **BigQuery** tables. Data analysis was also performed in BigQuery using **SQL**. Queries can be seen [here](/bigquery_analysis.sql). At the same time, a new `analytics_table` was created containing only the columns that will be needed to create the dashboard in **Looker Studio**. SQL query can be found [here](/create_analytics_table.sql).

![Alt Text](/images/bigquery_tables.png)
*Transformed data in BigQuery tables*

## Looker Studio Dashboard
The **Looker Studio** is connected directly to **BigQuery** and visualizes the data stored in `analytics_table`. [Here](https://lookerstudio.google.com/reporting/51cf4553-f029-48d6-91eb-aadb2ad084cb) is a link to the dashboard. 

![Alt Text](/images/looker_tripadvisor_prague_dashboard.png)
*Final Looker Studio dashboard* 

## Conclusion
This project demonstrates the successful implementation of a robust and automated ETL data pipeline using **Google Cloud Platform** and modern data engineering tools. By leveraging **Google Cloud Storage** for data storage, **MAGE** for workflow orchestration, **Pandas** for data transformation, and **BigQuery** for data storage and analysis, this pipeline efficiently manages the extraction, transformation, and loading of Tripadvisor Prague restaurant data.

The complete code for this data pipeline and the output data are available in the respective folders.

### Key Achievements:
- Successfully collected and processed Tripadvisor Prague restaurants data.
- Ensured reliable and scalable data storage in **Google BigQuery**.
- Maintained clear separation of ETL tasks, ensuring modularity and maintainability.

### Future Enhancements:

- Expand the analysis to include restaurants from multiple cities or regions.
- Integrate with additional data sources to provide a more comprehensive analysis.
- Implement error handling and alerting mechanisms for improved reliability.
- Extend the pipeline to include sentiment analysis or other NLP techniques on the restaurant reviews.