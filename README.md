# Github actions executions on:
Access the link https://github.com/Richardbnk/GCP_Data_Engineering_Trips/actions/runs/13599254659

Click on **"run-docker"**

Then click on **"Run Docker Container and Execute Application"**

# To build the docker just run this code on the root folder:

Start docker and run command lines:

- docker build -t myrepo-test .
- docker run --rm myrepo-test


# **Dockerized ETL Pipeline with BigQuery Integration**

## **Overview**

This project is an **ETL pipeline** that processes large-scale **trip data** from a CSV file and loads it into **Google BigQuery**. The project is designed for **efficiency and scalability**, utilizing **batch processing**, **partitioning**, and **clustering** in BigQuery. The processing for the reports are done using Big Query processing that offers better performance than Python only.

The pipeline consists of:

1. **Extracting** data from a CSV file.
2. **Transforming** the data (datetime conversion, coordinate cleaning).
3. **Loading** the processed data into BigQuery in chunks (for scalability).
4. **Running analytical queries** on the ingested data.
5. **Visualizing** the results using console logs.
8. There is a code for matplotlib charts, that is not running inside Docker, but is there just for demonstration.

This pipeline is **fully Dockerized**, ensuring it runs consistently across different environments.

---

## **1. Project Structure**

```
/etl_project
│── /src
│   ├── process_data.py              # ETL process for trips.csv
│   ├── run_queries.py               # Executes SQL queries on BigQuery and print insights for the challenge
│   ├── data_vizualization.py        # Displays data insights using console logs
│   ├── data_vizualization_charts.py # Displays data insights using matplotlib
│── /sql
│   ├── /ddl
│   │   ├── trips_ddl.sql       # create Raw Table for trips.csv
│   │   ├── grouped_trips.sql   # create Trusted table for grouped similar trips
│   ├── group_similar_trips.sql       # query to find similar trips
│   ├── weekly_avg_trips_bounding_box.sql # weekly averages within a region using bounding_box
│   ├── weekly_avg_trips_region.sql   # weekly averages per region
│   ├── latest_datasource_from_common_regions.sql # fetches latest data source for top 2 most common regions
│   ├── regions_of_cheap_mobile.sql   # identifies regions where a cheap_mobile appeared
│── requirements.txt    # Python dependencies
│── Dockerfile          # Docker configuration
│── .env                # Environment variables (optional)
│── trips.csv           # Raw trip data
│── data-project-452300-e2c341ffd483.json  # Google Cloud authentication key
```

---

## **2. Understanding the Python Scripts**

### **2.1. process_data.py**

**Role:** Extract data from `trips.csv`, transform, and load into BigQuery.

### **Key Steps:**

- Create or replace existing tables.
- Load environment variables for authentication.
- Read trip data in **chunks** (avoiding memory issues for big files).
- Clean coordinates and convert timestamps.
- Upload data to BigQuery using batch processing.
- Uses tqdm to show progress in real time.

Instead of uploading the entire dataset at once, we **process it in chunks** (`100,000` rows per batch). This speeds up the upload and avoids memory issues.

---

### **2.2. run_queries.py**

**Role:** Executes SQL queries on BigQuery to get **insights** from the raw_trip data from Big Query.

### **Key Steps:**

- Runs SQL scripts stored in `/sql` folder.
- Uses query parameters for filtering (bounding box for grid geospatial).
- Loads query results into Pandas DataFrames.
- Displays query results in the CMD console.

Instead of processing data in Python (which can be slow for large datasets), we **push the computations to BigQuery**, leveraging its optimized SQL engine.

---

### **2.3. data_vizualization.py**

**Role:** Generates **matplotlib charts** to visualize the processed trip data **directly in CMD**.

### **Key Steps:**

- Fetch data from BigQuery.
- Group trip data by region, coordinate grid (lat and long), time, and datasource.
- Generate charts and plots on PNG image files.
- Displays multiple graphs stacked in CMD.

The best way to vizualise data information would be to connect the data to a Data Vizualization tool.

So matplotlib would work better for console execution.

---

## **3. Dockerfile**

A **Dockerfile** defines how our container is built and executes the ETL pipeline, queries and data vizualization.

### **Key Sections:**

* Uses python:3.11
* Sets up the base image, ensuring our container has **Python 3.11**.dockerfile
* Installs **all required Python libraries** inside the container.
* Set environment variables
* Ensures Python logs appear **in real time** in the terminal.
* Adds the **CSV data file** inside the container.
* **Runs all three scripts sequentially** when the container starts.

---

## **4. Running the Dockerized ETL Pipeline**

### **Step 1: Build the Docker Image**

```bash
docker build -t trip-etl .
```

- This **creates a container image** named `trip-etl`.

### **Step 2: Run the Container**

```bash
docker run --rm -it trip-etl
```

- This starts the container and executes the **full ETL pipeline**.

---

## **5. Key Benefits of This Approach**

- **Scalable ETL pipeline** – Handles large data efficiently.
- **Modular design** – Scripts are structured logically.
- **Automated BigQuery integration** – No manual SQL execution required.
- **Fully containerized** – Works **consistently** across any machine.

---


## **Next Steps?**

- Integrate **Airflow** to automate ETL execution via CI/CD using Cloud Composer.
- Data quality to make sure that all ingested information is correct and standerlized.
- Alerts erros via a platform like slack.
