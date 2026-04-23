# 🎬 End-to-End Customer 360 Behavior Analysis Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-orange.svg)
![MySQL](https://img.shields.io/badge/MySQL-Datamart-lightgrey.svg)
![Tableau](https://img.shields.io/badge/Tableau-Public-blue.svg)

## 📌 Project Overview
This project is an End-to-End Data Engineering & Analytics solution designed for an OTT/Streaming platform. It processes raw search logs and viewing history to build a unified **Customer 360 Datamart**. The goal is to bridge the gap between user **Search Intent** (what they look for) and **Actual Watch Behavior** (what they actually consume), enabling data-driven content strategies and personalized recommendations.

🔗 **[Click here to view the Interactive Tableau Dashboard](https://public.tableau.com/views/Customer360BehaviorContentStrategyAnalysis/Dashboard1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)**

## 🏗️ Data Architecture (Medallion Concept)
The pipeline strictly follows the Medallion Architecture to ensure data quality and scalability:

1. **🥉 Bronze Layer (Raw Data):** Ingestion of unstructured/semi-structured `log_search` (Parquet) and `log_content` (JSON) containing timestamps, raw queries, and durations.
2. **🥈 Silver Layer (Enriched):** - Data cleaning, Regex pattern matching, and Timestamp formatting.
   - Human-in-the-loop mapping for unclassified search keywords.
   - Aggregation of viewing durations and calculation of `Active Ratio`.
3. **🥇 Gold Layer (Data Mart):** A fully flattened One-Big-Table (OBT) `customer_360_behavior` deployed in MySQL. Created via a Full Outer/Inner Join on `user_id` to capture both intent and consumption.

## 📊 Key Business Insights

Based on the 5,000+ user cohort analyzed across June and July, several critical insights were extracted using SQL and Tableau:

### 1. The 71% Preference Shift
* **Insight:** 71% of users changed their primary viewing category from June to July. 
* **Actionable Metric:** The recommendation engine must prioritize real-time session data over historical long-term preferences.

### 2. High Churn Risk in Specific Genres
* **Insight:** C-Drama has a surprisingly low retention rate (20%), with 267 users shifting their focus to Movies_General.
* **Actionable Metric:** Content acquisition teams should evaluate the current C-Drama catalog's quality or volume.

### 3. Golden Taste Combinations
* **Insight:** The combination of *Live TV + Movies* dominates the platform's engagement. Users exhibiting this "Multi-Taste" archetype (The Explorer) have an average active score of 81.72, significantly higher than single-taste loyalists (62.55).

## 📸 Dashboard Gallery

### 1. Customer 360 Overview
*Comparing Search Intent vs. Actual Watch Distribution.*
![Dashboard 1](dashboards/Dashboard_1.jpg)

### 2. Content Strategy & Shift Behavior
*Analyzing category stickiness and Top 10 transition flows.*
![Dashboard 2](dashboards/Dashboard_2.jpg)

### 3. User Profiling
*Deep dive into user archetypes and engagement levels.*
![Dashboard 3](dashboards/Dashboard_3.jpg)

## 💻 Tech Stack & Pipeline Execution

* **ETL Framework:** Apache Spark (PySpark)
* **Data Storage:** Local FS, MySQL (Serving Layer)
* **Analytics & BI:** SQL, Tableau Public

### How to Run the Pipeline
1. **Initialize Database:** Run the schema script to create the Datamart structure.
   ```bash
   mysql -u root -p < sql/init_schema.sql