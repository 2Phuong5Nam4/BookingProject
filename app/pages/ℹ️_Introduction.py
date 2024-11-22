import streamlit as st
import pandas as pd

# Title and Header
st.title("Booking Hotel Analysis on Booking Website")

# Course Information
st.header("Course Information")
st.markdown("""
- **Course Name:** Applications of Intelligent Data Analysis
- **Course Code:** CSC17107  
- **Instructor:** Dr. Nguyen Tien Huy  
- **Semester:** I  
- **Credits:** 4
""")
st.markdown("""
<style>
.custom-font {
    font-size:24px !important;
}
</style>
""", unsafe_allow_html=True)

# Project Overview
st.header("Project Overview")
# st.markdown('<p class="custom-font">The goal of this project is to analyze hotel booking data from a popular booking website. We aim to uncover insights related to booking patterns, customer preferences, and pricing strategies. The analysis will involve data cleaning, exploratory data analysis (EDA), and predictive modeling to forecast future bookings.</p>', unsafe_allow_html=True)
st.markdown("The goal of this project is to analyze hotel booking data from a popular [Booking Website](https://www.booking.com/searchresults.en-gb.html). We aim to uncover insights related to booking patterns, customer preferences, and pricing strategies. The analysis will involve data cleaning, exploratory data analysis (EDA), and predictive modeling to forecast future bookings.")
# Team Members
st.header("Team Members")
team_members = pd.DataFrame({
    'Name': ['Nguyen Phuong Nam', 'Nguyen Gia Phuc', 'Tran Dinh Nhat Tri', 'Nguyen Quoc Hung', 'Chau Quy Duong', 'Vo Minh Khue'],
    'ID': ['21120504', '21120539', '21120576', '21120464', '21120436', '21120486']
})
st.table(team_members)

# Title and Header
st.header("ETL Pipeline")
st.image("./static/pipeline.png", caption="ETL Pipeline")

# Extract (Scrapy)
st.markdown("### Extract (Scrapy)")
st.markdown("""
**Scrapy** is a powerful and flexible web scraping framework in Python. It is used to extract data from various booking websites. The process involves:

- **Defining Spiders**: Create Scrapy spiders to navigate through the website's structure and extract relevant data such as hotel names, locations, prices, ratings, and reviews.
- **Data Extraction**: Use XPath or CSS selectors to extract data from HTML pages. Scrapy handles the HTTP requests, parsing, and data extraction efficiently.
- **Data Storage**: Store the extracted data in JSON format for further processing.
""")

# Transform (Python/Pandas)
st.markdown("### Transform (Pandas)")
st.markdown("""
After extracting the data, it needs to be cleaned and transformed to make it suitable for analysis. This step involves:

- **Data Cleaning**: Remove duplicates, handle missing values, and correct data types.
- **Data Transformation**: Normalize data, aggregate information, and create new features (e.g., average price, total reviews).
- **Data Enrichment**: Enrich the dataset with additional information.
""")

# Load (PostgreSQL)
st.markdown("### Load (PostgreSQL)")
st.markdown("""
**PostgreSQL** is a robust, open-source relational database management system. It is used to store the cleaned and transformed data. The process involves:

- **Database Schema Design**: Design a schema that fits the structure of the extracted data. Create tables for hotels, bookings, reviews, etc.
- **Data Loading**: Use SQLAlchemy or psycopg2 to load the transformed data into PostgreSQL. Ensure data integrity and consistency by defining primary keys, foreign keys, and constraints.
- **Data Indexing**: Create indexes on frequently queried columns to optimize query performance.
""")
st.markdown("""Database schema stored in Postgres as follow:""")
st.image("./static/database.jpg", caption="Database Diagram")

# Orchestration (Apache Airflow)
st.markdown("### Orchestration (Apache Airflow)")
st.markdown("""
**Apache Airflow** is a platform to programmatically author, schedule, and monitor workflows. It is used to orchestrate the ETL pipeline. The process involves:

- **DAG Creation**: Define a Directed Acyclic Graph (DAG) that represents the ETL pipeline. Each task in the DAG corresponds to a step in the pipeline (e.g., scraping, cleaning, loading).
- **Task Dependencies**: Specify dependencies between tasks to ensure they are executed in the correct order. For example, the cleaning task should run after the scraping task.
- **Scheduling**: Schedule the DAG to run at regular intervals (e.g., daily, weekly) to keep the data up-to-date.
- **Monitoring**: Monitor the execution of tasks and handle failures gracefully. Airflow provides a web interface to track the status of tasks and pipelines.
""")

# Visualization and Interaction (Streamlit)
st.markdown("### Visualization and Interaction (Streamlit)")
st.markdown("""
**Streamlit** is an open-source app framework for building interactive web applications in Python. It is used to create a user-friendly interface for visualizing and interacting with the data. The process involves:

- **Data Retrieval**: Query the PostgreSQL database to retrieve the necessary data for visualization.
- **Data Visualization**: Use libraries like Matplotlib, Seaborn, or Plotly to create charts and graphs. Streamlit allows for easy integration of these visualizations into the web app.
- **User Interaction**: Implement features such as filters, dropdowns, and sliders to allow users to interact with the data. For example, users can filter hotels by location, price range, or rating.
- **Dashboard Creation**: Design a dashboard that presents key insights and metrics. Streamlit's layout options make it easy to create a clean and intuitive interface.
- **Chatbot (LLM)**: A chatbot powered by a Large Language Model (LLM) provides intelligent and context-aware responses to user queries.
- **Recommendation System**: A recommendation system suggests hotels based on user preferences and historical data.
""")