import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import numpy as np

locations = ["All", "Hạ Long", "Hội An", "Thừa Thiên Huế", "Nha Trang", "Đà Lạt"]

def get_data(query):
    conn = psycopg2.connect(
        host="localhost",
        database="BookingProject",
        user="postgres",
        password="nqh19082003"
    )
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- Query ---
query5 = """
WITH room_popularity AS (
    SELECT 
        CASE 
            WHEN acm.acm_location LIKE '%Hạ Long%' THEN 'Hạ Long'
            WHEN acm.acm_location LIKE '%Hội An%' THEN 'Hội An'
            WHEN acm.acm_location LIKE '%Thừa Thiên Huế%' THEN 'Thừa Thiên Huế'
            WHEN acm.acm_location LIKE '%Nha Trang%' THEN 'Nha Trang'
            WHEN acm.acm_location LIKE '%Đà Lạt%' THEN 'Đà Lạt'
            ELSE 'Other'
        END AS filtered_location,
        rm.rm_name,
        COUNT(*) AS count
    FROM 
        "Rooms" rm
    JOIN 
        "Accommodation" acm
    ON 
        rm.rm_accommodation_id = acm.acm_id
    WHERE 
        acm.acm_location SIMILAR TO '%(' || 'Hạ Long|Hội An|Thừa Thiên Huế|Nha Trang|Đà Lạt' || ')%'
    GROUP BY 
        filtered_location, rm.rm_name
)
SELECT 
    filtered_location AS acm_location,
    rm_name,
    count
FROM 
    room_popularity
ORDER BY 
    acm_location, count DESC;

"""

query6 = """
WITH filtered_feedback AS (
    SELECT 
        fb.fb_reviewed_date,
        acm.acm_location
    FROM 
        "Feedback" fb
    JOIN 
        "Accommodation" acm
    ON 
        fb.fb_accommodation_id = acm.acm_id
    WHERE 
        acm.acm_location SIMILAR TO '%(' || 'Hạ Long|Hội An|Thừa Thiên Huế|Nha Trang|Đà Lạt' || ')%'
),
feedback_with_weekly_info AS (
    SELECT 
        DATE_PART('year', fb_reviewed_date) AS year,
        DATE_PART('week', fb_reviewed_date) AS week,
        acm_location,
        COUNT(*) AS review_count
    FROM 
        filtered_feedback
    GROUP BY 
        year, week, acm_location
)
SELECT 
    year, 
    week, 
    acm_location, 
    review_count
FROM 
    feedback_with_weekly_info
ORDER BY 
    year, week, review_count DESC;
"""

query7 = """
WITH feedback_with_flags AS (
    SELECT 
        fb.fb_nationality,
        acm.acm_location,
        CASE 
            WHEN fb.fb_positive IS NOT NULL AND LENGTH(TRIM(fb.fb_positive)) > 0 THEN 1 
            ELSE 0 
        END AS is_positive,
        CASE 
            WHEN fb.fb_negative IS NOT NULL AND LENGTH(TRIM(fb.fb_negative)) > 0 THEN 1 
            ELSE 0 
        END AS is_negative
    FROM 
        "Feedback" fb
    JOIN 
        "Accommodation" acm
    ON 
        fb.fb_accommodation_id = acm.acm_id
    WHERE 
         acm.acm_location SIMILAR TO '%(' || 'Hạ Long|Hội An|Thừa Thiên Huế|Nha Trang|Đà Lạt' || ')%'
),
feedback_summary AS (
    SELECT 
        fb_nationality,
        acm_location,
        SUM(is_positive) AS pos_count,
        SUM(is_negative) AS neg_count
    FROM 
        feedback_with_flags
    GROUP BY 
        fb_nationality, acm_location
),
feedback_summary_filtered AS (
    SELECT 
        fb_nationality,
        acm_location,
        pos_count,
        neg_count
    FROM 
        feedback_summary
    WHERE 
        pos_count > (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pos_count) FROM feedback_summary)
        OR 
        neg_count > (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY neg_count) FROM feedback_summary)
)
SELECT 
    fb_nationality,
    acm_location,
    pos_count,
    neg_count
FROM 
    feedback_summary_filtered
ORDER BY 
    pos_count DESC, neg_count DESC;
"""
# --------

df5 = get_data(query5)
df6 = get_data(query6)
df7= get_data(query7)

def show():
    '''# Sample data
    np.random.seed(42)
    df = pd.DataFrame({
        'Category': ['A', 'B', 'C', 'D', 'E'],
        'Value': np.random.randint(1, 100, 5),
        'Date': pd.date_range(start='1/1/2023', periods=5)
    })

    # Create columns for grid layout
    col1, col2 = st.columns(2)

    # Line Chart
    with col1:
        st.subheader("Line Chart")
        fig_line = px.line(df, x='Date', y='Value', title='Value Over Time')
        st.plotly_chart(fig_line)

    # Bar Chart
    with col2:
        st.subheader("Bar Chart")
        fig_bar = px.bar(df, x='Category', y='Value', title='Value by Category')
        st.plotly_chart(fig_bar)

    # Pie Chart
    with col1:
        st.subheader("Pie Chart")
        fig_pie = px.pie(df, names='Category', values='Value', title='Value Distribution by Category')
        st.plotly_chart(fig_pie)'''
    selected_locations = st.selectbox(
        "Choose a location:", locations, index=0 
    )

    # Fig 5:
    if selected_locations != "All":
        filtered_df5 = df5[df5["acm_location"].str.contains(selected_locations, case=False, na=False)]
    else:
        filtered_df5 = df5

    fig5 = px.bar(
        filtered_df5, 
        x="rm_name", 
        y="count", 
        color="acm_location",
        title="Top 10 Popular Rooms by Area",
        labels={"rm_name": "Room name", "count": "Num of rooms"},
        category_orders={"acm_location": filtered_df5['acm_location'].unique().tolist()},
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.title("Popular room")
    st.plotly_chart(fig5)

    # Fig 6:
    df6["year"] = df6["year"].astype(int).astype(str)  
    df6["week"] = df6["week"].astype(int).astype(str)  
    df6["day"] = "1"  
    df6["week_start"] = pd.to_datetime(df6["year"] + " " + df6["week"] + " " + df6["day"], format="%Y %W %w")

    if selected_locations != "All":
        filtered_df6 = df6[df6["acm_location"].str.contains(selected_locations, case=False, na=False)]
    else:
        filtered_df6 = df6
    
    available_years = sorted(filtered_df6["year"].unique())
    selected_years = st.multiselect(
        "Choose years:", 
        options=available_years, 
        default=available_years  
    )
    filtered_df6 = filtered_df6[filtered_df6["year"].isin(selected_years)]

    fig6 = px.line(
        filtered_df6, 
        x="week_start", 
        y="review_count", 
        color="year",
        title="The number of reviews increases week by week",
        labels={"week_start": "Week", "review_count": "Num of reviews", "year": "Year"}
    )
    st.title("Review trends")
    st.plotly_chart(fig6)

    # Fig 7
    if selected_locations != "All":
        filtered_df7 = df7[df7["acm_location"].str.contains(selected_locations, case=False, na=False)]
    else:
        filtered_df7 = df7

    fig7 = px.bar(
        filtered_df7,
        x="fb_nationality", 
        y=["pos_count", "neg_count"],  
        color_discrete_map={"pos_count": "blue", "neg_count": "red"},  
        labels={
            "value": "Num of reviews",
            "variable": "Review type",
            "fb_nationality": "Nationality"
        },
        title="Distribution of positive and negative reviews by nationality",
        barmode="group"  
    )
    st.title("Distribution of Reviews by Nationality")
    st.plotly_chart(fig7)

    