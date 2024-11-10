def show():
    import streamlit as st
    import pandas as pd
    import numpy as np
    import plotly.express as px
    # Sample data
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
        st.plotly_chart(fig_pie)
    