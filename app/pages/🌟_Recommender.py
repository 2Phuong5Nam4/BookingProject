import streamlit as st
import pandas as pd

st.title("Recommended Hotels")

# Sample data
hotels = {
    'Hotel Name': ['Hotel A', 'Hotel B', 'Hotel C', 'Hotel D', 'Hotel E'],
    'Province': ['Hanoi', 'Ho Chi Minh City', 'Da Nang', 'Hue', 'Nha Trang'],
    'Rating': [4.5, 4.8, 4.2, 4.0, 4.7],
    'Price': [100, 150, 80, 70, 120]
}

df_hotels = pd.DataFrame(hotels)

# Display recommended hotels
st.header("Top Rated Hotels")
st.write(df_hotels)

# Filter by province
province = st.selectbox("Select Province", df_hotels['Province'].unique())
filtered_hotels = df_hotels[df_hotels['Province'] == province]

st.header(f"Hotels in {province}")
st.write(filtered_hotels)

# Define the options for the radio button group
options = ["Option 1", "Option 2", "Option 3"]

# Create a container for the radio buttons
container = st.container()

# Use CSS to style the radio buttons horizontally
container.markdown(
    """
    <style>
    .stRadio [role=radiogroup] {
        flex-direction: row !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Create the radio button group
selected_option = container.radio("Select an option", options)

# Display the selected option
st.write(f"You selected: {selected_option}")
