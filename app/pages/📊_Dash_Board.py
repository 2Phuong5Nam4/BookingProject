import streamlit as st
from streamlit_option_menu import option_menu

import geopandas as gpd

# Set page configura
# Title of the dashboard
st.header("📊 Dash Board")
from pages.dashboards import acc_dashboard
from pages.dashboards import price_dashboard
# Define the pages
PAGES = {
    "Accommodation Dashboard": acc_dashboard.show,
    "Price Dashboard": price_dashboard.show
}

# Function to handle navigation
def navigate_to(page):
    st.session_state.current_page = page

# on_change callback function
def on_change(key):
    selection = st.session_state[key]
    navigate_to(selection)
    
selected5 = option_menu(None, ["Accommodation Dashboard", "Price Dashboard"],
                        icons=["Luggage fill", "Cash coin"],
                        on_change=on_change, key='menu_5', orientation="horizontal")
# Initialize session state if not already initialized
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Accommodation Dashboard"

# Display the appropriate page based on the current page
PAGES[st.session_state.current_page]()
