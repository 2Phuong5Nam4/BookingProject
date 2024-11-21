import streamlit as st
from streamlit_option_menu import option_menu
import geopandas as gpd
import os
from streamlit_calendar import calendar


from pages.dashboards import acc_dashboard
from pages.dashboards import price_dashboard


# Function to handle navigation
def navigate_to(page):
    st.session_state.current_page = page

# on_change callback function
def on_change(key):
    selection = st.session_state[key]
    navigate_to(selection)

# Initialize session state if not already initialized
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Price Dashboard"

# Set page configura
# Title of the dashboard
# st.header("")
header = st.container()
col1, col2 = header.columns([2, 4])

with col1:  
    st.subheader("📊 Dash Board")
with col2:
    selected5 = option_menu(None, ["Accommodation Dashboard", "Price Dashboard"],
                            icons=["Luggage fill", "Cash coin"],
                            on_change=on_change, key='menu_5', orientation="horizontal")
# header.title("📊 Dash Board")
header.write("""<div class='fixed-header'/>""", unsafe_allow_html=True)
# Define the pages
PAGES = {
    "Accommodation Dashboard": lambda: acc_dashboard.show(),
    "Price Dashboard": lambda: price_dashboard.show(header)
}
# Display the appropriate page based on the current page
PAGES[st.session_state.current_page]()

# Read and inject CSS
css_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'styles', 'dashboard.css')
with open(css_path) as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
