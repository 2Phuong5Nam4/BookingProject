import streamlit as st
import numpy as np
import pandas as pd

import streamlit as st
from streamlit_option_menu import option_menu
st.set_page_config(page_title="The Ramsey Highlights", layout="wide")

# Add a big logo at the top of the sidebar
st.sidebar.image("https://www.pixenli.com/image/fm0aEpMI", use_container_width=False)   
pg = st.navigation([st.Page("pages/ℹ️_Introduction.py"), st.Page("pages/📊_Dash_Board.py"), st.Page("pages/🌟_Recommender.py"), st.Page("pages/🤖_Chat_bot.py")])
pg.run()

st.logo(
    "https://www.pixenli.com/image/fm0aEpMI",
    link="https://streamlit.io/gallery",
    icon_image="https://www.pixenli.com/image/fm0aEpMI",
)

# Add a title to the sidebar
st.sidebar.title("About")

# Add some content to the sidebar
st.sidebar.write("Nói j j đó về môn học và đồ án chẳng hạn.")

ms = st.session_state
if "themes" not in ms: 
  ms.themes = {"current_theme": "dark",
                "refreshed": False,
                
                "light": {"theme.base": "dark",
                            "theme.backgroundColor": "black",
                            "theme.primaryColor": "#c98bdb",
                            "theme.secondaryBackgroundColor": "#a9a9a9",
                            "theme.textColor": "white",
                            "theme.textColor": "white",
                            "button_face": "𖤓 Light"},

                "dark":  {"theme.base": "light",
                            "theme.backgroundColor": "white",
                            "theme.primaryColor": "#5591f5",
                            "theme.secondaryBackgroundColor": "#fbfbfb",
                            "theme.textColor": "#0a1464",
                            "button_face": "⏾ Dark"},
                    }
  
# Custom CSS for the buttons
button_css = """
<style>
.github-button {
    display: inline-block;
    padding: 10px 20px;
    color: white;
    background-color: #24292e;
    border-radius: 5px;
    text-decoration: none;
    font-size: 16px;
    margin-bottom:20px;
}

.github-button:hover {
    background-color: #404448;
}

.github-icon {
    fill: white;
    width: 24px;
    height: 24px;
    vertical-align: middle;
}
</style>
"""

# GitHub icon SVG
github_icon = """
<svg class="github-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">
    <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
</svg>
"""

# Inject custom CSS
st.markdown(button_css, unsafe_allow_html=True)
        
def ChangeTheme():
  previous_theme = ms.themes["current_theme"]
  tdict = ms.themes["light"] if ms.themes["current_theme"] == "light" else ms.themes["dark"]
  for vkey, vval in tdict.items(): 
    if vkey.startswith("theme"): st._config.set_option(vkey, vval)

  ms.themes["refreshed"] = False
  if previous_theme == "dark": ms.themes["current_theme"] = "light"
  elif previous_theme == "light": ms.themes["current_theme"] = "dark"

# Place the buttons in a row at the bottom of the sidebar
with st.sidebar:
    github_url = "https://github.com/your-repo-url"
    # st.markdown(f'<a href="{github_url}" class="github-button">{github_icon}</a>', unsafe_allow_html=True)
        
    btn_face = ms.themes["light"]["button_face"] if ms.themes["current_theme"] == "light" else ms.themes["dark"]["button_face"]
    # st.button(btn_face, on_click=ChangeTheme, use_container_width=True)
    
    left, right = st.columns(2)
    left.button(btn_face, on_click=ChangeTheme,use_container_width=True)
    right.link_button("Repository", use_container_width=True, url="https://github.com/your-repo-url")

        

if ms.themes["refreshed"] == False:
    ms.themes["refreshed"] = True
    st.rerun()