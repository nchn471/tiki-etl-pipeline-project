import streamlit as st
from streamlit_option_menu import option_menu
from sub_pages.dashboard import show_dashboard
from sub_pages.recommendation import show_recommendation
from sub_pages.about import show_about

#Layout
st.set_page_config(
    page_title="Tiki",
    layout="wide",
    page_icon=":mag_right:",
    initial_sidebar_state="expanded"
)

#Data Pull and Functions
st.markdown("""
<style>
.big-font {
    font-size:80px !important;
}
</style>
""", unsafe_allow_html=True)



with st.sidebar:
    selected = option_menu('Menu', ['About', 'Dashboard','Recommendation'], 
        icons=['info-circle','graph-up','search'],menu_icon='intersect', default_index=0)
    
if selected == "Dashboard":
    show_dashboard()
elif selected == "Recommendation":
    show_recommendation()
else:
    show_about()
