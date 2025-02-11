import streamlit as st
import streamlit.components.v1 as components
from streamlit_pdf_viewer import pdf_viewer



def show_dashboard():
    st.markdown(
        """
        <div style='display: flex; 
                    justify-content: center; 
                    align-items: center; 
                    # height: 55vh;'>
            <h1 style='font-size: 50px; 
                    color: #0073e6;   
                    font-weight: bold;
                    font-family: monospace;'>
                Dashboard
            </h1>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    tab1_path = "docker_images/streamlit/cloud_app/assets/dashboard/tab1.pdf"
    tab2_path = "docker_images/streamlit/cloud_app/assets/dashboard/tab2.pdf"
    tab3_path = "docker_images/streamlit/cloud_app/assets/dashboard/tab3.pdf"

    tab1, tab2, tab3 = st.tabs(["Tab 1", "Tab 2", "Tab 3"])
    with tab1:
        pdf_viewer(tab1_path)
    with tab2:
        pdf_viewer(tab2_path)
    with tab3:
        pdf_viewer(tab3_path)




















































    # st.image("https://raw.githubusercontent.com/nchn471/tiki-etl-pipeline-project/refs/heads/main/wordcloud.png?token=GHSAT0AAAAAAC3F4FSP6BQAFQZ3LS7GMB24Z4MTRJA")
