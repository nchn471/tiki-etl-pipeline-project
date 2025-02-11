import streamlit as st
import jwt
import time
import streamlit.components.v1 as components

# Thông tin cấu hình Metabase
METABASE_SITE_URL = "http://localhost:3000"
METABASE_SECRET_KEY = "3c7d8f26de15cd83ce19392443dc3e58f2210ad303ffaf52586bfd0cc326d18c"

# Tạo JWT token
payload = {
    "resource": {"dashboard": 4},
    "params": {},
    "exp": round(time.time()) + (60 * 10)  
}
token = jwt.encode(payload, METABASE_SECRET_KEY, algorithm="HS256")

iframeUrl = f"{METABASE_SITE_URL}/embed/dashboard/{token}#bordered=true&titled=true"

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

    components.iframe(iframeUrl, width=1400, height=2000, scrolling=True)























































    # st.image("https://raw.githubusercontent.com/nchn471/tiki-etl-pipeline-project/refs/heads/main/wordcloud.png?token=GHSAT0AAAAAAC3F4FSP6BQAFQZ3LS7GMB24Z4MTRJA")
