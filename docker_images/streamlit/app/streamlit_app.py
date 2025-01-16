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
    "exp": round(time.time()) + (60 * 10)  # Token hết hạn sau 10 phút
}
token = jwt.encode(payload, METABASE_SECRET_KEY, algorithm="HS256")

# Tạo URL nhúng Metabase dashboard
iframeUrl = f"{METABASE_SITE_URL}/embed/dashboard/{token}#bordered=true&titled=true"

# Cấu hình trang Streamlit
st.set_page_config(
    page_title="Metabase Dashboard",
    page_icon="📊",
    layout="wide"
)

# Tiêu đề ứng dụng
st.title("📊 Metabase Dashboard Viewer")

# Nhúng iframe qua `st.components.v1.iframe`
components.iframe(iframeUrl, width=1400, height=2000, scrolling=True)

# Thêm phần mô tả
st.write("Truy cập trực tiếp vào Metabase dashboard để có thêm tính năng tương tác.")
