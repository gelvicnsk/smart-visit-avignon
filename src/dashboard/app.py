import streamlit as st
import requests

st.set_page_config(page_title="Smart Visit Avignon", layout="wide")

st.title("Smart Visit Avignon")
st.write("Dashboard de suivi du projet Big Data")

try:
    response = requests.get("http://api:8000/health", timeout=3)
    st.success(f"API connectée : {response.json()}")
except Exception as e:
    st.error(f"API non disponible : {e}")
