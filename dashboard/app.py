import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="BillingShield", layout="wide")
st.title("BillingShield — Healthcare Payment Integrity Dashboard")

API_URL = "http://localhost:8000"

tab1, tab2, tab3 = st.tabs(["Summary", "Provider Lookup", "Cohort Explorer"])

with tab1:
    st.subheader("Risk Tier Summary")
    try:
        data = requests.get(f"{API_URL}/summary").json()
        df = pd.DataFrame(data["tiers"])
        st.dataframe(df, use_container_width=True)
    except:
        st.warning("API not running — start with: uvicorn api.main:app --reload")

with tab2:
    st.subheader("Provider Risk Lookup")
    npi = st.text_input("Enter Provider NPI")
    if npi:
        try:
            result = requests.get(f"{API_URL}/provider/{npi}").json()
            if "detail" in result:
                st.error(result["detail"])
            else:
                col1, col2, col3 = st.columns(3)
                col1.metric("Risk Score", round(result["risk_score"], 4))
                col2.metric("Risk Tier", result["risk_tier"])
                col3.metric("Charge Ratio", round(result["avg_charge_ratio"], 2))
                st.json(result)
        except:
            st.error("API not reachable")

with tab3:
    st.subheader("Cohort Explorer")
    col1, col2, col3 = st.columns(3)
    risk_tier = col1.selectbox("Risk Tier", ["", "Critical", "Elevated", "Normal"])
    state = col2.text_input("State (e.g. CA)")
    limit = col3.slider("Max Results", 10, 500, 100)

    if st.button("Search"):
        params = {"limit": limit}
        if risk_tier: params["risk_tier"] = risk_tier
        if state: params["state"] = state
        try:
            result = requests.get(f"{API_URL}/cohort", params=params).json()
            df = pd.DataFrame(result["providers"])
            st.write(f"Found {result['count']} providers")
            st.dataframe(df, use_container_width=True)
            csv = df.to_csv(index=False)
            st.download_button("Export CSV", csv, "providers.csv", "text/csv")
        except:
            st.error("API not reachable")
