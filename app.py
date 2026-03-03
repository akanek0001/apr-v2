import streamlit as st
from apr_logic import calculate_apr

st.title("APR Calculator V2")

principal = st.number_input("元本", value=100000)
rate = st.number_input("年利 (%)", value=10.0)
days = st.number_input("運用日数", value=30)
compound = st.checkbox("複利運用")

if st.button("計算"): 
    result = calculate_apr(principal, rate, days, compound)
    st.success(f"結果: {result:,.2f}")
