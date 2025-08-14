# Streamlit in Snowflake: Smoothie App (Badge 3) – DABW008-ready
# ---------------------------------------------------------------
# Notes:
# - Uses Streamlit connection for Snowflake and Snowpark session
# - Reads fruit options from SMOOTHIES.PUBLIC.FRUIT_OPTIONS
# - Validates name and ingredients
# - Writes orders safely via Snowpark DataFrame append
# - Stores ingredients as comma-separated list (no trailing spaces)
# - Shows nutrition info from Fruityvice without blocking insert

import json
import requests
import pandas as pd
import streamlit as st
from snowflake.snowpark.functions import col

# --- Page config
st.set_page_config(page_title="Smoothie Builder", layout="wide")

# --- Snowflake connection / session
cnx = st.connection("snowflake", type="snowflake")
session = cnx.session()

# --- UI Header
st.title("🥤 Customize Your Smoothie! 🥤")
st.write("Choose the fruits you want in your custom Smoothie! Up to five.")

# --- Name input
name_on_order = st.text_input("Name On Smoothie:").strip()
st.caption(f"The name on your smoothie will be: {name_on_order or '—'}")

# --- Load fruit options from Snowflake (cache to reduce queries)
@st.cache_data(ttl=300)
def load_fruit_options():
    sp_df = (
        session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS")
        .select(col("FRUIT_NAME"), col("SEARCH_ON"))
        .sort(col("FRUIT_NAME"))
    )
    return sp_df.to_pandas()

pd_df = load_fruit_options()

# Safety: ensure required columns exist
required_cols = {"FRUIT_NAME", "SEARCH_ON"}
if not required_cols.issubset(set(pd_df.columns)):
    st.error("FRUIT_OPTIONS table must contain FRUIT_NAME and SEARCH_ON columns.")
    st.stop()

# --- Ingredient picker
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    options=pd_df["FRUIT_NAME"].tolist(),
    max_selections=5,
)

# --- Nutrition display (non-blocking)
if ingredients_list:
    with st.expander("Nutrition details for selected fruits", expanded=True):
        for fruit_chosen in ingredients_list:
            row = pd_df.loc[pd_df["FRUIT_NAME"] == fruit_chosen]
            if row.empty:
                st.warning(f"No lookup found for {fruit_chosen}.")
                continue
            search_on = row["SEARCH_ON"].iloc[0]
            st.subheader(f"{fruit_chosen} – Nutrition Information")
            try:
                resp = requests.get(
                    f"https://fruityvice.com/api/fruit/{search_on}", timeout=8
                )
                resp.raise_for_status()
                data = resp.json()
                st.dataframe(data=data, use_container_width=True)
            except Exception as e:
                st.info(
                    f"Could not load nutrition for {fruit_chosen} right now. You can still place your order."
                )

# --- Build clean ingredients string
ingredients_string = ", ".join(ingredients_list) if ingredients_list else ""

# --- Validation and submit
submit_disabled = (not name_on_order) or (len(ingredients_list) == 0)
submit_col, preview_col = st.columns([1, 3])
with submit_col:
    submitted = st.button("Submit Order", disabled=submit_disabled)
with preview_col:
    st.write(
        "Preview:",
        {
            "NAME_ON_ORDER": name_on_order or "",
            "INGREDIENTS": ingredients_string or "",
        },
    )

if submitted:
    # Optional: ensure target table exists (idempotent). Comment out if your env already has it.
    session.sql(
        """
        CREATE TABLE IF NOT EXISTS SMOOTHIES.PUBLIC.ORDERS (
            ORDER_ID INTEGER AUTOINCREMENT,
            NAME_ON_ORDER STRING NOT NULL,
            INGREDIENTS STRING NOT NULL,
            ORDER_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    ).collect()

    # Safe append via Snowpark DataFrame (avoids SQL injection and quoting issues)
    df_to_write = session.create_dataframe(
        [[ingredients_string, name_on_order]],
        schema=["INGREDIENTS", "NAME_ON_ORDER"],
    )
    df_to_write.write.save_as_table("SMOOTHIES.PUBLIC.ORDERS", mode="append")

    st.success("Your Smoothie is ordered!")

    # Show the last few orders for feedback
    recent = session.sql(
        """
        SELECT ORDER_ID, NAME_ON_ORDER, INGREDIENTS, ORDER_TS
        FROM SMOOTHIES.PUBLIC.ORDERS
        ORDER BY ORDER_TS DESC
        LIMIT 10
        """
    ).to_pandas()
    st.dataframe(recent, use_container_width=True)
