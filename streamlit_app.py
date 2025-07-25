# Import python packages
import streamlit as st
# from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

import requests
# smoothiefroot_response = requests.get("https://fruityvice.com/api/fruit/watermelon")
# sf_df = st.dataframe(data = smoothiefroot_response.json(), use_container_width= True)

# cnx = st.connection("snowflake")
cnx = st.connection("snowflake", type="snowflake")
session = cnx.session()


# Write directly to the app
st.title(f"🥤 Customize Your Smoothie! 🥤")
st.write(
  """Choose the fruits you want in your custom Smoothie!
  """
)


name_on_order = st.text_input('Name On Smoothie:')
st.write('the name on your smoothie will be:', name_on_order)

# Get the current credentials
# session = get_active_session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
# st.dataframe(data=my_dataframe, use_container_width=True)

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    my_dataframe,
    max_selections=5
)

if ingredients_list:

    ingredients_string = ' '

    for fruit_chosen in ingredients_list:
        ingredients_string +=fruit_chosen + ' '
        smoothiefroot_response = requests.get("https://fruityvice.com/api/fruit/watermelon")
        sf_df = st.dataframe(data = smoothiefroot_response.json(), use_container_width= True)


    st.write(ingredients_string)

    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('""" + ingredients_string +  """',
        '""" + name_on_order +  """')
    """

    
    # st.write( my_insert_stmt)

    time_to_insert = st.button('Submit Order')
    
    if time_to_insert:

        session.sql(my_insert_stmt).collect()

        st.success('Your Smoothie is ordered!',icon="✅")
