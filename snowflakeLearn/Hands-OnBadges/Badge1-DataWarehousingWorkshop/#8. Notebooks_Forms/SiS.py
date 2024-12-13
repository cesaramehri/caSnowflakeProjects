# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Example Streamlit App :balloon:")
st.write(
    """Replace this example with your own code!
    **And if you're new to Streamlit,** check
    out our easy-to-follow guides at
    [docs.streamlit.io](https://docs.streamlit.io).
    """
)

# Get the current credentials
session = get_active_session()

# Add Input Fields (whatever you enter in the field box will be put into the variable)
fn = st.text_input('Fruit Name:')
rdc = st.selectbox('Root Depth:',('S','M','D'))

# Add a Submit Button to check, and Prepare to Write the Data to the Database
if st.button('Submit'):
    # st.write('Fruit Name entered is: ' + fn)
    # st.write('Root Depth Code chosen is: ' + rdc)
    sql_insert = 'INSERT INTO GARDEN_PLANTS.FRUITS.FRUIT_DETAILS SELECT \''+fn+'\',\''+rdc+'\''
    #st.write(sql_insert)
    result = session.sql(sql_insert)
    st.write(result)
