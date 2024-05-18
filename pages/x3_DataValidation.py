import streamlit as st

top_container = st.container()
first_Container = st.container()
second_container = st.container()
s = False

with top_container:
    st.title('OmniQSuite-Compare your data in a single platform')
    s = st.button(label="Next",use_container_width=True,type='primary')
if s==False:
    with first_Container:
            st.markdown('###')
            st.write('first container')
            
elif s:
    with second_container:
            st.write(s)
            st.write('second container')

