
import streamlit as st

def handle_submit_one():
  st.write("Submit button one clicked.")

def handle_submit_two():
  st.write("Submit button two clicked.")

with st.form(key="my_form"):
  st.text_input("Input field")
  submit_button_one = st.form_submit_button("Submit button one", on_click=handle_submit_one, clear_on_submit=True)
  submit_button_two = st.form_submit_button("Submit button two", on_click=handle_submit_two, clear_on_submit=True)

if submit_button_one or submit_button_two:
  st.write("Form submitted.")