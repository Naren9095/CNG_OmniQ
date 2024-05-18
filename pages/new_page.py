import streamlit as st

selected_name = None

def change_name():
    st.write(st.session_state['myselect'])

with st.form(key='source_'):
    name = st.multiselect('myselect',['kamal','naren'],['kamal','naren'],on_change=change_name,key='myselect')
    st.write(st.session_state['myselect'])
    submit_button = st.form_submit_button("Validate")
    if submit_button:
        st.success("Data validated 'validation_result_1'!")


# import streamlit as st

# def main():
#     items = [1,2,3,4]
#     def get_new_values_list():
#         st.write(st.session_state['issue'])
#     values = st.multiselect('issue', items, items, on_change=get_new_values_list, key='issue')
#     st.write(values) # < returns correct list

# if __name__ == '__main__':
#     main()