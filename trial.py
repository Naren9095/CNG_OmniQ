# Initial list of values
initial_values = ["value1", "value2", "value3"]

# List to hold selected values for each dropdown
# Use None to indicate no selection
selected_values = [None] * len(initial_values)

def select_value(dropdown_index, value):
    global initial_values, selected_values

    # Deselect the previous value if there was one
    if selected_values[dropdown_index] is not None:
        initial_values.append(selected_values[dropdown_index])

    # Update the selected value for the dropdown
    selected_values[dropdown_index] = value

    # Remove the selected value from the initial_values list
    if value in initial_values:
        initial_values.remove(value)

def deselect_value(dropdown_index):
    global initial_values, selected_values

    # Deselect the value and put it back to the list
    value = selected_values[dropdown_index]
    if value is not None:
        initial_values.append(value)
        selected_values[dropdown_index] = None

def get_available_values(dropdown_index):
    global initial_values, selected_values

    # Compute available values excluding the selected ones in other dropdowns
    available_values = set(initial_values)
    for i, selected_value in enumerate(selected_values):
        if i != dropdown_index and selected_value is not None:
            available_values.discard(selected_value)
    return list(available_values)

# Example usage
values = ["value1", "value2", "value3"]
initial_values = values[:]
selected_values = [None] * len(values)

print("Initial values: ", initial_values)
print("Selected values: ", selected_values)


# Selecting value1 in the first dropdown
select_value(0, "value1")
print(get_available_values(0))  # []
print(get_available_values(1))  # ['value2', 'value3']
print(get_available_values(2))  # ['value2', 'value3']

# Selecting value2 in the second dropdown
select_value(1, "value2")
print(get_available_values(0))  # []
print(get_available_values(1))  # []
print(get_available_values(2))  # ['value3']

# Deselecting value1 from the first dropdown
deselect_value(0)
print(get_available_values(0))  # ['value2', 'value3']
print(get_available_values(1))  # []
print(get_available_values(2))  # ['value2', 'value3']
