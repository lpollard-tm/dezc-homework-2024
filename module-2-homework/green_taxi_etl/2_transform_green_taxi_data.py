import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(merged_df, *args, **kwargs):
 
    data = merged_df[(merged_df['passenger_count'] != 0) & (merged_df['trip_distance'] != 0)]

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    unique_vendor_ids = data['VendorID'].unique()

    print(f"Preprocessing: The number of unique values in the 'VendorID' column: {len(unique_vendor_ids)}") 
    print(f"The unique values are: {', '.join(map(str, unique_vendor_ids))}")

    regex_pattern = re.compile(r'(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])')

    print("Preprocessing:", sum(data.columns.str.contains(regex_pattern)), "columns need to be renamed to snake case")
    
    data.columns = data.columns.map(lambda x: regex_pattern.sub('_', x).lower())

    return data


@test
def test_vendor_id(output, *args) -> None:
    """
    Check if 'vendor_id' is a column in the output DataFrame
    """
    assert 'vendor_id' in output.columns, "'vendor_id' is not one of the existing columns"

@test
def test_passenger_count(output, *args) -> None:
    """
    Check if passenger_count is greater than 0 for all rows
    """
    assert all(output['passenger_count'] > 0), 'You still have rows with a passenger_count value of 0'

@test
def test_trip_distance(output, *args) -> None:
    """
    Check if trip_distance is greater than 0 for all rows
    """
    assert all(output['trip_distance'] > 0), 'You still have rows with a trip_distance of 0'