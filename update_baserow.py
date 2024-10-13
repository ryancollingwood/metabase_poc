from copy import deepcopy
import os
from baserowapi import Baserow, Filter, GenericField, Table
from typing import Any, Dict, List, Union



def baserow_api() -> Baserow:
    baserow_url = os.getenv('BASEROW_URL')
    baserow_api_key = os.getenv('BASEROW_API_KEY')

    if baserow_url is None:
        raise ValueError("BASEROW_URL is None")
    if baserow_url[-1] == "/":
        baserow_url = baserow_url[:-1]
    
    if baserow_api_key is None:
        raise ValueError("BASEROW_API_KEY is None")

    return Baserow(url=baserow_url, token=baserow_api_key)


def baserow_table(table_id: int) -> Table:
    baserow = baserow_api()
    table = baserow.get_table(table_id)

    return table


def baserow_table_schema(table: Table) -> Dict[Dict, GenericField]:
    table_fields = table.field_names

    return {col: table.fields[col] for col in table_fields}


def baserow_update_row(table: Union[int, Table], data: Dict[str, Any], schema: Dict[Dict, GenericField] = None) -> int:
    """
    Update a row in a Baserow table.

    - For columns that aren't in the schema it will
        - Check if the column is an option for a multiple select or single select column
        - If it is, it will add the option to the column and remove the option from the data
        - If it isn't, it will raise an error
    - Checks for existence of a row by primary column value
    - If row does not exist, adds a new row, if it does exist, updates the row

    TODO:
    - Add support for updating multiple rows at once
    - Datetime support, given the various datetime formats in Baserow
    - Numeric support, given the various numeric formats in Baserow
    - File support?
    """
    if isinstance(table, int):
        table = baserow_table(table)

    if schema is None:
        table = baserow_table(table)
        schema = baserow_table_schema(table)
    
    primary_cols = [x for x in schema.values() if x.is_primary]
    if len(primary_cols) == 0:
        raise ValueError("No primary column found")
    
    filters = [Filter(x.name, data[x.name]) for x in primary_cols]
    matching_rows = table.get_rows(filters=filters, filter_type='AND')

    row_id = -1
    if len(matching_rows) == 1:
        row_id = matching_rows[0].id
    elif len(matching_rows) > 1:
        raise ValueError("Multiple rows found")
    
    for col in primary_cols:
        if col.is_read_only:
            raise ValueError("Primary column is read only")
        if col.name not in data:
            raise ValueError("Primary column is missing")
    
    update_data = deepcopy(data)
    option_cols = [x for x in schema.values() if x.TYPE in ["single_select", "multiple_select"]]
    option_values = dict()

    for col in option_cols:
        col_name = col.name
        option_values[col_name] = list()
        for option in col.options:
            if option in update_data:
                if update_data[option] in [1, True]:
                    option_values[col_name].append(option)
                update_data.pop(option, None)

        if col.TYPE == "single_select":
            if len(option_values[col_name]) > 1:
                raise ValueError(f"Multiple options for single select: {option_values[col_name]}")
    
    for col in option_cols:
        col_name = col.name
        if col.TYPE == "single_select":
            update_data[col_name] = option_values[col_name][0]
        else:
            update_data[col_name] = option_values[col_name]

    for column_name in update_data.keys():
        if column_name not in schema.keys():
            raise ValueError(f"Column not found in schema: {column_name}")
        
    if row_id == -1:
        added_row = table.add_rows(update_data)
        row_id = added_row[0].id
    else:
        update_data["id"] = row_id
        table.update_rows([update_data])
    
    return row_id

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    table_id = 841

    table = baserow_table(table_id)
    schema = baserow_table_schema(table)

    print(schema)

    # this should fail because we've included a column that is not in the schema
    data = {
        "foo": 1,
        "bar": 1,
        "question": "What is your name?",
        "extra": "Extra",
    }

    baserow_update_row(table, data, schema)

    print("Done")