import os
from dotenv import load_dotenv
from metabase_api import Metabase_API
from baserowapi import Baserow
from dataclasses import dataclass

@dataclass
class FieldDefinition:
    baserow_type: str
    is_read_only: bool
    is_primary: bool
    options: list



if __name__ == "__main__":
    load_dotenv()

    baserow_url = os.getenv('BASEROW_URL')
    baserow_api_key = os.getenv('BASEROW_API_KEY')

    if not baserow_url or not baserow_api_key:
        print("Baserow URL or API KEY not found")
        exit()

    print("Connecting:", baserow_url)
    
    baserow = Baserow(url=baserow_url, token=baserow_api_key)

    # Create a table instance using its ID
    table = baserow.get_table(839)

    # Print a list of field names in the table
    print(table.field_names)

    for col in ['category', 'single', "question"]:
        print(col, table.fields[col].TYPE)
        if table.fields[col].TYPE in ["single_select", "multiple_select"]:
            print(table.fields[col].options)
        print(table.fields[col].is_read_only)
        print("")

    print(dir(table.fields[col]))
    # Fetch a row using its ID
    my_row = table.get_row(29)

    print(my_row.to_dict())
