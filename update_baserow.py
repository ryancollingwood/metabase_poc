from copy import deepcopy
import os
from time import sleep
from baserowapi import Baserow, Filter, GenericField, Table
from baserowapi import MultipleSelectField, SingleSelectField, DateField
from typing import Any, Dict, List, Union
from urllib3.exceptions import HTTPError

class BaserowUpdater:
    def __init__(self, baserow_url: str, baserow_api_key: str, table: Union[int, Table], schema: Dict[str, Any] = None, retry_max_count: int = 3, retry_wait_seconds: int = 10):
        self.baserow_url = baserow_url.rstrip('/')
        self.retry_max_count = retry_max_count
        self.retry_wait_seconds = retry_wait_seconds

        self.baserow = self._baserow_api(baserow_api_key)

        if isinstance(table, int):
            self.table_id = table
            self.__get_table()
        elif isinstance(table, Table):
            self.table = table
            self.table_id = table.id
        else:
            raise ValueError("Table must be int or Table")

        self.schema = schema
        if schema is None:
            self.__get_table_schema()


    def _baserow_api(self, baserow_api_key: str) -> Baserow:
        if not self.baserow_url:
            raise ValueError("BASEROW_URL is None")

        if not baserow_api_key:
            raise ValueError("BASEROW_API_KEY is None")

        return Baserow(url=self.baserow_url, token=baserow_api_key)

    def __get_table(self) -> None:
        self.table = self.baserow.get_table(self.table_id)

    def __get_table_schema(self) -> None:
        table_fields = self.table.field_names
        self.schema = {col: self.table.fields[col] for col in table_fields}

    def __upsert_row_to_table(self, update_data: Dict[str, Any], row_id: int, retry_count: int = 0) -> int:
        try:
            if row_id == -1:
                added_row = self.table.add_rows(update_data)
                return added_row[0].id
            else:
                update_data["id"] = row_id
                self.table.update_rows([update_data])
                return row_id
        except HTTPError as e:
            print("update_or_add_row - HTTPError", e)
            if retry_count < self.retry_max_count:
                sleep((retry_count + 1) * self.retry_wait_seconds)
                return self.__upsert_row_to_table(update_data, row_id, retry_count + 1)
            raise e
        except Exception as e:
            raise e

    def find_rows(self, filters: List[Filter], filter_type='AND', retry_count=0):
        try:
            matching_rows = self.table.get_rows(filters=filters, filter_type=filter_type)
        except HTTPError as e:
            print("find_rows - HTTPError", e)
            if retry_count < self.retry_max_count:
                sleep((retry_count + 1) * self.retry_wait_seconds)
                return self.find_rows(filters, filter_type, retry_count + 1)
            raise e
        except Exception as e:
            raise e

        return matching_rows
    
    def update_row(self, data: Dict[str, Any], schema: Dict[Dict, GenericField] = None) -> int:
        schema = self.schema if schema is None else schema
        primary_cols = [x for x in schema.values() if x.is_primary]
        if len(primary_cols) == 0:
            raise ValueError("No primary column found")

        filters = [Filter(x.name, data[x.name]) for x in primary_cols]
        matching_rows = self.find_rows(filters)

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

        option_cols: List[Union[MultipleSelectField, SingleSelectField]] = [x for x in schema.values() if x.TYPE in ["single_select", "multiple_select"]]
        option_values: Dict[str, List] = dict()

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
            col_option_values = option_values[col.name]

            if len(col_option_values) == 0:
                continue

            if update_data.get(col.name) is not None:
                continue
            col_name = col.name
            if col.TYPE == "single_select":
                update_data[col_name] = col_option_values[0]
            else:
                update_data[col_name] = col_option_values

        date_cols: List[DateField] = [x for x in schema.values() if x.TYPE == "date"]
        for col in date_cols:
            if col.name not in update_data:
                continue

            if not isinstance(update_data[col.name], datetime):
                raise ValueError(f"Invalid date value for column: {col.name}")
            
            datetime_value: datetime = update_data[col.name]

            if col.date_include_time:
                if col.date_format == 'ISO':
                    update_data[col.name] = datetime_value.isoformat()
                elif col.date_format == 'US':
                    update_data[col.name] = datetime_value.strftime('%m/%d/%Y %I:%M:%S %p')
                else:
                    # likely to fail if the date format is not supported
                    update_data[col.name] = datetime_value.strftime(col.date_format)
            else:
                if col.date_format == 'ISO':
                    update_data[col.name] = datetime_value.date().isoformat()
                elif col.date_format == 'US':
                    update_data[col.name] = datetime_value.date().strftime('%m/%d/%Y')
                else:
                    # likely to fail if the date format is not supported
                    update_data[col.name] = str(datetime_value.date())

        for column_name in update_data.keys():
            if column_name not in schema.keys():
                raise ValueError(f"Column not found in schema: {column_name}")

        print(update_data)
        row_id = self.__upsert_row_to_table(update_data, row_id)

        return row_id

if __name__ == "__main__":
    from dotenv import load_dotenv
    from datetime import datetime
    load_dotenv()

    table_id = 842

    table_updater = BaserowUpdater(os.getenv('BASEROW_URL'), os.getenv('BASEROW_API_KEY'), table_id)

    print(table_updater.schema)

    data ={
        'Name': 'Hello world', 
        'Notes': 'This is a test note', 
        'Number': 123,
        'Price': 123.45,
        'Boolean': True,
        'Date european': datetime(2024, 1, 1, 0, 0, 0, 0),
        #'Date us include time': datetime(2024, 10, 12, 2, 35, 16, 552),
        'Single select': 'Option 2',
        'Multiple select': ['Option B', 'Option C'],
        'Rating': 3,
    }

    table_updater.update_row(data)

    print("Done")