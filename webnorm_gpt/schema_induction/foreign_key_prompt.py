FOREIGN_KEY_PROMPT_SYSTEM = """
You are an expert for database management.
Here you will be given a target table field and a list of candidate foreign key fields.
Your task is to identify which of the candidate fields are foreign keys to the target field.
There could be multiple correct answers. Also, some of the candidate fields may not be foreign keys at all.

## Example 1:

Target table name: `log::order.service.OrderServiceImpl.checkSecurityAboutOrder`
Target field name: `log_data.arguments.accountId`

Candidate table and field pairs:
`auth_user`, `user_id`
`consign_record`, `user_id`
`contacts`, `account_id`
`inside_money`, `user_id`
`orders`, `account_id`
`orders_other`, `account_id`
`user`, `user_id`
`user_roles`, `user_id`


Because `user` table is most likely to store user information, and `user_id` is a common field name for user id,
it is likely that `user_id` in `user` table is a foreign key to `accountId` in the target table.
So, in this example, the correct answers are: `user`, `user_id`

You should output a json string with the following format:
```json
{
    "foreign_keys": [
        {
            "table": "contacts",
            "field": "account_id"
        }
    ]
}
```

## Example 2:

Target table name: `log::foodsearch.service.FoodServiceImpl.createFoodOrder`
Target field name: `log_data.response.data.orderId`

Candidate table and field pairs:
`assurance`, `order_id`
`consign_record`, `order_id`
`food_order`, `order_id`
`inside_payment`, `order_id`
`orders`, `id`
`orders_other`, `id`

In this example, both `orders` and `orders_other` tables have a field named `id`.
It is likely that both `others` and `orders_other` tables have a field that is a foreign key to `order_id` in the target table.
So, in this example, the correct answers are: `orders`, `id` and `orders_other`, `id`

You should output a json string with the following format:
```json
{
    "foreign_keys": [
        {
            "table": "orders",
            "field": "id"
        },
        {
            "table": "orders_other",
            "field": "id"
        }
    ]
}
```

## Example 3:

Target table name: `orders`
Target field name: `seat_class`

Candidate table and field pairs:
`consign_price`, `within_price`
`consign_record`, `weight`
`contacts`, `document_type`
`food_order`, `food_type`
`food_order`, `price`
`orders_other`, `document_type`
`orders_other`, `seat_class`
`orders_other`, `status`
`route_distances`, `distances_order`
`route_stations`, `stations_order`
`station`, `stay_time`
`station_food_list`, `price`
`train_food_list`, `price`
`trip2`, `type`
`user`, `document_type`

In this example, `orders_other` table has a field named `seat_class`.
However, `seat_class` is just a common field name and is not necessarily a foreign key.
So in this example, none of the candidate fields are foreign keys to the target field.

So the output should be:
```json
{
    "foreign_keys": []
}
```

## Reminder:
- Some fields such as `status`, `type`, `price`, `weight`, `document_type`, `length` are common field names and are not necessarily foreign keys.
- If you are not sure, you can skip the field.

"""

FOREIGN_KEY_PROMPT_USER = """
## Problem:

Target table name: `{target_table_name}`
Target field name: `{target_field_name}`

Candidate table and field pairs:
{candidate_table_field_pairs}

## Answer:

"""

FOREIGN_KEY_PROMPT_FORMAT_CANDIDATE = "`{table_name}`, `{field_name}`"

FILTER_COLUMNS_SYSTEM = """
#Task
Given the table name and its columns, analyze the column names and their possible relationships to determine:

**Primary Key**: Identify the column(s) that uniquely identify each row in the table.

**Foreign Keys**: Identify columns that may reference primary keys in other tables.

Common patterns of these columns are columns ending with "id" or "key".

You should output a json with the following format:

```json
{
    "foreign_keys": ["column_name1", "column_name2", ...],
    "primary_keys": ["column_name1", "column_name2", ...]
}
```

"""

FILTER_COLUMNS_USER = """
#Input:

Table: 
{table_name}
Table Columns:
{columns}

#Output:
"""

EXTRACT_RELATED_TABLE_SYSTEM = """
#Task

Given possible foreign key columns of a table and the related entity list of the table (can be empty),
select relevant table(s) from the given table list based on the columns name given.
Please select as complete as possible.

*Always consider based on the given foreign key column name first. Be complete in your selection.*
*Make sure to also consider the entity in related entity list if available. Otherwise, judge based on foreign key column name.*

You should output a json with the following format:

```json
{
    "tables": ["table_name1", "table_name2", ...]
}
```

"""

EXTRACT_RELATED_Table_USER = """
#Input:

Table List:
{db_list}

Columns:
{columns}

Related Entity List:
{entity_list}

#Output:
"""
