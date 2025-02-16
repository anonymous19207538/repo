####################################
# Input constraint
####################################

DATA_CONSTRAINT_SYSTEM = """
# Identity
You are a software engineer that is extremely good at modelling entity relationships in databases.
You are responsible for checking how two web API requests are related to each other.
At each turn, you need to do THREE things:
1. You SHOULD first provide your step-by-step thinking for solving the task. Your thought process should be enclosed using "<thought>" tag. 
2. You SHOULD construct as many of the most important first-order logic constraints and output it in general format. Examples:
    - ∀x (isDog(x) → hasFourLegs(x)
    - ∃x (isCat(x) ∧ isBlack(x))
    - ∀x (isPerson(x) → ∃y (isDog(y) ∧ owns(x, y)))
    - ∀x ∀y ((isParent(x, y) ∧ isMale(x)) → isFather(x, y))
    - ∃x (isHuman(x) ∧ loves(x, Mary)),
    - ∀x (isStudent(x) ∧ studiesHard(x) → getsGoodGrades(x))
    - ∀x (isAnimal(x) → (∃y (isFood(y) ∧ eats(x, y))))
)
3. You SHOULD write a function in a block of Python code to solve the task based on the constraints.
"""

DATA_CONSTRAINT_USER = """
# Task

There are two type of events [A] and [B].

[A] {api_url1} has these parameters:

{api_param_desc1}

[A] {api_url1} has these response:

{api_response_desc1}

[B] {api_url2} has these parameters:

{api_param_desc2}

[B] {api_url2} has these response:

{api_response_desc2}

Your task is to find out the relationship between the parameters and responses of [A] and [B] by analyzing the logs:

The log pairs are:

{log_pairs}

Based on the logs, infer the possible relationships of attributes in [A] and [B] by referencing these common types of relationships:
1. Foreign key: an attribute in an entity that references the primary key attribute in another entity, both attributes must be the same data type.
2. Primary key: attribute(s) that can uniquely identify entities in an entity set.
3. Matching: an attribute(E.g: Price, ID) in an entity that must have the same value as an attribute in another entity, both attributes must be the same data type. (E.g: Price, ID)

Then, write a function that determines if instances of [A] and [B] are related to each other using their attributes.

# Guidelines
- You MUST treat an object instance as a dict in your function. 
- You MUST use the function signature `def check(log_a: dict, log_b: dict) -> bool`.
- You SHOULD return True if all checks passed, otherwise, raise Error on the specific violation with a detailed message (NO NOT use any return (E.g return False / return instance_A['authorization'] == instance_B['authorization']) here !! Directly Raise error).
- The log dict will contain arguments fields with the name "argument.field_name" and their values, and response fields with the name "response.field_name" and their values.
- You MUST access the field value using the key "argument.field_name" and "response.field_name".
- YOU DONT HAVE TO use all attributes of [A] and [B] in your function for matching. If an attribute is not helpful for matching, you can OMIT it.
- DO NOT output any code that is not related to the function, such as test cases.

"""

DATA_CONSTRAINT_FEEDBACK = """
Your code failed test cases. There should be AT LEAST ONE MATCH. Please try to correct the wrong condition or RELAX THE CONSTRAINTS to match instances from [A] to [B], and try again.
- You SHOULD return True if all checks passed, otherwise, raise Error on the specific violation with a detailed message (NO NOT use any return (E.g return False / return instance_A['authorization'] == instance_B['authorization']) here !! Directly Raise error). 

{reasons}
"""

####################################
# Flow constraint
####################################

FLOW_CONSTRAINT_SYSTEM = """
# Identity
You are a software engineer that is extremely good at flow control and logic.
At each turn, you should first provide your step-by-step thinking for solving the task. Your thought process should be enclosed using "<thought>" tag.
After that, you should use write a function in a block of Python code to solve the task.
"""

FLOW_CONSTRAINT_USER = """
# Task
After calling {parent_url}, the client can either call branch [A] or [B]:

[A] {child_url1}, which produces these logs:

{logs1}

[B] {child_url2}, which produces these logs:

{logs2}

Based on the logs produced by branches [A] and [B], 
Identify the variable name(s) that influence branching (e.g., Keyword: [role, userId])
Then, by using the variable name(s), construct the most important first-order logic constraints that causes the program to switch from branch [A] to [B] and output it in general format:
- ∀x (isDog(x) → hasFourLegs(x))
- ∃x (isCat(x) ∧ isBlack(x))
- ∀x (isPerson(x) → ∃y (isDog(y) ∧ owns(x, y)))
- ∀x ∀y ((isParent(x, y) ∧ isMale(x)) → isFather(x, y))
- ∃x (isHuman(x) ∧ loves(x, Mary))
- ∀x (isStudent(x) ∧ studiesHard(x) → getsGoodGrades(x))
- ∀x (isAnimal(x) → (∃y (isFood(y) ∧ eats(x, y))))

Then, by using the first-order logic constraint(s), write a function that determines which branch a log belongs to.

# Guidelines
- You MUST use the function signature `def is_branch_a(log: str) -> bool` for [{parent_url}->{child_url1}] and `def is_branch_b(log: str) -> bool` for [{parent_url}->{child_url2}].
- You SHOULD return True if ALL checks passed, otherwise, raise Error on the specific violation including the correct child_url and the trigger parameter(E.g: raise ValueError(f"should belong to /api/v1/url due to parameter condition")).
- DO NOT output any code that is not related to the function, such as test cases.
"""

FLOW_CONSTRAINT_FEEDBACK = """
Your code failed {fails} test cases. Please try again.

{reasons}
"""

####################################
# Commonsense constraint
####################################

COMMONSENSE_CONSTRAINT_SYSTEM = """
# Identity
You are a software engineer that is extremely good at understanding business logic and user requirements.
You are responsible for writing functions to validate the correctness and usefulness of input data.
At each turn, you should first provide your step-by-step thinking for solving the task. Your thought process should be enclosed using "<thought>" tag.
After that, you should use write a function in a block of Python code to solve the task.
"""

COMMONSENSE_CONSTRAINT_USER = """
# Task
This is a class with multiple fields:

{api_param_desc}

Instances of this class can be found in these logs:

{logs}

Based on the logs, infer the valid values for each field by referencing these common types of data validation:
1. Data Type Check: can the string value be converted to a correct data type? (e.g., "0.0" -> float 0.0)
2. Code Check: does the value fall within a valid list of values? (e.g., postal codes, country codes, NAICS industry codes)
3. Range Check: does the value fall within a logical numerical range? (e.g., temperature, latitude, price).
4. Format Check: does the value follow a predefined format? (e.g., UUID, email).
5. Consistency Check: are two or more values logically consistent with each other? (e.g., delivery date must be after shipping date).
6. Presence Check: an important field shouldn't be left blank (e.g., userID).
7. Vulunerablity Check: Ensure strings are not vulnerable to injection type attacks (XSS, log4j, SQL injection, etc.).

Then, write a function that determines if a value is valid for all the fields.

# Guidelines
- You CAN use existing formats like UUIDs and ISO standards.
- You CAN use Python regex library by importing it.
- You MUST treat an object instance as a dict in your function. 
- The log dict will contain arguments fields with the name "argument.field_name" and their values.
- You MUST access the field value using the key "argument.field_name".
- You MUST use the function signature `def check(log: dict) -> bool`.
- You SHOULD return True if all checks passed, otherwise, raise Error on the specific violation with a detailed message.
- DO NOT output any code that is not related to the function, such as test cases.
"""

COMMONSENSE_CONSTRAINT_FEEDBACK = """
Your code failed test cases. Please delete the condition which causes the error. 

{reasons}
"""


COMMONSENSE_CONSTRAINT_SYSTEM_FORMAT_JSON = """
# Identity
You are a software engineer that is extremely good at understanding business logic and user requirements.
You are responsible for writing functions to validate the correctness and usefulness of input data.
At each turn, you should first provide your step-by-step thinking for solving the task. Your thought process should be enclosed using "<thought>" tag.
After that, you should use write a function in a block of Python code to solve the task.

# Task

The task is to write a function that validates the correctness and usefulness of input data based on the logs.

Each log will be represented as a dictionary with fields and values.
"arguments" field will contain the input data fields and their values.
"env" field will contain the running environment details.
"db_info" field will contain some query results from the database based on some input data.
"related_events" field will contain some related events.

Your task is to write a Python 3 function that validates the correctness and usefulness of the input data based on the logs.

Based on the logs, infer the valid values for each field by referencing these common types of data validation:
1. Data Type Check: can the string value be converted to a correct data type? (e.g., "0.0" -> float 0.0)
2. Code Check: does the value fall within a valid list of values? (e.g., postal codes, country codes, NAICS industry codes)
3. Range Check: does the value fall within a logical numerical range? (e.g., temperature, latitude, price).
4. Format Check: does the value follow a predefined format? (e.g., UUID, email).
5. Consistency Check: are two or more values logically consistent with each other? (e.g., delivery date must be after shipping date).
6. Presence Check: an important field shouldn't be left blank (e.g., userID).
7. Vulunerablity Check: Ensure strings are not vulnerable to injection type attacks (XSS, log4j, SQL injection, etc.).

# Guidelines

- You CAN use existing formats like UUIDs and ISO standards.
- You CAN use Python regex library by importing it.
- You MUST treat an object instance as a dict in your function. 
- You MUST use the function signature `def check(log: dict) -> bool`.
- You SHOULD return True if all checks passed, otherwise, use assert or raise Error on the specific violation with a message.
- DO NOT output any code that is not related to the function, such as test cases.
- You DO NOT need to check information in http headers.

# Example

## Example Input

Here is an API with some parameter descriptions:
ShutdownInfo info

class ShutdownInfo {
    String computerId;
    int timeout;
}


Some instances of this API can be found in these logs:
{
    "arguments": {
        "computerId": "7e675c6f-f749-4432-81fb-0bb1caccb12f",
        "timeout": 10
    },
    "env": {},
    "db_info": {
        "computers": {
            "id": "7e675c6f-f749-4432-81fb-0bb1caccb12f",
            "status": 1,
        }
    },
    "related_events": {
        "api.queryComputers": {
            "arguments": {},
            "response": {
                "computers": [
                    {
                        "id": "7e675c6f-f749-4432-81fb-0bb1caccb12f",
                        "status": 1,
                    },
                    {
                        "id": "b534f052-011f-4431-8f23-66b8fb2fd38e",
                        "status": 0,
                    }
                ]
            }
        }
    }
}

## Example Output

<thought>
The computerId field should be a UUID, and the timeout field should be an integer.
The computerId field should be the same as the id field in the db_info.computers object.
The timeout field should be greater than 0.
The computerId field should be one of the id fields in the related_events.api.queryComputers.response.computers object.
</thought>

```python
import re

def check(log: dict) -> bool:
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    assert uuid_re.match(log["arguments"]["computerId"]), "computerId should be a UUID"
    assert isinstance(log["arguments"]["timeout"], int), "timeout should be an integer"
    assert log["arguments"]["timeout"] > 0, "timeout should be greater than 0"
    assert log["arguments"]["computerId"] == log["db_info"]["computers"]["id"], "computerId should be the same as the id field in the db_info.computers object"

    candidate_computer_ids = [computer["id"] for computer in log["related_events"]["api.queryComputers"]["response"]["computers"]]
    assert log["arguments"]["computerId"] in candidate_computer_ids, "computerId should be one of the id fields in the related_events.api.queryComputers.response.computers object"

    return True
"""


COMMONSENSE_CONSTRAINT_SYSTEM_FORMAT_JSON_SCHEMA_ONLY = """
# Identity
You are a software engineer that is extremely good at understanding business logic and user requirements.
You are responsible for writing functions to validate the correctness and usefulness of input data.
At each turn, you should first provide your step-by-step thinking for solving the task. Your thought process should be enclosed using "<thought>" tag.
After that, you should use write a function in a block of Python code to solve the task.

# Task

The task is to write a function that validates the correctness and usefulness of input data based on the logs.
The function should help to identify adversary that tamper with the input data to perform illegal action or pretend as other users to disrupt the system.

Each log will be represented as a dictionary with fields and values.
"arguments" field will contain the input data fields and their values.
"env" field will contain the running environment details.
"db_info" field will contain some query results from the database based on some input data.
"related_events" field will contain some related events.

Your task is to write a Python 3 function that validates the correctness and usefulness of the input data based on the logs.

Based on the logs, infer the valid values for each field by referencing these common types of data validation:
1. Format Check: does the value follow a predefined format? (e.g., UUID, email).
2. Vulunerablity Check: Ensure strings are not vulnerable to injection type attacks (XSS, log4j, SQL injection, etc.). Please focus on detecting SQL injection vulnerabilities, where "SELECT", "UPDATE", "DELETE", "INSERT" should not be present in the strings.
3. Range Check: does the value fall within a logical numerical range? (e.g., temperature, latitude, price).
4. Argument and Database Status check: Check if status/type fields in 'arguments' and 'db_info' are constantly equal to a specific value (e.g., db_info.order.status need to be equal to 1 to perform certain action).
5. Database integrity Check: Check if certain fields in "arguments" is consistent with the related field in "db_info". (e.g., arguments.accountId should be the same as the id field in the db_info.user object).
6. Environment integrity check: Check if certain fields in "arguments" is consistent with the related field in "env". (e.g., arguments.accountId should match env.userId).
7. Related events check: Check if certain fields in "arguments" is consistent with the related field in "related_events".(e.g., arguments.accountId should be one of the id fields in the related_events.api.queryUser.response.users object).
8. Environment Database integrity check: are two or more values between "db_info" and "env" logically consistent with each other? (e.g., db_info.order.userId should be the same as env.userId).



# Guidelines

- You CAN use existing formats like UUIDs and ISO standards.
- You CAN use Python regex library by importing it.
- You MUST treat an object instance as a dict in your function. 
- You MUST use the function signature `def check(log: dict) -> bool`.
- You SHOULD return True if all checks passed, otherwise, use assert or raise Error on the specific violation with a message.
- DO NOT output any code that is not related to the function, such as test cases.
- You DO NOT need to check information in http headers.
- You SHOULD NOT use current date time in your code, because the events should be time-independent.
- For Argument and Database Status check, set the constraint value(s) as the value(s) in comment start with "Past status feedback:" & from the current error feedback and markdown the status value that appeared in the error feedback in the Code Comment (start with "Past status Feedback:") next to the status constraint  (if the comment with "Past status feedback:" is not present, start markdown the value from feedback with this comment and remove other comment next status check; otherwise, append the status value to the existing comment).
- Always set a strict Argument and Database Status check constraint at the first trial if status fields are present in 'arguments' or 'db_info' and markdown the status in comment start with "Initial status guess:". (it is initial when there is no error feedback)
- For Database integrity check, environment integrity check and related events check, please pay attention to data fields with similar names or meanings.
- For Database integrity check and environment integrity check, please generate as more constraints as possible.
- For Related events check, only generate constraints for the most relevant events.

# Example

## Example Input

Here is an API with some parameter descriptions:
ShutdownInfo info

class ShutdownInfo {
    String computerId;
    int timeout;
}


Some schema of this API can be found in these logs:
{
    "arguments": {
        "computerId": <some str value>,
        "timeout": <some int value>,
        "comment": <some str value>
    },
    "env": {},
    "db_info": {
        "computers": {
            "id": <some str value>,
            "status": <some int value>,
        }
    },
    "related_events": {
        "api.queryComputers": {
            "arguments": {},
            "response": {
                "computers": [
                    {
                        "id": <some str value>,
                        "status": <some int value>,
                    },
                    <extra list items . . .>
                ]
            }
        }
    }
}

Some instances of this API can be found in these logs:
{
    "arguments": {
        "computerId": "7e675c6f-f749-4432-81fb-0bb1caccb12f",
        "timeout": 10,
        "comment": "shutdown target computer."
    },
    "env": {},
    "db_info": {
        "computers": {
            "id": "7e675c6f-f749-4432-81fb-0bb1caccb12f",
            "status": 1,
        }
    },
    "related_events": {
        "api.queryComputers": {
            "arguments": {},
            "response": {
                "computers": [
                    {
                        "id": "7e675c6f-f749-4432-81fb-0bb1caccb12f",
                        "status": 1,
                    },
                    {
                        "id": "b534f052-011f-4431-8f23-66b8fb2fd38e",
                        "status": 0,
                    }
                ]
            }
        }
    }
}


## Example Output

<thought>
The computerId field should be a UUID, and the timeout field should be an integer.
The computerId field should be the same as the id field in the db_info.computers object.
The timeout field should be greater than 0.
The computerId field should be one of the id fields in the related_events.api.queryComputers.response.computers object.
The comment field should not contain any SQL injection features.
</thought>

```python
import re

SQL_INJECTION_RE = re.compile(r"(SELECT|UPDATE|DELETE|INSERT)")

def check(log: dict) -> bool:
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    assert uuid_re.match(log["arguments"]["computerId"]), "computerId should be a UUID"
    assert isinstance(log["arguments"]["timeout"], int), "timeout should be an integer"
    assert log["arguments"]["timeout"] > 0, "timeout should be greater than 0"
    assert log["arguments"]["computerId"] == log["db_info"]["computers"]["id"], "computerId should be the same as the id field in the db_info.computers object"

    candidate_computer_ids = [computer["id"] for computer in log["related_events"]["api.queryComputers"]["response"]["computers"]]
    assert log["arguments"]["computerId"] in candidate_computer_ids, "computerId should be one of the id fields in the related_events.api.queryComputers.response.computers object"
    assert not SQL_INJECTION_RE.search(log["arguments"]["comment"]), "comment should not contain any SQL injection features"

    return True
"""


COMMONSENSE_CONSTRAINT_USER_FORMAT_JSON = """
# Your Task

Here is an API with some parameter descriptions:
{api_param_desc}

Some instances of this API can be found in these logs:
{logs}

**Go over each check in your thought process and write the corresponding code.**
"""

COMMONSENSE_CONSTRAINT_FEEDBACK_FORMAT_JSON = """
Your code failed test cases.

Please modify your code according to the feedback.
Please modify the condition which causes the error before deleting the condition.

{reasons}
"""
