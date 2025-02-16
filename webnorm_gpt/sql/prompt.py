####################################
# Extract Related entity
####################################

EXTRACT_RELATED_ENTITY_SYSTEM = """
#Task

You will be given a funtion method header. From the function method header, 
you need to select relevant schema(s) from below schema list based on the input parameters of the function.
Please select as complete as possible.
Schema List: {schema_list}
The output will directly pass to ast.literal.eval(). 
Please only output your selected schema(s) in list format, e.g. ['User', 'Order', 'Trip'], and do not include markdown symbols.
if you think none are relevant, please output an empty list.
"""

EXTRACT_RELATED_ENTITY_USER = """
#Input:

Function header: {function_header}

#Output:
"""
####################################
# Extract Foreign Key entity
####################################

EXTRACT_FOREIGN_KEY_ENTITY_SYSTEM = """
#Task 

You will be given a schema. Since these schema are extracted from microservices, the schemas are separated for each services. 
Please help me select from below schemas list to filter out some relevant schema based on the data fields of the given schema(s). 
Relevancy here means that there are foreign key(s) in the given schema referencing other schema.
(Note that some foreign key field may not have exact wording as the schema name, please make reasonable guess)
Please select as complete as possible.
Schema List: {schema_list}
The output will directly pass to ast.literal.eval(). 
Please only output your selected schema(s) in list format, e.g. ['User', 'Order', 'Trip'], and do not include extra symbols.
if you think none are relevant, please output an empty list.
"""

EXTRACT_FOREIGN_KEY_ENTITY_USER = """
#Input:

Given schema: {class_}

Provide your output in list.
#Output:
"""
####################################
# User context
####################################

USER_CONTEXT_SYSTEM = """
#Task 

You are extracting information to build ACL permission defense against Web Tamper attack. 
The attack will tamper input parameters value. 
Given the function header and function-related entity, decide whether it is needed to extract current login user information. 
Only Answer 'Yes/No'
"""

USER_CONTEXT_USER = """
#Input:

Function Header: {function_header}
Related entity: {entities}

#Output:
"""
####################################
# Generate SQL
####################################
GENERATE_SQL_SYSTEM = '''
#Task

You are given related entity schema(s), request function header and current user information (if available). 
Since an adversary may tamper part of the parameters,
your job is to generate SQL query to retrieve relevant field of each parameters such that
we can maintain data consistency between request parameter value , database and current login user information (if available).
(Note that some field in different table may reference the same object even with different field name, please make reasonable guess)
You are only extracting necessary field from related schema to cross check with input parameter and current user information. 
You may not need to use all schema(s) in the related entity schema.
When generating the sql query, note that you are only given information from the input parameters and current user information (if available) for querying. 
When you reference input parameter value in the query, please use {request.parameter_name} to represent the value of the parameter.
if the parameter is a class, you can reference the field of the class by {request.parameter_name.field_name}.
When you reference current user information in the query, please use {current_user.parameter_name} to represent the value of the current user information.
Keep you query as simple as possible.
here is an example:

#example

#Input
Function header: public Response topup(String walletId, String userId, HttpHeaders headers)
related entity schema:
"Wallet":[
["walletId","String",],
["accountId","String",],
["balance","float",]]

"User": [
["userId","String"],
["userName","String"],
["password","String"],
["gender","int"],
["documentType","int"],
["documentNum","String"],
["email","String"]]

current user information: userId, username (this field might be useful to ensure the consistency of parameter value with current login user.)

#Output:
{"wallet_account_id": "SELECT w.accountId FROM Wallet w WHERE w.walletId == {request.walletId}"}

The output will directly pass to ast.literal.eval(). 
Please follow the example and only output in dictionary format and do not use any markdown symbol.
'''

GENERATE_SQL_USER = """
#Input

Function header: {function_header}
related entity schema: {entities}
current user information: {user_context}

#Output:
"""
