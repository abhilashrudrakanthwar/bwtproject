import snowflake.connector

class SnowflakeConnector:
    def __init__(self, account, user, password, warehouse, database, schema, role):  # Fixed the method name
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Parameters (Ideally should be loaded from environment variables or a config file)
account = "GSSKGAZ-UI91111"
user = "ABHILASHR"
password = "Abhi@1997"  # Removed extra space
database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCH_SF1"      # Removed extra space
warehouse = "COMPUTE_WH"
role = "ACCOUNTADMIN"

# Usage
sf_connector = SnowflakeConnector(
    account=account,
    user=user,
    password=password,
    database=database,
    schema=schema,
    warehouse=warehouse,
    role=role
)

try:
    sf_connector.connect()
    query = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER LIMIT 10"
    result = sf_connector.execute_query(query)
    for row in result:
        print(row)
finally:
    sf_connector.close_connection()
