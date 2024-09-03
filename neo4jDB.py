from neo4j import GraphDatabase


# Class for the establishment of the Neo4J connection and the execution of queries
class Neo4J_DB:
    """
    A class to manage the connection to a Neo4j database.

    This class encapsulates methods for connecting to a Neo4j database, executing queries,
    and handling the database session.

    Attributes:
        __uri (str): The URI for the Neo4j database.
        __user (str): The username for the Neo4j database.
        __password (str): The password for the Neo4j database.
        __driver: The Neo4j database driver instance.

    Methods:
        query(query, parameters=None, db=None): Executes a given query on the database.
        close(): Closes the database connection.
    """

    # Initialize connection to Neo4j database
    def __init__(self, uri, username, password):
        self.__uri = uri
        self.__username = username
        self.__password = password
        self.__driver = None

        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__username, self.__password))
            print("Neo4j driver was created successfully")
        except Exception as e:
            print(f'Failed to create Neo4j driver, due to {e}')


    # Execute a query against the Neo4j database
    def query(self, query, parameters=None, db="users"):
        """
        This method opens a session, runs the specified query, and then closes the session.
        It returns the results of the query as a list.
        """
        try:
            with self.__driver.session(database=db) as session:
                result = session.run(query, parameters)
                return [record for record in result]
            
        except Exception as e:
            print(f"Query failed: {e}")
            return []


    # Close connection to database
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
            print("Connection to Neo4j database closed.")
