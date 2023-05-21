import logging

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

class NeoApp:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "CREATE (p1:Person { name: $person1_name }) "
            "CREATE (p2:Person { name: $person2_name }) "
            "CREATE (p1)-[:KNOWS]->(p2) "
            "RETURN p1, p2"
        )
        result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
            return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except Neo4jError as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_person, person_name)
            for record in result:
                print("Found person: {record}".format(record=record))
    
    @staticmethod
    def _find_persons(self, tx):
        query = (
            "MATCH (p:Person) "
            "RETURN p.name AS name"
        )
        result = tx.run(query)
        return [record["name"] for record in result]
    
    def find_people(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_persons)
            return result

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [record["name"] for record in result]

    def create_neo_app(self):
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
        uri = "neo4j+s://fbb1df5b.databases.neo4j.io"
        user = "neo4j"
        password = "<NCJMoRmWFfrQtpkTivqCPLvhu0OiiIhd-9NVUgfUvRA"
        neoapp = NeoApp(uri, user, password)

        return neoapp