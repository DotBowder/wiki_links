# Written by: Daniel Bowder
# Github: https://github.com/DotBowder



from neo4j.v1 import GraphDatabase

class NEO4J_CONNECT(object):

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def Close(self):
        self.driver.close()

    def Setup_Constraints(self):
        with self.driver.session() as session:
            with session.begin_transaction() as transaction:
                result = transaction.run("CREATE CONSTRAINT ON (n:Article) "
                                         "ASSERT n.name is UNIQUE ")

        with self.driver.session() as session:
            with session.begin_transaction() as transaction:
                result = transaction.run("CREATE CONSTRAINT ON (n:Article) "
                                         "ASSERT n.id is UNIQUE ")

    def Create_Nodes(self):
        with self.driver.session() as session:
            with session.begin_transaction() as transaction:
                result = transaction.run("USING PERIODIC COMMIT "
                                         "LOAD CSV FROM 'file:///master_ids.tsv' AS line FIELDTERMINATOR '\t' "
                                         "CREATE (:Article {name: line[0], id: line[1]})")

    def Create_Relationships(self):
        with self.driver.session() as session:
            with session.begin_transaction() as transaction:
                result = transaction.run("USING PERIODIC COMMIT "
                                         "LOAD CSV FROM 'file:///relationships.tsv' AS line FIELDTERMINATOR '\t' "
                                         "MATCH (source:Article {id: line[0] }) "
                                         "MATCH (dest:Article {id: line[1] }) "
                                         "MERGE (source)-[:LINKSTO {strength: line[2]}]->(dest) ")

if __name__ == "__main__":
    neo = NEO4J_CONNECT("bolt://10.1.1.141", "neo4j", "mysillypassword") # Don't use a password like that....
    neo.Setup_Constraints()
    neo.Create_Nodes()
    neo.Create_Relationships()
    neo.Close()
