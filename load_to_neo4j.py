import pandas as pd
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"


def load_nodes(driver, users_dataset, jobs_dataset, states_dataset):
    # Create users
    with driver.session() as session:
        with session.begin_transaction() as tx:
            # Remove all user nodes
            tx.run("MATCH (u:User) DETACH DELETE u")

            for _, row in users_dataset.iterrows():
                tx.run("CREATE (u:User {UserID: $UserID, Major: $Major})",
                    UserID=row["UserID"],
                    Major=row["Major"])
                
    # Create jobs
    with driver.session() as session:
        with session.begin_transaction() as tx:
            # Remove all job nodes
            tx.run("MATCH (j:Job) DETACH DELETE j")

            for _, row in jobs_dataset.iterrows():
                tx.run("CREATE (j:Job {JobID: $JobID, Title: $Title})",
                    JobID=row["JobID"],
                    Title=row["Title"])
                
    # Create states
    with driver.session() as session:
        with session.begin_transaction() as tx:
            # Remove all state nodes
            tx.run("MATCH (s:State) DETACH DELETE s")

            for _, row in states_dataset.iterrows():
                tx.run("CREATE (s:State {State: $State, StateID: $StateID})",
                    State=row["State"],
                    StateID=row["StateID"])


def load_relationships(driver, applications_dataset, users_dataset, jobs_dataset):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for index, row in applications_dataset.iterrows():
                tx.run("MATCH (u:User {UserID: $UserID}), (j:Job {JobID: $JobID}) "
                    "CREATE (u)-[:APPLIED_TO {ApplicationDate: $ApplicationDate}]->(j)",
                    UserID=row["UserID"],
                    JobID=row["JobID"],
                    ApplicationDate=row["ApplicationDate"])
                
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for index, row in users_dataset.iterrows():
                tx.run("MATCH (u:User {UserID: $UserID}), (s:State {State: $State}) "
                    "CREATE (u)-[:LIVES_IN]->(s)",
                    UserID=row["UserID"],
                    State=row["State"])

    with driver.session() as session:
        with session.begin_transaction() as tx:
            for index, row in jobs_dataset.iterrows():
                tx.run("MATCH (j:Job {JobID: $JobID}), (s:State {State: $State}) "
                    "CREATE (j)-[:LOCATED_IN]->(s)",
                    JobID=row["JobID"],
                    State=row["State"])


if __name__ == '__main__':
    users_dataset = pd.read_csv("data/users.csv")
    applications_dataset = pd.read_csv("data/applications.csv")
    jobs_dataset = pd.read_csv("data/jobs.csv")
    states_dataset = pd.read_csv("data/states.csv")

    driver = GraphDatabase.driver(URI, auth=("neo4j", "1234"))
                
    load_nodes(driver, users_dataset, jobs_dataset, states_dataset)
    load_relationships(driver, applications_dataset, users_dataset, jobs_dataset)