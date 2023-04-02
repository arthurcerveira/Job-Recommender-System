import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm


URI = "bolt://localhost:7687"


def load_nodes(driver, users_dataset, jobs_dataset, states_dataset):
    print("Creating users...")

    with driver.session() as session:
        with session.begin_transaction() as tx:
            # Remove all user nodes
            tx.run("MATCH (u:User) DETACH DELETE u")

            for _, row in tqdm(users_dataset.iterrows(), total=len(users_dataset)):
                tx.run("CREATE (u:User {UserID: $UserID, Major: $Major})",
                    UserID=row["UserID"],
                    Major=row["Major"])
                
    print("\nCreating jobs...")

    with driver.session() as session:
        with session.begin_transaction() as tx:
            # Remove all job nodes
            tx.run("MATCH (j:Job) DETACH DELETE j")

            for _, row in tqdm(jobs_dataset.iterrows(), total=len(jobs_dataset)):
                tx.run("CREATE (j:Job {JobID: $JobID, Title: $Title})",
                    JobID=row["JobID"],
                    Title=row["Title"])
                
    print("\nCreating states...")

    with driver.session() as session:
        with session.begin_transaction() as tx:
            # Remove all state nodes
            tx.run("MATCH (s:State) DETACH DELETE s")

            for _, row in tqdm(states_dataset.iterrows(), total=len(states_dataset)):
                tx.run("CREATE (s:State {State: $State, StateID: $StateID})",
                    State=row["State"],
                    StateID=row["StateID"])


def load_relationships(driver, applications_dataset, users_dataset, jobs_dataset):
    print("\nCreating relationships...")

    with driver.session() as session:
        with session.begin_transaction() as tx:
            for _, row in tqdm(applications_dataset.iterrows(), total=len(applications_dataset)):
                tx.run("MATCH (u:User {UserID: $UserID}), (j:Job {JobID: $JobID}) "
                    "CREATE (u)-[:APPLIED_TO {ApplicationDate: $ApplicationDate}]->(j)",
                    UserID=row["UserID"],
                    JobID=row["JobID"],
                    ApplicationDate=row["ApplicationDate"])
                
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for _, row in tqdm(users_dataset.iterrows(), total=len(users_dataset)):
                tx.run("MATCH (u:User {UserID: $UserID}), (s:State {State: $State}) "
                    "CREATE (u)-[:LIVES_IN]->(s)",
                    UserID=row["UserID"],
                    State=row["State"])

    with driver.session() as session:
        with session.begin_transaction() as tx:
            for _, row in tqdm(jobs_dataset.iterrows(), total=len(jobs_dataset)):
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