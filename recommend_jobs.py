import sys
from pprint import pprint
from py2neo import Graph
import json
import random


URI = "bolt://localhost:7687"
g = Graph(URI, auth=("neo4j", "1234"))

query = """
    MATCH (u1:User)-[:APPLIED_TO]->(j:Job)<-[:APPLIED_TO]-(u2:User)
    WHERE u1.UserID <> u2.UserID
    WITH u1, u2, COUNT(DISTINCT j) as intersection

    MATCH (u:User)-[:APPLIED_TO]->(j:Job)
    WHERE u in [u1, u2]
    WITH u1, u2, intersection, COUNT(DISTINCT j) as union

    WITH u1, u2, intersection, union, (intersection * 1.0 / union) as jaccard_index

    ORDER BY jaccard_index DESC, u2.UserID
    WHERE jaccard_index <> 0
    AND jaccard_index <> 1
    WITH u1, COLLECT([u2.UserID, jaccard_index, intersection, union])[0..$k] as neighbors

    WHERE SIZE(neighbors) > 10   // return users with enough neighbors
    RETURN u1.UserID as user, neighbors
"""

neighbors = {}
for i in g.run(query, k = 100).data():
    neighbors[i["user"]] = i["neighbors"]

# select 10 random neighbors
nearest_neighbors = []

for i in range(20):
    cid = random.choice(list(neighbors.keys()))

    # query all jobs applied to by user 793350
    query = """
        MATCH (u:User)-[a:APPLIED_TO]->(j:Job)
        WHERE u.UserID = $cid
        // RETURN u.UserID as user, COLLECT([j.Title, a.ApplicationDate]) as jobs
        RETURN u.UserID as user, COLLECT([j.Title]) as jobs
    """

    jobs = {}
    for i in g.run(query, cid = cid).data():
        jobs[i["user"]] = i["jobs"]

    print(f"\n\n# jobs applied by user {cid}:")
    pprint(jobs)

    nearest_neighbors = [neighbors[cid][i][0] for i in range(len(neighbors[cid]))]

    query = """
            MATCH (u1:User),
                  (neighbor:User)-[:APPLIED_TO]->(j:Job)    // all jobs applied to by neighbors
            WHERE u1.UserID = $cid
                AND neighbor.UserID in $nearest_neighbors
                AND not (u1)-[:APPLIED_TO]->(j)                    // filter for jobs that our user hasn't applied to

            WITH u1, j, COUNT(DISTINCT neighbor) as countnns // times applied to by nns
            ORDER BY u1.UserID, countnns DESC
            RETURN u1.UserID as user, COLLECT([j.Title, countnns])[0..$n] as recommendations
            """

    recommendations = {}
    for i in g.run(query, cid = cid, nearest_neighbors = nearest_neighbors, n = 5).data():
        recommendations[i["user"]] = i["recommendations"]
        
    print(f"\n# jobs recommended to user {cid}:")
    pprint(recommendations)

# save recommendations to json
with open("recommendations.json", "w") as f:
    json.dump(recommendations, f, indent=4)