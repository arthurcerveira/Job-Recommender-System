import pandas as pd


def get_jobs_dataset(n=1000):
    jobs_window_1 = pd.read_csv('raw-data/splitjobs/jobs1.tsv', 
                            sep='\t',
                            on_bad_lines='skip',
                            low_memory=False)

    jobs_dataset = jobs_window_1.sample(n)

    return jobs_dataset[["JobID", "Title", "State"]]


def get_applications_dataset(jobs_dataset):
    applications = pd.read_csv('raw-data/apps.tsv', sep='\t')

    unique_jobs = jobs_dataset["JobID"].unique()

    applications_dataset = applications.loc[applications["JobID"].isin(unique_jobs)]

    return applications_dataset[["UserID", "JobID", "ApplicationDate"]]


def get_users_dataset(applications_dataset):
    users = pd.read_csv('raw-data/users.tsv', sep='\t')

    users_applications = applications_dataset["UserID"].unique()

    users_dataset = users.loc[users["UserID"].isin(users_applications)]

    return users_dataset[["UserID", "State", "Major"]]


def get_states_dataset(users_dataset, jobs_dataset):
    unique_states = set(users_dataset["State"].dropna().unique())
    unique_states = unique_states.union(
        set(jobs_dataset["State"].dropna().unique())
    )

    states_dataset = pd.DataFrame({
        "State": list(unique_states),
        "StateID": range(len(unique_states))
    })

    states_dataset = states_dataset.loc[
        states_dataset["State"].str.len() == 2
    ]

    return states_dataset[["State", "StateID"]]


if __name__ == '__main__':
    jobs_dataset = get_jobs_dataset(n=1000)
    applications_dataset = get_applications_dataset(jobs_dataset)
    users_dataset = get_users_dataset(applications_dataset)
    states_dataset = get_states_dataset(users_dataset, jobs_dataset)

    jobs_dataset.to_csv("data/jobs.csv", index=False)
    applications_dataset.to_csv("data/applications.csv", index=False)
    users_dataset.to_csv("data/users.csv", index=False)
    states_dataset.to_csv("data/states.csv", index=False)
