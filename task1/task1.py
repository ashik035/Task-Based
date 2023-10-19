from collections import defaultdict
import pandas as pd

def find_top_investors(records):
    # Count the number of unique syndicates for each investor and their total investment amount
    investors = defaultdict(lambda: [0, set()])  # {investor_id: [total_amount, set(syndicate_ids)]}

    for record in records:
        investor_id, syndicate_id, transaction_amount, _ = record

        investors[investor_id][0] += transaction_amount
        investors[investor_id][1].add(syndicate_id)

    # Sort investors based on the number of unique syndicates in descending order
    sorted_investors = sorted(investors.items(), key=lambda x: len(x[1][1]), reverse=True)

    # Return the top 5 investors with the highest number of unique syndicates and their total investment amount
    return sorted_investors[:5]

def processData(top_investors):
    # Create a pandas DataFrame from the top investors list
    df = pd.DataFrame(top_investors, columns=["Investor ID", "Investment Details"])

    # Extract total investment amount and number of unique syndicates into separate columns
    df["Total Investment Amount"] = df["Investment Details"].apply(lambda x: x[0])
    df["Number of Unique Syndicates"] = df["Investment Details"].apply(lambda x: len(x[1]))

    # Drop the original "Investment Details" column
    df.drop("Investment Details", axis=1, inplace=True)

    # Sort the DataFrame by Investor ID
    df_sorted = df.sort_values("Investor ID")
    return df_sorted


def main():
    # Example usage
    records = [
        (1, 100, 5000, '2021-01-01'),
        (2, 100, 3000, '2021-02-01'),
        (1, 200, 7000, '2021-03-01'),
        (6, 300, 2000, '2021-04-01'),
        (2, 400, 4000, '2021-05-01'),
        (1, 300, 1000, '2021-06-01'),
        (3, 200, 6000, '2021-07-01'),
        (6, 500, 9000, '2021-08-01'),
        (2, 300, 1500, '2021-09-01'),
        (4, 200, 3000, '2021-10-01'),
        (1, 400, 2500, '2021-11-01'),
        (5, 280, 2600, '2021-11-01'),
        (5, 250, 3500, '2021-11-01'),
        (6, 250, 3500, '2021-11-01'),
        (5, 250, 4500, '2021-11-01'),
    ]

    # empty checking
    if not records:
        print("No records found.")
        return

    top_investors = find_top_investors(records)

    # Processing data to show by a dataframe
    df = processData(top_investors)
    print(df)


if __name__ == '__main__':
    main()