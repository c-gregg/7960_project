#
import os
import argparse
import pandas as pd
import datetime

def process_csv(file_path):
    df = pd.read_csv(file_path)

    # Drop rows where the second element is null - remove bad requests
    df = df.dropna(subset=[df.columns[1]])

    result_df = pd.DataFrame()
    # Iterate over each row and create new rows for every 10 columns
    # for i in range(1, len(df) - 9):
    #     row_data = df.iloc[i:i+10].values.flatten().tolist()
    #     reshaped_data = np.array(row_data).reshape(1, -1)  # Reshape to 1 row
    #     # result_df = pd.concat([result_df, pd.DataFrame(row_data)], ignore_index=True)
    #     result_df = pd.concat([result_df, pd.DataFrame(reshaped_data, columns=result_df.columns)], ignore_index=True)
    for _, row in df.iterrows():
        rlist = row.values.flatten().tolist()[2:]
        for i in range(len(rlist) - 9):
            result_df = pd.concat([result_df, pd.DataFrame(rlist[i: i + 10]).T], ignore_index=True)

    return result_df.dropna()
def main(source_dir_path):

    final_df = pd.DataFrame()
    i = 0
    y_data = [y for y in os.listdir(source_dir_path) if y.endswith(".csv")]
    for filename in y_data:
        i += 1
        result_df = process_csv(os.path.join(source_dir_path, filename))

        if not result_df.empty:
            final_df = pd.concat([final_df, result_df], ignore_index=True)
        print(i)

    final_df.to_csv(f'../data/combined_data_{today}.csv', index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dir_path",
        help="path to a directory with csv files",
        required=True
        )
    args = parser.parse_args()

    today = datetime.date.today().strftime('%Y%m%d')

    main(
        source_dir_path=args.dir_path,
        )