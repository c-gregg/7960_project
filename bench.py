import os
import argparse
import pandas as pd
import datetime
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error


def generate_ols_estimates(path_to_data = 'data/combined_data_20241202.csv'):
    data = pd.read_csv(path_to_data)
    predicted_values = []

    for index, row in data.iterrows():
        row_data = row.values[0:9]

        x = np.arange(len(row_data)).reshape(-1, 1)
        y = row_data  # Row values as y-coordinates

        model = LinearRegression()
        model.fit(x, y)

        x_pred = np.array([[9]])
        y_pred = model.predict(x_pred)

        predicted_values.append(y_pred[0])

    data['estimate'] = predicted_values

    output_file_path = f'data/ols_benchmark{today}.csv'
    data.to_csv(output_file_path, index=False)

    print(f"Updated DataFrame saved to {output_file_path}")
    mse = mean_squared_error(data['9'], data['estimate'])
    return mse

def main(source_data_path):
    mse = generate_ols_estimates(source_data_path) #'data/combined_data_20241202.csv'
    print(mse)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_path",
        help="path to a directory with yahoo data",
        required=True
        )
    args = parser.parse_args()

    today = datetime.date.today().strftime('%Y%m%d')

    main(
        source_data_path=args.data_path,
        )
