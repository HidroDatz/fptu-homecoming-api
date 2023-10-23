from flask import Flask, request
from controller import s3_controller
import pandas as pd
import json

# Create a Flask application instance
app = Flask(__name__)


# Define a route and a function to handle it
@app.route("/")
def hello_world():
    return "Hello, World!"


@app.route("/update_data_in_s3", methods=["POST", "OPTIONS"])
def get_data_in_s3():
    bucket_name = "fptu-homecoming-2023"
    excel_file_path = "fptu-homecoming-2023.xlsx"
    is_get_success = s3_controller.get_normal_files_in_bucket(bucket_name)
    if is_get_success:
        df = pd.read_excel(excel_file_path, sheet_name=0)
        data = request.form
        print(data)
        json_string = next(iter(data.keys()))
        json_string = json.loads(json_string)
        relevant_columns = [
            "fullName",
            "phoneNumber",
            "email",
            "batch",
            "studentId",
            "transportation",
            "currentJob",
            "company",
        ]
        new_data = {col: json_string.get(col, "") for col in relevant_columns}

        # # # Concatenate the new data to the DataFrame
        df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)

        # # Write the updated DataFrame back to the Excel file
        df.to_excel(excel_file_path, index=False)
        s3_controller.upload_file_to_s3(bucket_name, excel_file_path)
        return {"message": "Update data success"}


if __name__ == "__main__":
    app.run()
