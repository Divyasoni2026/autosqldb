import boto3
import csv
import sqlite3

s3 = boto3.client("s3")

def lambda_handler(event, context):

    try:
        # 1️⃣ Get S3 bucket and file details from event
        bucket = "csvautodata"
        key = "uploadeddata/data.csv"

        print("Bucket:", bucket)
        print("File:", key)

        # local_csv = "input.csv"
        # local_db = "students.db"

        local_csv = "/tmp/input.csv"
        local_db = "/tmp/students.db"

        # 2️⃣ Download CSV from S3
        s3.download_file(bucket, key, local_csv)
        print("CSV downloaded successfully")

        # 3️⃣ Create SQLite database
        conn = sqlite3.connect(local_db)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS students (
                id INTEGER,
                name TEXT,
                age INTEGER,
                city TEXT,
                marks INTEGER
            )
        """)

        # 4️⃣ Read CSV and insert into table
        with open(local_csv, "r") as file:
            reader = csv.DictReader(file)

            for row in reader:
                cursor.execute("""
                    INSERT INTO students (id, name, age, city, marks)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    int(row["id"]),
                    row["name"],
                    int(row["age"]) if row["age"] else None,
                    row["city"],
                    int(row["marks"])
                ))

        conn.commit()
        conn.close()

        print("Database created successfully")

        # 5️⃣ Upload database back to S3
        s3.upload_file(local_db, bucket, "students.db")
        print("Database uploaded to S3")

        return {
            "statusCode": 200,
            "message": "Database created and uploaded successfully 🚀!!"
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "message": str(e)
        }
