import pandas as pd
import requests
import json
import logger

def main():

    with open("/usr/local/airflow/output/output.csv", "a", encoding="utf-8") as f:

        if f.tell() == 0:
            f.write("Gender,FirstName,LastName,Location,Email,Username,DoB,Age,Phone,Nationality\n")

        for _ in range(100):
            person = requests.get("https://randomuser.me/api").text
            data = json.loads(person)
            results = data["results"]

            try:
                for result in results:
                    f.write(f"{result["gender"]},"
                            f"{result["name"]["first"]},"
                            f"{result["name"]["last"]},"
                            f"{result["location"]["city"]},"
                            f"{result["email"]},"
                            f"{result["login"]["username"]},"
                            f"{result["dob"]["date"]},"
                            f"{result["dob"]["age"]},"
                            f"{result["phone"]},"
                            f"{result["nat"]}\n"
                        )
                    print("User data successfuly loaded.")
                    logger.logger.info("User data successfuly loaded.")
            except Exception as e:
                print(f"Error: {e}\n {result}\n")
            
    f.close()


