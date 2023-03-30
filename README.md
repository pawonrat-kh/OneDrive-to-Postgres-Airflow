# OneDrive-to-Postgres-Airflow
This project is about creating a Data pipeline on Apache Airflow via Docker. In this project is a duplication of files from OneDrive to store into Postgres database.

## About Data pipeline detail

<img src="https://user-images.githubusercontent.com/90255313/228546392-d98c9bb5-088c-4ba2-8e8f-ed3c58a8fbbd.png" width="700">

The process are
1. Using Access token from Graph API for authorization to access files in One Drive.
2. This project focus on .csv file. However, in script I write more detail in case of .xlsx file and add conditions about created date and modified date for specific file.
3. After accessed file in One Drive, I collected .csv file at folder name is 'Transaction' in DataFrame form and converted to sql database form for storing into Postgres by using Pandas.

## Tool and skill usages
1. Apache Airflow
2. Docker
3. Python programming language
4. Graph API
5. Authorization by API Key
