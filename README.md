# d2k_submission

Overview
The d2k_tech_git.py script automates downloading, processing, and analyzing NYC Taxi and Limousine Commission (TLC) trip data. It extracts data, saves it to CSV files, uploads it to a MySQL database, and generates visualizations.

Prerequisites
Install the following:

Python 3.x
Required packages: requests, beautifulsoup4, pandas, sqlalchemy, mysql-connector-python, matplotlib
Install packages using pip:

bash
Copy code
pip install requests beautifulsoup4 pandas sqlalchemy mysql-connector-python matplotlib

Database Configuration:

Update user, password, host, and database with your MySQL credentials in the script.
Run the Script:

bash
Copy code
python d2k_tech_git.py

Output:

CSV files from .parquet files.
Data uploaded to MySQL database.
Plots of total trips per day and fare distribution by passenger count.
Script Steps
Fetch Data:

Sends a GET request to the TLC Trip Record Data page.
Parses the page to find .parquet file links.
Process Data:

Reads .parquet files into DataFrames.
Cleans and transforms the data.
Calculates trip duration and average speed.
Aggregates data by day for total trips and average fare.
Saves processed data to CSV files.
Upload to MySQL:

Connects to MySQL using SQLAlchemy.
Uploads CSV data to database tables.
Executes queries for visualization.
Generates and displays plots.

Author
Sumiran Gupta

