import requests
import pandas as pd
import sqlite3
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_game_log_fetch.log"),  # Log to a file
        logging.StreamHandler()                        # Log to console
    ]
)

logging.info("Script started.")

# Define the API endpoint and headers
url = "https://stats.nba.com/stats/leaguegamelog"
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/stats/",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

# Define query parameters
params = {
    "Counter": "1000",
    "DateFrom": "",
    "DateTo": "",
    "Direction": "DESC",
    "ISTRound": "",
    "LeagueID": "00",
    "PlayerOrTeam": "P",
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "Sorter": "DATE",
}

logging.info("API endpoint and headers defined.")
logging.info("Query parameters set.")

try:
    # Make the API request
    logging.info("Sending request to NBA stats API...")
    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()  # Raise an error if the request fails
    logging.info("Request successful. Response received.")

    # Parse JSON data
    logging.info("Parsing response JSON data...")
    data = response.json()
    logging.info("JSON data parsed successfully.")

    # Extract headers and rows from the resultSets
    logging.info("Extracting headers and rows from the response...")
    result_sets = data.get("resultSets", [])
    if result_sets:
        game_log = result_sets[0]  # Assuming the first resultSet contains game logs
        headers = game_log.get("headers", [])
        rows = game_log.get("rowSet", [])
        logging.info("Headers and rows extracted.")

        # Convert data to a pandas DataFrame
        logging.info("Converting data to a pandas DataFrame...")
        df = pd.DataFrame(rows, columns=headers)
        logging.info("Data successfully converted to DataFrame.")
        logging.info(f"DataFrame shape: {df.shape}")

        # Log data types and sample data
        logging.info("Logging column data types and a sample of the data:")
        logging.info(f"Column data types:\n{df.dtypes}")
        logging.info(f"Sample data:\n{df.head()}")

        # Database connection and storage
        logging.info("Connecting to SQLite database...")
        conn = sqlite3.connect("nba_game_logs.db")
        cursor = conn.cursor()

        # Create the table if it doesn't exist
        logging.info("Creating table 'game_logs' if not exists...")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS game_logs (
            SEASON_ID TEXT,
            PLAYER_ID INTEGER,
            PLAYER_NAME TEXT,
            TEAM_ID INTEGER,
            TEAM_ABBREVIATION TEXT,
            TEAM_NAME TEXT,
            GAME_ID TEXT,
            GAME_DATE TEXT,
            MATCHUP TEXT,
            WL TEXT,
            MIN INTEGER,
            FGM INTEGER,
            FGA INTEGER,
            FG_PCT REAL,
            FG3M INTEGER,
            FG3A INTEGER,
            FG3_PCT REAL,
            FTM INTEGER,
            FTA INTEGER,
            FT_PCT REAL,
            OREB INTEGER,
            DREB INTEGER,
            REB INTEGER,
            AST INTEGER,
            STL INTEGER,
            BLK INTEGER,
            TOV INTEGER,
            PF INTEGER,
            PTS INTEGER,
            PLUS_MINUS INTEGER,
            FANTASY_PTS REAL,
            VIDEO_AVAILABLE INTEGER,
            PRIMARY KEY (GAME_ID, PLAYER_ID)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        logging.info("Table 'game_logs' is ready.")

        # Insert data into the database
        logging.info("Inserting data into the 'game_logs' table...")
        df.to_sql("game_logs", conn, if_exists="append", index=False, method="multi")
        logging.info("Data inserted successfully into 'game_logs'.")

        # Close the database connection
        conn.close()
        logging.info("Database connection closed.")
    else:
        logging.warning("No resultSets found in the response.")
except requests.exceptions.RequestException as e:
    logging.error(f"Request failed: {e}")
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")

logging.info("Script finished.")

