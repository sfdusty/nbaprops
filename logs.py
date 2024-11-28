# logs.py

import requests
import pandas as pd
import sqlite3
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple

# ------------------------ Configuration ------------------------

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
# NOTE: Replace 'YOUR_API_KEY_HERE' with your actual NBA API key if required.
# The NBA Stats API typically doesn't require an API key, but headers are necessary to mimic a browser request.
url = "https://stats.nba.com/stats/leaguegamelog"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/58.0.3029.110 Safari/537.3",
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
    "Season": "2024-25",          # Update the season as needed
    "SeasonType": "Regular Season",
    "Sorter": "DATE",
}

# Database name
DATABASE_NAME = "nba.db"

# ------------------------ Database Initialization ------------------------

def initialize_database(db_name: str) -> None:
    """
    Initialize the SQLite database with 'players' and 'game_logs' tables.
    Creates the tables if they do not already exist.
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Create 'players' table
        create_players_table = """
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY,
            primary_name TEXT UNIQUE NOT NULL,
            alternate_names TEXT,
            position TEXT,
            current_team TEXT
        );
        """
        cursor.execute(create_players_table)

        # Create 'game_logs' table
        create_game_logs_table = """
        CREATE TABLE IF NOT EXISTS game_logs (
            game_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            game_id TEXT NOT NULL,
            game_date TEXT,
            team_id INTEGER,
            team_abbreviation TEXT,
            minutes_played INTEGER,
            fgm INTEGER,
            fga INTEGER,
            fg_pct REAL,
            fg3m INTEGER,
            fg3a INTEGER,
            fg3_pct REAL,
            ftm INTEGER,
            fta INTEGER,
            ft_pct REAL,
            oreb INTEGER,
            dreb INTEGER,
            reb INTEGER,
            ast INTEGER,
            stl INTEGER,
            blk INTEGER,
            tov INTEGER,
            pf INTEGER,
            pts INTEGER,
            plus_minus INTEGER,
            fantasy_pts REAL,
            video_available INTEGER,
            FOREIGN KEY (player_id) REFERENCES players(player_id),
            UNIQUE (player_id, game_id)
        );
        """
        cursor.execute(create_game_logs_table)

        # Commit changes
        conn.commit()
        logging.info(f"Database '{db_name}' initialized with 'players' and 'game_logs' tables.")
    except sqlite3.Error as e:
        logging.error(f"Error initializing database '{db_name}': {e}")
    finally:
        conn.close()

# ------------------------ API Fetching Functions ------------------------

def fetch_game_logs(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch game logs from the NBA Stats API based on provided parameters.
    """
    try:
        logging.info("Sending request to NBA Stats API...")
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()  # Raise an error if the request fails
        logging.info("Request successful. Response received.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return {}

# ------------------------ Data Parsing Functions ------------------------

def parse_players(game_logs: List[Dict[str, Any]]) -> List[Tuple[int, str, str, str, str]]:
    """
    Extract unique player information from game logs.
    Returns a list of tuples containing player_id, primary_name, alternate_names, position, current_team.
    """
    players = {}
    for log in game_logs:
        player_id = log.get('PLAYER_ID')
        primary_name = log.get('PLAYER_NAME')
        alternate_names = ""  # NBA Stats API might not provide alternate names
        position = log.get('POSITION', "")
        current_team = log.get('TEAM_ABBREVIATION', "")

        if player_id and primary_name:
            if player_id not in players:
                players[player_id] = (player_id, primary_name, alternate_names, position, current_team)
            else:
                # Optionally, update player info if necessary
                pass

    return list(players.values())

def parse_game_log_data(game_logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract game log statistics from game logs.
    Returns a list of dictionaries ready for database insertion.
    """
    parsed_logs = []
    for log in game_logs:
        parsed_log = {
            'player_id': log.get('PLAYER_ID'),
            'game_id': log.get('GAME_ID'),
            'game_date': log.get('GAME_DATE'),
            'team_id': log.get('TEAM_ID'),
            'team_abbreviation': log.get('TEAM_ABBREVIATION'),
            'minutes_played': log.get('MIN'),
            'fgm': log.get('FGM'),
            'fga': log.get('FGA'),
            'fg_pct': log.get('FG_PCT'),
            'fg3m': log.get('FG3M'),
            'fg3a': log.get('FG3A'),
            'fg3_pct': log.get('FG3_PCT'),
            'ftm': log.get('FTM'),
            'fta': log.get('FTA'),
            'ft_pct': log.get('FT_PCT'),
            'oreb': log.get('OREB'),
            'dreb': log.get('DREB'),
            'reb': log.get('REB'),
            'ast': log.get('AST'),
            'stl': log.get('STL'),
            'blk': log.get('BLK'),
            'tov': log.get('TOV'),
            'pf': log.get('PF'),
            'pts': log.get('PTS'),
            'plus_minus': log.get('PLUS_MINUS'),
            'fantasy_pts': log.get('FANTASY_PTS'),
            'video_available': log.get('VIDEO_AVAILABLE'),
        }
        parsed_logs.append(parsed_log)
    return parsed_logs

# ------------------------ Database Operations ------------------------

def upsert_player(conn: sqlite3.Connection, player_data: Tuple[int, str, str, str, str]) -> None:
    """
    Insert a new player or update an existing player's information.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO players (player_id, primary_name, alternate_names, position, current_team)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                primary_name=excluded.primary_name,
                alternate_names=excluded.alternate_names,
                position=excluded.position,
                current_team=excluded.current_team;
        """, player_data)
        conn.commit()
        logging.info(f"Upserted player '{player_data[1]}' (ID: {player_data[0]}).")
    except sqlite3.Error as e:
        logging.error(f"Error upserting player '{player_data[1]}' (ID: {player_data[0]}): {e}")

def upsert_game_log(conn: sqlite3.Connection, game_log_data: Dict[str, Any]) -> None:
    """
    Insert a new game log or update an existing one if it exists.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO game_logs (
                player_id, game_id, game_date, team_id, team_abbreviation,
                minutes_played, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk,
                tov, pf, pts, plus_minus, fantasy_pts, video_available
            ) VALUES (
                :player_id, :game_id, :game_date, :team_id, :team_abbreviation,
                :minutes_played, :fgm, :fga, :fg_pct, :fg3m, :fg3a, :fg3_pct,
                :ftm, :fta, :ft_pct, :oreb, :dreb, :reb, :ast, :stl, :blk,
                :tov, :pf, :pts, :plus_minus, :fantasy_pts, :video_available
            )
            ON CONFLICT(player_id, game_id) DO UPDATE SET
                game_date=excluded.game_date,
                team_id=excluded.team_id,
                team_abbreviation=excluded.team_abbreviation,
                minutes_played=excluded.minutes_played,
                fgm=excluded.fgm,
                fga=excluded.fga,
                fg_pct=excluded.fg_pct,
                fg3m=excluded.fg3m,
                fg3a=excluded.fg3a,
                fg3_pct=excluded.fg3_pct,
                ftm=excluded.ftm,
                fta=excluded.fta,
                ft_pct=excluded.ft_pct,
                oreb=excluded.oreb,
                dreb=excluded.dreb,
                reb=excluded.reb,
                ast=excluded.ast,
                stl=excluded.stl,
                blk=excluded.blk,
                tov=excluded.tov,
                pf=excluded.pf,
                pts=excluded.pts,
                plus_minus=excluded.plus_minus,
                fantasy_pts=excluded.fantasy_pts,
                video_available=excluded.video_available;
        """, game_log_data)
        conn.commit()
        logging.info(f"Upserted game log for player ID {game_log_data['player_id']} in game ID {game_log_data['game_id']}.")
    except sqlite3.Error as e:
        logging.error(f"Error upserting game log for player ID {game_log_data['player_id']} in game ID {game_log_data['game_id']}: {e}")

# ------------------------ Main Execution Flow ------------------------

def main():
    """
    Main function to orchestrate fetching, parsing, and storing game logs.
    """
    # Initialize the database and tables
    initialize_database(DATABASE_NAME)

    # Connect to the database
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        logging.info(f"Connected to database '{DATABASE_NAME}'.")
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database '{DATABASE_NAME}': {e}")
        return

    # Fetch game logs from the API
    response_json = fetch_game_logs(params)

    # Check if response contains data
    if not response_json:
        logging.warning("No data received from API. Exiting.")
        conn.close()
        return

    # Extract headers and rows from the resultSets
    result_sets = response_json.get("resultSets", [])
    if not result_sets:
        logging.warning("No resultSets found in the response. Exiting.")
        conn.close()
        return

    game_log_set = result_sets[0]  # Assuming the first resultSet contains game logs
    headers = game_log_set.get("headers", [])
    rows = game_log_set.get("rowSet", [])

    if not headers or not rows:
        logging.warning("No headers or rows found in the first resultSet. Exiting.")
        conn.close()
        return

    # Convert data to a pandas DataFrame
    df = pd.DataFrame(rows, columns=headers)
    logging.info("Data successfully converted to DataFrame.")
    logging.info(f"DataFrame shape: {df.shape}")

    # Log data types and sample data
    logging.info("Logging column data types and a sample of the data:")
    logging.info(f"Column data types:\n{df.dtypes}")
    logging.info(f"Sample data:\n{df.head()}")

    # Parse player data and game log data
    game_logs = df.to_dict(orient='records')
    players = parse_players(game_logs)
    parsed_game_logs = parse_game_log_data(game_logs)

    logging.info(f"Parsed {len(players)} unique players.")
    logging.info(f"Parsed {len(parsed_game_logs)} game logs.")

    # Upsert players and game logs into the database
    for player in players:
        upsert_player(conn, player)

    for log in parsed_game_logs:
        # Ensure that player_id and game_id exist before inserting
        if log['player_id'] and log['game_id']:
            upsert_game_log(conn, log)
        else:
            logging.warning(f"Incomplete game log data: {log}")

    # Close the database connection
    conn.close()
    logging.info(f"Database '{DATABASE_NAME}' connection closed.")

    logging.info("Script finished.")

if __name__ == "__main__":
    main()

