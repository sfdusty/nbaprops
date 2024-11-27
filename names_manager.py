import sqlite3
from fuzzywuzzy import fuzz, process
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("name_management.log"),  # Log to a file
        logging.StreamHandler()                      # Log to console
    ]
)

def fetch_all_tables(database):
    """Fetch all table names in a SQLite database."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables

def fetch_unique_names_from_tables(database, tables, column):
    """Fetch unique names from a specified column across multiple tables."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    unique_names = set()
    for table in tables:
        try:
            query = f"SELECT DISTINCT {column} FROM {table};"
            cursor.execute(query)
            names = [row[0] for row in cursor.fetchall()]
            unique_names.update(names)
        except sqlite3.Error as e:
            logging.warning(f"Skipped table {table} due to error: {e}")
    conn.close()
    return unique_names

def fetch_game_logs_names(database, table, primary_column, alternate_column):
    """Fetch unique names from the PLAYER_NAME and AlternateName columns."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    query = f"SELECT DISTINCT {primary_column}, {alternate_column} FROM {table};"
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    # Combine names from both columns
    names = set()
    for primary, alternate in rows:
        names.add(primary)
        if alternate:
            names.add(alternate)
    return names

def update_alternate_name(database, table, primary_column, alternate_column, primary_name, alternate_name):
    """Update the alternate name for a specific primary name."""
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()

        query = f"UPDATE {table} SET {alternate_column} = ? WHERE {primary_column} = ?;"
        cursor.execute(query, (alternate_name, primary_name))
        conn.commit()
        logging.info(f"Updated AlternateName: {alternate_name} for PLAYER_NAME: {primary_name}.")
    except sqlite3.Error as e:
        logging.error(f"SQLite error occurred while updating alternate name: {e}")
    finally:
        conn.close()

def verify_names(props_names, game_logs_names):
    """Verify props names against game logs names."""
    unmatched_names = []
    for name in props_names:
        best_match, score = process.extractOne(name, game_logs_names, scorer=fuzz.ratio)
        if score < 100:
            unmatched_names.append((name, best_match, score))
    return unmatched_names

def main():
    # Database and table configurations
    props_db = "nba_props.db"
    game_logs_db = "nba_game_logs.db"
    props_column = "Player"
    game_logs_table = "game_logs"
    game_logs_column = "PLAYER_NAME"
    alternate_column = "AlternateName"

    try:
        # Fetch all tables in props database
        props_tables = fetch_all_tables(props_db)
        logging.info(f"Found {len(props_tables)} tables in {props_db}.")

        # Fetch unique names from props database
        props_names = fetch_unique_names_from_tables(props_db, props_tables, props_column)
        logging.info(f"Fetched {len(props_names)} unique names from {props_db}.")

        # Fetch unique names from game logs database
        game_logs_names = fetch_game_logs_names(game_logs_db, game_logs_table, game_logs_column, alternate_column)
        logging.info(f"Fetched {len(game_logs_names)} unique names from {game_logs_table} in {game_logs_db}.")

        # Verify names
        unmatched = verify_names(props_names, game_logs_names)
        if not unmatched:
            logging.info("All names in nba_props.db match a name in nba_game_logs.db.")
        else:
            logging.info("Unmatched names:")
            for name, best_match, score in unmatched:
                logging.info(f"Name: {name}, Closest Match: {best_match}, Similarity: {score}%")

            # Prompt user to resolve mismatches
            for name, best_match, score in unmatched:
                print(f"\nPotential mismatch: Name: {name}, Closest Match: {best_match}, Similarity: {score}%")
                user_input = input(f"Is '{name}' the same as '{best_match}'? (y/n): ").strip().lower()
                if user_input == 'y':
                    update_alternate_name(game_logs_db, game_logs_table, game_logs_column, alternate_column, best_match, name)
                else:
                    print(f"Skipping update for '{name}'. You can manually resolve this later.")

        # Summary
        logging.info(f"Total unmatched names: {len(unmatched)}")

    except sqlite3.Error as e:
        logging.error(f"SQLite error occurred: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()

