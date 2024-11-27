import sqlite3

def calculate_per_minute_stats(database, table_name, new_table_name):
    """Calculate per-minute stats for relevant columns and create a new table."""
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()

        # Define relevant columns to calculate per-minute stats
        relevant_columns = [
            "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", 
            "OREB", "DREB", "REB", "AST", "STL", "BLK", 
            "TOV", "PF", "PTS", "PLUS_MINUS", "FANTASY_PTS"
        ]

        # Check if MIN column exists and is of the correct type
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = {col[1]: col[2] for col in cursor.fetchall()}
        if "MIN" not in schema or schema["MIN"] not in ("REAL", "INTEGER"):
            print("Error: 'MIN' column is missing or not a numeric type.")
            return

        # Create the new table schema
        cursor.execute(f"PRAGMA table_info({table_name});")
        original_schema = cursor.fetchall()
        create_table_query = f"CREATE TABLE IF NOT EXISTS {new_table_name} ("
        for col in original_schema:
            create_table_query += f"{col[1]} {col[2]}, "
        for col in relevant_columns:
            create_table_query += f"{col}_PER_MIN REAL, "
        create_table_query = create_table_query.rstrip(", ") + ");"
        cursor.execute(create_table_query)

        # Fetch data from the original table
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Process each row and calculate per-minute stats
        for row in rows:
            row_dict = dict(zip(column_names, row))
            if row_dict["MIN"] and row_dict["MIN"] > 0:  # Avoid division by zero
                for col in relevant_columns:
                    row_dict[f"{col}_PER_MIN"] = row_dict[col] / row_dict["MIN"] if row_dict[col] is not None else None
            else:
                for col in relevant_columns:
                    row_dict[f"{col}_PER_MIN"] = None

            # Insert the data into the new table
            placeholders = ", ".join(["?" for _ in row_dict])
            insert_query = f"INSERT INTO {new_table_name} ({', '.join(row_dict.keys())}) VALUES ({placeholders});"
            cursor.execute(insert_query, tuple(row_dict.values()))

        conn.commit()
        print(f"New table '{new_table_name}' with per-minute stats created successfully.")

    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    database_path = "nba_game_logs.db"  # Change to your database path
    original_table_name = "game_logs"  # Change to your table name if different
    new_table_name = "game_logs_per_minute"  # Name for the new table

    calculate_per_minute_stats(database_path, original_table_name, new_table_name)

