import os
import sqlite3
import pandas as pd

def list_sqlite_databases(directory):
    """List all SQLite database files in the given directory."""
    return [file for file in os.listdir(directory) if file.endswith(".db")]

def list_tables(database):
    """List all tables in a SQLite database."""
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()

        # Fetch all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        return tables

    except sqlite3.Error as e:
        print(f"SQLite error occurred while fetching tables from {database}: {e}")
        return []
    finally:
        conn.close()

def display_table_schema_and_sample(database, table):
    """Display the schema and a sample of data for a specific table."""
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()

        # Display schema
        print(f"\nSchema for table: {table}")
        cursor.execute(f"PRAGMA table_info({table});")
        schema = cursor.fetchall()
        if not schema:
            print("  (No columns found)")
        else:
            schema_df = pd.DataFrame(schema, columns=["Index", "Column", "Type", "Not Null", "Default Value", "Primary Key"])
            print(schema_df[["Column", "Type", "Not Null", "Primary Key"]])  # Show relevant columns only

        # Display sample data
        print("\nSample data:")
        cursor.execute(f"SELECT * FROM {table} LIMIT 5;")
        rows = cursor.fetchall()
        if not rows:
            print("  (No data found)")
        else:
            # Fetch column names for headers
            column_names = [desc[0] for desc in cursor.description]
            # Use Pandas to display rows cleanly
            df = pd.DataFrame(rows, columns=column_names)
            print(df)

    except sqlite3.Error as e:
        print(f"SQLite error occurred while fetching data from {table}: {e}")
    finally:
        conn.close()

def explore_databases():
    """Display all databases and their tables, then allow table exploration."""
    # Get current directory dynamically
    directory = os.getcwd()

    # List all databases dynamically
    databases = list_sqlite_databases(directory)
    if not databases:
        print("No SQLite databases found.")
        return

    # Display databases and tables in one view
    db_table_map = {}
    print("\nDatabases and their tables:")
    for db_idx, db in enumerate(databases, start=1):
        tables = list_tables(db)
        db_table_map[db_idx] = {"database": db, "tables": tables}
        print(f"\n{db_idx}. Database: {db}")
        if not tables:
            print("  (No tables found)")
        else:
            for tbl_idx, table in enumerate(tables, start=1):
                print(f"    {db_idx}.{tbl_idx} Table: {table}")

    # Prompt user to select a table to explore
    choice = input("\nEnter the number corresponding to the table (e.g., 1.2 for DB 1, Table 2): ").strip()
    try:
        db_idx, tbl_idx = map(int, choice.split('.'))
        selected_db = db_table_map[db_idx]["database"]
        selected_table = db_table_map[db_idx]["tables"][tbl_idx - 1]
        display_table_schema_and_sample(selected_db, selected_table)
    except (ValueError, KeyError, IndexError):
        print("Invalid selection. Returning to main menu.")

def manage_tables():
    """Display all databases and their tables, then allow table deletion."""
    # Get current directory dynamically
    directory = os.getcwd()

    # List all databases dynamically
    databases = list_sqlite_databases(directory)
    if not databases:
        print("No SQLite databases found.")
        return

    # Display databases and tables in one view
    db_table_map = {}
    print("\nDatabases and their tables:")
    for db_idx, db in enumerate(databases, start=1):
        tables = list_tables(db)
        db_table_map[db_idx] = {"database": db, "tables": tables}
        print(f"\n{db_idx}. Database: {db}")
        if not tables:
            print("  (No tables found)")
        else:
            for tbl_idx, table in enumerate(tables, start=1):
                print(f"    {db_idx}.{tbl_idx} Table: {table}")

    # Prompt user to select a table to delete
    choice = input("\nEnter the number corresponding to the table you want to delete (e.g., 1.2 for DB 1, Table 2): ").strip()
    try:
        db_idx, tbl_idx = map(int, choice.split('.'))
        selected_db = db_table_map[db_idx]["database"]
        selected_table = db_table_map[db_idx]["tables"][tbl_idx - 1]

        # Confirm deletion
        confirmation = input(f"Are you sure you want to delete the table '{selected_table}' from '{selected_db}'? (yes/no): ").strip().lower()
        if confirmation != 'yes':
            print(f"Deletion of table '{selected_table}' cancelled.")
            return

        # Delete table
        conn = sqlite3.connect(selected_db)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {selected_table};")
        conn.commit()
        print(f"Table '{selected_table}' has been successfully deleted from '{selected_db}'.")
    except (ValueError, KeyError, IndexError):
        print("Invalid selection. Returning to main menu.")
    except sqlite3.Error as e:
        print(f"SQLite error occurred while deleting table {selected_table}: {e}")
    finally:
        conn.close()

def main():
    """Main menu with prompt-based navigation."""
    while True:
        print("\n--- SQLite Database Tool ---")
        print("1. Explore Databases")
        print("2. Manage Tables")
        print("3. Exit")
        choice = input("\nSelect an option: ").strip()

        if choice == "1":
            explore_databases()
        elif choice == "2":
            manage_tables()
        elif choice == "3":
            print("Exiting program. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()

