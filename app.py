import streamlit as st
import pandas as pd
import sqlite3

# Database file path
DB_PATH = "nba_props.db"

# Function to fetch table names (markets)
def get_available_markets():
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(query, conn)["name"].tolist()
    conn.close()
    return tables

# Function to fetch and format prop data
def get_props_data(selected_market):
    # Connect to the database
    conn = sqlite3.connect(DB_PATH)

    # Query the data from the selected market table
    query = f"SELECT Player, Position, Team, Opponent, Selection, PropLine, Odds, Bookie FROM {selected_market}"
    data = pd.read_sql_query(query, conn)
    conn.close()

    # Process data to get the desired output format
    grouped_data = format_props_data(data)
    return grouped_data

# Function to format data for display
def format_props_data(data):
    # Pivot the data to show lines and odds for each bookie
    pivot_data = data.pivot_table(
        index=["Player", "Position", "Team", "Opponent", "Selection"],
        columns="Bookie",
        values=["PropLine", "Odds"],
        aggfunc="first"
    )
    
    # Flatten the multi-index columns for cleaner display
    pivot_data.columns = [f"{bookie} {col}" for col, bookie in pivot_data.columns]
    pivot_data.reset_index(inplace=True)

    # Split into "Over" and "Under" rows and interleave
    over_data = pivot_data[pivot_data["Selection"] == "Over"].drop(columns=["Selection"])
    under_data = pivot_data[pivot_data["Selection"] == "Under"].drop(columns=["Selection"])
    over_data["Type"] = "Over"
    under_data["Type"] = "Under"

    # Combine Over and Under rows for each player
    formatted_data = pd.concat([over_data, under_data]).sort_values(
        by=["Player", "Team", "Opponent", "Type"]
    )

    # Dynamically rearrange columns to ensure each bookie's line and odds are grouped
    base_columns = ["Player", "Position", "Team", "Opponent", "Type"]
    
    # Desired bookie order
    desired_bookie_order = ["DraftKings", "FanDuel", "ESPNBet", "BetMGM", "Caesars"]

    # Create ordered columns for bookie lines and odds
    reordered_bookie_columns = []
    for bookie in desired_bookie_order:
        reordered_bookie_columns.extend([f"{bookie} PropLine", f"{bookie} Odds"])

    # Combine base columns with reordered bookie columns
    reordered_columns = base_columns + reordered_bookie_columns
    return formatted_data[reordered_columns]

# Streamlit app
st.title("NBA Prop Markets Viewer")

# Sidebar for selecting prop market
st.sidebar.header("Filters")
available_markets = get_available_markets()
selected_market = st.sidebar.selectbox("Select Prop Market", options=available_markets)

# Display selected market data
if selected_market:
    st.subheader(f"Player Prop Market Odds - {selected_market}")
    props_data = get_props_data(selected_market)
    st.dataframe(props_data, use_container_width=True)
else:
    st.warning("Please select a prop market from the sidebar.")
