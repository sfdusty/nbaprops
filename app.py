import streamlit as st
import pandas as pd
import sqlite3

DB_PATH = "nba_props.db"

st.markdown(
    """
    <style>
    .block-container { padding: 1rem; max-width: 100%; width: 100%; }
    .stDataFrame > div { max-height: 80vh; overflow: auto; }
    .css-1d391kg { overflow-x: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

def get_available_markets():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in cursor.fetchall()]
    market_types = {}
    for table in tables:
        try:
            query = f"SELECT DISTINCT Market FROM '{table}'"
            market_data = pd.read_sql_query(query, conn)
            for market_type in market_data["Market"].unique():
                market_types[market_type] = table
        except Exception as e:
            st.write(f"Skipping table {table}: {e}")
    conn.close()
    return market_types

def get_last_update_time(table_name):
    conn = sqlite3.connect(DB_PATH)
    query = f"SELECT MAX(ScriptTimestamp) as LastUpdate FROM '{table_name}'"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result["LastUpdate"].iloc[0] if not result.empty else None

def get_team_filter_options(data):
    return sorted(data["Team"].unique())

def get_props_data(selected_market, selected_table, selected_team=None):
    conn = sqlite3.connect(DB_PATH)
    query = f"SELECT Player, Position, Team, Opponent, Selection, PropLine, Odds, Bookie, ScriptTimestamp FROM '{selected_table}' WHERE Market = ?"
    data = pd.read_sql_query(query, conn, params=(selected_market,))
    conn.close()
    if selected_team:
        data = data[data["Team"] == selected_team]
    return format_props_data(data)

def format_props_data(data):
    pivot_data = data.pivot_table(
        index=["Player", "Position", "Team", "Opponent", "Selection"],
        columns="Bookie",
        values=["PropLine", "Odds"],
        aggfunc="first"
    )
    pivot_data.columns = [f"{bookie} {col}" for col, bookie in pivot_data.columns]
    pivot_data.reset_index(inplace=True)

    over_data = pivot_data[pivot_data["Selection"] == "Over"].drop(columns=["Selection"])
    under_data = pivot_data[pivot_data["Selection"] == "Under"].drop(columns=["Selection"])
    over_data["Type"] = "Over"
    under_data["Type"] = "Under"
    formatted_data = pd.concat([over_data, under_data]).sort_values(by=["Player", "Team", "Opponent", "Type"])

    base_columns = ["Player", "Position", "Team", "Opponent", "Type"]
    desired_bookie_order = ["DraftKings", "FanDuel", "ESPNBet", "BetMGM", "Caesars"]
    bookie_name_map = {"DraftKings": "DK", "FanDuel": "FD", "ESPNBet": "EB", "BetMGM": "MGM", "Caesars": "CZ"}

    reordered_bookie_columns = [
        col for bookie in desired_bookie_order for col in [f"{bookie} PropLine", f"{bookie} Odds"] if col in formatted_data.columns
    ]
    final_columns = base_columns + reordered_bookie_columns
    formatted_data = formatted_data[final_columns]

    column_rename_map = {
        "Player": "Player Name",
        "Position": "Pos",
        "Team": "Team",
        "Opponent": "Opp",
        "Type": "Bet Type",
    }
    for bookie, short_name in bookie_name_map.items():
        column_rename_map[f"{bookie} PropLine"] = f"{short_name} Line"
        column_rename_map[f"{bookie} Odds"] = f"{short_name} Odds"

    return formatted_data.rename(columns=column_rename_map)

st.title("NBA Prop Markets Viewer")
st.sidebar.header("Filters")
available_markets = get_available_markets()

if not available_markets:
    st.warning("No available markets found.")
else:
    market_names = list(available_markets.keys())
    selected_market_name = st.sidebar.selectbox("Select Prop Market", options=market_names)
    selected_table = available_markets[selected_market_name]

    if selected_market_name:
        last_update = get_last_update_time(selected_table)
        st.sidebar.markdown(f"**Last Update:** {last_update}")

        props_data = get_props_data(selected_market_name, selected_table)
        team_options = get_team_filter_options(props_data)

        selected_team = st.sidebar.selectbox("Filter by Team", options=["All Teams"] + team_options)
        if selected_team != "All Teams":
            props_data = get_props_data(selected_market_name, selected_table, selected_team)

        st.subheader(f"Player Prop Market Odds - {selected_market_name}")
        st.dataframe(props_data, use_container_width=True, height=680)
    else:
        st.warning("Please select a prop market from the sidebar.")
