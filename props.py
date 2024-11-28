import requests
import sqlite3
import logging
from datetime import datetime

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("prop_markets.log"),
        logging.StreamHandler()
    ]
)

# Constants
BASE_URL_OFFERS = "https://api.bettingpros.com/v3/offers"
API_KEY = "CHi8Hy5CEE4khd46XNYL23dCFX96oUdw6qOt1Dnh"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "x-api-key": API_KEY,
    "Sec-GPC": "1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache"
}

BOOKIE_MAP = {
    0: "BettingPros",
    19: "BetMGM",
    12: "DraftKings",
    10: "FanDuel",
    33: "ESPNBet",
    # Add additional book IDs as required
}

MARKET_MAP = {
    156: "Points_o_u",
    157: "Rebounds_o_u",
    151: "Assists_o_u",
    162: "3PM_o_u",
    160: "Steals_o_u",
    152: "Blocks_o_u",
    335: "Points_Assists",
    336: "Points_Rebounds",
    337: "Rebounds_Assists",
    338: "Points_Rebounds_Assists",
    346: "Fantasy_Score"
}

DB_FILE = "nba.db"

# Fetch offers data
def fetch_offers(event_ids, market_id, location="OH", limit=100, page=1):
    event_ids_str = ":".join(map(str, event_ids))
    params = {
        "sport": "NBA",
        "market_id": market_id,
        "event_id": event_ids_str,
        "location": location,
        "limit": limit,
        "page": page
    }
    response = requests.get(BASE_URL_OFFERS, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()

# Parse offers data
def parse_offers_data(offers_data, market_name, script_timestamp):
    organized_data = []

    for offer in offers_data.get('offers', []):
        event_id = offer.get('event_id', 'Unknown')
        selections = offer.get('selections', [])
        participants = offer.get('participants', [{}])
        
        player_info = participants[0].get('player', {})
        player_name = participants[0].get('name', 'Unknown Player')
        position = player_info.get('position', 'Unknown')
        player_team = player_info.get('team', 'Unknown')

        for selection in selections:
            label = selection.get('label', 'Unknown Label')  # "Over" or "Under"
            books = selection.get('books', [])
            
            for book in books:
                most_recent_line = None
                for line in book.get('lines', []):
                    if not line.get('active', False):
                        continue
                    if most_recent_line is None or line['updated'] > most_recent_line['updated']:
                        most_recent_line = line

                if most_recent_line:
                    book_id = book.get('id', 'N/A')
                    bookie_name = BOOKIE_MAP.get(book_id, f"Book ID {book_id}")
                    organized_data.append({
                        'script_timestamp': script_timestamp,
                        'market': market_name,
                        'player': player_name,
                        'position': position,
                        'team': player_team,
                        'event_id': event_id,
                        'selection': label,
                        'prop_line': most_recent_line.get('line', 'N/A'),
                        'odds': most_recent_line.get('cost', 'N/A'),
                        'bookie': bookie_name,
                        'source_updated': most_recent_line.get('updated', 'N/A')
                    })

    return organized_data

# Save to database
def save_to_database(db_file, table_name, data):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        script_timestamp TEXT,
        market TEXT,
        player TEXT,
        position TEXT,
        team TEXT,
        event_id TEXT,
        selection TEXT,
        prop_line REAL,
        odds REAL,
        bookie TEXT,
        source_updated TEXT
    )
    """)

    # Insert data into the table
    for entry in data:
        cursor.execute(f"""
        INSERT INTO {table_name} (
            script_timestamp, market, player, position, team, event_id,
            selection, prop_line, odds, bookie, source_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry['script_timestamp'], entry['market'], entry['player'], entry['position'],
            entry['team'], entry['event_id'], entry['selection'], entry['prop_line'],
            entry['odds'], entry['bookie'], entry['source_updated']
        ))

    conn.commit()
    conn.close()

# Main function to fetch and track prop markets
def track_prop_markets(event_ids):
    script_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    table_name = "prop_lines"

    for market_id, market_name in MARKET_MAP.items():
        try:
            logging.info(f"Fetching data for market: {market_name} (ID: {market_id})")
            offers_data = fetch_offers(event_ids, market_id)
            parsed_data = parse_offers_data(offers_data, market_name, script_timestamp)
            save_to_database(DB_FILE, table_name, parsed_data)
            logging.info(f"Saved {len(parsed_data)} entries for market: {market_name}.")
        except Exception as e:
            logging.error(f"Error tracking market {market_name}: {e}")

# Example Usage
if __name__ == "__main__":
    # Replace with actual event IDs fetched dynamically or hardcoded for now
    event_ids = [25313, 25314, 25315, 25316]  # Example event IDs
    track_prop_markets(event_ids)

