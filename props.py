import requests
import sqlite3
from datetime import datetime
import logging
from collections import Counter

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_props_fetch.log"),  # Log to a file
        logging.StreamHandler()                      # Log to console
    ]
)

# Constants
BASE_URL_EVENTS = "https://api.bettingpros.com/v3/events"
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
    19: "BetMGM",
    12: "DraftKings",
    10: "FanDuel",
    33: "ESPNBet",
    18: "Caesars"
}

MARKET_MAP = {
    156: "Points o/u",
    157: "Rebounds o/u",
    151: "Assists o/u",
    162: "3PM o/u",
    160: "Steals o/u",
    152: "Blocks o/u",
    335: "Points+Assists",
    336: "Points+Rebounds",
    337: "Rebounds+Assists",
    338: "Points+Rebounds+Assists",
    346: "Fantasy Score"
}

def fetch_events_with_teams(date):
    """Fetch event IDs and team info."""
    params = {"sport": "NBA", "date": date}
    response = requests.get(BASE_URL_EVENTS, headers=HEADERS, params=params)
    response.raise_for_status()
    data = response.json()

    event_team_map = {}
    for event in data.get('events', []):
        event_id = event['id']
        home_team = event.get('home', 'Unknown')
        away_team = event.get('visitor', 'Unknown')
        event_team_map[event_id] = {'home_team': home_team, 'away_team': away_team}
    return event_team_map

def fetch_offers(event_ids, market_id, location="OH", limit=100, page=1):
    """Fetch offers data."""
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

def parse_offers_data(offers_data, event_team_map, script_timestamp, market_name):
    """Parse offers data into structured format."""
    organized_data = []
    bookie_count = Counter()
    players = set()

    for offer in offers_data.get('offers', []):
        event_id = offer.get('event_id')
        team_info = event_team_map.get(event_id, {'home_team': 'Unknown', 'away_team': 'Unknown'})
        selections = offer.get('selections', [])
        participants = offer.get('participants', [{}])
        
        player_info = participants[0].get('player', {})
        player_name = participants[0].get('name', 'Unknown Player')
        position = player_info.get('position', 'Unknown')
        player_team = player_info.get('team', 'Unknown')

        players.add(player_name)  # Track unique players

        for selection in selections:
            label = selection.get('label', 'Unknown Label')  # "Over" or "Under"
            books = selection.get('books', [])
            
            for book in books:
                book_id = book.get('id')
                if book_id == 0:  # Exclude Book ID 0
                    continue

                most_recent_line = None
                for line in book.get('lines', []):
                    if not line.get('active', False):  # Skip inactive lines
                        continue
                    if most_recent_line is None or line['updated'] > most_recent_line['updated']:
                        most_recent_line = line

                if most_recent_line:
                    bookie_name = BOOKIE_MAP.get(book_id, f"Book ID {book_id}")
                    bookie_count[bookie_name] += 1  # Track bookie totals

                    organized_data.append({
                        'Script Timestamp': script_timestamp,
                        'Market': market_name,
                        'Player': player_name,
                        'Position': position,
                        'Team': team_info.get('home_team') if player_team == team_info.get('home_team') else team_info.get('away_team'),
                        'Opponent': team_info.get('away_team') if player_team == team_info.get('home_team') else team_info.get('home_team'),
                        'Selection': label,
                        'Prop Line': most_recent_line.get('line', 'N/A'),
                        'Odds': most_recent_line.get('cost', 'N/A'),
                        'Bookie': bookie_name,
                        'Source Updated Timestamp': most_recent_line.get('updated', 'N/A')
                    })

    return organized_data, players, bookie_count

def store_data_in_database(data, table_name):
    """Store parsed data in SQLite database and display the table schema."""
    try:
        logging.info("Connecting to SQLite database...")
        conn = sqlite3.connect("nba_props.db")
        cursor = conn.cursor()

        # Create table with market ID as the name if it doesn't exist
        logging.info(f"Creating table '{table_name}' if it doesn't exist...")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ScriptTimestamp TEXT,
            Market TEXT,
            Player TEXT,
            Position TEXT,
            Team TEXT,
            Opponent TEXT,
            Selection TEXT,
            PropLine REAL,
            Odds INTEGER,
            Bookie TEXT,
            SourceUpdatedTimestamp TEXT,
            PRIMARY KEY (ScriptTimestamp, Player, Selection, Bookie, PropLine)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Display the table schema
        logging.info(f"Displaying schema for table '{table_name}':")
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()
        for column in schema:
            logging.info(f"Column: {column}")

        # Insert data
        logging.info(f"Inserting data into the '{table_name}' table...")
        for entry in data:
            cursor.execute(f"""
            INSERT OR IGNORE INTO {table_name} 
            (ScriptTimestamp, Market, Player, Position, Team, Opponent, Selection, PropLine, Odds, Bookie, SourceUpdatedTimestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entry['Script Timestamp'], entry['Market'], entry['Player'], entry['Position'],
                entry['Team'], entry['Opponent'], entry['Selection'],
                entry['Prop Line'], entry['Odds'], entry['Bookie'],
                entry['Source Updated Timestamp']
            ))
        conn.commit()
        logging.info(f"Data successfully stored in table '{table_name}'.")
    except sqlite3.Error as e:
        logging.error(f"SQLite error occurred: {e}")
    finally:
        conn.close()

def main():
    date_today = datetime.now().strftime("%Y-%m-%d")
    script_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Fetch events
        logging.info("Fetching event IDs and team information...")
        event_team_map = fetch_events_with_teams(date_today)
        if not event_team_map:
            logging.warning("No events found for the given date.")
            return

        logging.info(f"Fetched Event IDs with Teams: {event_team_map}")

        # Cycle through market IDs
        for market_id, market_name in MARKET_MAP.items():
            logging.info(f"Processing market: {market_name} (Market ID: {market_id})")
            table_name = f"market_{market_id}"  # Use market ID as table name

            # Fetch offers
            logging.info(f"Fetching offers data for market ID {market_id}...")
            event_ids = list(event_team_map.keys())
            offers_data = fetch_offers(event_ids, market_id=market_id)
            logging.info(f"Fetched offers data for market: {market_name}.")

            # Parse data
            logging.info("Parsing offers data...")
            parsed_data, players, bookie_count = parse_offers_data(offers_data, event_team_map, script_timestamp, market_name)
            logging.info(f"Parsed data count for {market_name}: {len(parsed_data)} entries.")
            logging.info(f"Unique players retrieved for {market_name}: {len(players)}")
            logging.info("Bookie totals:")
            for bookie, count in bookie_count.items():
                logging.info(f"{bookie}: {count} entries")

            # Store data in database
            logging.info(f"Storing parsed data for {market_name} in database...")
            store_data_in_database(parsed_data, table_name)

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error occurred: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()

