# lichess_data_fetch.py

# ---------------------------
# Step 0: Import Libraries
# ---------------------------

import berserk
import pandas as pd
import chess
import os
import time

# ---------------------------
# Step 1: Set Your Lichess API Token
# ---------------------------

token = ""  # Replace with your actual token

if not token:
    raise ValueError("Lichess API token not provided. Please set the token variable.")

# ---------------------------
# Step 2: Initialize the Lichess Client
# ---------------------------

session = berserk.TokenSession(token)
client = berserk.Client(session=session)

# ---------------------------
# Step 3: Fetch Top Players
# ---------------------------

def fetch_top_players(perf_type='blitz', count=100):
    try:
        leaderboard = client.users.get_leaderboard(perf_type=perf_type, count=count)
        usernames = [user['username'] for user in leaderboard]
        return usernames
    except berserk.exceptions.ResponseError as e:
        print(f"Error fetching leaderboard for {perf_type}: {e}")
        return []

# Define performance types and number of players per type
performance_types = {
    'blitz': 100,
    'bullet': 100,
    'rapid': 100,
    'classical': 100
}

# Fetch top players from all specified performance types
all_usernames = set()
for perf_type, count in performance_types.items():
    players = fetch_top_players(perf_type, count)
    all_usernames.update(players)
    print(f"Fetched {len(players)} top {perf_type} players.")

print(f"Total unique usernames fetched: {len(all_usernames)}")

# ---------------------------
# Step 4: Define Helper Functions
# ---------------------------

def fetch_games_with_retry(username, max_games, retries=3, delay=5):
    for attempt in range(retries):
        try:
            games_generator = client.games.export_by_player(
                username,
                max=max_games,
                perf_type='classical',  # Fetch standard chess games
                moves=True,
                pgn_in_json=False,
                clocks=False,
                evals=False,
                opening=True,
                as_pgn=False
            )
            games = list(games_generator)
            return games
        except berserk.exceptions.ResponseError as e:
            if e.status_code == 429:
                print(f"  Rate limit exceeded. Waiting for {delay} seconds before retrying...")
                time.sleep(delay)
            else:
                print(f"  Response error: {e}. Retrying ({attempt+1}/{retries})...")
                time.sleep(delay)
        except Exception as e:
            print(f"  Unexpected error: {e}. Retrying ({attempt+1}/{retries})...")
            time.sleep(delay)
    print(f"  Failed to fetch games for user {username} after {retries} attempts.")
    return []

def get_opening_name_from_moves(moves):
    """
    Determines the opening name based on the move sequence using the chess library.
    :param moves: String of moves in UCI format separated by spaces
    :return: Opening name or 'Unknown'
    """
    move_list = moves.split()
    board = chess.Board()
    opening_name = 'Unknown'
    eco_code = 'Unknown'

    for move_uci in move_list[:20]:  # Limit to first 20 moves
        try:
            move = chess.Move.from_uci(move_uci)
            if move not in board.legal_moves:
                break  # Illegal move
            board.push(move)
            # Use the built-in ECO data from python-chess
            opening_name = chess.polyglot.opening_name(board)
            if opening_name != "Unknown Opening":
                eco_code = 'N/A'  # ECO code is not available via this method
                break
        except Exception as e:
            print(f"Error parsing move {move_uci}: {e}")
            break  # Invalid move

    opening = f"{eco_code}: {opening_name}"
    return opening

# ---------------------------
# Step 5: Fetch and Process Games
# ---------------------------

max_games_per_user = 100  # Adjust as needed
sleep_time = 1  # Seconds between user requests

all_game_data = []

print("Starting to fetch games for each user...")
test_usernames = list(all_usernames)
for idx, username in enumerate(test_usernames, 1):
    print(f"Fetching games for user {idx}/{len(test_usernames)}: {username}")
    games = fetch_games_with_retry(username, max_games_per_user)
    print(f"  Fetched {len(games)} games for user {username}.")

    # Process each game
    for game in games:
        try:
            white_player = game['players']['white'].get('user', {}).get('name', 'Anonymous')
            white_rating = game['players']['white'].get('rating', None)

            black_player = game['players']['black'].get('user', {}).get('name', 'Anonymous')
            black_rating = game['players']['black'].get('rating', None)

            # Game ID and Link
            game_id = game['id']
            link = f"https://lichess.org/{game_id}"

            # Try to get opening from Lichess API
            opening_info = game.get('opening', {})
            opening_name = opening_info.get('name', 'Unknown')
            opening_eco = opening_info.get('eco', 'Unknown')
            opening = f"{opening_eco}: {opening_name}"

            # If opening is Unknown, try using chess library
            if opening_name == 'Unknown' or opening_eco == 'Unknown':
                moves = game.get('moves', '')
                if not moves:
                    print(f"Game {game_id} has no moves.")
                else:
                    print(f"Game {game_id} moves: {moves}")
                opening = get_opening_name_from_moves(moves)

            # Result
            winner = game.get('winner', 'draw')

            # Move count
            move_count = len(game.get('moves', '').split())

            # Append to all_game_data
            all_game_data.append({
                'game_id': game_id,
                'white_player': white_player,
                'white_rating': white_rating,
                'black_player': black_player,
                'black_rating': black_rating,
                'opening': opening,
                'winner': winner,
                'move_count': move_count,
                'link': link
            })
        except Exception as e:
            print(f"  Error processing game {game.get('id', 'Unknown')}: {e}")
            continue  # Skip to the next game

    # Sleep to respect rate limits
    time.sleep(sleep_time)

print(f"Total games fetched: {len(all_game_data)}")

# ---------------------------
# Step 6: Compile Data into a Pandas DataFrame
# ---------------------------

# Create DataFrame from all_game_data
df = pd.DataFrame(all_game_data)

# Display the first few rows
print("Sample Data:")
print(df.head())

# Optional: Handle missing data or perform additional cleaning
initial_shape = df.shape
df_cleaned = df[df['opening'] != 'Unknown: Unknown']
final_shape = df_cleaned.shape
print(f"Dropped {initial_shape[0] - final_shape[0]} games with unknown openings.")

# ---------------------------
# Step 7: Export Data to CSV
# ---------------------------

csv_file_path = 'lichess_games_data.csv'
df_cleaned.to_csv(csv_file_path, index=False)
print(f"Data successfully exported to {csv_file_path}")
