import pandas as pd
import json
from typing import Dict
from sqlalchemy import create_engine
from db_config import config
import logging
import hashlib

logging.basicConfig(
    filename="etl.log",
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d | %(levelname)s | %(module)s | %(funcName)s | %(message)s",
    datefmt="%y-%b-%Y %H:%M:%S",
)


DATABASE_URL = f"postgresql+psycopg2://{config.db_username}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_name}"
ENGINE = create_engine(DATABASE_URL)

SCHEMA = "analytics"


# ===========================
# Common Functions
# ===========================
def str_to_min(x):
    """
    A function that takes in soccer timestamp and converts to minutes
    """
    if pd.notna(x):
        hrs, mins, secs = x.split(":")
        return round((float(hrs) * 60) + float(mins) + (float(secs) / 60))


def str_to_secs(x):
    """
    A function that takes in soccer timestamp and converts to seconds
    """
    if pd.notna(x):
        hrs, mins, secs = x.split(":")
        return int(int(hrs) * 3600) + (int(mins) * 60) + float(secs)


def calculate_minutes_played(row, max_time):
    """
    A function that takes in a row and max time of a match and calculates minutes played by a player
    """
    if pd.notna(row.start_time) and pd.notna(row.end_time):
        minutes = row.end_time - row.start_time
    elif pd.notna(row.start_time) and pd.isna(row.end_time):
        minutes = max_time - row.start_time
    else:
        minutes = 0

    return minutes


def generate_hash_id(row):
    """
    A function that takes in a row a generates a hash id using the row information
    """
    info = "".join(row.astype("str").values.tolist()).replace(" ", "")

    # Create a hashlib object using the SHA-256 algorithm
    sha256 = hashlib.sha256()

    # Update the hash object with the data to be hashed
    sha256.update(info.encode("utf-8"))

    # Get the hexadecimal representation of the hashed data
    hash_id = sha256.hexdigest()

    return hash_id


# ===========================
# Read Files
# ===========================
def read_txt(file_path: str):
    """
    A function that takes a .txt file of tracking data and returns a processed Pandas dataframe
    """
    # Read the JSON file
    logging.info(f"Reading {file_path} file running")
    with open(file_path) as f:
        lines = f.readlines()

    data_list = []

    # Load each line as a JSON object
    for i, line in enumerate(lines):
        try:
            # Parse the line as JSON
            json_data = json.loads(line)

            # Extract relevant data
            info = {
                "tracks_data": json_data["data"],
                "possession": json_data["possession"],
                "frame": json_data["frame"],
                "timestamp": json_data["timestamp"],
                "period": json_data["period"],
            }

            # Append the extracted data to the list
            data_list.append(info)

        except json.JSONDecodeError:
            # Handle any decoding errors if needed
            logging.error(f"Skipping invalid JSON line {i}: {line}")

    def check_tdata(x):
        if isinstance(x, list) and len(x) == 0:
            res = pd.NA
        else:
            res = x

        return res

    # Convert tracking data to dataframe
    df = pd.DataFrame(data_list)

    # Convertin empty list to null values
    df["tracks_data"] = df["tracks_data"].apply(lambda x: check_tdata(x))

    # Each dict in the list is converted to a new row
    df = df.explode(["tracks_data"])
    df.reset_index(inplace=True, drop=True)

    # Expanding tracks data dict keys to columns
    t_data = pd.json_normalize(df["tracks_data"])

    # Expanding possession dict keys to columns
    p_data = pd.json_normalize(df["possession"])
    p_data.columns = ["group", "p_trackable_object"]

    # Merging all the files
    final = pd.concat([df, t_data, p_data], axis=1)

    # Converting timestamp to seconds
    final["timestamp"] = final["timestamp"].apply(lambda x: str_to_secs(x))

    logging.info(f"Tracking data generated from {file_path}")
    return final


def read_json(file_path: str):
    """
    A function that takes a metadata json file path and returns a dictionary
    """
    logging.info(f"Reading {file_path} file running")
    with open(file_path, "r") as file:
        # Load the JSON data from the file
        data = json.load(file)

    logging.info(f"Match metadata from {file_path} read")
    return data


# ===========================
# Extract and Transform Data
# ===========================
# 1. Team Dimension
def create_team_dim(data: Dict):
    """
    A function that takes in metadata dict and returns a team pandas dataframe
    """
    return pd.DataFrame([data["home_team"], data["away_team"]])


def process_players_info(data: Dict, max_time: float):
    """
    A function that takes in metadata dict and max_time of a match and return a pandas dataframe with all player information
    """
    logging.info(f"Process players info running")
    p_info = pd.DataFrame(data["players"])
    p_info["player_role"] = p_info["player_role"].apply(lambda x: x["name"])
    p_info["match_id"] = data["id"]
    p_info["start_time"] = p_info["start_time"].apply(lambda x: str_to_secs(x))
    p_info["end_time"] = p_info["end_time"].apply(lambda x: str_to_secs(x))
    p_info["seconds_played"] = p_info.apply(
        lambda row: calculate_minutes_played(row, max_time), axis=1
    )

    logging.info(f"Players info generated")

    return p_info


# 2. Players Dimension
def create_players_dim(df: pd.DataFrame):
    """
    A function that takes in all player information dataframe and returns a player pandas dataframe
    """
    logging.info(f"Create players dim running")
    dim = df[
        [
            "id",
            "first_name",
            "last_name",
            "short_name",
            "birthday",
            "gender",
            "trackable_object",
        ]
    ].copy()

    logging.info(f"Players dim data generated")

    return dim


# 3. Performance Fact
def create_perfom_fact(df: pd.DataFrame):
    """
    A function that takes in all player information dataframe and returns a player performance pandas dataframe
    """
    logging.info(f"Create Performance Fact running")
    fact = df[
        [
            "match_id",
            "id",
            "team_id",
            "seconds_played",
            "player_role",
            "goal",
            "yellow_card",
            "red_card",
            "injured",
            "own_goal",
        ]
    ].copy()
    fact = fact.rename({"id": "player_id"}, axis=1)

    fact["id"] = fact.apply(lambda row: generate_hash_id(row), axis=1)

    logging.info(f"Player Performance Fact Data generated")
    return fact


# 4. Match Dimension
def create_match_dim(data: Dict):
    """
    A function that takes in metadata dict and returns a match pandas dataframe
    """
    logging.info(f"Create match dim running")
    match_id = data["id"]
    home_team_score = data["home_team_score"]
    away_team_score = data["away_team_score"]
    date_played = data["date_time"]
    stadium = data["stadium"]["name"]
    home_team = data["home_team"]["name"]
    away_team = data["away_team"]["name"]
    home_team_coach = f"{data['home_team_coach']['first_name']} {data['home_team_coach']['last_name']}"
    away_team_coach = f"{data['away_team_coach']['first_name']} {data['away_team_coach']['last_name']}"
    competion = data["competition_edition"]["name"]
    competion_round = data["competition_round"]["name"]

    dim = pd.DataFrame(
        [
            {
                "id": match_id,
                "stadium": stadium,
                "competition": competion,
                "round": competion_round,
                "date_played": date_played,
                "home_team": home_team,
                "away_team": away_team,
                "home_team_coach": home_team_coach,
                "away_team_coach": away_team_coach,
                "home_team_score": home_team_score,
                "away_team_score": away_team_score,
            }
        ]
    )

    logging.info(f"Match Dim data generated")

    return dim, match_id


# 5. Spatial Fact
def create_spatial_fact(df: pd.DataFrame, match_id: int):
    """
    A function that takes in a pandas dataframe with tracking data and returns a subset pandas
    dataframe with spatial locations of players and the ball
    """
    logging.info(f"Create spatial fact running")
    fact = df[["trackable_object", "period", "timestamp", "x", "y", "z"]].copy()
    fact["match_id"] = match_id
    fact = fact[fact["trackable_object"].notna()].drop_duplicates().copy()

    fact["id"] = fact.apply(lambda row: generate_hash_id(row), axis=1)

    logging.info(f"Spatial fact data generated")
    return fact


# 6. Possession Fact
def create_poss_fact(df: pd.DataFrame, df2: pd.DataFrame, data: Dict, match_id: int):
    """
    A function that takes in a pandas dataframe with tracking data, another pandas dataframe with player info, metadata and match id and returns a subset pandas
    dataframe with possession tracking
    """
    logging.info(f"Create possession fact running")
    fact = df[["p_trackable_object", "group", "timestamp"]].copy()
    fact["match_id"] = match_id
    fact["group"] = fact["group"].replace(
        {"away team": data["away_team"]["id"], "home team": data["home_team"]["id"]}
    )
    fact.columns = ["trackable_object", "team_id", "timestamp", "match_id"]

    fact = (
        fact[~fact["trackable_object"].isna()]
        .sort_values("timestamp", ascending=True)[
            [
                "trackable_object",
                "timestamp",
                "match_id",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
        .copy()
    )
    fact = pd.merge(
        fact,
        df2[["trackable_object", "team_id"]].drop_duplicates(),
        how="inner",
        on="trackable_object",
    )

    fact["id"] = fact.apply(lambda row: generate_hash_id(row), axis=1)

    logging.info(f"Possession fact data generated")

    return fact


# ===========================
# Load Data
# ===========================
def load_data_to_db(df, table: str, name: str):
    """
    A function that takes in a dataframe and table in database to load data
    """
    logging.info(f"Load Data to DB running")
    try:
        df.to_sql(
            name=table,
            schema=SCHEMA,
            con=ENGINE,
            if_exists="append",
            index=False,
        )

        logging.info(
            f"{name} Dimension/Fact with {df.shape[0]} Records loaded to {SCHEMA}.team_dim"
        )
    except Exception as e:
        logging.exception(e.orig)


if __name__ == "__main__":
    try:
        metadata = read_json("src_de_sample_data/10000_metadata.json")
        final = read_txt("src_de_sample_data/10000_tracking.txt")
        max_match_time = final.nlargest(1, columns="timestamp")["timestamp"].values[0]

        # Generate Team Dim Data & Load to DB
        team_dim = create_team_dim(metadata)
        load_data_to_db(team_dim, "team_dim", "Team")

        p_info = process_players_info(metadata, max_match_time)

        # Generate Player Dim Data & Load to DB
        player_dim = create_players_dim(p_info)
        load_data_to_db(player_dim, "player_dim", "Player")

        # Generate Player Performace Fact Data & Load to DB
        perf_fact = create_perfom_fact(p_info)
        load_data_to_db(perf_fact, "player_performance_fact", "Player Performance")

        # Generate Match Dim Data & Load to DB
        match_dim, match_id = create_match_dim(metadata)
        load_data_to_db(match_dim, "match_dim", "Match")

        # Generate Spatial Fact Data & Load to DB
        spatial_fact = create_spatial_fact(final, match_id)
        load_data_to_db(spatial_fact, "spatial_fact", "Spatial")

        # # Generate Possession Fact Data & Load to DB
        poss_fact = create_poss_fact(final, p_info, metadata, match_id)
        load_data_to_db(poss_fact, "possession_fact", "Possession")
    except Exception as e:
        logging.error(f"{str(e.orig)}")
