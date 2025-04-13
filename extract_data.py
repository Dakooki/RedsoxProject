import csv
import os
from datetime import datetime
import pytz

# Belgium timezone
belgium_tz = pytz.timezone('Europe/Brussels')
ingestion_time = datetime.now(belgium_tz).isoformat()

# 🔁 File paths
csv_filename_games_wide = '/Desktop/baseball_project/raw_data/games_wide.csv'
csv_filename_games_post_wide = '/Desktop/baseball_project/raw_data/games_post_wide.csv'
csv_filename_schedules = '/Desktop/baseball_project/raw_data/schedules.csv'

#selected_schema
field_names_games_post_wide = [
    'gameId', 'seasonId', 'seasonType', 'year', 'startTime', 'type', 'gameStatus', 'attendance',
    'dayNight', 'duration', 'durationMinutes', 'awayTeamId', 'awayTeamName', 'homeTeamId',
    'homeTeamName', 'venueId', 'venueName', 'venueSurface', 'venueCapacity', 'venueCity',
    'venueState', 'venueZip', 'venueMarket', 'homeFinalRuns', 'homeFinalHits', 'homeFinalErrors',
    'awayFinalRuns', 'inningHalf', 'inningEventType', 'inningHalfEventSequenceNumber',
    'ingestion_time'  
]

field_names_games_wide = [
    'gameId', 'seasonId', 'seasonType', 'year', 'startTime', 'type', 'gameStatus', 'attendance',
    'dayNight', 'duration', 'durationMinutes', 'awayTeamId', 'awayTeamName', 'homeTeamId',
    'homeTeamName', 'venueId', 'venueName', 'venueSurface', 'venueCapacity', 'venueCity',
    'venueState', 'venueZip', 'venueMarket', 'homeFinalRuns', 'homeFinalHits', 'homeFinalErrors',
    'awayFinalRuns', 'inningHalf', 'inningEventType', 'inningHalfEventSequenceNumber',
    'ingestion_time'  #
]

field_names_schedules = [
    'gameId', 'gameNumber', 'seasonId', 'year', 'type', 'dayNight', 'duration', 'duration_minutes',
    'homeTeamId', 'homeTeamName', 'awayTeamId', 'awayTeamName', 'startTime', 'attendance',
    'status', 'created', 'ingestion_time'  
]

# Output directory and filenames
output_dir = 'C:/Users/akram.dabbar/Desktop/baseball_project/staging/'
os.makedirs(output_dir, exist_ok=True)
output_filename_games_post_wide = os.path.join(output_dir, 'staged_games_post_wide.csv')
output_filename_games_wide = os.path.join(output_dir, 'staged_games_wide.csv')
output_filename_schedules = os.path.join(output_dir, 'staged_schedules.csv')

try:
    # games_wide
    with open(csv_filename_games_wide, 'r', newline='', encoding='utf-8') as infile, \
         open(output_filename_games_wide, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=field_names_games_wide)
        writer.writeheader()
        for row in reader:
            row['ingestion_time'] = ingestion_time
            writer.writerow({field: row.get(field, '') for field in field_names_games_wide})
    print(f"✅ Data written to '{output_filename_games_wide}'")

    # games_post_wide
    with open(csv_filename_games_post_wide, 'r', newline='', encoding='utf-8') as infile, \
         open(output_filename_games_post_wide, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=field_names_games_post_wide)
        writer.writeheader()
        for row in reader:
            row['ingestion_time'] = ingestion_time
            writer.writerow({field: row.get(field, '') for field in field_names_games_post_wide})
    print(f"✅ Data written to '{output_filename_games_post_wide}'")

    # schedules
    with open(csv_filename_schedules, 'r', newline='', encoding='utf-8') as infile, \
         open(output_filename_schedules, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=field_names_schedules)
        writer.writeheader()
        for row in reader:
            row['ingestion_time'] = ingestion_time
            writer.writerow({field: row.get(field, '') for field in field_names_schedules})
    print(f"✅ Data written to '{output_filename_schedules}'")

except FileNotFoundError as e:
    print(f"❌ Error: File not found. {e}")
