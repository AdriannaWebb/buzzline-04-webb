"""
json_consumer_webb.py

Read a JSON-formatted file as it is being written. 

Example JSON message:
{"message": "I just saw a movie! It was amazing.", "author": "Eve"}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
import os # for file operations
import sys # to exit early
import time
import pathlib
from datetime import datetime
from collections import defaultdict  # data structure for counting author occurrences

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_logger import logger


#####################################
# Set up Paths - read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures
#####################################

# Dictionary to store message counts by hour and category
# Structure: {hour: {category: count}}
hourly_category_counts = defaultdict(lambda: defaultdict(int))

#####################################
# Set up live visuals
#####################################

fig, ax = plt.subplots()
plt.ion()  # Turn on interactive mode for live updates

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart():
    """Update the live chart with hourly category message counts."""
    # Clear the previous chart
    ax.clear()

    # Define colors for each category
    category_colors = {
        'humor': 'red',
        'tech': 'blue', 
        'food': 'green',
        'travel': 'orange',
        'entertainment': 'purple',
        'gaming': 'brown',
        'other': 'gray'
    }

    # Create hours list (0-23)
    hours = list(range(24))
    
    # Plot a line for each category that has data
    for category in category_colors.keys():
        # Get counts for this category across all hours
        counts = [hourly_category_counts[hour][category] for hour in hours]
        
        # Only plot if there's some data for this category
        if sum(counts) > 0:
            ax.plot(hours, counts, 
                   label=category.capitalize(), 
                   color=category_colors[category],
                   marker='o',
                   linewidth=2)

    # Set up the chart labels and title
    ax.set_xlabel("Hour of Day (0-23)")
    ax.set_ylabel("Number of Messages")
    ax.set_title("Messages by Hour and Category")
    
    # Set x-axis to show all hours
    ax.set_xlim(0, 23)
    ax.set_xticks(range(0, 24, 2))  # Show every other hour to avoid crowding
    
    # Add legend
    ax.legend(loc='upper right')
    
    # Add grid for easier reading
    ax.grid(True, alpha=0.3)

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)


#####################################
# Process Message Function
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)
       
        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the 'timestamp' and 'category' fields from the message
            timestamp_str = message_dict.get("timestamp", "")
            category = message_dict.get("category", "other")
            
            if timestamp_str:
                # Parse the timestamp to extract the hour
                # Format: "2025-01-29 14:35:20"
                timestamp_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                hour = timestamp_obj.hour  # Extract hour (0-23)
                
                logger.info(f"Message received at hour {hour} in category: {category}")

                # Increment the count for this hour and category
                hourly_category_counts[hour][category] += 1

                # Log the updated counts
                logger.info(f"Updated hourly category counts for hour {hour}: {dict(hourly_category_counts[hour])}")

                # Update the chart
                update_chart()

                # Log the updated chart
                logger.info(f"Chart updated successfully")
            else:
                logger.error(f"Missing timestamp in message: {message}")

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except ValueError as e:
        logger.error(f"Error parsing timestamp: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main Function
#####################################


def main() -> None:
    """
    Main entry point for the consumer.
    - Monitors a file for new messages and updates a live chart.
    """

    logger.info("START consumer.")

    # Verify the file we're monitoring exists if not, exit early
    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        # Try to open the file and read from it
        with open(DATA_FILE, "r") as file:

            # Move the cursor to the end of the file
            file.seek(0, os.SEEK_END)
            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                # Read the next line from the file
                line = file.readline()

                # If we strip whitespace from the line and it's not empty
                if line.strip():  
                    # Process this new message
                    process_message(line)
                else:
                    # otherwise, wait a half second before checking again
                    logger.debug("No new messages. Waiting...")
                    delay_secs = 0.5 
                    time.sleep(delay_secs) 
                    continue 

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
