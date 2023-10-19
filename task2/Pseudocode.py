from kafka import KafkaConsumer
from datetime import datetime, timedelta

# Threshold for transaction amount exceeding alert
threshold = 5000

# Sliding window size for transaction rate
sliding_window_size = timedelta(hours=1)

# Spike threshold for transaction rate
spike_threshold = 10

# Configure Kafka consumer
consumer = KafkaConsumer('transactions', bootstrap_servers='localhost:9092')

# Sliding window to track transaction rate
sliding_window = []

# Function to monitor and process transactions
def monitor_transactions():
    for message in consumer:
        transaction = parse_transaction(message.value)
        process_transaction(transaction)

# Function to process an incoming transaction
def process_transaction(transaction):
    check_threshold_exceeding(transaction)
    check_transaction_rate_spike(transaction)

# Function to check if transaction amount exceeds the threshold
def check_threshold_exceeding(transaction):
    if transaction.amount > threshold:
        trigger_threshold_exceeding_alert(transaction.syndicate, transaction.fund_manager)

# Function to check for a spike in the transaction rate
def check_transaction_rate_spike(transaction):
    current_timestamp = transaction.timestamp
    update_sliding_window(current_timestamp)
    transaction_rate = calculate_transaction_rate()
    if transaction_rate > average_rate * spike_threshold:
        trigger_transaction_rate_spike_alert(transaction.syndicate, transaction.fund_manager)

# Function to update the sliding window with the current timestamp
def update_sliding_window(current_timestamp):
    sliding_window.append(current_timestamp)
    # Remove timestamps that are outside the sliding window
    while sliding_window[0] < current_timestamp - sliding_window_size:
        sliding_window.pop(0)

# Function to calculate the transaction rate within the sliding window
def calculate_transaction_rate():
    window_size = len(sliding_window)
    if window_size > 0:
        transaction_rate = len(sliding_window) / sliding_window_size.total_seconds()
        return transaction_rate
    return 0

# Function to trigger an alert for exceeding the threshold amount
def trigger_threshold_exceeding_alert(syndicate, fund_manager):
    alert_message = f"Threshold exceeding transaction detected for syndicate {syndicate} and fund manager {fund_manager}."
    send_alert(alert_message)

# Function to trigger an alert for a spike in the transaction rate
def trigger_transaction_rate_spike_alert(syndicate, fund_manager):
    alert_message = f"Spike in transaction rate detected for syndicate {syndicate} and fund manager {fund_manager}."
    send_alert(alert_message)

# Function to send an alert
def send_alert(message):
    # Implementation to send the alert to the corresponding syndicate and fund manager
    print(message)  # Placeholder implementation for demonstration purposes

# Helper function to parse the transaction message
def parse_transaction(message):
    # Implementation to parse the transaction message and extract relevant fields
    pass

# Consumer configuration and start monitoring transactions
monitor_transactions()