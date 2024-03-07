import psycopg2
import pandas as pd
import mplfinance as mpf

# Connect to the PostgreSQL database
def connect_to_database():
    conn = psycopg2.connect(
        dbname="AAPL",
        user="postgres",
        password="insta@0903",
        host="localhost",
        port="5432"
    )
    return conn

def calculate_profit_loss(data):
    positions = {'long': None}
    trades = []
    overall_profit_loss = 0

    for index, row in data.iterrows():
        # Buy Signal
        if row['Buy_Signal']:
            if positions['long'] is None:
                positions['long'] = {'entry_price': row['Open'], 'entry_date': index}  # Use index instead of 'Date'
        # Sell Signal or Close Buy Position
        elif row['Sell_Signal'] or row['Close_Buy_Position']:
            if positions['long']:
                entry_price = positions['long']['entry_price']
                profit_loss = row['Open'] - entry_price
                trades.append((positions['long']['entry_date'], index, entry_price, row['Open'], profit_loss))  # Use index instead of 'Date'
                overall_profit_loss += profit_loss
                positions['long'] = None

    return trades, overall_profit_loss

# Query data from the database
def query_data(conn, table_name):
    try:
        query = f"SELECT * FROM \"{table_name}\";"
        data = pd.read_sql(query, conn)
        # Remove leading or trailing whitespaces from column names
        data.columns = data.columns.str.strip()
        data['Date'] = pd.to_datetime(data['Date'])
        data.set_index('Date', inplace=True)
        return data
    except Exception as e:
        print(f"Error querying data from table {table_name}: {e}")

# Calculate Simple Moving Average (SMA)
def calculate_sma(data, window):
    return data['Close'].rolling(window=window).mean()

# Generate buy/sell signals based on specified conditions
def generate_signals(data):
    # Calculate SMAs
    data['SMA_5'] = calculate_sma(data, 5)
    data['SMA_10'] = calculate_sma(data, 10)
    data['SMA_20'] = calculate_sma(data, 20)
    data['SMA_50'] = calculate_sma(data, 50)
    data['SMA_200'] = calculate_sma(data, 200)
    data['SMA_500'] = calculate_sma(data, 500)
    
    # Generate signals
    data['Buy_Signal'] = (data['SMA_50'] > data['SMA_500'])
    data['Sell_Signal'] = (data['SMA_20'] < data['SMA_200']) 
    data['Close_Buy_Position'] = (data['SMA_10'] < data['SMA_20']) & (data['SMA_10'].shift(1) > data['SMA_20'].shift(1))
    data['Close_Sell_Position'] = (data['SMA_5'] < data['SMA_10'])
    
    return data

# Main function
def main():
    conn = connect_to_database()
    if conn:
        table_names = ["AAPL", "HDB", "INR=X", "JIOFIN", "MARA", "TATAMOTORS", "TSLA"]
        for table_name in table_names:
            data = query_data(conn, table_name)
            if data is not None:
                signals_data = generate_signals(data)
                mpf.plot(signals_data, type='candle', style='charles', ylabel='Price',
                         volume=True, show_nontrading=True, title=f'{table_name}')

                trades, profit_loss = calculate_profit_loss(signals_data)
                print(f"Trades for {table_name}:")
                for trade in trades:
                    print(trade)
                print(f"Overall Profit/Loss for {table_name}:", profit_loss)
                print()
    conn.close()


if __name__ == "__main__":
    main()
