import pandas as pd
import json

# Paths to input files
csv_drivers_path = "C:\\Users\\dell\\Desktop\\data cleaning\\drivers.csv"
csv_payments_path = "C:\\Users\\dell\\Desktop\\data cleaning\\payments.csv"
excel_deliveries_path = "C:\\Users\\dell\\Desktop\\data cleaning\\deliveries.xlsx"
excel_shipments_path = "C:\\Users\\dell\\Desktop\\data cleaning\\shipments.xlsx"
json_customers_path = "C:\\Users\\dell\\Desktop\\data cleaning\\customers.json"

# Path for the output Excel file
output_excel_path = "C:\\Users\\dell\\Desktop\\data cleaning\\etl data\\cleaned_data.xlsx"

# Load data from files
drivers_df = pd.read_csv(csv_drivers_path)
payments_df = pd.read_csv(csv_payments_path)
deliveries_df = pd.read_excel(excel_deliveries_path)
shipments_df = pd.read_excel(excel_shipments_path)

with open(json_customers_path, 'r') as f:
    customers_data = json.load(f)
customers_df = pd.DataFrame(customers_data)

# Cleaning Drivers Table
def clean_drivers(df):
    def extract_name_from_email(row):
        if pd.isna(row['first_name']) or pd.isna(row['last_name']):
            first_name, last_name = row['email'].split('@')[0].split('.')
            return pd.Series({'first_name': first_name, 'last_name': last_name})
        return pd.Series({'first_name': row['first_name'], 'last_name': row['last_name']})

    missing_names = df[df['first_name'].isna() | df['last_name'].isna()]
    df.update(missing_names.apply(extract_name_from_email, axis=1))
    return df

drivers_df = clean_drivers(drivers_df)

# Cleaning Payments Table
def clean_payments(payments_df, deliveries_df, shipments_df):
    def determine_payment_method(row):
        if pd.isna(row['payment_method']):
            delivery_date = deliveries_df.loc[deliveries_df['delivery_id'] == row['delivery_id'], 'delivery_date']
            if not delivery_date.empty and delivery_date.iloc[0] == row['payment_date']:
                return "cash"
            return "paypal/credit card"
        return row['payment_method']

    def calculate_payment_amount(row):
        if pd.isna(row['payment_amount']):
            delivery_cost = deliveries_df.loc[deliveries_df['delivery_id'] == row['delivery_id'], 'delivery_cost'].fillna(0)
            item_price = shipments_df.loc[shipments_df['shipment_id'] == row['shipment_id'], 'item_price'].fillna(0)
            return delivery_cost.iloc[0] + item_price.iloc[0] if not delivery_cost.empty and not item_price.empty else 0
        return row['payment_amount']

    payments_df['payment_method'] = payments_df.apply(determine_payment_method, axis=1)
    payments_df['payment_amount'] = payments_df.apply(calculate_payment_amount, axis=1)
    return payments_df

payments_df = clean_payments(payments_df, deliveries_df, shipments_df)

# Cleaning Deliveries Table
def clean_deliveries(deliveries_df, payments_df, shipments_df):
    def calculate_delivery_cost(row):
        if pd.isna(row['delivery_cost']):
            payment_amount = payments_df.loc[payments_df['delivery_id'] == row['delivery_id'], 'payment_amount'].fillna(0)
            item_price = shipments_df.loc[shipments_df['delivery_id'] == row['delivery_id'], 'item_price'].fillna(0)
            return payment_amount.iloc[0] - item_price.iloc[0] if not payment_amount.empty and not item_price.empty else 0
        return row['delivery_cost']

    deliveries_df['delivery_cost'] = deliveries_df.apply(calculate_delivery_cost, axis=1)
    return deliveries_df

deliveries_df = clean_deliveries(deliveries_df, payments_df, shipments_df)

# Cleaning Shipments Table
def clean_shipments(shipments_df, payments_df):
    def calculate_item_price(row):
        if pd.isna(row['item_price']):
            payment_amount = payments_df.loc[payments_df['shipment_id'] == row['shipment_id'], 'payment_amount'].fillna(0)
            other_prices = shipments_df.loc[(shipments_df['delivery_id'] == row['delivery_id']) & (shipments_df['shipment_id'] != row['shipment_id']), 'item_price'].fillna(0)
            return payment_amount.iloc[0] - other_prices.sum() if not payment_amount.empty else 0
        return row['item_price']

    shipments_df['item_price'] = shipments_df.apply(calculate_item_price, axis=1)
    return shipments_df

shipments_df = clean_shipments(shipments_df, payments_df)


# Writing cleaned data to an Excel file
with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
    customers_df.to_excel(writer, index=False, sheet_name='Customers')
    drivers_df.to_excel(writer, index=False, sheet_name='Drivers')
    payments_df.to_excel(writer, index=False, sheet_name='Payments')
    shipments_df.to_excel(writer, index=False, sheet_name='Shipments')
    deliveries_df.to_excel(writer, index=False, sheet_name='Deliveries')

print(f"Cleaned data has been saved to {output_excel_path}")
