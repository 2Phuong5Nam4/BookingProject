import pandas as pd
import os
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import unicodedata


# Các hàm phụ trợ
# ----------------------------------------------------------------------------------------


def remove_accents(input_str):
    # Loại bỏ dấu tiếng Việt bằng cách chuẩn hóa Unicode và lọc ký tự gốc
    nfkd_str = unicodedata.normalize('NFKD', input_str)
    return ''.join([c for c in nfkd_str if not unicodedata.combining(c)])


def identify_pet_friendly(pet_info_series):
    return pet_info_series.str.contains('not allowed', case=False, na=False) == False


def extract_checkin_checkout_times(time_string):
    # Case: Availabel 24 hours
    if 'Available 24 hours' in time_string:
        return ('00:00', '23:59')  # Checkin: 00:00 và Checkout: 23:59
    time_string = time_string.strip().lower()

    if 'from' in time_string and 'to' in time_string:
        # Case: "From XX:00 to XX:00"
        parts = time_string.replace('from', '').replace('to', '').split()
        checkin_start = parts[0].strip()
        checkin_end = parts[1].strip()
        return (checkin_start, checkin_end)

    elif 'until' in time_string:
        # Case: "Until XX:00" (Only checkout_end is specified)
        checkout_end = time_string.replace('until', '').strip()
        return (None, checkout_end)

    return (None, None)


def clean_area_column(area_series):
    return area_series.apply(lambda x: float(re.search(r'\d+', str(x)).group(0)) if pd.notnull(x) else 0.0)


def clean_num_guests_column(guests_series):
    return guests_series.apply(lambda x: int(re.search(r'\d+', str(x)).group(0)) if pd.notnull(x) else 0)


def calculate_future_interval(checkin_series, checkout_series):
    return (checkout_series - checkin_series).dt.days


def clean_discount_column(discount_series):
    return discount_series.apply(lambda x: float(re.search(r'\d+(\.\d+)?', str(x)).group(0)) if pd.notnull(x) else 0.0)


# --------------------------------------------------------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------------------------------------------
# Hàm xử lý bảng Accommodation
def AccommodationProcess(execution_date, **kwargs):
    # execution_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_acc_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    # Convert the JSON data to a Pandas DataFrame
    data = pd.DataFrame(json_data)

    data = data.dropna(subset=['id'])

    data['star'] = data['star'].fillna(0)
    accommodation_df = pd.DataFrame({
        'acm_id': data['id'].astype(int),
        'acm_name': data['name'].astype(str),
        'acm_type': data['typeId'].astype(str),
        'acm_star_rating': data['star'].astype(int),
        'acm_amenities': data['unities'].astype(str),
        'acm_description': data['description'].astype(str),
        'acm_customer_rating': data['reviewScore'].astype(float),
        'acm_review_count': data['reviewCount'].astype(int),
        'acm_location': data['location'].astype(str),
        'acm_address': data['address'].astype(str),
        'acm_lat': data['lat'].astype(float).round(6),
        'acm_long': data['lng'].astype(float).round(6),
        'acm_url': data['url'].astype(str)
    })

    pg_hook = PostgresHook(postgres_conn_id='my_rds_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        for _, row in accommodation_df.iterrows():
            insert_stmt = """
                INSERT INTO "Accommodation" (acm_id, acm_name, acm_type, acm_star_rating, acm_amenities, 
                                           acm_description, acm_customer_rating, acm_review_count, acm_address, 
                                           acm_location, acm_lat, acm_long, acm_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (acm_id) DO UPDATE SET
                    acm_name = EXCLUDED.acm_name,
                    acm_type = EXCLUDED.acm_type,
                    acm_star_rating = EXCLUDED.acm_star_rating,
                    acm_amenities = EXCLUDED.acm_amenities,
                    acm_description = EXCLUDED.acm_description,
                    acm_customer_rating = EXCLUDED.acm_customer_rating,
                    acm_review_count = EXCLUDED.acm_review_count,
                    acm_address = EXCLUDED.acm_address,
                    acm_location = EXCLUDED.acm_location,
                    acm_lat = EXCLUDED.acm_lat,
                    acm_long = EXCLUDED.acm_long,
                    acm_url = EXCLUDED.acm_url;
            """
            conn.execute(insert_stmt, (
                row['acm_id'], row['acm_name'], row['acm_type'], row['acm_star_rating'], row['acm_amenities'],
                row['acm_description'], row['acm_customer_rating'], row['acm_review_count'], row['acm_address'],
                row['acm_location'], row['acm_lat'], row['acm_long'], row['acm_url']
            ))
    print("Data loaded successfully to Accommodation table.")


# Hàm xử lý bảng Disciplines
def DisciplinesProcess(execution_date, **kwargs):
    # execution_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_acc_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    # Convert the JSON data to a Pandas DataFrame
    data = pd.DataFrame(json_data)
    data = data.dropna(subset=['id'])

    disciplines_df = pd.DataFrame({
        'dis_accommodation_id': data['id'].astype(int),
        'dis_is_pet_allowed': identify_pet_friendly(data['petInfo']),
        'dis_credit_card_required': data['paymentMethods'].str.contains('credit card', case=False, na=False),
        # 'dis_deposit_required': np.where(data['paymentMethods'].str.contains('deposit', case=False, na=False), 1.0, 0.0),
        'dis_payment_methods': data['paymentMethods'],
        'dis_smoking_allowed': data['unities'].str.contains('smoking', case=False, na=False),
        'dis_checkin_start': data['checkinTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[0]),
        'dis_checkin_end': data['checkinTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[1]),
        'dis_checkout_start': data['checkoutTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[0]),
        'dis_checkout_end': data['checkoutTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[1])
    })

    # ti = kwargs['ti']
    # ti.xcom_push(key='disciplines_data', value=disciplines_df.to_dict(orient='records'))

    pg_hook = PostgresHook(postgres_conn_id='my_rds_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        for _, row in disciplines_df.iterrows():
            insert_stmt = """
                INSERT INTO "Disciplines" (dis_accommodation_id, dis_is_pet_allowed, dis_credit_card_required, 
                                         dis_payment_methods, dis_smoking_allowed, dis_checkin_start, 
                                         dis_checkin_end, dis_checkout_start, dis_checkout_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dis_accommodation_id) DO UPDATE SET
                    dis_is_pet_allowed = EXCLUDED.dis_is_pet_allowed,
                    dis_credit_card_required = EXCLUDED.dis_credit_card_required,
                    dis_payment_methods = EXCLUDED.dis_payment_methods,
                    dis_smoking_allowed = EXCLUDED.dis_smoking_allowed,
                    dis_checkin_start = EXCLUDED.dis_checkin_start,
                    dis_checkin_end = EXCLUDED.dis_checkin_end,
                    dis_checkout_start = EXCLUDED.dis_checkout_start,
                    dis_checkout_end = EXCLUDED.dis_checkout_end;
            """
            conn.execute(insert_stmt, tuple(row))
    print("Data loaded successfully to Disciplines table.")


# Hàm xử lý theo bảng Room
def RoomProcess(execution_date, **kwargs):
    # execution_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_price_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    # Convert the JSON data to a Pandas DataFrame
    data = pd.DataFrame(json_data)
    data = data.dropna(subset=['accommodationId'])

    data['roomArea'] = clean_area_column(data['roomArea'])
    data['numGuests'] = clean_num_guests_column(data['numGuests'])

    room_df = pd.DataFrame({
        'rm_room_id': data['roomId'].fillna(0).astype(int),
        'rm_accommodation_id': data['accommodationId'].astype(int),
        'rm_name': data['roomName'].astype(str),
        'rm_guests_number': data['numGuests'],
        'rm_area': data['roomArea'],
        'rm_bed_types': data['beds']
    })

    # ti = kwargs['ti']
    # ti.xcom_push(key='room_data', value=room_df.to_dict(orient='records'))

    pg_hook = PostgresHook(postgres_conn_id='my_rds_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        for _, row in room_df.iterrows():
            insert_stmt = """
                INSERT INTO "Rooms" (rm_room_id, rm_accommodation_id, rm_name, rm_guests_number, rm_area, rm_bed_types)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (rm_room_id, rm_accommodation_id) DO UPDATE SET
                    rm_name = EXCLUDED.rm_name,
                    rm_guests_number = EXCLUDED.rm_guests_number,
                    rm_area = EXCLUDED.rm_area,
                    rm_bed_types = EXCLUDED.rm_bed_types;
            """
            conn.execute(insert_stmt, tuple(row))
    print("Data loaded successfully to Rooms table.")


# Hàm xử lý theo bảng Bed_price
def BedPriceProcess(execution_date, **kwargs):
    # execution_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_price_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    # Convert the JSON data to a Pandas DataFrame
    data = pd.DataFrame(json_data)
    data = data.dropna(subset=['accommodationId'])
    # data['price'] = data['price'].astype('int64')
    # data['price'] = data['price'].fillna(data['price'].mean())
    data['checkin'] = pd.to_datetime(data['checkin'], format='%Y-%m-%d', errors='coerce').dt.date
    data['checkout'] = pd.to_datetime(data['checkout'], format='%Y-%m-%d', errors='coerce').dt.date
    data['discount'] = clean_discount_column(data['discount'])

    crawled_date = datetime.today().date()

    bed_price_df = pd.DataFrame({
        'bp_crawled_date': crawled_date,
        'bp_future_interval': (data['checkin'] - crawled_date).apply(lambda x: x.days),
        'bp_room_id': data['roomId'].fillna(0).astype(int),
        'bp_accommodation_id': data['accommodationId'],
        'bp_price': data['price'],
        'bp_current_discount': data['discount']
    })

    # ti = kwargs['ti']
    # ti.xcom_push(key='bed_price_data', value=bed_price_df.to_dict(orient='records'))

    pg_hook = PostgresHook(postgres_conn_id='my_rds_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        for _, row in bed_price_df.iterrows():
            insert_stmt = """
                INSERT INTO "Bed_price" (bp_crawled_date, bp_future_interval, bp_room_id, bp_accommodation_id, bp_price, bp_current_discount)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (bp_crawled_date, bp_room_id, bp_accommodation_id) DO UPDATE SET
                    bp_future_interval = EXCLUDED.bp_future_interval,
                    bp_price = EXCLUDED.bp_price,
                    bp_current_discount = EXCLUDED.bp_current_discount;
            """
            conn.execute(insert_stmt, tuple(row))
    print("Data loaded successfully to Bed_price table.")