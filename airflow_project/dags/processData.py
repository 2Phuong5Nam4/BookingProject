import pandas as pd
import re
from datetime import datetime
import psycopg2
from requests.exceptions import RequestException
import time
import ssl
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.errors import ForeignKeyViolation, InvalidColumnReference

# Supporting functions
# ----------------------------------------------------------------------------------------

def identify_pet_friendly(value):
    return False if value is None or 'not allowed' in value.lower() else True

def is_credit_card_required(list_value):
    return False if list_value is None or 'Cash' in list_value else True

def is_smoking_allowed(list_value):
    return False if list_value is None or 'Non-smoking rooms' in list_value else True

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

def clean_bed_str(value):
    return int(re.search(r'\d+', str(value)).group(0)) if value is not None else 0

#def clean_discount_column(value):
    #return float(re.search(r'\d+(\.\d+)?', str(value)).group(0)) if value is not None else 0.0 

def clean_discount_column(value):
    if value is not None:
        match = re.search(r'\d+(\.\d+)?', str(value))
        return float(match.group(0)) if match else 0.0
    return 0.0

def clean_text(text):
    if pd.isna(text):
        return text
    cleaned_text = re.sub(r'[^^\w\s]', '', text)  
    return cleaned_text
# --------------------------------------------------------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------------------------------------------
# Process Accommodation table
def AccommodationProcess(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_acc_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    data = pd.DataFrame(json_data)
    data = data.dropna(subset=['id'])
   
    accommodation_data = pd.DataFrame({
        'acm_id': data['id'].astype(int), 
        'acm_name': data['name'].astype(str),  
        'acm_type': data['typeId'].astype(str),  
        'acm_star_rating': data['star'].fillna(0).astype(int),
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

    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        for _, row in accommodation_data.iterrows():
            insert_stmt = """
                INSERT INTO "Accommodation" (acm_id, acm_name, acm_type, acm_star_rating, acm_amenities, 
                                          acm_description, acm_customer_rating, acm_review_count, 
                                          acm_location, acm_address, acm_lat, acm_long, acm_url)
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

# Process Disciplines table
def DisciplinesProcess(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_acc_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    data = pd.DataFrame(json_data)
    data = data.dropna(subset=['id'])

    disciplines_data = pd.DataFrame({
        'dis_accommodation_id': data['id'].astype(int),
        'dis_is_pet_allowed': data['petInfo'].apply(identify_pet_friendly).astype(bool),
        'dis_credit_card_required': data['paymentMethods'].apply(is_credit_card_required).astype(bool),

        'dis_payment_methods': data['paymentMethods'].astype(str),
        'dis_smoking_allowed': data['unities'].apply(is_smoking_allowed).astype(bool),
        'dis_checkin_start': data['checkinTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[0]),
        'dis_checkin_end': data['checkinTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[1]),
        'dis_checkout_start': data['checkoutTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[0]),
        'dis_checkout_end': data['checkoutTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[1])
    }) 

    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        for _, row in disciplines_data.iterrows():
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

# Process Room table
def RoomsProcess(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_price_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    data = pd.DataFrame(json_data)
    data = data.dropna(subset=['roomId', 'accommodationId'])  

    rooms_data = pd.DataFrame({
        'rm_room_id': data['roomId'].astype(int),               
        'rm_accommodation_id': data['accommodationId'].astype(int),      
        'rm_name': data['roomName'].astype(str),                
        'rm_guests_number': data['numGuests'].apply(clean_bed_str).astype(int),
        'rm_area': data['roomArea'].apply(clean_bed_str).astype(float),
        'rm_bed_types': data['beds'].astype(str)
    })
    
    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        for _, row in rooms_data.iterrows():
            insert_stmt = """
                INSERT INTO "Rooms" (rm_room_id, rm_accommodation_id, rm_name, rm_guests_number,
                                   rm_area, rm_bed_types)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (rm_room_id, rm_accommodation_id) DO UPDATE SET
                    rm_name = EXCLUDED.rm_name,
                    rm_guests_number = EXCLUDED.rm_guests_number,
                    rm_area = EXCLUDED.rm_area,
                    rm_bed_types = EXCLUDED.rm_bed_types;
            """
            conn.execute(insert_stmt, tuple(row))

    print("Data loaded successfully to Rooms table.")

# Process Bed_price table
def BedPriceProcess(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_price_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    data = pd.DataFrame(json_data)
    data = data.dropna(subset=['roomId', 'accommodationId'])

    data['checkin'] = pd.to_datetime(data['checkin'], format='%Y-%m-%d', errors='coerce').dt.date
    data['checkout'] = pd.to_datetime(data['checkout'], format='%Y-%m-%d', errors='coerce').dt.date

    crawled_date = pd.to_datetime(kwargs['execution_date'], format='%Y-%m-%d', errors='coerce').date()

    bed_price_data = pd.DataFrame({
        'bp_crawled_date': crawled_date,
        'bp_future_interval': (data['checkin'] - crawled_date).apply(lambda x: x.days).astype(int),
        'bp_room_id': data['roomId'].astype(int),
        'bp_accommodation_id': data['accommodationId'].astype(int),
        'bp_price': data['price'].astype(int),
        'bp_current_discount': data['discount'].apply(clean_discount_column).astype(float)
    })
    
    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()

    # with engine.connect() as conn:
    #     for _, row in bed_price_data.iterrows():
    #         insert_stmt = """
    #             INSERT INTO "Bed_price" (bp_crawled_date, bp_future_interval, bp_room_id, bp_accommodation_id, bp_price, bp_current_discount)
    #             VALUES (%s, %s, %s, %s, %s, %s);
    #         """
    #         conn.execute(insert_stmt, tuple(row))
    bed_price_data.to_sql('Bed_price', engine, if_exists='append', index=False)
    print("Data loaded successfully to Bed_price table.")

def FeedBackProcess(**kwargs):
    ti = kwargs['ti']
    num_chunks = ti.xcom_pull(task_ids='push_json_comment_to_xcom', key='comment_json_data_num_chunks')
    print(f"Num chunks: {num_chunks}")
    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    # Retrieve each chunk
    num_rows = 0
    num_success = 0
    for i in range(num_chunks):
        chunk_key = f"comment_json_data_chunk_{i}"
        chunk = ti.xcom_pull(task_ids="push_json_comment_to_xcom", key=chunk_key)

        data = pd.DataFrame(chunk)
        data = data.dropna(subset=['accommodationId', 'roomId', 'reviewUrl'], how='any')
        # data = data.dropna(subset=['accommodationId', 'roomId'], how='any')
        data = data.drop_duplicates()
        data['reviewedDate'] = pd.to_datetime(data['reviewedDate'], unit='s', errors='coerce').dt.date
        num_rows += data.shape[0]

        data['title'] = data['title'].apply(clean_text)
        data['positiveText'] = data['positiveText'].apply(clean_text)
        data['negativeText'] = data['negativeText'].apply(clean_text)

        feedback_data = pd.DataFrame({
            'fb_accommodation_id': data['accommodationId'].astype(int),
            'fb_room_id': data['roomId'].astype(int), 
            'fb_reviewed_date': data['reviewedDate'],
            'fb_language_used': data['language'],
            'fb_title': data['title'],
            'fb_positive': data['positiveText'],
            'fb_negative': data['negativeText'],
            'fb_num_nights': data['numNights'].astype(int, errors='ignore'),
            'fb_nationality': data['guestCountry'],
            'fb_customer_type': data['customerType'],
            'fb_checkin_date': pd.to_datetime(data['checkinDate'], format='%Y-%m-%d', errors='coerce').dt.date,
            'fb_scoring': data['reviewScore'].astype(float, errors='ignore') ,
            'fb_review_url': data['reviewUrl'],
        })


        with engine.connect() as conn:
            for _, row in feedback_data.iterrows():
                insert_stmt = """
                    INSERT INTO "Feedback" (fb_accommodation_id, fb_room_id, fb_reviewed_date,
                                         fb_language_used, fb_title, fb_positive, fb_negative,
                                         fb_num_nights, fb_nationality, fb_customer_type,
                                         fb_checkin_date, fb_scoring, fb_review_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (fb_room_id, fb_accommodation_id, fb_review_url) DO UPDATE SET
                        fb_language_used = EXCLUDED.fb_language_used,
                        fb_title = EXCLUDED.fb_title,
                        fb_positive = EXCLUDED.fb_positive,
                        fb_negative = EXCLUDED.fb_negative,
                        fb_num_nights = EXCLUDED.fb_num_nights,
                        fb_nationality = EXCLUDED.fb_nationality,
                        fb_customer_type = EXCLUDED.fb_customer_type,
                        fb_checkin_date = EXCLUDED.fb_checkin_date,
                        fb_scoring = EXCLUDED.fb_scoring,   
                        fb_reviewed_date = EXCLUDED.fb_reviewed_date;
                """
                try:
                    conn.execute(insert_stmt, tuple(row))
                except ForeignKeyViolation as e:
                    print(f"Caught ForeignKeyViolation exception: {e}")
                except InvalidColumnReference as e:
                    print(f"Caught InvalidColumnReference exception: {e}")
                except Exception as e:
                    print(f"Caught a different exception: {e}")
                else:
                    num_success += 1
                    print(f"Successfully inserted row: {num_success}")

    print("Data loaded successfully to Feedback table.")
    print(f"Total rows: {num_rows}, successfully inserted: {num_success}")