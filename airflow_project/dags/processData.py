import pandas as pd
import os
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Các hàm phụ trợ
# ----------------------------------------------------------------------------------------
provinces = [
    'An Giang', 'Bà Rịa - Vũng Tàu', 'Bạc Liêu', 'Bắc Kạn', 'Bắc Giang', 'Bắc Ninh', 
    'Bến Tre', 'Bình Dương', 'Bình Định', 'Bình Phước', 'Bình Thuận', 'Cà Mau', 
    'Cao Bằng', 'Cần Thơ', 'Đà Nẵng', 'Đắk Lắk', 'Đắk Nông', 'Điện Biên', 'Đồng Nai', 
    'Đồng Tháp', 'Gia Lai', 'Hà Giang', 'Hà Nam', 'Hà Nội', 'Hà Tĩnh', 'Hải Dương', 
    'Hải Phòng', 'Hậu Giang', 'Hòa Bình', 'Hưng Yên', 'Khánh Hòa', 'Kiên Giang', 
    'Kon Tum', 'Lai Châu', 'Lâm Đồng', 'Lạng Sơn', 'Lào Cai', 'Long An', 'Nam Định', 
    'Nghệ An', 'Ninh Bình', 'Ninh Thuận', 'Phú Thọ', 'Phú Yên', 'Quảng Bình', 
    'Quảng Nam', 'Quảng Ngãi', 'Quảng Ninh', 'Quảng Trị', 'Sóc Trăng', 'Sơn La', 
    'Tây Ninh', 'Thái Bình', 'Thái Nguyên', 'Thanh Hóa', 'Thừa Thiên Huế', 'Tiền Giang', 
    'TP HCM', 'Trà Vinh', 'Tuyên Quang', 'Vĩnh Long', 'Vĩnh Phúc', 'Yên Bái'
]

def extract_province(address):
    address = address.title()
    for province in provinces:
        if re.search(province, address, re.IGNORECASE):
            return province
    return "Unknown" 

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
    #execution_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_acc_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    # Convert the JSON data to a Pandas DataFrame
    data = pd.DataFrame(json_data)

   
    data['star'] = data['star'].fillna(0)
    accommodation_df = pd.DataFrame({
        'id': data['id'].astype(int), 
        'name': data['name'].astype(str),  
        'type': data['typeId'].astype(str),  
        'star_rating': data['star'].astype(int),  
        'customer_rating': data['reviewScore'].astype(float),  
        'review_count': data['reviewCount'].astype(int), 
        'utilities': data['unities'].astype(str), 
        'description': data['description'].astype(str),  
        'province': data['address'].apply(lambda x: extract_province(x)).astype(str), 
        'location': data["address"].astype(str),  
        'lat': data['lat'].astype(float),  
        'long': data['lng'].astype(float)  
    })

    # Nếu dùng XCom
    #ti = kwargs['ti']
    #ti.xcom_push(key='accomodation_data', value=accommodation_df.to_dict(orient='records'))  

    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    accommodation_df.to_sql('Accommodation', engine, if_exists='append', index=False)
    print("Data loaded successfully to Accomodation table.")

# Hàm xử lý bảng Descriplines
def DescriplinesProcess(execution_date, **kwargs):
    #execution_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_acc_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    # Convert the JSON data to a Pandas DataFrame
    data = pd.DataFrame(json_data)

    descriplines_df = pd.DataFrame({
        'id': data['id'].astype(int),
        'is_pet_allowed': identify_pet_friendly(data['petInfo']),
        'credit_card_required': data['paymentMethods'].str.contains('credit card', case=False, na=False),
        #'deposit_required': np.where(data['paymentMethods'].str.contains('deposit', case=False, na=False), 1.0, 0.0),
        'payment_method': data['paymentMethods'],
        'smoking_allowed': data['unities'].str.contains('smoking', case=False, na=False),
        'checkin_start': data['checkinTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[0]),
        'checkin_end': data['checkinTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[1]),
        'checkout_start': data['checkoutTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[0]),
        'checkout_end': data['checkoutTime'].apply(lambda x: extract_checkin_checkout_times(str(x))[1])
    })
    
    #ti = kwargs['ti']
    #ti.xcom_push(key='descriplines_data', value=descriplines_df.to_dict(orient='records'))  

    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    descriplines_df.to_sql('Descriplines', engine, if_exists='append', index=False)
    print("Data loaded successfully to Descriplines table.")

# Hàm xử lý theo bảng Room
def RoomProcess(execution_date, **kwargs):
    #execution_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_price_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    # Convert the JSON data to a Pandas DataFrame
    data = pd.DataFrame(json_data)

    data['roomArea'] = clean_area_column(data['roomArea'])
    data['numGuests'] = clean_num_guests_column(data['numGuests'])    

    room_df = pd.DataFrame({
    'room_id': data['roomId'].fillna(0).astype(int),               
    'acc_id': data['accommodationId'].astype(int),      
    'name': data['roomName'].astype(str),                
    'guests_number': data['numGuests'],  
    'area': data['roomArea'], 
    'bed_types': data['beds']
    })
    
    #ti = kwargs['ti']
    #ti.xcom_push(key='room_data', value=room_df.to_dict(orient='records'))  

    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    room_df.to_sql('Room', engine, if_exists='append', index=False)
    print("Data loaded successfully to Room table.")

# Hàm xử lý theo bảng Bed_price
def BedPriceProcess(execution_date, **kwargs):
    #execution_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='push_json_price_to_xcom', key='scrapy_json_data')
    print(f"DataFrame: {json_data}")

    # Convert the JSON data to a Pandas DataFrame
    data = pd.DataFrame(json_data)
    # data['price'] = data['price'].astype('int64')
    # data['price'] = data['price'].fillna(data['price'].mean())
    data['checkin'] = pd.to_datetime(data['checkin'], format='%Y-%m-%d', errors='coerce')
    data['checkout'] = pd.to_datetime(data['checkout'], format='%Y-%m-%d', errors='coerce')    

    data['future_interval'] = calculate_future_interval(data['checkin'], data['checkout'])
    data['discount'] = clean_discount_column(data['discount'])

    bed_price_df = pd.DataFrame({
    #'crawl_date': ,            
    'when': data['checkin'],      
    'future_interval': data['future_interval'],                
    'room_id': data['roomId'].fillna(0).astype(int),  
    'price': data['price'], 
    'current_discount': data['discount']
    })
    
    #ti = kwargs['ti']
    #ti.xcom_push(key='bed_price_data', value=bed_price_df.to_dict(orient='records'))  

    pg_hook = PostgresHook(postgre_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    bed_price_df.to_sql('Bed_price', engine, if_exists='append', index=False)
    print("Data loaded successfully to Bed_price table.")


    