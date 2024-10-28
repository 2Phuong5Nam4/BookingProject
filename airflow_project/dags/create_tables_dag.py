from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'Duong',
    'start_date': datetime(2024, 10, 20),
}

with DAG(
    dag_id='create_all_tables_dag',
    default_args=default_args,
    description='DAG để tạo tất cả các bảng trong một task',
    schedule_interval='@once', 
    catchup=False,
) as dag:

    create_all_tables = PostgresOperator(
        task_id='create_all_tables',
        postgres_conn_id='postgres_default', # Nhớ chỉnh lại id kết nối cho phù hợp nha
        sql="""
        CREATE TABLE IF NOT EXISTS Province (
            pvc_id VARCHAR(20) PRIMARY KEY,
            pvc_name VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS Accommodation (
            acm_id INT PRIMARY KEY,  
            acm_name VARCHAR(50),
            acm_type VARCHAR(20),
            acm_star_rating FLOAT,
            acm_amenities VARCHAR(100),
            acm_description VARCHAR(1000),
            acm_customer_rating FLOAT,
            acm_review_count INT,
            acm_province VARCHAR(20),
            acm_location VARCHAR(50),
            acm_lat DECIMAL(9, 6),
            acm_long DECIMAL(9, 6),
            CONSTRAINT fk_accomodation_province_province FOREIGN KEY (acm_province)
                REFERENCES Province (pvc_id)
        );

        CREATE TABLE IF NOT EXISTS Rooms (
            rm_room_id INT,
            rm_accommodation_id INT,
            rm_name VARCHAR(50),
            rm_guests_number INT,
            rm_area FLOAT,
            rm_bed_types VARCHAR(50),
            PRIMARY KEY (rm_room_id, rm_accomodation_id),
            CONSTRAINT fk_rooms_accomodation_id FOREIGN KEY (rm_accomodation_id)
                REFERENCES Accomodation (acm_id)
        );

        CREATE TABLE IF NOT EXISTS Bed_price (
            bp_crawled_date DATE,
            bp_when TIME,
            bp_future_interval INT,
            bp_room_id INT,
            bp_accomodation_id INT,
            bp_price INT,
            bp_current_discount FLOAT,
            PRIMARY KEY (bp_crawled_date, bp_room_id, bp_accomodation_id),
            CONSTRAINT fk_beds_rooms_id FOREIGN KEY (bp_room_id, bp_accomodation_id)
                REFERENCES Rooms (rm_room_id, rm_accomodation_id)
        );

        CREATE TABLE IF NOT EXISTS Disciplines (
            dis_accomodation_id INT PRIMARY KEY,
            dis_is_pet_allowed BOOLEAN,
            dis_credit_card_required BOOLEAN,
            dis_deposit_required FLOAT,
            dis_payment_methods VARCHAR(50),
            dis_smoking_allowed BOOLEAN,
            dis_checkin_start TIME,
            dis_checkin_end TIME,
            dis_checkout_start TIME,
            dis_checkout_end TIME,
            CONSTRAINT fk_disciplines_accomodation_id FOREIGN KEY (dis_accomodation_id)
                REFERENCES Accomodation (acm_id)
        );

        CREATE TABLE IF NOT EXISTS Feedback (
            fb_accomodation_id INT,
            fb_room_id INT,
            fb_nationality VARCHAR(20),
            fb_date TIMESTAMP,
            fb_title VARCHAR(100),
            fb_positive VARCHAR(100),
            fb_negative VARCHAR(100),
            fb_scoring INT,
            fb_language_used VARCHAR(15),
            CONSTRAINT fk_feedback_rooms_id FOREIGN KEY (fb_accomodation_id, fb_room_id)
                REFERENCES Rooms (rm_room_id, rm_accomodation_id)
        );
        """
    )
