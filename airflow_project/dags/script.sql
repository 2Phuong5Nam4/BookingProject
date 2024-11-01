
	CREATE TABLE IF NOT EXISTS "Accommodation" (
		acm_id INT PRIMARY KEY,
		acm_name VARCHAR,
		acm_type VARCHAR,
		acm_star_rating FLOAT,
		acm_amenities VARCHAR,
		acm_description VARCHAR,
		acm_customer_rating FLOAT,
		acm_review_count INT,
		acm_address VARCHAR,
		acm_location VARCHAR,
		acm_lat DECIMAL(9, 6),
		acm_long DECIMAL(9, 6),
		acm_url VARCHAR
	);

	CREATE TABLE IF NOT EXISTS "Rooms" (
		rm_room_id INT,
		rm_accommodation_id INT,
		rm_name VARCHAR,
		rm_guests_number INT,
		rm_area FLOAT,
		rm_bed_types VARCHAR,
		PRIMARY KEY (rm_room_id, rm_accommodation_id),
		CONSTRAINT fk_rooms_accomodation_id FOREIGN KEY (rm_accommodation_id) REFERENCES "Accommodation" (acm_id)
	);

	CREATE TABLE IF NOT EXISTS "Bed_price" (
        bp_crawled_date DATE,
        bp_future_interval INT,
        bp_room_id INT,
        bp_accommodation_id INT,
        bp_price INT,
        bp_current_discount FLOAT,
        CONSTRAINT fk_beds_rooms_id FOREIGN KEY (bp_room_id, bp_accommodation_id) REFERENCES "Rooms" (rm_room_id, rm_accommodation_id),
        CONSTRAINT bed_price_unique UNIQUE (bp_crawled_date, bp_room_id, bp_accommodation_id)
    );

	CREATE TABLE IF NOT EXISTS "Disciplines" (
		dis_accommodation_id INT PRIMARY KEY,
		dis_is_pet_allowed BOOLEAN,
		dis_credit_card_required BOOLEAN,
		dis_payment_methods VARCHAR,
		dis_smoking_allowed BOOLEAN,
		dis_checkin_start TIME,
		dis_checkin_end TIME,
		dis_checkout_start TIME,
		dis_checkout_end TIME,
		CONSTRAINT fk_disciplines_accommodation_id FOREIGN KEY (dis_accommodation_id) REFERENCES "Accommodation" (acm_id)
	);

	CREATE TABLE IF NOT EXISTS "Feedback" (
		fb_accommodation_id INT,
		fb_room_id INT,
		fb_nationality VARCHAR,
		fb_date TIMESTAMP,
		fb_title VARCHAR,
		fb_positive VARCHAR,
		fb_negative VARCHAR,
		fb_scoring INT,
		fb_language_used VARCHAR,
		CONSTRAINT fk_feedback_rooms_id FOREIGN KEY (fb_accommodation_id, fb_room_id) REFERENCES "Rooms" (rm_room_id, rm_accommodation_id)
	);
