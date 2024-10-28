CREATE TABLE IF NOT EXISTS "Province" (
	pvc_id VARCHAR PRIMARY KEY,
	pvc_name VARCHAR
);

CREATE TABLE IF NOT EXISTS "Accommodation" (
	acm_id INT PRIMARY KEY,
	acm_name VARCHAR,
	acm_type VARCHAR,
	acm_star_rating FLOAT,
	acm_amenities VARCHAR,
	acm_description VARCHAR,
	acm_customer_rating FLOAT,
	acm_review_count INT,
	acm_province VARCHAR,
	acm_location VARCHAR,
	acm_lat DECIMAL(9, 6),
	acm_long DECIMAL(9, 6),
	CONSTRAINT fk_accommodation_province_province FOREIGN KEY (acm_province) REFERENCES "Province" (pvc_id)
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
	CONSTRAINT fk_beds_rooms_id FOREIGN KEY (bp_room_id, bp_accommodation_id) REFERENCES "Rooms" (rm_room_id, rm_accommodation_id)
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

INSERT INTO "Province" (pvc_id, pvc_name) VALUES
('Unknown', 'Không rõ'),
('AG', 'An Giang'),
('BRVT', 'Bà Rịa - Vũng Tàu'),
('BL', 'Bạc Liêu'),
('BK', 'Bắc Kạn'),
('BG', 'Bắc Giang'),
('BN', 'Bắc Ninh'),
('BTre', 'Bến Tre'),
('BinhD', 'Bình Dương'),
('BDinh', 'Bình Định'),
('BPhuoc', 'Bình Phước'),
('BThuan', 'Bình Thuận'),
('CM', 'Cà Mau'),
('CB', 'Cao Bằng'),
('CT', 'Cần Thơ'),
('DNang', 'Đà Nẵng'),
('DLak', 'Đắk Lắk'),
('DNong', 'Đắk Nông'),
('DB', 'Điện Biên'),
('DNai', 'Đồng Nai'),
('DThap', 'Đồng Tháp'),
('GL', 'Gia Lai'),
('HG', 'Hà Giang'),
('HNam', 'Hà Nam'),
('HNoi', 'Hà Nội'),
('HT', 'Hà Tĩnh'),
('HD', 'Hải Dương'),
('HP', 'Hải Phòng'),
('HGiang', 'Hậu Giang'),
('HB', 'Hòa Bình'),
('HY', 'Hưng Yên'),
('KH', 'Khánh Hòa'),
('KG', 'Kiên Giang'),
('KT', 'Kon Tum'),
('LC', 'Lai Châu'),
('LDong', 'Lâm Đồng'),
('LS', 'Lạng Sơn'),
('LCai', 'Lào Cai'),
('LA', 'Long An'),
('ND', 'Nam Định'),
('NA', 'Nghệ An'),
('NB', 'Ninh Bình'),
('NThuan', 'Ninh Thuận'),
('PT', 'Phú Thọ'),
('PY', 'Phú Yên'),
('QB', 'Quảng Bình'),
('QNam', 'Quảng Nam'),
('QNgai', 'Quảng Ngãi'),
('QNinh', 'Quảng Ninh'),
('QT', 'Quảng Trị'),
('ST', 'Sóc Trăng'),
('SL', 'Sơn La'),
('TN', 'Tây Ninh'),
('TB', 'Thái Bình'),
('TNguyen', 'Thái Nguyên'),
('TH', 'Thanh Hóa'),
('TTH', 'Thừa Thiên Huế'),
('TG', 'Tiền Giang'),
('HCM', 'TP HCM'),
('TV', 'Trà Vinh'),
('TQ', 'Tuyên Quang'),
('VL', 'Vĩnh Long'),
('VP', 'Vĩnh Phúc'),
('YB', 'Yên Bái');
