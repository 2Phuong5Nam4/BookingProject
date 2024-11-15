import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px


charts = """
- Dash tổng quan về acc:
	+ thông tin tổng quan các khu vực: tỷ lệ các loại hình ở từng khu vực ,mức giá trung bình của từng loại hình ,số điểm đánh giá của khách
	+ Bộ lọc cho 5 khu vực
		* Chart về phân bố chỗ ở ở các địa điểm (có bộ lọc thể hiện số sao, review count, review score) -> tìm kiếm các khu vực hot -> map
		* Phân bố khách du lịch theo quốc gia, theo loại khách ở các địa điểm-> khu vực thu hút khách du lịch ở quốc gia nào, loại khách thường xuyên đến
		* Phản hồi tích cực và tiêu cực của khách hàng ở từng khu vực như thế nào?
		* Khu vực nào có điểm đánh giá khách hàng trung bình cao nhất?
		--> Dương
		* Các loại phòng nào phổ biến nhất tại từng khu vực?
		* Lượng tăng đánh giá tăng theo tuần, tháng -> Khu vực nào đang hot
		* Phân bố pos, neg theo loại khách, quốc gia khách, location, acctype -> Những điều kiện để có đánh giá tốt
- Dash về giá:
	+ Các bộ lọc (khu vực, accType, star, stay duration)
		* Kiểm tra discount xuất hiện cao nhất ở loại phòng nào, thời gian nào\
		--> Hưng
		* Biến động giá theo ngày cào -> giá thay đổi như thế nào trong các thời điểm khác nhau
		* Cố định checkin - checkout, so sánh các ngày cào -> Xem được đặt trước bao nhiêu ngày là được giá hời
		* Cố định room, so sánh giá các khoảng thời gian checkin, checkout -> ở bao nhiêu ngày là được hời nhất
		* So sánh giá tiền với đánh giá, số sao, diện tích / đầu người -> nên bỏ bao nhiêu tiền sẽ được trải nghiệm tốt nhất
		-->Nam
"""

# Initialize connection.
conn = st.connection("postgresql", type="sql")


provinces = {
    "All": "All",
    "Hạ Long": "Ha Long, Quang Ninh, Vietnam",
    "Hội An": "Hoi An, Quang Nam, Vietnam",
    "Thừa Thiên Huế": "Thua Thien Hue, Vietnam",
    "Nha Trang": "Nha Trang, Khanh Hoa, Vietnam",
    "Đà Lạt": "Da Lat, Lam Dong, Vietnam"
}

acc_types = conn.query("SELECT DISTINCT acm_type FROM public.\"Accommodation\";", ttl="10m")[
    "acm_type"].tolist()
acc_types.insert(0, "All")


# Filter data based on the selected options:
# - 1. acc_location
# - 2. acc_type
# - 3. acc_star_rating
# - 4. acc_customer_rating
# - 5. acc_review_count
# - 6. rm_guests_number
# - 7. bp_future_interval
def postgres_query_line_chart(location=None, type=None, star_range=None, customer_rating=None, review_count=None, guests_number=None, future_interval=None):
    base = """
    SELECT bp.bp_crawled_date, AVG(bp.bp_price) as avg_price
    FROM public."Bed_price" bp
    JOIN public."Rooms" rm ON bp.bp_room_id = rm.rm_room_id and bp.bp_accommodation_id = rm.rm_accommodation_id
    JOIN public."Accommodation" acm ON bp.bp_accommodation_id = acm.acm_id
    """
    conditions = []
    if location is not None and location != "All":
        conditions.append(f"acm.acm_location = '{provinces[location]}'")

    if type is not None and type != "All":
        conditions.append(f"acm.acm_type = '{type}'")

    if star_range is not None:
        conditions.append(f"acm.acm_star_rating BETWEEN {
                          star_range[0]} AND {star_range[1]}")

    if customer_rating is not None:
        conditions.append(f"acm.acm_customer_rating BETWEEN {
                          customer_rating[0]} AND {customer_rating[1]}")
    if future_interval is not None:
        conditions.append(f"bp.bp_future_interval BETWEEN {
                          future_interval[0]} AND {future_interval[1]}")
    if guests_number is not None:
        conditions.append(f"rm.rm_guests_number BETWEEN {
                          guests_number[0]} AND {guests_number[1]}")
    # if review_count is not None:
        # conditions.append(f"acm.acm_review_count BETWEEN {review_count[0]} AND {review_count[1]}")
    if conditions:
        base += " WHERE " + " AND ".join("("+c+")" for c in conditions)
    base += " GROUP BY bp.bp_crawled_date"
    print(base)
    df = conn.query(base, ttl="10m")
    print(df)
    return df


def postgres_query_bar_chart(location=None, type=None, star_range=None, customer_rating=None, review_count=None, guests_number=None):
    base = """
    SELECT bp.bp_future_interval, AVG(bp.bp_price) as avg_price
    FROM public."Bed_price" bp
    JOIN public."Rooms" rm ON bp.bp_room_id = rm.rm_room_id and bp.bp_accommodation_id = rm.rm_accommodation_id
    JOIN public."Accommodation" acm ON bp.bp_accommodation_id = acm.acm_id
    """
    conditions = []
    if location is not None and location != "All":
        conditions.append(f"acm.acm_location = '{provinces[location]}'")
    if type is not None and type != "All":
        conditions.append(f"acm.acm_type = '{type}'")
    if star_range is not None:
        conditions.append(f"acm.acm_star_rating BETWEEN {
                          star_range[0]} AND {star_range[1]}")
    if customer_rating is not None:
        conditions.append(f"acm.acm_customer_rating BETWEEN {
                          customer_rating[0]} AND {customer_rating[1]}")
    if guests_number is not None:
        conditions.append(f"rm.rm_guests_number BETWEEN {
                          guests_number[0]} AND {guests_number[1]}")
    # if review_count is not None:
        # conditions.append(f"acm.acm_review_count BETWEEN {review_count[0]} AND {review_count[1]}")
    if conditions:
        base += " WHERE " + " AND ".join("("+c+")" for c in conditions)
    base += " GROUP BY bp.bp_future_interval"
    df = conn.query(base, ttl="10m")
    return df


def show():
    # Create columns for filtering components
    col1, _, col2 = st.columns([2, 1, 3])
    col3, _, col4 = st.columns([2, 1, 3])
    col5, _, col6 = st.columns([2, 1, 3])

    # Select province in the first column
    with col1:
        location_option = st.selectbox(
            "Choose a province:", list(provinces.keys()))

    # Select star rating range in the second column
    with col2:
        rating_range = st.slider(
            "Select star rating range:", min_value=1, max_value=5, value=(1, 5), step=1)

    # Select customer rating range in the second column
    with col4:
        customer_range = st.slider("Select customer rating range:",
                                   min_value=1.0, max_value=10.0, value=(1.0, 10.0), step=0.5)

    # with col3:

    with col5:
        acc_type_option = st.selectbox(
            "Choose a accpmmodation type:", acc_types)

    with col6:
        guests_number = st.slider(
            "Select guests number:", min_value=1, max_value=30, value=(1, 30), step=1)

    col7, col8 = st.columns(2)
    # Line Chart for price in crawled date
    with col7:
        subheader = st.subheader("Line Chart for Price Over Time")
        subcol1, subcol2 = st.columns([4, 6])
        with subcol1:
            future_interal_range = st.slider(
                "Select future interal range:", min_value=0, max_value=13, value=(0, 13), step=1)
        df = postgres_query_line_chart(location=location_option, type=acc_type_option, star_range=rating_range,
                                       customer_rating=customer_range, future_interval=future_interal_range, guests_number=guests_number)

        st.plotly_chart(px.line(df, x='bp_crawled_date', y='avg_price'))

    # Bar Chart for price in future interval
    with col8:
        subheader = st.subheader("Bar Chart for Price by Future Interval")
        df = postgres_query_bar_chart(location=location_option, type=acc_type_option,
                                      star_range=rating_range, customer_rating=customer_range, guests_number=guests_number)
        st.plotly_chart(
            px.bar(df,
                   x=df.columns[0],
                   y=df.columns[1]
                   )
        )
    # * So sánh giá tiền với đánh giá, số sao, diện tích / đầu người -> nên bỏ bao nhiêu tiền sẽ được trải nghiệm tốt nhất
    col9, col10 = st.columns(2)
    df = conn.query("SELECT acm.acm_customer_rating, acm.acm_star_rating, AVG(bp.bp_price) as avg_price FROM public.\"Bed_price\" bp JOIN public.\"Accommodation\" acm ON bp.bp_accommodation_id = acm.acm_id GROUP BY acm.acm_customer_rating, acm.acm_star_rating;", ttl="10m")
    subcol1, subcol2 = st.columns([2, 2])
    with col9:
        st.subheader("Price by Customer Rating and Star Rating")
        st.plotly_chart(
            px.scatter(df, x='acm_customer_rating', y='avg_price')
        )
    with col10:
        st.subheader("Price by Star Rating")
        st.plotly_chart(
            px.scatter(df, x='acm_star_rating', y='avg_price')
        )
    col11, col12, col13 = st.columns(3)
    with col11:
        # Compare price by acc_type
        st.subheader("Average Price by Accommodation Type")
        df = conn.query("SELECT acm.acm_type, AVG(bp.bp_price) as avg_price FROM public.\"Bed_price\" bp JOIN public.\"Accommodation\" acm ON bp.bp_accommodation_id = acm.acm_id GROUP BY acm.acm_type;", ttl="10m")
        st.plotly_chart(
            px.bar(df, x='acm_type', y='avg_price')
        )
    with col12:
        # Percent of price by location
        st.subheader("Average Price by Location")
        df = conn.query("SELECT acm.acm_location, AVG(bp.bp_price) as avg_price FROM public.\"Bed_price\" bp JOIN public.\"Accommodation\" acm ON bp.bp_accommodation_id = acm.acm_id GROUP BY acm.acm_location;", ttl="10m")
        reversed_provinces = {v: k for k, v in provinces.items()}
        df['acm_location'] = df['acm_location'].map(reversed_provinces)
        st.plotly_chart(
            px.bar(df, x='acm_location', y='avg_price')
        )
    with col13:
        # Percent of room by location
        st.subheader("Percent of Room by Location")
        df = conn.query("SELECT acm.acm_location, COUNT(bp.bp_room_id) as room_count FROM public.\"Bed_price\" bp JOIN public.\"Accommodation\" acm ON bp.bp_accommodation_id = acm.acm_id GROUP BY acm.acm_location;", ttl="10m")
        # map province to location
        reversed_provinces = {v: k for k, v in provinces.items()}
        df['acm_location'] = df['acm_location'].map(reversed_provinces)
        df =df.dropna(subset=['acm_location'])
        st.plotly_chart(
            px.pie(df, values='room_count', names='acm_location')
        )
    

    # Average price by guests number
    st.subheader("Average Price by Guests Number")
    df = conn.query("SELECT rm.rm_guests_number, AVG(bp.bp_price) as avg_price FROM public.\"Bed_price\" bp JOIN public.\"Rooms\" rm ON bp.bp_room_id = rm.rm_room_id and bp.bp_accommodation_id = rm.rm_accommodation_id GROUP BY rm.rm_guests_number;", ttl="10m")
    st.plotly_chart(
        px.line(df, x='rm_guests_number', y='avg_price')
    )

