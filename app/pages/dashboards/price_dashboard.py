import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import os
from streamlit_calendar import calendar
from datetime import datetime, timedelta

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

#  "Vung Tau, Ba Ria - Vung Tau, Vietnam": "lang=en-gb&dest_id=-3733750&dest_type=CITY",
#     "Da Nang, Vietnam": "lang=en-gb&dest_id=-3712125&dest_type=CITY",
#     "Phu Quoc, Kien Giang , Vietnam": "lang=en-gb&dest_id=-3726177&dest_type=CITY",
#     "Sapa, Lao Cai, Vietnam": "lang=en-gb&dest_id=-3728113&dest_type=CITY",
#     "Phong Nha, Quang Binh, Vietnam": "lang=en-gb&dest_id=-3725312&dest_type=CITY"
provinces = {
    "All": "All",
    "Hạ Long": "Ha Long, Quang Ninh, Vietnam",
    "Hội An": "Hoi An, Quang Nam, Vietnam",
    "Thừa Thiên Huế": "Thua Thien Hue, Vietnam",
    "Nha Trang": "Nha Trang, Khanh Hoa, Vietnam",
    "Đà Lạt": "Da Lat, Lam Dong, Vietnam",
    "Vũng Tàu": "Vung Tau, Ba Ria - Vung Tau, Vietnam",
    "Đà Nẵng": "Da Nang, Vietnam",
    "Phú Quốc": "Phu Quoc, Kien Giang , Vietnam",
    "Sapa": "Sapa, Lao Cai, Vietnam",
    "Phong Nha": "Phong Nha, Quang Binh, Vietnam"
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
    SELECT bp.bp_crawled_date, AVG(bp.bp_price) as avg_price, COUNT(bp.bp_price) as room_count, acm.acm_location
    FROM public."Bed_price" bp
    JOIN public."Rooms" rm ON bp.bp_room_id = rm.rm_room_id and bp.bp_accommodation_id = rm.rm_accommodation_id
    JOIN public."Accommodation" acm ON bp.bp_accommodation_id = acm.acm_id
    """
    conditions = []
    if location is not None and location != ["All"]:
        location_condition = " OR ".join(
            f"acm.acm_location = '{provinces[loc]}'" for loc in location)
        conditions.append(location_condition)

    if type is not None and type != "All":
        conditions.append(f"acm.acm_type = '{type}'")

    if star_range is not None:
        conditions.append(f"acm.acm_star_rating BETWEEN {star_range[0]} AND {star_range[1]}")

    if customer_rating is not None:
        conditions.append(f"acm.acm_customer_rating BETWEEN {customer_rating[0]} AND {customer_rating[1]}")
    if future_interval is not None:
        conditions.append(f"bp.bp_future_interval BETWEEN {future_interval[0]} AND {future_interval[1]}")
    if guests_number is not None:
        conditions.append(f"rm.rm_guests_number BETWEEN {guests_number[0]} AND {guests_number[1]}")
    # if review_count is not None:
        # conditions.append(f"acm.acm_review_count BETWEEN {review_count[0]} AND {review_count[1]}")
    if conditions:
        base += " WHERE " + " AND ".join("("+c+")" for c in conditions)
    base += " GROUP BY bp.bp_crawled_date, acm.acm_location"
    # print(base)
    df = conn.query(base, ttl="10m")
    # print(df)
    return df


def postgres_query_bar_chart(location=None, type=None, star_range=None, customer_rating=None, review_count=None, guests_number=None, checkin_date=None):
    base = """
    SELECT 
        bp.bp_future_interval, 
        AVG(bp.bp_price) as avg_price, 
        acm.acm_location, 
        count(bp.bp_room_id) as room_count,
        bp.bp_crawled_date + bp.bp_future_interval * INTERVAL '1 day' as bp_checkin_date
    FROM 
        public."Bed_price" bp
    JOIN 
        public."Rooms" rm ON bp.bp_room_id = rm.rm_room_id and bp.bp_accommodation_id = rm.rm_accommodation_id
    JOIN 
        public."Accommodation" acm ON bp.bp_accommodation_id = acm.acm_id
    """
    conditions = []
    if location is not None and location != ["All"]:
        location_condition = " OR ".join(
            f"acm.acm_location = '{provinces[loc]}'" for loc in location)
        conditions.append(location_condition)
    if type is not None and type != "All":
        conditions.append(f"acm.acm_type = '{type}'")
    if star_range is not None:
        conditions.append(f"acm.acm_star_rating BETWEEN {star_range[0]} AND {star_range[1]}")
    if customer_rating is not None:
        conditions.append(f"acm.acm_customer_rating BETWEEN {customer_rating[0]} AND {customer_rating[1]}")
    if guests_number is not None:
        conditions.append(f"rm.rm_guests_number BETWEEN {guests_number[0]} AND {guests_number[1]}")
    if checkin_date is not None:
        conditions.append(f"bp_checkin_date = '{checkin_date}'")
    # if review_count is not None:
        # conditions.append(f"acm.acm_review_count BETWEEN {review_count[0]} AND {review_count[1]}")
    if conditions:
        base += " WHERE " + " AND ".join("("+c+")" for c in conditions)
    base += " GROUP BY bp.bp_future_interval, acm.acm_location, bp_checkin_date"
    df = conn.query(base, ttl="10m")
    return df


def postgres_query_calendar_prices(location=None, type=None, star_range=None, customer_rating=None, guests_number=None):
    base = """
    SELECT bp.bp_crawled_date + bp.bp_future_interval * INTERVAL '1 day' AS bp_checkin_date, avg(bp.bp_price) as avg_price
    FROM public."Bed_price" bp
    """
    conditions = []
    if location is not None and location != ["All"]:
        location_condition = " OR ".join(
            f"acm.acm_location = '{provinces[loc]}'" for loc in location)
        conditions.append(location_condition)
    # ...existing condition checks...
    # if conditions:
    #     base += " WHERE " + " AND ".join("("+c+")" for c in conditions)
    base += " GROUP BY bp_checkin_date ORDER BY bp_checkin_date"
    return conn.query(base, ttl="10m")


def create_collapsible_container(title, content_func):
    with st.expander(title, expanded=False):
        return content_func()


def show(header):

    if 'location_option' not in st.session_state:
        st.session_state.location_option = list(provinces.keys())
        st.session_state.location_option.pop(0)

    with header:
        def filter_widgets():
            def update_selection(selected_option):
                if "All" in selected_option and len(selected_option) > 1:
                    if selected_option[0] == "All":
                        selected_option.pop(0)
                    else:
                        st.session_state.location_option = ["All"]

            location_option = st.multiselect(
                "Choose provinces:",
                options=provinces,
                default=st.session_state.location_option,
                on_change=lambda: update_selection(
                    st.session_state.location_option),
                key="location_option",
            )
            acc_type_option = st.selectbox(
                "Choose accommodation type:", options=acc_types)
            rating_range = st.slider(
                "Select star rating range:", min_value=1, max_value=5, value=(1, 5), step=1)
            customer_range = st.slider("Select customer rating range:",
                                       min_value=1.0, max_value=10.0, value=(1.0, 10.0), step=0.5)
            guests_number = st.slider(
                "Select guests number:", min_value=1, max_value=30, value=(1, 30), step=1)

            return location_option, acc_type_option, rating_range, customer_range, guests_number

        filters = create_collapsible_container("Filters", filter_widgets)
        location_option, acc_type_option, rating_range, customer_range, guests_number = filters

    df = postgres_query_line_chart(location=location_option, type=acc_type_option, star_range=rating_range,
                                   customer_rating=customer_range, guests_number=guests_number)

    # Create subplots: 2 rows, 1 column, shared x-axis
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05)
    x = df['bp_crawled_date'].unique()

    # Add line chart
    for location in location_option:
        if location == "All":
            df_location = df
        else:
            df_location = df[df['acm_location'] == provinces[location]]

        y_line = df_location.groupby('bp_crawled_date')['avg_price'].mean()

        fig.add_scatter(x=x, y=y_line, mode='lines+markers',
                        name=location, row=1, col=1, line_shape='spline')
    # Add bar chart
    y_bar = df['room_count'].groupby(df['bp_crawled_date']).sum()
    fig.add_trace(go.Bar(x=x,
                         y=y_bar,
                         name='Room Count',
                         marker_color='blue',
                         ), row=2, col=1)
    fig.update_layout(
        yaxis_title='Price', xaxis2_title='Crawled Date', yaxis2_title='Room Count',
        showlegend=True,
        title_text='Price Over Time',
        height=800,
        # Move legend to the center
    )
    col1, col2 = st.columns([6, 4])
    with col1:
        st.plotly_chart(fig, use_container_width=True)

    room_count_df = df.groupby('acm_location')['room_count'].sum()
    room_count_df = room_count_df.reset_index()
    room_count_df['acm_location'] = room_count_df['acm_location'].map(
        {v: k for k, v in provinces.items()})
    # Create pie chart
    fig_1 = px.pie(room_count_df, values='room_count', names='acm_location',
                   title='Room Count by Location')
    fig_1.update_traces(textposition='inside', textinfo='percent+label')

    # Bar chart for avg price by location
    avg_price_df = df.groupby('acm_location')['avg_price'].mean()
    avg_price_df = avg_price_df.reset_index()
    avg_price_df['acm_location'] = avg_price_df['acm_location'].map(
        {v: k for k, v in provinces.items()})
    fig = px.bar(avg_price_df, x='acm_location', y='avg_price',
                 title='Average Price by Location')

    with col2:
        # subcol1, subcol2 = st.columns(2)
        fig_1.update_layout(
            margin=dict(r=0),
            height=400
        )
        fig.update_layout(
            margin=dict(r=0),
            height=400
        )
        # with subcol1:
        #     st.plotly_chart(fig_1)
        # with subcol2:
        #     st
        st.plotly_chart(fig_1)
        st.plotly_chart(fig)

    # Bar Chart for price in future interval
    # subheader = st.subheader("Bar Chart for Price by Future Interval")
    # df = postgres_query_bar_chart(location=location_option, type=acc_type_option,
    #                             star_range=rating_range, customer_rating=customer_range, guests_number=guests_number)
    # x = df['bp_future_interval'].unique()
    # fig = make_subplots(
    #     rows=2, cols=1, shared_xaxes=True, vertical_spacing=0)
    # if location_option == ["All"]:
    #     y = df.groupby('bp_future_interval')['avg_price'].mean()
    #     fig.add_trace(go.Scatter(x=x,
    #                                 y=y,
    #                                 mode='lines',
    #                                 name='All',
    #                                 ), row=1, col=1)
    # else:
    #     for location in location_option:
    #         df_location = df[df['acm_location'] == provinces[location]]
    #         y = df_location.groupby('bp_future_interval')['avg_price'].mean()
    #         fig.add_scatter(x=x,y=y,mode='lines',name=location, row=1, col=1)

    # y_bar = df['room_count'].groupby(df['bp_future_interval']).sum()
    # fig.add_trace(go.Bar(x=x,
    #                         y=y_bar,
    #                         name='Room Count',
    #                         marker_color = 'blue'
    #     ), row=2, col=1)
    # fig.update_layout(
    #     xaxis2_title='Future Interval', yaxis_title='Price', yaxis2_title='Room Count', showlegend=True,
    #         legend=dict(
    #             yanchor="bottom",
    #             y=0.99,
    #             xanchor="right",
    #             x=1.05
    #         ),
    #         modebar=dict(
    #             orientation='v',
    #             # position='right'
    #         ),
    #         margin=dict(r=0)  # Reduce right margin
    # )
    # st.plotly_chart(fig, use_container_width=True)

    # Add calendar section before bar chart
    st.subheader("Price Calendar")

    # Get price data for calendar
    col3, col4 = st.columns([4, 6])
    with col3:
        st.write("Select a date to view price")
        calendar_df = postgres_query_calendar_prices(
            location=location_option,
            type=acc_type_option,
            star_range=rating_range,
            customer_rating=customer_range,
            guests_number=guests_number
        )

        # Convert prices to calendar events
        calendar_events = []
        for _, row in calendar_df.iterrows():
            date_str = row['bp_checkin_date'].strftime("%Y-%m-%d")
            calendar_events.append({
                "title": f"{row['avg_price']:.0f}",
                "start": date_str,
                "allDay": True,
                "backgroundColor": f"rgba(0, 128, 0, {min(row['avg_price']/calendar_df['avg_price'].max(), 1)})"
            })

        calendar_options = {
            "headerToolbar": {
                "left": "prev,next today",
                "center": "title",
                "right": "dayGridMonth"
            },
            "initialView": "dayGridMonth",
            "selectable": True,
            "dateClick": True
        }

        custom_css = """
            .fc-daygrid-day:hover {
                background-color: #f0f0f0;
                cursor: pointer;
            }
            .fc-event-title {
                font-weight: bold;
                text-align: center;
            }
        """

        selected_date = calendar(
            events=calendar_events,
            options=calendar_options,
            custom_css=custom_css
        )


# {'callback': 'select', 
#  'select': {'allDay': True, 'start': '2024-11-18T17:00:00.000Z', 'end': '2024-11-19T17:00:00.000Z', 'view': {'type': 'dayGridMonth', 'title': 'November 2024',
#     'activeStart': '2024-10-26T17:00:00.000Z', 'activeEnd': '2024-12-07T17:00:00.000Z', 'currentStart': '2024-10-31T17:00:00.000Z', 'currentEnd': '2024-11-30T17:00:00.000Z'
#     }
# }
# }
  # Store selected date in session state
    if selected_date and 'select' in selected_date:
        # convert selected date to date time
        selected_date = datetime.strptime(selected_date['select']['end'], "%Y-%m-%dT%H:%M:%S.%fZ")
        selected_date = selected_date.strftime("%Y-%m-%d")
        st.session_state.selected_date = selected_date

    # Update existing bar chart code to use selected date
    if 'selected_date' in st.session_state:
        df = postgres_query_bar_chart(
            location=location_option,
            type=acc_type_option,
            star_range=rating_range,
            customer_rating=customer_range,
            guests_number=guests_number,
            checkin_date=st.session_state.selected_date
        )
        df = df.groupby('bp_future_interval')['avg_price'].mean()
        fig = px.bar(df, x=df.index, y='avg_price',
                     title='Average Price by Future Interval')
        fig.update_layout(
            xaxis_title='Checkin Date', yaxis_title='Price',
            showlegend=False,
            margin=dict(r=0)
        )
        col4.plotly_chart(fig, use_container_width=True)

        # # * So sánh giá tiền với đánh giá, số sao, diện tích / đầu người -> nên bỏ bao nhiêu tiền sẽ được trải nghiệm tốt nhất
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
        df = df.dropna(subset=['acm_location'])
        st.plotly_chart(
            px.pie(df, values='room_count', names='acm_location')
        )

    # Average price by guests number
    st.subheader("Average Price by Guests Number")
    df = conn.query("SELECT rm.rm_guests_number, AVG(bp.bp_price) as avg_price FROM public.\"Bed_price\" bp JOIN public.\"Rooms\" rm ON bp.bp_room_id = rm.rm_room_id and bp.bp_accommodation_id = rm.rm_accommodation_id GROUP BY rm.rm_guests_number;", ttl="10m")
    st.plotly_chart(
        px.line(df, x='rm_guests_number', y='avg_price')
    )
