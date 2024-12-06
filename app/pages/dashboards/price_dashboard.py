import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging as log
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

def remove_outliers(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    mask = (df[column] >= Q1 - 1.5 * IQR) & (df[column] <= Q3 + 1.5 * IQR)
    return df[mask]

def price_over_checkin_query():
    base = """
    SELECT 
        bp.bp_future_interval, bp.bp_checkin, AVG(bp.bp_price) as avg_price, COUNT(*) as room_count, acm.acm_group
    FROM 
        public."Bed_price" bp
    JOIN 
        public."Rooms" rm ON bp.bp_room_id = rm.rm_room_id and bp.bp_accommodation_id = rm.rm_accommodation_id
    JOIN 
        public."Accommodation" acm ON bp.bp_accommodation_id = acm.acm_id
    GROUP BY 
        bp.bp_future_interval, bp.bp_checkin, acm.acm_group
    """
    
    df = conn.query(base, ttl="10m")
    # print(df)
    return df
def price_over_location_query():
    base = """
    SELECT acm.acm_location, bp.bp_accommodation_id, bp.bp_room_id, AVG(bp.bp_price) as avg_price
    FROM public."Bed_price" bp
    JOIN
        public."Accommodation" acm ON bp.bp_accommodation_id = acm.acm_id
    GROUP BY bp.bp_accommodation_id, bp.bp_room_id, acm.acm_location
    """
    df = conn.query(base, ttl="10m")
    return df

def price_over_future_interval_query():
    base = """
    SELECT bp.bp_future_interval,  acm.acm_type, AVG(bp.bp_price) as avg_price
    FROM public."Bed_price" bp
    JOIN
        public."Accommodation" acm ON bp.bp_accommodation_id = acm.acm_id
    GROUP BY bp.bp_future_interval,  acm.acm_type
    """
    df = conn.query(base, ttl="10m")
    return df

def price_over_star_rating_query():
    base = """
    SELECT acm.acm_id, acm.acm_star_rating, acm.acm_customer_rating, AVG(bp.bp_price) as avg_price
    FROM public."Bed_price" bp
    JOIN
        public."Accommodation" acm ON bp.bp_accommodation_id = acm.acm_id
    GROUP BY acm.acm_id, acm.acm_star_rating, acm.acm_customer_rating
    """
    df = conn.query(base, ttl="10m")
    return df



def min_max_scale_to_neg1_1(series):
    """Scale values to [-1,1] range"""
    min_val = series.min()
    max_val = series.max()
    if min_val == max_val:
        return series * 0  # Return zeros if all values are the same
    return 2 * (series - min_val) / (max_val - min_val) - 1





def create_collapsible_container(title, content_func):
    with st.expander(title, expanded=False):
        return content_func()


def show(header):

    if 'location_option' not in st.session_state:
        st.session_state.location_option = list(provinces.keys())
        st.session_state.location_option.pop(0)

    # with header:
    #     def filter_widgets():
    #         def update_selection(selected_option):
    #             if "All" in selected_option and len(selected_option) > 1:
    #                 if selected_option[0] == "All":
    #                     selected_option.pop(0)
    #                 else:
    #                     st.session_state.location_option = ["All"]
    #         col1, _, col2 = st.columns([4, 2, 4])
    #         with col1:
    #             location_option = st.multiselect(
    #                 "Choose provinces:",
    #                 options=provinces,
    #                 default=st.session_state.location_option,
    #                 on_change=lambda: update_selection(
    #                     st.session_state.location_option),
    #                 key="location_option",
    #             )
    #         with col2:
    #             acc_type_option = st.selectbox(
    #                 "Choose accommodation type:", options=acc_types)
    #         col3, _, col4 = st.columns([4, 2, 4])
    #         with col3:
           
    #             rating_range = st.slider(
    #                 "Select star rating range:", min_value=1, max_value=5, value=(1, 5), step=1)
    #         with col4:
    #             customer_range = st.slider("Select customer rating range:",
    #                                     min_value=1.0, max_value=10.0, value=(1.0, 10.0), step=0.5)
    #         col5, _, col6 = st.columns([4, 2, 4])
    #         with col5:
    #             guests_number = st.slider(
    #                 "Select guests number:", min_value=1, max_value=20, value=(1, 20), step=1)
    #         with col6:
    #             min_review_count = st.number_input(
    #                 "Enter minimum review count:", value=0, step=1)

    #         return location_option, acc_type_option, rating_range, customer_range, guests_number, min_review_count

        # filters = create_collapsible_container("Filters", filter_widgets)
        # location_option, acc_type_option, rating_range, customer_range, guests_number, min_review_count = filters

    df = price_over_checkin_query()
    df = df.groupby(['bp_checkin', 'acm_group']).mean().reset_index()


    # Create subplots: 2 rows, 1 column, shared x-axis
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05)
    x = df['bp_checkin'].unique()
    groups = df['acm_group'].unique()
    
    # Add line chart with hover labels
    for group in groups:
        temp = df[df['acm_group'] == group]
        y_line = temp.groupby('bp_checkin')['avg_price'].mean()
        
        fig.add_scatter(
            x=x, 
            y=y_line, 
            mode='lines',
            name=group, 
            row=1, 
            col=1, 
            line_shape='spline'
        )

    # Add bar chart with showlegend=False 
    for group in groups:
        temp = df[df['acm_group'] == group]
        fig.add_trace(go.Bar(
            x=temp['bp_checkin'],
            y=temp['room_count'],
            name=f'{group} Count',
            showlegend=False,
        ), row=2, col=1)

    # Update layout
    fig.update_layout(
        barmode='stack',
        yaxis_title='Price', 
        xaxis2_title='Checkin date', 
        yaxis2_title='Room Count',
        title_text='Price Over Time',
        height=400,
        legend=dict(
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            orientation="h"
        ),
    )

    col1, col2, col3 = st.columns([6, 3, 3])
    with col1:
        st.plotly_chart(fig, use_container_width=True)


    room_count_df = price_over_location_query()
    room_count_df['acm_location'] = room_count_df['acm_location'].map(
        {v: k for k, v in provinces.items()})
    
    # Box plot for price by location
    fig = px.box(remove_outliers(room_count_df, 'avg_price'), x='acm_location', y='avg_price',
                 title='Price by Location')
    
    # Create pie chart
    room_count_df = room_count_df.groupby('acm_location')['avg_price'].count().reset_index()
    fig_1 = px.pie(room_count_df, values='avg_price', names='acm_location',
                   title='Room Count by Location')
    fig_1.update_traces(textposition='inside', textinfo='percent+label')


    with col2:
        # subcol1, subcol2 = st.columns(2)
        fig_1.update_layout(
            margin=dict(r=0),
            height=400,
            template='plotly'
        )

        st.plotly_chart(fig_1)


    col6, col7, col8 = st.columns([3, 3, 6])
    with col3:
        fig.update_layout(
            margin=dict(r=0),
            height=400
        )
        st.plotly_chart(fig)
    df = price_over_star_rating_query()
    df = remove_outliers(df, 'avg_price')
    with col6:
        fig =  px.scatter(df, x='acm_customer_rating', y='avg_price', title='Price by Customer Rating')
        fig.update_layout(
            margin=dict(r=0),
            height=400
        )
        st.plotly_chart(
           fig
        )
    with col7:
        # box plot for price by star rating
        fig = px.box(df, x='acm_star_rating', y='avg_price',
                     title='Price by Star Rating')
        fig.update_layout(
            margin=dict(r=0),
            height=400
        )
        st.plotly_chart(fig)
    with col8:
        # Bubble chart for price by star rating and customer rating
        fig = px.scatter(df, x='acm_star_rating', y='acm_customer_rating', size='avg_price',
                        title='Price by Star Rating and Customer Rating')
        fig.update_layout(
            margin=dict(r=0),
            height=400
        )
        st.plotly_chart(fig)
            

    # Add calendar section before bar chart
    # with col5:
    df = price_over_future_interval_query()

    df['bp_future_interval'] = df['bp_future_interval'].astype(str)
    desired_y_axis = ["0d", "1d", "2d", "5d", "7d", "14d", "30d"]
    # Convert 0, 1, 2, 5, 7, 14, 30 to 0d, 1d, 2d, 5d, 7d, 14d, 30d
    df['bp_future_interval'] = df['bp_future_interval'].apply(lambda x: f"{x}d")
    heatmap_data = (
        df.pivot(index='bp_future_interval', columns='acm_type', values='avg_price')
        .reindex(index=desired_y_axis, fill_value=0)  # Fill missing values with 0 (or NaN)
    ).apply(min_max_scale_to_neg1_1)
    fig = px.imshow(heatmap_data, 
                    labels=dict(color="Price"), 
                    y=desired_y_axis,
                    color_continuous_scale='Blues',  # Red for decrease, Blue for increase
                color_continuous_midpoint=0,    # Center color scale at 0
                    aspect='auto',
                    title="Price by Future Interval"
    )
    fig.update_layout(
        xaxis_showticklabels=True,
        yaxis_showticklabels=True,
        xaxis_title="",
        yaxis_title=""
    )
    st.plotly_chart(fig, use_container_width=True)
    