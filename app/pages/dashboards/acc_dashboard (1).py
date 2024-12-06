def show():
    import streamlit as st
    import pandas as pd
    import plotly.express as px
    import psycopg2
    from sqlalchemy import create_engine
    import json
    import plotly.graph_objects as go
    from matplotlib import cm
    from matplotlib.colors import Normalize, to_hex
    from streamlit.components.v1 import html
    import seaborn as sns
    from matplotlib.colors import LinearSegmentedColormap
    import matplotlib.pyplot as plt


    @st.cache_data
    def load_data():
        DB_username = "airflow"
        DB_password = "airflow"
        DB_host = "localhost"
        DB_name = "BookingProject"

        engine = create_engine(f"postgresql+psycopg2://{DB_username}:{DB_password}@{DB_host}:5432/{DB_name}")

        try:
            query_acc = 'SELECT * FROM "Accommodation"'
            query_price = '''
                SELECt * FROM "Bed_price"
            '''
            query_room = 'select * from "Rooms"'
            query_fb = 'SELECT * FROM "Feedback"'
            df_acc = pd.read_sql(query_acc, engine)
            df_price = pd.read_sql(query_price, engine)
            df_fb = pd.read_sql(query_fb, engine)
            df_rooms = pd.read_sql(query_room, engine)

            print("Fetched data successfully!")
            return df_acc, df_price, df_fb, df_rooms
        
        except Exception as e:
            print("An error has occurred: ", e)

    # @st.cache_data
    # def load_data():
    #     path = 'C:/Users/ADMIN/Downloads/'
    #     df_acc = pd.read_csv(path + 'Accommodation.csv')
    #     df_price = pd.read_csv(path + 'bed_prices.csv')
    #     df_fb = pd.read_csv(path + 'feedbacks.csv')
    #     df_rooms = pd.read_csv(path + 'room_types.csv')
    #     return df_acc, df_price, df_fb, df_rooms
    
    df_acc, df_price, df_fb, df_rooms = load_data()

    replace_map = {
        'Phong Nha-Ke Bang National Park': 'Quảng Bình',
        'Phong Nha, Quang Binh, Vietnam': 'Quảng Bình',
        'Vung Tau, Vietnam': 'Vũng Tàu',
        'Vung Tau, Ba Ria - Vung Tau, Vietnam': 'Vũng Tàu',
        'Phu Quoc, Kien Giang , Vietnam': 'Kiên Giang',
        'Sapa, Lao Cai, Vietnam': 'Lào Cai',
        'Da Nang, Vietnam': 'Đà Nẵng',
        'Hoi An, Quang Nam, Vietnam': 'Quảng Nam',
        'Da Lat, Lam Dong, Vietnam': 'Lâm Đồng',
        'Thua Thien Hue, Vietnam': 'Thừa Thiên Huế',
        'Nha Trang, Khanh Hoa, Vietnam': 'Khánh Hòa',
        'Ha Long, Quang Ninh, Vietnam': 'Quảng Ninh'
    }

    df_acc['acm_location'] = df_acc['acm_location'].replace(replace_map)

    acm_type_mapping = {
        '201': 'Apartments',
        '204': 'Hotels',
        '222': 'Homestays',
        '216': 'Guest houses',
        '208': 'Bed and breakfasts',
        '203': 'Hostels',
        '226': 'Love hotels',
        '213': 'Villas',
        '220': 'Holiday homes',
        '225': 'Capsule hotels',
        '205': 'Motels',
        '206': 'Resorts',
        '210': 'Farm stays',
        '214': 'Campsites',
        '221': 'Lodges',
        '228': 'Chalets',
        '224': 'Luxury tents',
        '223': 'Country houses'
    }

    df_acc['acm_type'] = df_acc['acm_type'].replace(acm_type_mapping)
    
    print(df_acc['acm_type'].nunique())
    
    with st.container():
        col1, col2 = st.columns([5, 5])

        # Cột thứ nhất
        with col1:
            # Container đầu tiên chiếm 1/4 chiều cao của container cha
            with st.container():
                # Áp dụng CSS để kiểm soát chiều cao container
                # html("""
                # <style>
                # .container1 {
                #     height: 25vh;  /* Chiều cao 1/4 của container cha */
                #     background-color: #f0f0f0; /* Màu nền tượng trưng */
                #     padding: 10px;
                #     border-radius: 5px;
                # }
                # </style>
                # """, height=150)
                
                col1_sub1, col1_sub2, col1_sub3 = st.columns(3)
                # 1. Tỉnh có nhiều chỗ ở nhất
                most_types = df_acc['acm_location'].value_counts().idxmax()
                most_types_value = df_acc['acm_location'].value_counts().max()

                # 2. Tỉnh có giá phải chăng nhất (trung bình giá thấp nhất)
                average_prices = df_price.merge(df_acc, left_on='bp_accommodation_id', right_on='acm_id')\
                    .groupby('acm_location')['bp_price'].mean()
                most_affordable = average_prices.idxmin()
                most_affordable_value = average_prices.min()

                # 3. Tỉnh có lượt đánh giá tích cực nhiều nhất
                df_fb['positive_feedback'] = df_fb['fb_positive'].apply(lambda x: 1 if pd.notna(x) and x != 'Nothing' else 0)
                positive_feedback_counts = df_fb.merge(df_acc, left_on='fb_accommodation_id', right_on='acm_id')\
                    .groupby('acm_location')['positive_feedback'].sum()
                highest_positive_feedback = positive_feedback_counts.idxmax()
                highest_positive_feedback_value = positive_feedback_counts.max()

                # Tạo dữ liệu tổng hợp
                summary = pd.DataFrame({
                    'Metric': ['Most types', 'Most affordable', 'Highest positive feedback'],
                    'Location': [most_types, most_affordable, highest_positive_feedback],
                    'Value': [most_types_value, most_affordable_value, highest_positive_feedback_value]
                })
                
                # Nội dung của từng cột
                with col1_sub1:
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #FF0000, #FFD700);
                        flex-direction: column;
                        padding: 10px;
                        border-radius: 10px;
                        text-align: center;
                        justify-content: center;
                        align-items: center";
                        background: linear-gradient(135deg, #6C63FF, #A898F8)>
                        <p style="margin: 0; color: #fff;">Most types</p>
                        <h3 style="margin: 0; color: #fff;">{most_types}</h3>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col1_sub2:
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #FF0000, #FF8C00);
                        flex-direction: column;
                        padding: 10px;
                        border-radius: 10px;
                        text-align: center;
                        justify-content: center;
                        align-items: center">
                        <p style="margin: 0; color: #fff;">Most affordable</p>
                        <h3 style="margin: 0; color: #fff;">{most_affordable}</h3>
                    </div>
                    """, unsafe_allow_html=True)
                    
                with col1_sub3:
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #FFD700, #FF8C00);
                        flex-direction: column;
                        padding: 10px;
                        border-radius: 10px;
                        text-align: center;
                        justify-content: center;
                        align-items: center">
                        <p style="margin: 0; color: #fff;">Highest positive feedback</p>
                        <h3 style="margin: 0; color: #fff;">{highest_positive_feedback}</h3>
                    </div>
                    """, unsafe_allow_html=True)
                
            # Container thứ hai chiếm phần còn lại và được chia thành 4 ô
            with st.container():
                st.markdown("""
                    <style>
                    .row-container {
                        margin: 0;  /* Xóa khoảng cách dọc */
                        padding: 0; /* Xóa padding giữa các container */
                    }
                    </style>
                    """, unsafe_allow_html=True)
                
                #########################################################
                row1, row2 = st.columns(2)  # Hàng 1
                room_with_star = df_rooms.merge(df_acc[['acm_id', 'acm_star_rating', 'acm_customer_rating', 'acm_location', 'acm_review_count', 'acm_type']],
                                                left_on='rm_accommodation_id',
                                                right_on='acm_id',
                                                how='left')
                room_with_price = df_price.merge(room_with_star, left_on='bp_room_id', right_on='rm_room_id', how='left')

                ########################################################
                row3, row4 = st.columns(2)  # Hàng 2
                price_by_location_type = (
                    room_with_price.groupby(['acm_location', 'acm_type'])['bp_price']
                    .mean()
                    .reset_index()
                )

                top_acm_types = price_by_location_type.groupby('acm_type')['bp_price'].mean().nlargest(5).index
                filtered_price_by_type = price_by_location_type[price_by_location_type['acm_type'].isin(top_acm_types)]
                
                # Vẽ 4 biểu đồ
                with row1:
                    avg_price_by_star = room_with_price.groupby('acm_star_rating')['bp_price'].mean().reset_index()

                    fig_bar1 = px.bar(
                        avg_price_by_star,
                        x='acm_star_rating',
                        y='bp_price',
                        title='Average price by star rating',
                        labels={'acm_star_rating': 'Star rating', 'bp_price': 'Average price (VND)'},
                        text=None,
                        color='bp_price',
                        color_continuous_scale='Oranges'
                    )

                    fig_bar1.update_layout(
                        xaxis_title="Star rating",
                        yaxis_title="Average price (VND)",
                        coloraxis_showscale=False
                    )
                    
                    st.plotly_chart(fig_bar1)

                    
                with row2:
                    room_with_price['acm_customer_rating'] = room_with_price['acm_customer_rating'].round()
                    avg_price_by_rating = room_with_price.groupby('acm_customer_rating')['bp_price'].mean().reset_index()

                    fig_bar2 = px.bar(
                        avg_price_by_rating,
                        x='acm_customer_rating',
                        y='bp_price',
                        title='Average price by score rating',
                        labels={'acm_customer_rating': 'Score', 'bp_price': 'Average price (VND)'},
                        text=None,
                        color='acm_customer_rating',
                        color_continuous_scale='Reds'
                    )

                    fig_bar2.update_layout(
                        xaxis_title="Score rating",
                        yaxis_title="Average price (VND)",
                        coloraxis_showscale=False
                    )

                    st.plotly_chart(fig_bar2)
                
                st.markdown('<div class="row-container"></div>', unsafe_allow_html=True)  # Thêm khoảng cách nhỏ giữa các hàng

        # Cột thứ hai
        with col2:
            row3, row4 = st.columns(2)  # Hàng 2
            price_by_location_type = (
                room_with_price.groupby(['acm_location', 'acm_type'])['bp_price']
                .mean()
                .reset_index()
            )

            top_acm_types = price_by_location_type.groupby('acm_type')['bp_price'].mean().nlargest(5).index
            filtered_price_by_type = price_by_location_type[price_by_location_type['acm_type'].isin(top_acm_types)]
            
            with row3:
                fig_bar3 = px.bar(
                    filtered_price_by_type,
                    x='acm_location',
                    y='bp_price',
                    color='acm_type',
                    barmode='group',
                    title='Average Price by Top 5 Accommodation Types and Location',
                    labels={'acm_location': 'Location', 'bp_price': 'Average Price (VND)', 'acm_type': 'Accommodation Type'},
                    color_continuous_scale='YlOrBr',
                    height=570
                )
                fig_bar3.update_layout(
                    xaxis_tickangle=45,
                    xaxis_title=None,
                    yaxis_title=None,
                    showlegend=False,
                    coloraxis_showscale=False
                )
                st.plotly_chart(fig_bar3)
                
            with row4:
                low_acm_types = price_by_location_type.groupby('acm_type')['bp_price'].mean().nsmallest(5).index
                filtered_price_by_low_type = price_by_location_type[price_by_location_type['acm_type'].isin(low_acm_types)]

                fig_bar4 = px.bar(
                    filtered_price_by_low_type,
                    x='acm_location',
                    y='bp_price',
                    color='acm_type',
                    barmode='group',
                    title='Average Price by Bottom 5 Accommodation Types and Location',
                    labels={'acm_location': 'Location', 'bp_price': 'Average Price (VND)', 'acm_type': 'Accommodation Type'},
                    color_continuous_scale='OrRd',
                    height=570        
                )
                    
                fig_bar4.update_layout(
                    xaxis_tickangle=45,
                    xaxis_title=None,
                    showlegend=False,
                    coloraxis_showscale=False
                )
                    
                st.plotly_chart(fig_bar4)
                
    with st.container():
        row1, row2, row3, row4, row5 = st.columns([1,1,1,1,1])
        
        def plot_accommodation_map(df, province, map_style="carto-positron"):
            province_data = df[df['acm_location'] == province]
            
            type_counts = province_data['acm_type'].value_counts(normalize=True)
            
            top_types = type_counts.head(7).index
            province_data['acm_type_grouped'] = province_data['acm_type'].apply(
                lambda x: x if x in top_types else 'Other'
            )
            
            # Vẽ map bằng Plotly Express
            fig = px.scatter_mapbox(
                province_data,
                lat='acm_lat',
                lon='acm_long',
                color='acm_type_grouped',
                size_max=15,
                zoom=12,
                mapbox_style=map_style,
                title=f"Accommodation Map of {province}",
                color_discrete_sequence=px.colors.sequential.Oranges
            )
            fig.update_layout(showlegend=False)
            return fig
        
        with row1:
            fig_map = plot_accommodation_map(df_acc, 'Quảng Ninh', map_style="carto-positron")
            st.plotly_chart(fig_map)
            
        with row2:
            fig_map = plot_accommodation_map(df_acc, 'Quảng Bình', map_style="carto-positron")
            st.plotly_chart(fig_map)
        
        with row3:
            fig_map = plot_accommodation_map(df_acc, 'Vũng Tàu', map_style="carto-positron")
            st.plotly_chart(fig_map)
        
        with row4:
            fig_map = plot_accommodation_map(df_acc, 'Kiên Giang', map_style="carto-positron")
            st.plotly_chart(fig_map)
        
        with row5:
            fig_map = plot_accommodation_map(df_acc, 'Lào Cai', map_style="carto-positron")
            st.plotly_chart(fig_map)
            
        row6, row7, row8, row9, row10 = st.columns([1,1,1,1,1])
        
        with row6:
            fig_map = plot_accommodation_map(df_acc, 'Đà Nẵng', map_style="carto-positron")
            st.plotly_chart(fig_map)
        
        with row7:
            fig_map = plot_accommodation_map(df_acc, 'Quảng Nam', map_style="carto-positron")
            st.plotly_chart(fig_map)
        
        with row8:
            fig_map = plot_accommodation_map(df_acc, 'Lâm Đồng', map_style="carto-positron")
            st.plotly_chart(fig_map)
    
        with row9:
            fig_map = plot_accommodation_map(df_acc, 'Thừa Thiên Huế', map_style="carto-positron")
            st.plotly_chart(fig_map)
        
        with row10:
            fig_map = plot_accommodation_map(df_acc, 'Khánh Hòa', map_style="carto-positron")
            st.plotly_chart(fig_map)