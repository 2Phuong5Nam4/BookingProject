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
    import pydeck as pdk
    import pycountry_convert as pc
    from plotly.colors import sample_colorscale

    def postgres_query_accommodation(location=None, type=None, star_range=None, customer_rating=None, guests_number=None, review_count=None):
        conn = st.connection("postgresql", type="sql")
        base = """
            SELECT 
            a.acm_id, 
            a.acm_lat, 
            a.acm_long, 
            a.acm_location, 
            a.acm_name,
            f.fb_accommodation_id, 
            f.fb_positive, 
            f.fb_negative,
            f.fb_nationality
        FROM 
            public."Accommodation" a
        INNER JOIN 
            public."Feedback" f
        ON 
            a.acm_id = f.fb_accommodation_id;
        """
        # conditions = []
        
        # if location is not None and location != ["All"]:
        #     location_condition = " OR ".join(
        #         f"acm.acm_location = '{provinces[loc]}'" for loc in location)
        #     conditions.append(location_condition)
        # if type is not None and type != "All":
        #     conditions.append(f"acm.acm_type = '{type}'")
        # if star_range is not None:
        #     conditions.append(f"acm.acm_star_rating BETWEEN {star_range[0]} AND {star_range[1]}")
        # if customer_rating is not None:
        #     conditions.append(f"acm.acm_customer_rating BETWEEN {customer_rating[0]} AND {customer_rating[1]}")
        # # if future_interval is not None:
        # #     conditions.append(f"bp.bp_future_interval BETWEEN {future_interval[0]} AND {future_interval[1]}")
        # if guests_number is not None:
        #     conditions.append(f"rm.rm_guests_number BETWEEN {guests_number[0]} AND {guests_number[1]}")
        # if review_count is not None:
        #     conditions.append(f"acm.acm_review_count >= {review_count}")
        # if conditions:
        #     base += " WHERE " + " AND ".join("("+c+")" for c in conditions)
        # base += " GROUP BY bp_checkin_date ORDER BY bp_checkin_date"
        return conn.query(base, ttl="10m")

    def create_collapsible_container(title, content_func):
        with st.expander(title, expanded=False):
            return content_func()

    def preprocess(inner_join):
        result_df = inner_join.groupby('fb_accommodation_id')[['fb_positive', 'fb_negative']].count().reset_index()
        result_df = pd.merge(result_df, inner_join[['fb_accommodation_id', 'acm_lat', 'acm_long', 'acm_name', 'acm_location']], on='fb_accommodation_id', how='left').drop_duplicates(subset='fb_accommodation_id')
        result_df['pos_ratio'] = result_df['fb_positive'] / (result_df['fb_positive'] + result_df['fb_negative']) * 100
        result_df['pos_ratio'] = result_df['pos_ratio'].round(2)  # Round to 2 decimal places

        result_df.rename(columns={'acm_lat': 'lat', 'acm_long': 'lon'}, inplace=True)
        return result_df
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
    replace_titel = {
        'Quảng Bình': 'Phong Nha',
        'Vũng Tàu': 'Vũng Tàu',
        'Kiên Giang': 'Phú Quốc',
        'Lào Cai': 'Sa Pa',
        'Đà Nẵng': 'Đà Nẵng',
        'Quảng Nam': 'Hội An',
        'Lâm Đồng': 'Đà Lạt',
        'Thừa Thiên Huế': 'Huế',
        'Khánh Hòa': 'Nha Trang',
        'Quảng Ninh': 'Hạ Long'

    }


    df_acc['acm_location'] = df_acc['acm_location'].replace(replace_map)



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
                most_types = df_acc['acm_location'].replace(replace_titel).value_counts().idxmax()

                # 2. Tỉnh có giá phải chăng nhất (trung bình giá thấp nhất)
                average_prices = df_price.merge(df_acc, left_on='bp_accommodation_id', right_on='acm_id')\
                    .groupby('acm_location')['bp_price'].mean()
                most_affordable = average_prices.idxmin()

                # 3. Tỉnh có lượt đánh giá tích cực nhiều nhất
                df_fb['positive_feedback'] = df_fb['fb_positive'].apply(lambda x: 1 if pd.notna(x) and x != 'Nothing' else 0)
                positive_feedback_counts = df_fb.merge(df_acc, left_on='fb_accommodation_id', right_on='acm_id')\
                    .groupby('acm_location')['positive_feedback'].sum()
                highest_positive_feedback = positive_feedback_counts.idxmax()
                
                # Nội dung của từng cột
                with col1_sub1:
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #0000FF, #87CEFA);
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
                        background: linear-gradient(135deg, #4682B4, #B0E0E6);
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
                        background: linear-gradient(135deg, #1E90FF, #ADD8E6);
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
                row1, row2 = st.columns([3,4])  # Hàng 1
                room_with_star = df_rooms.merge(df_acc[['acm_id', 'acm_star_rating', 'acm_customer_rating', 'acm_location', 'acm_review_count', 'acm_group']],
                                                left_on='rm_accommodation_id',
                                                right_on='acm_id',
                                                how='left')
                room_with_price = df_price.merge(room_with_star, left_on='bp_room_id', right_on='rm_room_id', how='left')

                ########################################################
                # Vẽ 2 biểu đồ
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
                        color_continuous_scale='PuBu'
                    )

                    fig_bar1.update_layout(
                        xaxis_title="Star rating",
                        yaxis_title="Average price (VND)",
                        coloraxis_showscale=False,
                        height=400
                    )
                    
                    st.plotly_chart(fig_bar1)

                    
                with row2:
                    nationality_totals = df_fb.groupby("fb_nationality").size().reset_index(name="total_count")
                    nationality_totals_sorted = nationality_totals.sort_values(by="total_count", ascending=False)
                    top_7_nationalities = nationality_totals_sorted.head(7)

                    colors = sample_colorscale(px.colors.sequential.Blues, [i / max(len(top_7_nationalities) - 1, 1) for i in range(len(top_7_nationalities))])[::-1]

                    fig = go.Figure()

                    for i, row in enumerate(top_7_nationalities.itertuples()):
                        fig.add_trace(go.Bar(
                            x=[row.fb_nationality],
                            y=[row.total_count],
                            marker_color=colors[i],
                            name=row.fb_nationality,
                        ))

                    fig.update_layout(
                        yaxis_title="Number of tourists",
                        showlegend=True,
                        height=400,
                        margin=dict(t=50, b=50, l=50, r=50),
                        title='Tourists Nationality Distribution'
                    )
                
                   
                    st.plotly_chart(fig, config={"displayModeBar": False}, use_container_width=True)
                
                st.markdown('<div class="row-container"></div>', unsafe_allow_html=True)  # Thêm khoảng cách nhỏ giữa các hàng

        # Cột thứ hai
        with col2:
            eda_df = postgres_query_accommodation()
            df = eda_df[['fb_nationality', 'acm_location']]
            # Function to map nationality to continent
            def country_to_continent(country_name):
                try:
                    country_alpha2 = pc.country_name_to_country_alpha2(country_name)
                    country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
                    country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
                    return country_continent_name
                except:
                    return 'Unknown'
            # Apply the function to map nationalities to continents
            df['Continent'] = df['fb_nationality'].apply(country_to_continent)
            df['acm_location'] = df['acm_location'].replace(replace_map).replace(replace_titel)
            # Filter out nationalities that are not Vietnam
            # df = df[df['fb_nationality'] != 'Vietnam']
            # Group by location and continent to get counts
            grouped = df.groupby(['acm_location', 'Continent']).size().unstack(fill_value=0)
            # Calculate percentages
            grouped_percent = grouped.div(grouped.sum(axis=1), axis=0) * 100
            # Calculate the total percentage for each location
            grouped_percent['Total'] = grouped_percent.sum(axis=1)
            # Sort by the total percentage in ascending order
            grouped_percent_sorted = grouped_percent.sort_values(by='Total', ascending=True).drop(columns='Total')
            # Melt the DataFrame to long format for Plotly
            grouped_percent_melted = grouped_percent_sorted.reset_index().melt(id_vars='acm_location', var_name='Continent', value_name='Percentage')
            # Plotting the stacked bar chart using Plotly
            fig = px.bar(grouped_percent_melted, 
                        x='acm_location', 
                        y='Percentage', 
                        color='Continent', 
                        title='Percentage of Each Visitor Continent in Each Location (Sorted by Magnitude, Smaller on Top)',
                        labels={'acm_location': 'Location', 'Percentage': 'Percentage'},
                        barmode='stack')
            fig.update_layout(
                height=540
            )
            st.plotly_chart(fig, use_container_height=True)

                
    with st.container():
        count_by_location_type = (
            room_with_price.groupby(['acm_location', 'acm_group'])['bp_price']
            .count()
            .reset_index(name='count')
        )

        # Lấy top 5 loại hình chỗ ở phổ biến nhất (dựa trên tổng số lượng toàn quốc)
        top_acm_types = count_by_location_type.groupby('acm_group')['count'].sum().nlargest(5).index

        # Lọc dữ liệu chỉ bao gồm các loại hình top 5
        filtered_count_by_type = count_by_location_type[count_by_location_type['acm_group'].isin(top_acm_types)]
        filtered_count_by_type = filtered_count_by_type.replace({'acm_location': replace_titel})

        custom_blues = sample_colorscale(px.colors.sequential.Blues, [0.3, 0.45, 0.6, 0.75, 0.9])

        # Vẽ biểu đồ hiển thị số lượng
        fig_bar3 = px.bar(
            filtered_count_by_type,
            x='acm_location',
            y='count',
            color='acm_group',
            barmode='group',
            title='Top 5 Accommodation Types by Count and Location',
            labels={'acm_location': 'Location', 'count': 'Count', 'acm_type': 'Accommodation Type'},
            # height=570,
            # color_discrete_sequence=custom_blues
        )
        fig_bar3.update_layout(
            xaxis_tickangle=0,
            xaxis_title=None,
            yaxis_title=None,
            showlegend=True,  # Hiển thị chú thích
            coloraxis_showscale=True
        )
        st.plotly_chart(fig_bar3)
            
    with st.container():
        row1, row2, row3, row4, row5 = st.columns([1,1,1,1,1])
        
        def plot_accommodation_map(df, province, map_style="carto-positron"):
            province_data = df[df['acm_location'] == province]
            
            type_counts = province_data['acm_group'].value_counts(normalize=True).sort_values(ascending=False)
            
            top_types = type_counts.head(7).index
            province_data['acm_type_grouped'] = province_data['acm_group'].apply(
                lambda x: x if x in top_types else 'Other'
            )
            colors = px.colors.diverging.Picnic[::-1]
            # Vẽ map bằng Plotly Express
            fig = px.scatter_mapbox(
                province_data,
                lat='acm_lat',
                lon='acm_long',
                color='acm_type_grouped',
                size_max=15,
                zoom=12,
                mapbox_style=map_style,
                title=f"Accommodation Map of {replace_titel[province]}",
                color_discrete_sequence=colors
            )
            fig.update_layout(
                showlegend=True,
                legend=dict(
                    x=0.01,  # Legend nằm gần mép trái bản đồ
                    y=0.01,  # Legend nằm gần mép dưới bản đồ
                    xanchor="left",  # Căn lề trái
                    yanchor="bottom",  # Căn lề dưới
                    bgcolor="rgba(255, 255, 255, 0.6)",  # Nền trắng trong suốt
                    bordercolor="black",  # Viền màu đen
                    borderwidth=1
                ),
                margin=dict(t=50, b=50))
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
    with st.container():
        st.title("GeoMap Visualization")
        eda_df = postgres_query_accommodation()
        result_df = preprocess(eda_df)
        # Define a function to map percentage ranges to colors
        def assign_color(percentage):
            if percentage < 20:
                return [34, 139, 34, 150]  # Green for 0-20%
            elif percentage < 50:
                return [255, 215, 0, 150]  # Yellow for 20-50%
            elif percentage < 80:
                return [255, 140, 0, 150]  # Orange for 50-80%
            else:
                return [255, 69, 0, 150]  # Red for 80-100%
        col11, col12 = st.columns(2)
        with col11:
            # Apply the color mapping function
            result_df['color'] = result_df['pos_ratio'].apply(assign_color)
            province = ['Tất cả'] + result_df['acm_location'].unique().tolist()
            selected_province = st.selectbox("Chọn tỉnh thành", province)
            selected_location = {"lat": 16.0, "lon": 108.0}
            if selected_province == 'Tất cả':
                filtered_data = result_df
                selected_location = {"lat": 16.0, "lon": 108.0}
                zoom = 9
            else:
                filtered_data = result_df[result_df['acm_location'] == selected_province]
                if not filtered_data.empty:
                    selected_location = {"lat": filtered_data['lat'].mean(), "lon": filtered_data['lon'].mean()}
                    zoom = 12
                else:
                    selected_location = {"lat": 16.0, "lon": 108.0}
                    zoom = 6
            # Define a ColumnLayer
            column_layer = pdk.Layer(
                "ColumnLayer",
                result_df,
                get_position=["lon", "lat"],  # Longitude and Latitude for column placement
                get_elevation="pos_ratio",  # Column height based on 'value'
                elevation_scale=1,  # Scale factor for height
                radius=200,  # Radius of the base of each column
                get_fill_color="color",  # Use the custom colors
                pickable=True,  # Enable tooltips
            )
            # Tooltip configuration
            tooltip = {
                "html": """
                <b>Name:</b> {acm_name}<br/>
                <b>Positive percent:</b> {pos_ratio}%<br/>
                """,
                "style": {"backgroundColor": "steelblue", "color": "white"}
            }
            # Set the initial view state
            view_state = pdk.ViewState(
                latitude=selected_location['lat'],  # Approximate center of Vietnam
                longitude=selected_location['lon'],
                zoom=zoom,
                pitch=40
            )
            # Create and save the Deck
            deck = pdk.Deck(
                layers=[column_layer],
                initial_view_state=view_state,
                tooltip=tooltip
            )
            # Save the visualization to an HTML file
            st.pydeck_chart(deck)
        with col12:
            # Plot multiple columns bar chart for negative and positive feedback
            eda_df = postgres_query_accommodation()
            result_df = preprocess(eda_df)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=result_df['acm_location'].replace(replace_map).replace(replace_titel),
                y=result_df['fb_positive'],
                name='Positive feedback',
                marker_color='green'
            ))
            fig.add_trace(go.Bar(
                x=result_df['acm_location'].replace(replace_map).replace(replace_titel),
                y=result_df['fb_negative'],
                name='Negative feedback',
                marker_color='red'
            ))
            fig.update_layout(
                barmode='group',  # Changed from 'stack' to 'group'
                xaxis_title='Accommodation name',
                yaxis_title='Feedback count',
                title='Feedback count by accommodation',
                showlegend=True
            )
            st.plotly_chart(fig)
