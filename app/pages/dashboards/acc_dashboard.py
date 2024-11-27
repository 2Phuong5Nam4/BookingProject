import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import numpy as np

locations = ["All", "Hạ Long", "Hội An", "Thừa Thiên Huế", "Nha Trang", "Đà Lạt", "Vũng Tàu", "Đà Nẵng", "Phú Quốc", "Sapa", "Phong Nha"]

def get_data(query):
    conn = psycopg2.connect(
        host="localhost",
        database="BookingProject",
        user="airflow",
        password="airflow"
    )
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- Query ---
query5 = """
WITH room_popularity AS (
    SELECT 
        CASE 
            WHEN acm.acm_location LIKE '%Ha Long%' THEN 'Hạ Long'
            WHEN acm.acm_location LIKE '%Hoi An%' THEN 'Hội An'
            WHEN acm.acm_location LIKE '%Thua Thien Hue%' THEN 'Thừa Thiên Huế'
            WHEN acm.acm_location LIKE '%Nha Trang%' THEN 'Nha Trang'
            WHEN acm.acm_location LIKE '%Da Lat%' THEN 'Đà Lạt'
            WHEN acm.acm_location LIKE '%Vung Tau%' THEN 'Vũng Tàu'
            WHEN acm.acm_location LIKE '%Da Nang%' THEN 'Đà Nẵng'
            WHEN acm.acm_location LIKE '%Phu Quoc%' THEN 'Phú Quốc'
            WHEN acm.acm_location LIKE '%Sapa%' THEN 'Sapa'
            WHEN acm.acm_location LIKE '%Phong Nha%' THEN 'Phong Nha'
            ELSE 'Other'
        END AS filtered_location,
        rm.rm_name,
        COUNT(*) AS count
    FROM 
        "Rooms" rm
    JOIN 
        "Accommodation" acm
    ON 
        rm.rm_accommodation_id = acm.acm_id
    WHERE 
        acm.acm_location SIMILAR TO '%(' || 'Ha Long|Hoi An|Thua Thien Hue|Nha Trang|Da Lat|Vung Tau|Da Nang|Phu Quoc|Sapa|Phong Nha' || ')%'
    GROUP BY 
        filtered_location, rm.rm_name
)
SELECT 
    filtered_location AS acm_location,
    rm_name,
    count
FROM 
    room_popularity
ORDER BY 
    acm_location, count DESC;
"""

query6 = """
WITH filtered_feedback AS (
    SELECT 
        fb.fb_reviewed_date,
        acm.acm_location
    FROM 
        "Feedback" fb
    JOIN 
        "Accommodation" acm
    ON 
        fb.fb_accommodation_id = acm.acm_id
    WHERE 
        acm.acm_location SIMILAR TO '%(' || 'Ha Long|Hoi An|Thua Thien Hue|Nha Trang|Da Lat' || ')%'
),
feedback_with_weekly_info AS (
    SELECT 
        DATE_PART('year', fb_reviewed_date) AS year,
        DATE_PART('week', fb_reviewed_date) AS week,
        acm_location,
        COUNT(*) AS review_count
    FROM 
        filtered_feedback
    GROUP BY 
        year, week, acm_location
)
SELECT 
    year, 
    week, 
    acm_location, 
    review_count
FROM 
    feedback_with_weekly_info
ORDER BY 
    year, week, review_count DESC;
"""

query7 = """
WITH feedback_with_flags AS (
    SELECT 
        fb.fb_nationality,
        acm.acm_location,
        CASE 
            WHEN fb.fb_positive IS NOT NULL AND LENGTH(TRIM(fb.fb_positive)) > 0 THEN 1 
            ELSE 0 
        END AS is_positive,
        CASE 
            WHEN fb.fb_negative IS NOT NULL AND LENGTH(TRIM(fb.fb_negative)) > 0 THEN 1 
            ELSE 0 
        END AS is_negative
    FROM 
        "Feedback" fb
    JOIN 
        "Accommodation" acm
    ON 
        fb.fb_accommodation_id = acm.acm_id
    WHERE 
         acm.acm_location SIMILAR TO '%(' || 'Ha Long|Hoi An|Thua Thien Hue|Nha Trang|Da Lat' || ')%'
),
feedback_summary AS (
    SELECT 
        fb_nationality,
        acm_location,
        SUM(is_positive) AS pos_count,
        SUM(is_negative) AS neg_count
    FROM 
        feedback_with_flags
    GROUP BY 
        fb_nationality, acm_location
),
feedback_summary_filtered AS (
    SELECT 
        fb_nationality,
        acm_location,
        pos_count,
        neg_count
    FROM 
        feedback_summary
    WHERE 
        pos_count > (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pos_count) FROM feedback_summary)
        OR 
        neg_count > (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY neg_count) FROM feedback_summary)
)
SELECT 
    fb_nationality,
    acm_location,
    pos_count,
    neg_count
FROM 
    feedback_summary_filtered
ORDER BY 
    pos_count DESC, neg_count DESC;
"""

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
    # from dash import dcc, html
    # from streamlit_plotly_events import plotly_events

    

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
                SELECt bp_accommodation_id,
                    ROUND(AVG(bp_price), 2) AS average_price,
                    ROUND(MIN(bp_price), 2) AS lowest_price,
                    ROUND(MAX(bp_price), 2) AS highest_price
                FROM "Bed_price"
                WHERE bp_future_interval = 0
                GROUP BY bp_crawled_date, bp_accommodation_id
                HAVING bp_crawled_date = MAX(bp_crawled_date)
            '''
            query_fb = 'SELECT * FROM "Feedback"'
            df_acc = pd.read_sql(query_acc, engine)
            df_price = pd.read_sql(query_price, engine)
            df_fb = pd.read_sql(query_fb, engine)

            print("Fetched data successfully!")
        
        except Exception as e:
            print("An error has occurred: ", e)

        finally:
            engine.dispose()

        return df_acc, df_price, df_fb

    df_acc, df_price, df_fb = load_data()
    df = pd.merge(df_acc, df_price, left_on='acm_id', right_on='bp_accommodation_id', how='inner')

    replace_map = {
        'Hoi An, Quang Nam, Vietnam': 'Quảng Nam',
        'Da Lat, Lam Dong, Vietnam': 'Lâm Đồng',
        'Thua Thien Hue, Vietnam': 'Thừa Thiên Huế',
        'Nha Trang, Khanh Hoa, Vietnam': 'Khánh Hòa',
        'Ha Long, Quang Ninh, Vietnam': 'Quảng Ninh',
        'Vung Tau, Ba Ria - Vung Tau, Vietnam': 'Bà Rịa - Vũng Tàu',
        'Da Nang, Vietnam': 'Đà Nẵng',
        'Phu Quoc, Kien Giang , Vietnam': 'Kiên Giang',
        'Sapa, Lao Cai, Vietnam': 'Lào Cai',
        'Phong Nha, Quang Binh, Vietnam': 'Quảng Bình'
    }

    df['acm_location'] = df['acm_location'].replace(replace_map)

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

    df['acm_type'] = df['acm_type'].map(acm_type_mapping).combine_first(df['acm_type'])
    
    col1, col2 = st.columns([0.6, 0.4])

    # Load GeoJSON file
    @st.cache_data
    def load_geojson():
        with open('province.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    geo_data = load_geojson()

    # Map visualization
    with col2:
        st.subheader("Bản đồ thông tin tổng quan các nơi")

        provinces = ['Tất cả'] + df['acm_location'].unique().tolist()
        selected_province = st.selectbox("Chọn tỉnh thành", provinces)

        if selected_province == 'Tất cả':
            filtered_data = df
            center = {"lat": 16.0, "lon": 108.0}
            zoom = 6
        else:
            filtered_data = df[df['acm_location'] == selected_province]
            if not filtered_data.empty:
                center = {"lat": filtered_data['acm_lat'].mean(), "lon": filtered_data['acm_long'].mean()}
                zoom = 9
            else:
                center = {"lat": 16.0, "lon": 108.0}
                zoom = 6

        accommodation_summary = filtered_data.groupby('acm_location').agg(
            total_accommodations=('acm_location', 'size'),
            average_rating=('acm_customer_rating', 'mean')
        ).reset_index()

        accommodation_summary['tooltip'] = (
            accommodation_summary['acm_location'] + '<br>' +
            'Tổng số chỗ ở: ' + accommodation_summary['total_accommodations'].astype(str) + '<br>' +
            'Đánh giá trung bình của khách: ' + accommodation_summary['average_rating'].round(2).astype(str)
        )
        
        fig = px.choropleth_mapbox(
            accommodation_summary,
            geojson=geo_data,
            locations='acm_location',
            featureidkey='properties.ten_tinh',
            color='total_accommodations',
            hover_name='tooltip',
            hover_data={'acm_location': False, 'total_accommodations': False},
            color_continuous_scale="OrRd",
            range_color=(
                accommodation_summary['total_accommodations'].min(),
                accommodation_summary['total_accommodations'].max()
            ),
            mapbox_style="carto-positron",
            center=center,
            zoom=zoom,
            opacity=0.7,
            labels={'total_accommodations': 'Số lượng chỗ ở'}
        )

        fig.add_trace(go.Scattermapbox(
            lat=filtered_data['acm_lat'],
            lon=filtered_data['acm_long'],
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=8,
                color='green'
            ),
            text=df['acm_name'],
            hoverinfo='text'
        ))

        fig.update_layout(
            margin={"r": 0,"t": 0,"l": 0,"b": 0},
            height=550
        )

        st.plotly_chart(fig, use_container_height=True)
    


        
    with col1:
        # Remarkable information
        with st.container():
            st.markdown("### Thông tin nổi bật")

            with st.container():
                col2_subcol1, col2_subcol2, col2_subcol3 = st.columns([1,1,1])
                
                top_review = df.loc[df['acm_review_count'].idxmax()]

                top_review_name = top_review['acm_name']
                top_review_rating = top_review['acm_customer_rating']
                top_review_link = top_review['acm_url']

                with col2_subcol1:
                    st.markdown(
                        """
                        <div style="text-align: center;">
                            <div style="font-size: 16px; font-weight: bold; margin-bottom: 8px;">Nơi được quan tâm nhất</div>
                            <div style="font-size: 32px; color: orange; font-weight: bold;">
                                <a href="{top_review_link}" target="_blank" style="text-decoration: none; color: orange;">
                                    {top_review_name}
                                </a>
                            </div>
                        </div>
                        """.format(top_review_name=top_review_name, top_review_link=top_review_link),
                        unsafe_allow_html=True
                    )

                with col2_subcol2:
                    st.markdown(
                        """
                        <div style="text-align: center;">
                            <div style="font-size: 16px; font-weight: bold; margin-bottom: 8px;">Đánh giá trung bình</div>
                            <div style="font-size: 32px; color: orange; font-weight: bold;">
                                {top_review_rating}
                            </div>
                        </div>
                        """.format(top_review_rating=top_review_rating),
                        unsafe_allow_html=True
                    )
                
                with col2_subcol3:
                    # Drop null row
                    df_fb.dropna(subset=['fb_positive'], inplace=True)
                    df_fb['is_positive'] = df_fb['fb_positive'].apply(lambda x: x.strip() != "")

                    positive_feedback_ratio = df_fb.groupby('fb_accommodation_id').agg(
                        total_feedback=('fb_positive', 'size'),
                        positive_feedback=('is_positive', 'sum')
                    ).reset_index()

                    positive_feedback_ratio['positive_feedback_ratio'] = (
                        positive_feedback_ratio['positive_feedback'] / positive_feedback_ratio['total_feedback'] * 100
                    ).round(0)

                    selected_accommodation_id = top_review['acm_id']
                    positive_ratio = int(positive_feedback_ratio.loc[
                        positive_feedback_ratio['fb_accommodation_id'] == selected_accommodation_id,
                        'positive_feedback_ratio'
                    ].values[0])

                    st.markdown(
                        f"""
                        <div style="text-align: center;">
                            <div style="font-size: 16px; font-weight: bold; margin-bottom: 8px;">Phản hồi tích cực</div>
                            <div style="font-size: 32px; color: orange; font-weight: bold;">
                                {positive_ratio}%
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

        with st.container():
            nationality_totals = df_fb.groupby("fb_nationality").size().reset_index(name="total_count")
            nationality_totals_sorted = nationality_totals.sort_values(by="total_count", ascending=False)
            top_7_nationalities = nationality_totals_sorted.head(7)

            max_value = max(len(top_7_nationalities) - 1, 1)
            colors = [
                f"rgba(255, {55 + int(200 * (i / max_value))}, {55 + int(200 * (i / max_value))}, 1)"
                for i in range(len(top_7_nationalities))
            ]

            fig = go.Figure()

            for i, row in enumerate(top_7_nationalities.itertuples()):
                fig.add_trace(go.Bar(
                    x=[row.fb_nationality],
                    y=[row.total_count],
                    marker_color=colors[i],
                    name=row.fb_nationality,
                ))

            fig.update_layout(
                yaxis_title="Lượng khách",
                showlegend=False,
                height=400,
                margin=dict(t=50, b=50, l=50, r=50),
            )
        
            st.markdown("#### Phân bổ quốc tịch khách hàng")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    col3, col4 = st.columns([0.4, 0.6])
    with col3:
        st.markdown("### Tỷ lệ phân bổ các loại hình")

        def generate_tooltip_info(df):
            total_accommodations = len(df)
            tooltip_info = df.groupby('acm_type').agg(
                count=('acm_type', 'size'),
                avg_price=('average_price', 'mean'),
                max_price=('highest_price', 'max'),
                min_price=('lowest_price', 'min')
            ).reset_index()
            tooltip_info['distribution_ratio'] = (tooltip_info['count'] / total_accommodations) * 100
            return tooltip_info

        col1_subcol1, col1_subcol2 = st.columns([0.8, 0.2])

        with col1_subcol2:
            province_options = df['acm_location'].unique().tolist()
            selected_provinces = []
            for province in province_options:
                if st.checkbox(province):
                    selected_provinces.append(province)

            if selected_provinces:
                filtered_df = df[df['acm_location'].isin(selected_provinces)]
            else:
                filtered_df = df

        with col1_subcol1:
            pie_data = generate_tooltip_info(filtered_df)
            norm = Normalize(vmin=pie_data['count'].min(), vmax=pie_data['count'].max())
            colors = [to_hex(cm.Reds(norm(value))) for value in pie_data['count']]

            fig = go.Figure(data=[go.Pie(
                labels=pie_data['acm_type'],
                values=pie_data['count'],
                hovertemplate=(
                    "<span style='text-align: left; display: block;'>"
                    "<b>Loại hình: %{label}</b><br>"
                    "Số lượng: %{value}<br>"
                    "Giá trung bình: %{customdata:.2f}"
                    "</span>"
                ),
                textinfo='percent',
                textposition='inside',
                marker=dict(colors=colors),
                customdata=pie_data['avg_price']
            )])

            fig.update_layout(
                margin=dict(t=0, b=0, l=0, r=0),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
    selected_locations = st.selectbox(
        "Choose a location:", locations, index=0 
    )

        # Fig 7: Distribution of reviews
    with col4:
        df7= get_data(query7)
        if selected_locations != "All":
            filtered_df7 = df7[df7["acm_location"].str.contains(selected_locations, case=False, na=False)]
        else:
            filtered_df7 = df7
            
        aggregated_df7 = (
            filtered_df7
              .melt(
                id_vars=["fb_nationality"], 
                value_vars=["pos_count", "neg_count"], 
                var_name="review_type", 
                value_name="review_count"
            )
            .groupby(["fb_nationality", "review_type"], as_index=False)["review_count"]
            .sum()  
        )
        top10_nationalities = (aggregated_df7.groupby("fb_nationality")["review_count"].sum().nlargest(10).index)
        aggregated_df7_top10 = aggregated_df7[aggregated_df7["fb_nationality"].isin(top10_nationalities)]

        fig7 = px.bar(
            aggregated_df7_top10,
            x="fb_nationality", 
            y="review_count",  
            color="review_type",
            labels={
                "review_count": "Num of Reviews", 
                "review_type": "Review type",
                "fb_nationality": "Nationality"
            },
            title="Top 10 Nationalities with Number of Reviews",
            barmode="group"
        )
        st.markdown("### Phân bổ số lượng đánh giá theo quốc tịch")
        st.plotly_chart(fig7)

    # Fig 5: Popular room
    df5 = get_data(query5)
    if selected_locations != "All":
        filtered_df5 = df5[df5["acm_location"].str.contains(selected_locations, case=False, na=False)]
    else:
        filtered_df5 = df5

    filtered_df5 = (
        filtered_df5
        .sort_values("count", ascending=False)  
        .groupby("acm_location")  
        .head(10)  
        .reset_index(drop=True)  
    )

    fig5 = px.bar(
        filtered_df5, 
        x="rm_name", 
        y="count", 
        color="acm_location",
        title="Top 10 Popular Rooms by Area",
        labels={"rm_name": "Room name", "count": "Num of rooms"},
        category_orders={"acm_location": filtered_df5['acm_location'].unique().tolist()},
        color_discrete_sequence=px.colors.qualitative.Set2,
        text_auto=True
    )
    st.markdown("#### Những Loại Phòng Phổ Biến")
    st.plotly_chart(fig5)

    # Fig 6: Review trend by week
    df6 = get_data(query6)
    df6["year"] = df6["year"].astype(int).astype(str)  
    df6["week"] = df6["week"].astype(int) 

    if selected_locations != "All":
        filtered_df6 = df6[df6["acm_location"].str.contains(selected_locations, case=False, na=False)]
    else:
        filtered_df6 = df6
    
    available_years = sorted(filtered_df6["year"].unique())
    selected_years = st.multiselect(
        "Choose years:", 
        options=available_years, 
        default=available_years  
    )
    filtered_df6 = filtered_df6[filtered_df6["year"].isin(selected_years)]

    fig6 = px.line(
        filtered_df6, 
        x="week", 
        y="review_count", 
        color="year",
        title="The number of reviews increases week by week",
        labels={"week": "Week", "review_count": "Num of reviews", "year": "Year"}
    )
    st.markdown("#### Xu Hướng Review")
    st.plotly_chart(fig6)


