import folium
from streamlit_folium import st_folium
import streamlit as st
import pandas as pd
import psycopg2
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import requests
from bs4 import BeautifulSoup
from io import BytesIO
from IPython.display import Image

# Kết nối tới database
conn = psycopg2.connect(
    host="localhost",
    database="BookingProject",
    user="airflow",
    password="airflow"
)

# Áp dụng CSS
st.markdown("""
    <style>
        .stTitle {
            color: #007BFF;
            font-size: 36px;
            text-align: center;
            margin-bottom: 20px;
        }
        .custom-write {
            margin-bottom: 26px; 
            margin-top: 4px;
            font-weight: bold;
            font-size: 16px;
        }
        .select-box-container {
            margin-top: -10px;
            font-size: 14px;
        }
        .highlight-box {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            margin-top: 20px;
            box-shadow: 0px 1px 4px rgba(0, 0, 0, 0.1);
        }
        .dataframe td, .dataframe th {
            font-size: 14px;
        }
        .dataframe {
            border-collapse: collapse;
            border: 1px solid #dee2e6;
            width: 100%;
        }
        .dataframe thead th {
            background-color: #007BFF;
            color: white;
            text-align: left;
            padding: 10px;
        }
        .dataframe tbody td {
            padding: 10px;
        }
        .stButton>button {
            color: white;
            background-color: #007BFF;
            border: 2px solid #D1D5DB;
            border-radius: 6px; 
            box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1);
            padding: 10px 20px;
            font-size: 14px;
        }
        .stTextInput > div > div > input {
        border: 2px solid #D1D5DB; /* Đường viền màu xám nhạt */
        border-radius: 6px; /* Bo góc nhẹ */
        padding: 8px; /* Khoảng cách bên trong */
        box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1); /* Bóng mờ bên trong */
        transition: border-color 0.3s ease; /* Hiệu ứng chuyển đổi */
        }
        
        .stSelectbox > div > div {
        border: 2px solid #E5E7EB; /* Đường viền màu xám sáng */
        border-radius: 6px; /* Bo góc nhẹ */
        box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1); /* Bóng mờ bên trong */
        transition: border-color 0.3s ease; /* Hiệu ứng chuyển đổi */
        }
    </style>
""", unsafe_allow_html=True)

st.markdown('<div class="stTitle">Top Picks for Your Stay</div>', unsafe_allow_html=True)

accommodation_types = [
    'Apartments',
    'Hostels',
    'Hotels',
    'Motels',
    'Resorts',
    'Bed and breakfasts',
    'Farm stays',
    'Villas',
    'Campsites',
    'Guest houses',
    'Holiday homes',
    'Lodges',
    'Homestays',
    'Country houses',
    'Luxury tents',
    'Capsule hotels',
    'Love hotels',
    'Chalets',
    'Boats',
    'Inns',
    'Aparthotels',
    'Cruises'
]

location = ["Ha Long, Quang Ninh, Vietnam", "Hoi An, Quang Nam, Vietnam",
            "Thua Thien Hue, Vietnam", "Nha Trang, Khanh Hoa, Vietnam",
            "Da Lat, Lam Dong, Vietnam", "Vung Tau, Ba Ria - Vung Tau, Vietnam", "Da Nang, Vietnam",
            "Phu Quoc, Kien Giang , Vietnam", "Sapa, Lao Cai, Vietnam", "Phong Nha, Quang Binh, Vietnam"]

col1, col2 = st.columns([1, 2], vertical_alignment="center")

with col1:
    st.markdown('<p class="custom-write">Accommodation Type:</p>', unsafe_allow_html=True)
    st.markdown('<p class="custom-write">Location:</p>', unsafe_allow_html=True)

with col2:
    option1 = st.selectbox(
        "Tùy chọn:",
        options=accommodation_types,
        label_visibility="collapsed",
        key=f"selectbox_1"
    )
    option2 = st.selectbox(
        "Tùy chọn:",
        options=location,
        label_visibility="collapsed",
        key=f"selectbox_2"
    )
user_input_address = st.text_input("Specific address: ")
user_input_others = st.text_input("Other things you want (amentities, price per night,...): ")


query = """
    SELECT * 
    FROM public."Accommodation" as ac
    JOIN public."Bed_price" as bp ON ac.acm_id = bp.bp_accommodation_id
    JOIN public."Rooms" AS rm ON rm.rm_accommodation_id = ac.acm_id
    WHERE ac.acm_location = %s
"""
df = pd.read_sql(query, conn, params=(option2,))
new_df = df[['acm_id', 'acm_name', 'acm_amenities', 'acm_address',
             'acm_description', 'acm_location', 'rm_name', 'rm_guests_number',
             'rm_bed_types',
             'bp_price', 'acm_url', 'acm_lat', 'acm_long', 'acm_star_rating', 'acm_customer_rating']]

# Xử lý dữ liệu và tính toán
df_copy = new_df.copy()
df_copy['rm_guests_number'] = df_copy['rm_guests_number'].astype(str) + " people"
df_copy['bp_price'] = df_copy['bp_price'].astype(str) + " VND"
df_copy["acm_amenities"] = df_copy["acm_amenities"].str.replace("[", "").str.replace("]", "").str.replace("'", "")
df_copy["rm_bed_types"] = df_copy["rm_bed_types"].str.replace("[", "").str.replace("]", "").str.replace("'", "")
df_copy['room_description'] = df_copy['acm_address'] + "Suitable for " + df_copy['rm_guests_number'] + ' with Bed type: ' + df_copy['rm_bed_types'] + '. Price: ' + df_copy['bp_price'] + ' per night.'
df_copy = df_copy.drop(columns=['rm_guests_number', 'rm_bed_types', 'bp_price', 'acm_location', 'acm_address'])
df_copy = df_copy.groupby('acm_id').agg({
    'acm_name': 'first',
    'acm_amenities': 'first',
    'acm_description': 'first',
    'acm_lat': 'first',
    'acm_long': 'first',
    'acm_url': 'first',
    'acm_star_rating': 'first',
    'acm_customer_rating': 'first',
    'rm_name': lambda x: '. '.join(x.unique()),
    'room_description': lambda x: '. '.join(x.unique())
}).reset_index()

# Tạo cột 'combined' và tính độ tương đồng cosine
temp_df = df_copy.copy()
temp_df['combined'] = (
        temp_df['acm_name'] + " " +
        temp_df['acm_description'] + " " +
        temp_df['acm_amenities'] + " " +
        temp_df['rm_name'] + " " +
        temp_df['room_description']
)
user_input = user_input_address + user_input_others
user_input = user_input.lower()
vectorizer = TfidfVectorizer(stop_words='english')
user_input_vector = vectorizer.fit_transform([user_input] + temp_df['combined'].tolist())
cosine_similarities = cosine_similarity(user_input_vector[0:1], user_input_vector[1:])
df_copy['similarity'] = cosine_similarities.flatten()

recommended_hotels = df_copy.sort_values(by='similarity', ascending=False)
recommended_hotels = recommended_hotels[['acm_name', 'acm_url','acm_customer_rating','acm_star_rating']]
recommended_hotels.rename(columns={
    'acm_name': 'Accommodation',
    'acm_url': 'Link to accommodation',
    'acm_customer_rating': 'Customer Rating (0-10)',
    'acm_star_rating': 'Star Rating (0-5)'
}
    , inplace=True)
# Hiển thị kết quả trong bảng đẹp
st.markdown('<div class="highlight-box">Recommended Hotels:</div>', unsafe_allow_html=True)

result = recommended_hotels.head(5)
# gửi request đi rồi parse lấy url ảnh từng chỗ ở
img_urls = []
for url in result['Link to accommodation']:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    img_tags = soup.find_all('img')
    temp = img_tags.pop(0)['src']
    img_urls.append(temp)

# thêm râu ria cho display markdown được...
for index, url in enumerate(img_urls):
    img_urls[index] = '<img src="' + url + '/image.jpg" width="100">'

result['Image'] = img_urls
result = result[['Image', 'Accommodation', 'Customer Rating (0-10)', 'Star Rating (0-5)']]
result=(result.style.set_table_attributes('style="width:100%; border-collapse:collapse;"') \
        .set_table_styles([{
            'selector': 'th',
            'props': [('text-align', 'center')]
        }]) \
        .set_properties(**{'text-align': 'center'}, subset=['Customer Rating (0-10)','Star Rating (0-5)']) \
        .format(
            subset=['Customer Rating (0-10)', 'Star Rating (0-5)'],
            formatter="{:.1f}"
        ))
html_table = result.to_html(escape=False)
st.markdown(html_table, unsafe_allow_html=True)

df_for_map = df_copy.copy().sort_values(by='similarity', ascending=False).head(5)


# Tạo bản đồ tại vị trí trung tâm
def render_map(data):
    map_center = [data['acm_lat'].mean(), data['acm_long'].mean()]
    my_map = folium.Map(location=map_center, zoom_start=12)

    # Thêm các điểm khách sạn từ dữ liệu
    for _, row in data.iterrows():
        folium.Marker(
            location=[row['acm_lat'], row['acm_long']],
            popup=f"{row['acm_name']}<br><a href='{row['acm_url']}' target='_blank'>Đặt ngay</a>",
        ).add_to(my_map)

    # Hiển thị bản đồ trong Streamlit
    return st_folium(my_map, width=700, height=500)


render_map(df_for_map)
