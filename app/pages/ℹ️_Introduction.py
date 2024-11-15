import streamlit as st

st.title("Accommodation data")
provinces = {
    "All": "All",
    "Hạ Long":"Ha Long, Quang Ninh, Vietnam",
    "Hội An": "Hoi An, Quang Nam, Vietnam",
    "Thừa Thiên Huế": "Thua Thien Hue, Vietnam",
    "Nha Trang": "Nha Trang, Khanh Hoa, Vietnam",
    "Đà Lạt": "Da Lat, Lam Dong, Vietnam"
}
# Create columns for filtering components
col1, col2= st.columns([2, 3])
col3, col4= st.columns([2, 3])

# Select province in the first column
with col1:
    selected_option = st.selectbox("Choose a province:", list(provinces.keys()))

# Select star rating range in the second column
with col2:
    rating_range = st.slider("Select star rating range:", min_value=1, max_value=5, value=(1, 5), step=1)

# Select customer rating range in the second column
with col4:
    customer_range = st.slider("Select customer rating range:", min_value=1.0, max_value=10.0, value=(1.0, 10.0), step=0.5)

with col3:
    limitation = st.number_input("Number of data returned:", min_value=0, max_value=100, value=10, step=1)

# Initialize connection.
conn = st.connection("postgresql", type="sql")

def postgres_query(province=None, star_range=None, customer_rating=None, limit=10):
    base = "SELECT * FROM public.\"Accommodation\""
    conditions = []
    if province is not None and province != "All":
        conditions.append(f"acm_location = '{provinces[province]}'")
    if star_range is not None:
        conditions.append(f"acm_star_rating BETWEEN {star_range[0]} AND {star_range[1]}")
    
    if customer_rating is not None:
        conditions.append(f"acm_customer_rating BETWEEN {customer_range[0]} AND {customer_range[1]}")
    if conditions:
        base += " WHERE " + " AND ".join(conditions)
    if limit is not None:
        base += f" LIMIT {limit};"
    print(base)
    df = conn.query(base, ttl="10m")
    return df

# Perform query.
df = postgres_query(province=selected_option, star_range = rating_range, customer_rating=customer_range, limit=limitation)
st.table(df)
