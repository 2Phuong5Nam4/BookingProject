import os
import time
import ast
from dotenv import load_dotenv

import streamlit as st
from openai import OpenAI

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

load_dotenv()

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

days_stay = [0, 1, 2, 3, 5, 7, 14, 30]

db_script = f'''
	CREATE TABLE "Accommodation" (# Bảng khách sạn
		acm_id INT PRIMARY KEY,# ID khách sạn
		acm_name VARCHAR,# Tên khách sạn
		acm_type VARCHAR,# Loại khách sạn: string
		acm_star_rating FLOAT,# Đánh giá số sao
		acm_amenities VARCHAR,# Tiện nghi: string của list các tiện nghi (VD: '['Outdoor swimming pool', 'Free WiFi']')
		acm_description VARCHAR,# Mô tả
		acm_customer_rating FLOAT,# Đánh giá của khách hàng
		acm_review_count INT,# Số lượt đánh giá
		acm_address VARCHAR,# Địa chỉ
		acm_location VARCHAR,# Tỉnh/thành phố. Phải là một trong các loại: {"; ".join(location)}
		acm_lat DECIMAL(9, 6),# Vĩ độ khách sạn
		acm_long DECIMAL(9, 6),# Kinh độ khách sạn
		acm_url VARCHAR # URL của khách sạn
	);

	CREATE TABLE "Rooms" ( # Bảng phòng
		rm_room_id INT,# ID phòng
		rm_accommodation_id INT,# ID khách sạn
		rm_name VARCHAR,# Tên phòng
		rm_guests_number INT,# Số lượng khách
		rm_area FLOAT,# Diện tích phòng
		rm_bed_types VARCHAR,# Loại giường: string của list các loại giường (VD: '['1 double bed', '1 single bed']')
		PRIMARY KEY (rm_room_id, rm_accommodation_id),
		CONSTRAINT fk_rooms_accomodation_id FOREIGN KEY (rm_accommodation_id) REFERENCES "Accommodation" (acm_id)
	);

	CREATE TABLE "Bed_price" ( # Bảng giá giường
		bp_crawled_date DATE,# Ngày lấy dữ liệu
		bp_future_interval INT,# Khoảng cách ngày từ ngày lấy dữ liệu đến ngày check-in
		bp_room_id INT,# ID phòng
		bp_accommodation_id INT,# ID khách sạn
		bp_price INT,# Giá giường
		bp_current_discount FLOAT,# Giảm giá hiện tại
		CONSTRAINT fk_beds_rooms_id FOREIGN KEY (bp_room_id, bp_accommodation_id) REFERENCES "Rooms" (rm_room_id, rm_accommodation_id)
	);

	CREATE TABLE "Disciplines" ( # Bảng quy định
		dis_accommodation_id INT PRIMARY KEY,# ID khách sạn
		dis_is_pet_allowed BOOLEAN,# Cho phép mang thú cưng?
		dis_credit_card_required BOOLEAN,# Yêu cầu thẻ tín dụng?
		dis_payment_methods VARCHAR,# Phương thức thanh toán: string của list các phương thức thanh toán (VD: '['Cash', 'Credit card']')
		dis_smoking_allowed BOOLEAN,# Cho phép hút thuốc?
		dis_checkin_start TIME,# Giờ check-in bắt đầu
		dis_checkin_end TIME,# Giờ check-in kết thúc
		dis_checkout_start TIME,# Giờ check-out bắt đầu
		dis_checkout_end TIME,# Giờ check-out kết thúc
    # Nếu giờ bắt đầu là 0:00 và giờ kết thúc là 23:59 thì hiểu là 24/24.
		CONSTRAINT fk_disciplines_accommodation_id FOREIGN KEY (dis_accommodation_id) REFERENCES "Accommodation" (acm_id)
	);

	CREATE TABLE "Feedback" ( # Bảng phản hồi
		fb_accommodation_id INT,# ID khách sạn
		fb_room_id INT,# ID phòng
		fb_nationality VARCHAR,# Quốc tịch
		fb_date TIMESTAMP,# Ngày đánh giá
		fb_title VARCHAR,# Tiêu đề
		fb_positive VARCHAR,# Phản hồi tích cực
		fb_negative VARCHAR,# Phản hồi tiêu cực
		fb_scoring INT,# Điểm đánh giá
		fb_language_used VARCHAR,# Ngôn ngữ sử dụng
		CONSTRAINT fk_feedback_rooms_id FOREIGN KEY (fb_accommodation_id, fb_room_id) REFERENCES "Rooms" (rm_room_id, rm_accommodation_id)
	);
'''

conn = st.connection('postgresql', type='sql')

client = OpenAI(
    api_key=os.getenv('GEMINI_API_KEY'),
    base_url='https://generativelanguage.googleapis.com/v1beta/'
)

tools = [
    {
        'type': 'function',
        'function': {
            'name': 'generate_sql_query',
            'description': f'Sinh câu lệnh SQL từ ngôn ngữ tự nhiên nếu cần thiết. Tên table trong script phải được đặt trong dấu nháy kép (quotation marks) và chuỗi nằm trong dấu nháy đơn (single quotes). Luôn kèm theo acm_url nếu câu hỏi liên quan đến việc tìm kiếm thông tin khách sạn.',
            'parameters': {
                'type': 'object',
                'properties': {
                    'query': {
                        'type': 'string',
                        'description': 'Câu lệnh SQL được sinh từ yêu cầu ngôn ngữ tự nhiên của người dùng.'
                    },
                    'content': {
                        'type': 'string',
                        'description': 'Câu mở đầu cho câu trả lời với trường hợp trả lời thành công.'
                    },
                    'apology': {
                        'type': 'string',
                        'description': 'Đưa ra yêu cầu thử lại sau.'
                    }
                },
                'required': ['query', 'content', 'apology']
            }
        }
    },

    {
        'type': 'function',
        'function': {
            'name': 'extract_information_for_recommendation',
            'description': 'Trích xuất các thông tin từ yêu cầu người dùng để đưa ra gợi ý. Thực hiện điều này nếu có yêu cầu cần gợi ý khách sạn.',
            'parameters': {
                'type': 'object',
                'properties': {
                    'accommodation_type': {
                        'type': 'string',
                        'description': f'Loại khách sạn. Phải là một trong các loại: {"; ".join(accommodation_types)}.'
                    },
                    'location': {
                        'type': 'string',
                        'description': f'Địa điểm. Phải là một trong các địa điểm: {"; ".join(location)}.'
                    },
                    'days_stay': {
                        'type': 'integer',
                        'description': f'Số ngày muốn ở. Phải là một trong các giá trị: {"; ".join([str(i) for i in days_stay])}.'
                    },
                    'specific_address': {
                        'type': 'string',
                        'description': 'Địa chỉ cụ thể.'
                    },
                    'others': {
                        'type': 'string',
                        'description': 'Thông tin khác. VD: tiện nghi, giá tiền mỗi đêm,...\nNếu người dùng nhập tiếng anh thì sửa lỗi chính tả nếu có, nếu người dùng nhập tiếng việt thì chuyển sang tiếng anh.'
                    }
                },
                'required': ['accommodation_type', 'location', 'days_stay']
            }
        }
    }
]

def run_query(tool_args):
    content = tool_args['content'].replace('\n', '')
    query = tool_args['query'].replace('\\', '').replace('`', '')

    query_result = conn.query(query)

    result = {'text': content, 'data': query_result}

    return result

def run_recommendation_query(tool_args):
    option1 = tool_args['accommodation_type']
    option2 = tool_args['location']
    option3 = tool_args['days_stay']

    try:
        user_input_address = tool_args['specific_address']
    except:
        user_input_address = ''

    try:
        user_input_others = tool_args['others']
    except:
        user_input_others = ''

    print('Accommodation type:', option1)
    print('Location:', option2)
    print('Days stay:', option3)
    print('Specific address:', user_input_address)
    print('Others:', user_input_others)

    query = f'''
        SELECT * 
        FROM public."Accommodation" as ac
        JOIN public."Bed_price" as bp ON ac.acm_id = bp.bp_accommodation_id
        JOIN public."Rooms" AS rm ON rm.rm_accommodation_id = ac.acm_id
        WHERE ac.acm_location = '{option2}' AND bp.bp_future_interval = '{option3}' AND ac.acm_type= '{option1}'
    '''

    df = conn.query(query)

    new_df = df[['acm_id', 'acm_name', 'acm_amenities' ,'acm_address',
                'acm_description', 'acm_location', 'rm_name', 'rm_guests_number',
                'rm_bed_types',
                'bp_price', 'acm_url']]

    # Xử lý dữ liệu và tính toán
    df_copy = new_df.copy()
    df_copy['rm_guests_number'] = df_copy['rm_guests_number'].astype(str) + " people"
    df_copy['bp_price'] = df_copy['bp_price'].astype(str) + " VND"
    df_copy["acm_amenities"] = df_copy["acm_amenities"].str.replace("[", "").str.replace("]", "").str.replace("'", "")
    df_copy["rm_bed_types"] = df_copy["rm_bed_types"].str.replace("[", "").str.replace("]", "").str.replace("'", "")
    df_copy['room_description'] = df_copy['acm_address'] + "Suitable for " + df_copy['rm_guests_number'] + ' with Bed type: ' + df_copy['rm_bed_types'] + '. Price: ' + df_copy['bp_price'] + ' per night.'
    df_copy = df_copy.drop(columns=['rm_guests_number', 'rm_bed_types', 'bp_price', 'acm_location' ,'acm_address'])
    df_copy = df_copy.groupby('acm_id').agg({
        'acm_name': 'first',
        'acm_amenities': 'first',
        'acm_description': 'first',
        'acm_url': 'first',
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
    recommended_hotels = recommended_hotels[['acm_name', 'acm_url']]
    recommended_hotels.rename(columns={
        'acm_name' :'Accommodation',
        'acm_url' :'Link to accommodation'
    }, inplace=True)


    result = {'text': None, 'data': recommended_hotels.head(5)}

    return result

def response_generator(messages, tool_option):
    max_retries = 1
    retry_count = 0

    print(messages)

    reduced_messages = [{'role': m['role'], 'content': m['content']}
                        for m in messages if m['role'] != 'assistant'].copy()

    while retry_count < max_retries:
        # Generate a response using the client
        try:
            if tool_option is None:
                response = client.chat.completions.create(
                    model='gemini-1.5-pro',
                    messages=reduced_messages
                ).choices[0]
            else:
                response = client.chat.completions.create(
                    model='gemini-1.5-flash',
                    messages=reduced_messages,
                    tools=[tools[tool_option]],
                    tool_choice='auto'
                ).choices[0]
        except Exception as e:
            print(f"Exception occurred: {e}")
            result = {'text': f'Lỗi: {e}\n', 'data': None}
            break

        print(response)

        # Extract content and tool calls
        main_content = response.message.content if response.message.content else None
        tool_calls = response.message.tool_calls[0] if response.message.tool_calls else None

        print('Main content:', main_content)
        print('Tool calls:', tool_calls)

        # If no content and no tool calls, retry
        if not main_content and not tool_calls:
            retry_count += 1
            continue

        if main_content:  # Handle main content response
            result = {'text': main_content, 'data': None}
            break
        else:  # Handle tool-specific logic
            try:
                tool_name = tool_calls.function.name
                tool_args = ast.literal_eval(tool_calls.function.arguments)
            except Exception as e:
                print(f"Exception occurred: {e}")
                retry_count += 1

            # Handle `generate_sql_query` tool
            if tool_name == 'generate_sql_query':
                try:
                    result = run_query(tool_args)
                    break
                except Exception as e:
                    print(f"Exception occurred: {e}")
                    retry_count += 1
                    reduced_messages.append({
                        'role': 'assistant',
                        'content':
                            f'Lỗi: {e}\nCó thể script SQL không chính xác do tên bảng thiếu dấu dấu nháy kép (quotation marks) và chuỗi (string) thiếu dấu nháy đơn (single quotes). Hãy kiểm tra lại script SQL.'
                    })

            # Handle `extract_information_for_recommendation` tool
            elif tool_name == 'extract_information_for_recommendation':
                try:
                    result = run_recommendation_query(tool_args)
                    break
                except Exception as e:
                    print(f"Exception occurred: {e}")
                    retry_count += 1
                    reduced_messages.append({'role': 'assistant', 'content': f'Lỗi: {e}\n'})
        
    if retry_count >= max_retries:
        result = {'text': '', 'data': None}

    for k, v in result.items():
        if k == 'text' and v is not None:
            for word in v.split():
                yield word + ' '
                time.sleep(0.01)
        elif v is not None:
            yield v

col1, col2 = st.columns([1, 2], vertical_alignment="center")

selection_dict = {
    'Không': None,
    'Truy vấn từ database': 0,
    'Gợi ý': 1
}

prompt_dict = {
    'Không': 'Bạn là một người trợ lý đang giúp người dùng giải đáp thắc mắc về khách sạn ở Việt Nam.',
    'Truy vấn từ database': f'Bạn là một người trợ lý đang giúp người dùng truy vấn database về khách sạn được lưu bằng PostgreSQL. Nếu câu hỏi cần truy lấy thông tin từ database, hãy sử dụng công cụ sinh script SQL từ ngôn ngữ tự nhiên. Nếu câu hỏi liên quan đến cấu trúc của database, hãy dựa vào script cung cấp. Không được phép xóa hay thay đổi database. Database chỉ có 5 bảng: "Accommodation", "Rooms", "Bed_price", "Disciplines", "Feedback". Hãy nhớ kỹ script tạo database:\n{db_script}.',
    'Gợi ý': 'Bạn là một người trợ lý đang giúp người dùng tìm kiếm khách sạn phù hợp với yêu cầu. Hãy sử dụng công cụ trích xuất thông tin từ yêu cầu người dùng để đưa ra gợi ý. Các trường cần thiết để trả lời câu hỏi là: Loại khách sạn, Địa điểm, Số ngày muốn ở. Nếu người dùng không cung cấp đủ thông tin, hãy yêu cầu họ cung cấp thêm thông tin.'
}

with col1:
    st.markdown('<p class="custom-write">Công cụ hỗ trợ:</p>', unsafe_allow_html=True)

with col2:
    tool_option = st.selectbox(
        'Options:',
        options=['Truy vấn từ database', 'Gợi ý', 'Không'],
        label_visibility="collapsed",
        key='selectbox'
    )

st.title("Chatbot")
# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [
        {'role': 'system', 'content': prompt_dict[tool_option]}
    ]

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# Define custom CSS to change the text color of the chat input text holder
custom_css = """
<style>
.stTextInput > div > div > input {
    color: #5591f5; /* Change this to your desired text color */
}
</style>
"""

# Inject the custom CSS into the Streamlit app
st.markdown(custom_css, unsafe_allow_html=True)
# Accept user input
if prompt := st.chat_input("Hỏi gì đó?"):
    # Change system prompt based on tool option
    st.session_state.messages[0]["content"] = prompt_dict[tool_option]
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        response = st.write_stream(
            response_generator(st.session_state.messages, selection_dict[tool_option]))
    # Add assistant response to chat history
    st.session_state.messages.append(
        {"role": "assistant", "content": response})