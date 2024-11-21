import os
import time
import ast
from dotenv import load_dotenv

import streamlit as st
from openai import OpenAI

load_dotenv()

db_script = '''
	CREATE TABLE IF NOT EXISTS "Accommodation" ( # Bảng khách sạn
		acm_id INT PRIMARY KEY,     # ID khách sạn
		acm_name VARCHAR,           # Tên khách sạn
		acm_type VARCHAR,           # Loại khách sạn: string của số (VD: '213')
		acm_star_rating FLOAT,      # Đánh giá số sao
		acm_amenities VARCHAR,      # Tiện nghi: string của list các tiện nghi (VD: '['Outdoor swimming pool', 'Free WiFi']')
		acm_description VARCHAR,    # Mô tả
		acm_customer_rating FLOAT,  # Đánh giá của khách hàng
		acm_review_count INT,       # Số lượt đánh giá
		acm_address VARCHAR,        # Địa chỉ
		acm_location VARCHAR,       # Tỉnh/thành phố
		acm_lat DECIMAL(9, 6),      # Vĩ độ khách sạn
		acm_long DECIMAL(9, 6),     # Kinh độ khách sạn
		acm_url VARCHAR             # URL của khách sạn
	);

	CREATE TABLE IF NOT EXISTS "Rooms" ( # Bảng phòng
		rm_room_id INT,             # ID phòng
		rm_accommodation_id INT,    # ID khách sạn
		rm_name VARCHAR,            # Tên phòng
		rm_guests_number INT,       # Số lượng khách
		rm_area FLOAT,              # Diện tích phòng
		rm_bed_types VARCHAR,       # Loại giường: string của list các loại giường (VD: '['1 Double Bed', '1 Single Bed']')
		PRIMARY KEY (rm_room_id, rm_accommodation_id),
		CONSTRAINT fk_rooms_accomodation_id FOREIGN KEY (rm_accommodation_id) REFERENCES "Accommodation" (acm_id)
	);

	CREATE TABLE IF NOT EXISTS "Bed_price" ( # Bảng giá giường
		bp_crawled_date DATE,       # Ngày lấy dữ liệu
		bp_future_interval INT,     # Khoảng cách ngày từ ngày lấy dữ liệu đến ngày check-in
		bp_room_id INT,             # ID phòng
		bp_accommodation_id INT,    # ID khách sạn
		bp_price INT,               # Giá giường
		bp_current_discount FLOAT,  # Giảm giá hiện tại
		CONSTRAINT fk_beds_rooms_id FOREIGN KEY (bp_room_id, bp_accommodation_id) REFERENCES "Rooms" (rm_room_id, rm_accommodation_id)
	);

	CREATE TABLE IF NOT EXISTS "Disciplines" ( # Bảng quy định
		dis_accommodation_id INT PRIMARY KEY,   # ID khách sạn
		dis_is_pet_allowed BOOLEAN,             # Cho phép mang thú cưng?
		dis_credit_card_required BOOLEAN,       # Yêu cầu thẻ tín dụng?
		dis_payment_methods VARCHAR,            # Phương thức thanh toán: string của list các phương thức thanh toán (VD: '['Cash', 'Credit card']')
		dis_smoking_allowed BOOLEAN,            # Cho phép hút thuốc?
		dis_checkin_start TIME,                 # Giờ check-in bắt đầu
		dis_checkin_end TIME,                   # Giờ check-in kết thúc
		dis_checkout_start TIME,                # Giờ check-out bắt đầu
		dis_checkout_end TIME,                  # Giờ check-out kết thúc
    # Nếu giờ bắt đầu là 0:00 và giờ kết thúc là 23:59 thì hiểu là 24/24.
		CONSTRAINT fk_disciplines_accommodation_id FOREIGN KEY (dis_accommodation_id) REFERENCES "Accommodation" (acm_id)
	);

	CREATE TABLE IF NOT EXISTS "Feedback" ( # Bảng phản hồi
		fb_accommodation_id INT,  # ID khách sạn
		fb_room_id INT,           # ID phòng
		fb_nationality VARCHAR,   # Quốc tịch
		fb_date TIMESTAMP,        # Ngày đánh giá
		fb_title VARCHAR,         # Tiêu đề
		fb_positive VARCHAR,      # Phản hồi tích cực
		fb_negative VARCHAR,      # Phản hồi tiêu cực
		fb_scoring INT,           # Điểm đánh giá
		fb_language_used VARCHAR, # Ngôn ngữ sử dụng
		CONSTRAINT fk_feedback_rooms_id FOREIGN KEY (fb_accommodation_id, fb_room_id) REFERENCES "Rooms" (rm_room_id, rm_accommodation_id)
	);
'''

conn = st.connection('postgresql', type='sql')
client = OpenAI(
    api_key="AIzaSyBaFKxfQl9WAD3uJsRGiJJEoK2UY1k2iLg",
    base_url='https://generativelanguage.googleapis.com/v1beta/'
)

tools = [
  {
    'type': 'function',
    'function': {
      'name': 'generate_sql_query',
      'description': f'Sinh câu lệnh SQL từ ngôn ngữ tự nhiên nếu cần thiết.',
      'parameters': {
        'type': 'object',
        'properties': {
          'query': {
            'type': 'string',
            'description': 'Câu lệnh SQL được sinh từ yêu cầu ngôn ngữ tự nhiên của người dùng.'
          },
          'content': {
            'type': 'string',
            'description': 'Câu mở đầu cho câu trả lời.'
          },
          'sorry': {
            'type': 'string',
            'description': 'Đưa ra yêu cầu thử lại sau.'
          }
        },
        'required': ['query', 'content', 'sorry']
      },
    }
  }
]

def response_generator(messages):
    re_try = 3
    try_count = 0
    response = None
    res = None

    reduced_messages = [{'role': m['role'], 'content': m['content']} for m in messages if m['role'] != 'assistant']

    while try_count < re_try:
      response = client.chat.completions.create(
          model='gemini-1.5-flash',
          messages=reduced_messages,
          tools=tools,
          tool_choice='auto'
      )

      main_content = response.choices[0].message.content
      function_call = response.choices[0].message.tool_calls
      function_call = ast.literal_eval(function_call[0].function.arguments) if function_call != [] else None

      print(main_content)
      print(function_call)

      if main_content:
        res = {'text': main_content, 'data': None}
        break
      else:
        content = function_call['content']
        query = function_call['query'].replace('\n', '')

        try:
          if query != '':
            res_query = conn.query(query)
            res = {'text': content, 'data': res_query}
          else:
            res = {'text': content, 'data': None}
          
          break

        except Exception as e:
          print(e)
          try_count += 1
          if try_count >= re_try:
            res = {'text': function_call['sorry'], 'data': None}

    for k, v in res.items():
      if k == 'text':
        for word in v.split():
          yield word + ' '
          time.sleep(0.01)
      elif v is not None:
        yield v


st.title("Simple chat")
# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [
       {'role': 'system', 'content': 'Bạn là một người trợ lý đang giúp người dùng truy vấn cơ sở dữ liệu về khách sạn thông qua cơ sở dữ liệu PostgreSQL. Tên bảng phải được đặt trong dấu ngoặc kép. Không được phép xóa hay thay đổi database. Nếu câu hỏi của người dùng không thể truy vấn trực tiếp từ cơ sở dữ liệu, hãy sử dụng công cụ sinh câu lệnh SQL từ ngôn ngữ tự nhiên. Nếu không có kết quả trả về, trả lời dựa trên script được cung cấp hoặc trả lời trực tiếp. Nếu không thể trả lời, hãy thông báo cho người dùng. Cấu trúc cơ sở dữ liệu PostgreSQL được cung cấp trong script:\n{db_script}. Database chỉ có 5 bảng: "Accommodation", "Rooms", "Bed_price", "Disciplines", "Feedback".'}
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
if prompt := st.chat_input("What is up?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        response = st.write_stream(response_generator(st.session_state.messages))
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": response})