import boto3
import os
from botocore.client import Config
from pathlib import Path
import glob
import psycopg2
import json
import pprint
import datetime
import copy

version_list = [
    {
        'version': '1',
        "answers": [
      {
        "question": "Откуда вы узнали о нашем сервисе?",
        "options": {}
      },
      {
        "question": "Ваш опыт торговли на маркетплейсах?",
        "options": {
          "1": "Менее 1 года",
          "2": "1 - 2 года",
          "3": "3 - 5 лет",
          "4": "Более 5 лет"
        }
      },
      {
        "question": "На скольких маркетплейсах вы ведете торговлю?",
        "options": {
          "1": "1",
          "2": "2",
          "3": "3",
          "4": "Более 3"
        }
      },
      {
        "question": "Вы можете описать своими словами, как вы поняли, для чего нужен наш сервис?",
        "options": {}
      },
      {
        "question": "Какие преимущества сервиса вы отметили, насколько они важны для вас?",
        "options": {}
      },
      {
        "question": "Возникло ли у вас желание воспользоваться сервисом?",
        "options": {
          "1": "Да",
          "2": "Нет"
        }
      },
      {
        "question": "Поясните, пожалуйста, почему вы приняли в предыдущем вопросе такое решение?",
        "options": {}
      },
      {
        "question": "Что мы могли бы добавить в сервис, чтобы у вас возникло желание им воспользоваться?",
        "options": {}
      },
      {
        "question": "Какими сервисами (аналитическими, логистическими и т.д.) вы сейчас пользуетесь?",
        "options": {}
      }
    ]
    }
]



access_key = 'dp-prod-interview-user'
secret_key = 'kyJrYJ0T2O7UVv3q'
endpoint_url = 'https://dp-prod-s3api.russianpost.ru'
db_name = "client-configuration-1"
login = "client-configuration-ro-user"
password ="900u2uSuqK41G8Qe"
server_address = "10.193.95.245"

sql_query = """select 
       legal_entity_name ,
	   date(created_at) 
from "client-configuration-1".client_configuration.seller s 
where id = %s;
"""
sql_query_client = """
SELECT 
    s.legal_entity_name,
    DATE(s.created_at) AS created_date,
    COUNT(sm.id) AS marketplace_count  
FROM "client-configuration-1".client_configuration.seller s 
LEFT JOIN "client-configuration-1".client_configuration.seller_marketplace sm 
    ON sm.seller_id = s.id 
WHERE s.id = %s
GROUP BY 
    s.legal_entity_name, 
    DATE(s.created_at)
ORDER BY created_date;
"""

sql_query_payment = """
select count(p.id)
from "payment-service-1".payment_service.seller s 
left join "payment-service-1".payment_service.payment p 
on s.id = p.seller_id 
where s.client_configuration_seller_id = %s
"""




seller_id_test='eb444d7e-3e4b-4bb1-881f-3d2451d01a4a'


path = Path('C:/Users/Nikolay/Downloads/interview')
downloaded_files = os.listdir(path)


def get_new_file_from_s3(path, access_key, secret_key, endpoint_url):
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        config=Config(signature_version='s3v4')
        )


    try:
        response = s3_client.list_objects(Bucket='interview')
        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']
                if file_key  not in downloaded_files:
                    local_path = os.path.join(path, file_key)
                    s3_client.download_file('interview', file_key , local_path )
                    status = 'Ok'
                    return status
    except Exception as e:
        print(f"Ошибка: {e}")
        status = 'error'
        return(status)


def get_info_about_seller(server_address, db_name, login, password, sql_query, seller_id ):
    result_array = []
    try:
        conn = psycopg2.connect(dbname=db_name, user=login, host=server_address, password=password)
        with conn.cursor() as curs:
            try:
                curs.execute(sql_query, (seller_id,))
                result_array = curs.fetchall()
            except (Exception, psycopg2.DatabaseError) as error:
                message = f"При выполнении запроса обнаружилась следующая ошибка: {error}"
                print(message)  
                return -1   
    except Exception as e:
        print(f"Ошибка: {e}")
        return -1  
    return result_array


res = get_info_about_seller(server_address, db_name, login, password, sql_query_client, seller_id_test)

print(res)
""" date_obj=res[0][1]
date_string = date_obj.strftime('%Y-%m-%d')
print(date_string)   """


def serilalize_json_data_form_s3(path_to_folder):
    processed_data = []
    for file in path_to_folder.glob('*.JSON'):
        try:
            with open(file, 'r',  encoding='Utf-8') as interview_file:
                interview_data = json.load(interview_file)
                #print(interview_data[0])
                if  isinstance(interview_data[0], dict) and interview_data[0].get('seller_id'):
                    answer_dict ={}
                    answer_dict['seller_id'] = interview_data[0]['seller_id']
                    for i in enumerate(interview_data[0]['answers'], start=1):
                        answer_dict[i[0]] = i[1].get('value')
                        # print(i[0]) 
                        # print(i[1].get('value') )
                processed_data.append(answer_dict)
        except FileNotFoundError:
            print("Error: The specified JSON file was not found.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from file: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    return processed_data

res2 = serilalize_json_data_form_s3(path)

""" pprint.pprint(res2)
print(len(res2))

for i in enumerate(version_list[0]['answers']):
    print(i)
    if i[1]['options']:
        print('Это закрытый вопрос: ' + i[1]['question'] + ' Со следующими ответами ' + str(i[1]['options']) )
    else:
        print('Это открытый вопрос : ' + i[1]['question']) """


def collect_all_info_about_seller(info_about_seller_array):
    result_array =[]
    for i in info_about_seller_array:
        if i.get('seller_id'):
            info_about_seller_from_bd = get_info_about_seller(server_address, db_name, login, password, sql_query_client, i.get('seller_id'))
            if info_about_seller_from_bd:
                date_obj=info_about_seller_from_bd[0][1]
                date_string = date_obj.strftime('%Y-%m-%d')
                i['date_of_registration'] = date_string
                i['company_name'] = info_about_seller_from_bd[0][0]
                i['count_seller_marketplaces'] = info_about_seller_from_bd[0][2]
            result_array.append(i)
        else:
            print('An unexpected error occurred: seller_id not found')
            break
    return result_array
        

test_1 = collect_all_info_about_seller(res2)
pprint.pprint(test_1)

def prepare_referance_list_two_types_questions(referance_interview_list):
    open_questions_list = []
    closed_question_list = []
    for i in enumerate(referance_interview_list[0]['answers'], start=1):
        if i[1]['options']:
            closed_question_dict={}
            closed_question_dict={
                'question_number': i[0],
                'question': i[1]['question'],
                'options' : i[1]['options'],
                'score_options':dict.fromkeys(range(1, (len(i[1]['options']) + 1)), 0)
            }
            closed_question_list.append(closed_question_dict)
        else:
            open_questions_dict={}
            open_questions_dict={
                'question_number': i[0],
                'question': i[1]['question']
            }
            open_questions_list.append(open_questions_dict)
    return open_questions_list, closed_question_list

two_types_of_questions_tuple = prepare_referance_list_two_types_questions(version_list)
#pprint.pprint(two_types_of_questions_tuple)

def score_open_questions(prepared_list_question, all_info_about_sellers_list):
    prepared_list_question_deep_copy = copy.deepcopy(prepared_list_question)
    for i in prepared_list_question_deep_copy:
        for j in all_info_about_sellers_list:
            try:
                value = int(j.get(i['question_number']))
                i['score_options'][value] += 1
            except:
                print('An unexpected error occurred: wrong number of question')
                break
    return prepared_list_question_deep_copy

test_32=score_open_questions(two_types_of_questions_tuple[1], test_1)

pprint.pprint(test_32)
pprint.pprint(two_types_of_questions_tuple[1])           






#def plot_a_graph(referance_list_interview, all_sellers_info_list):
# def send_data_to_llm()
# def prepare_summary_report()
# def post_info_on_conflunce() 


#my_dict = {'apple': 1, 'banana': 2}
#value = my_dict.get('banana') # вернет 2
#value_not_exist = my_dict.get('grape', 'Ключа нет') # вернет 'Ключа нет'
#print(value)
#print(value_not_exist)