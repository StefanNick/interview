import boto3
import os
from botocore.client import Config
from pathlib import Path
import glob
import psycopg2
import json
import pprint

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


seller_id='eb444d7e-3e4b-4bb1-881f-3d2451d01a4a'


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


res = get_info_about_seller(server_address, db_name, login, password, sql_query, seller_id)

print(res)





def serilalize_data(path_to_folder):
    processed_data = []
    for file in path_to_folder.glob('*.JSON'):
        
        try:
            with open(file, 'r',  encoding='Utf-8') as interview_file:
                interview_data = json.load(interview_file)
                #print(interview_data[0])
                if  isinstance(interview_data[0], dict) and interview_data[0].get('seller_id'):
                    pprint.pprint(interview_data[0]['seller_id'] )
                    answer_dict ={}
                    for i in interview_data[0]['answers']:
                        answer_dict_elem = {
                            i["question"] : i['value']
                            }
                        answer_dict.update(answer_dict_elem)
                    processed_data_elem = {
                        'seller_id': interview_data[0].get('seller_id'),
                        'version': interview_data[0].get('version'),
                        'answers' : answer_dict
                        }
                    processed_data.append(processed_data_elem)
        except FileNotFoundError:
            print("Error: The specified JSON file was not found.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from file: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    return processed_data

res2 = serilalize_data(path)

print(res2)


my_dict = {'apple': 1, 'banana': 2}
value = my_dict.get('banana') # вернет 2
value_not_exist = my_dict.get('grape', 'Ключа нет') # вернет 'Ключа нет'
print(value)
print(value_not_exist)