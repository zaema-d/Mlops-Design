
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import string
import re
# !pip install nltk
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
# !pip install pandas
import pandas as pd

titles=[]
descriptions=[]
urls = []
clean_titles=[]
clean_descriptions=[]

def extract_title_and_description(url):
    try:
        response = requests.get(url)
        html_content = response.content
        soup = BeautifulSoup(html_content, 'html.parser')
        title = soup.find('title').get_text() if soup.find('title') else None
        description_tag = soup.find('meta', attrs={'name': 'description'})
        description = description_tag['content'] if description_tag else None
        return title, description
    except Exception as e:
        print(f"Error fetching content from {url}: {e}")
        return None, None




def clean_text(text):
    clean_text = re.sub(r'<[^>]+>', '', text)
    clean_text = clean_text.lower()

    words = word_tokenize(clean_text)
    
    words = [word for word in words if word.isalpha()]

    stop_words = set(stopwords.words('english'))
    words = [word for word in words if word not in stop_words]

    lemmatizer = WordNetLemmatizer()
    lemmatized_words = [lemmatizer.lemmatize(word) for word in words]

    clean_text = ' '.join(lemmatized_words)
    
    return clean_text

# nltk.download('punkt')
# nltk.download('stopwords')
# nltk.download('wordnet')


def extract():
    sources = ['https://www.dawn.com/', 'https://www.bbc.com/']
    reqs = requests.get(sources[0])
    
    soup = BeautifulSoup(reqs.text, 'html.parser')
    # print(soup)

    for link in soup.find_all('a',class_='story__link'):
        if link.get('href') is not None and link.get('href').startswith('http'):
           urls.append(link.get('href'))
    urls
   
    for i in urls:
        title, description = extract_title_and_description(i)
        if title and description:
            # print(f"Title: {title}")
            # print(f"Description: {description}")
            titles.append(title)
            descriptions.append(description)
        else:
            print("Title and/or description not found.")


def transform():
    for i in titles:
        clean_titles.append(clean_text(i))
    for i in descriptions:
        clean_descriptions.append(clean_text(i))



def load():
    df=pd.DataFrame()
    df=df.assign(Title=clean_titles,Description=clean_descriptions)
    display(df)
    df.to_csv('webscrapped.csv',index=False)


extract()
transform()
load()
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='i202416_A2',
    default_args=default_args,
    description='webscraping automation ',
     schedule=timedelta(days=1),
)


task1 = PythonOperator(
    task_id = "Task_1",
    python_callable = extract,
    dag = dag
)

task2 = PythonOperator(
    task_id = "Task_2",
    python_callable = transform,
    dag=dag
)


task3 = PythonOperator(
    task_id = "Task_3",
    python_callable = load,
    dag=dag
)

task4 = BashOperator(
    task_id= 'Task4',
    bash_command='dvc add webscrapped.csv',
    dag=dag
)

task5 = BashOperator(
    task_id='Task5',
    bash_command='git commit -am "update data" && git push origin main',
    dag=dag
)
task6 = BashOperator(
    task_id='task6',
    bash_command='dvc push',
    dag=dag,
)
task1 >> task2 >> task3 >> task4 >> task5>>task6




