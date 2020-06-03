import pandas as pd
import os
import time
import re
import tempfile
import boto3
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from io import BytesIO


def crawl_corona_cases ():
    #Path of webdriver
    #chromedriver is located in /usr/local/bin/chromedriver 
    PATH = 'chromedriver'
    driver = webdriver.Chrome(PATH)
    
    #Go to URL of web
    driver.get("https://coronaboard.kr/")
    
    #Check if the title of the page is correct
    assert "코로나19(COVID-19) 실시간 상황판" in driver.title
    
    #Give 3 second pause
    time.sleep(10)

    #Parse data using beautiful soup
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    #Put the parsed data into a variable
    #1. last update time (output: 2020.5.25.10:40:17)
    last_update_time = soup.find('span',{'id':'last-updated'}).get_text()
    last_update_time = last_update_time.split()
    last_update_time = last_update_time[2]+last_update_time[3]+last_update_time[4]+last_update_time[6]
    
    #2. world total confirmed cases (output: int)
    world_total = soup.find('div',{'class':'col-4 col-sm-4 col-md-3 text-center'}).get_text()
    world_total = world_total.split('(')
    world_total_confirmed = int(world_total[0].replace(',',''))
    
    #3. world new confirmed cases for the last 24hours (output: int)
    world_new_confirmed = world_total[1].split(')')
    if world_new_confirmed[0] != '-':
        world_new_confirmed = int(world_new_confirmed[0].replace(',','').replace('+',''))
    else:
        world_new_confirmed = 0

    
    #4. world total mortality count (output: int)
    world_mortality = soup.find('div',{'class':'col-4 col-sm-4 col-md-2 text-center'}).get_text()
    world_mortality = world_mortality.split('(')
    world_total_mortality = int(world_mortality[0].replace(',',''))

    #5. world new mortality count (output: int)
    world_new_mortality = world_mortality[1].split(')')
    if world_new_mortality[0] != '-':
        world_new_mortality = int(world_new_mortality[0].replace(',','').replace('+',''))
    else:
        world_new_mortality = 0
    
    #6. Total confirmed cases in Korea
    korea_total = soup.find('div',{'class':'col-3 col-sm-3 col-md-2 text-center'}).get_text()
    korea_total = korea_total.split('(')
    korea_total_confirmed = int(korea_total[0].replace(',',''))

    #7. New confirmed cases in Korea
    korea_new_confirmed = korea_total[1].split(')')
    if korea_new_confirmed[0] !='-':
        korea_new_confirmed = int(korea_new_confirmed[0].replace(',','').replace('+',''))
    else:
        korea_new_confirmed = 0
    #8. Total mortality in Korea
    korea_mortality = soup.find('div',{'class':'col-3 col-sm-3 col-md-1 text-center'}).get_text()
    korea_mortality = korea_mortality.split('(')
    korea_total_mortality = int(korea_mortality[0].replace(',',''))

    #9. New mortality in Korea
    korea_new_mortality = korea_mortality[1].split(')')
    if korea_new_mortality[0] != '-':
        korea_new_mortality = int(korea_new_mortality[0].replace('+','')) 
    else:
        korea_new_mortality = 0
    driver.close()
    
    var_list = [
        last_update_time, 
        world_total_confirmed, 
        world_new_confirmed, 
        world_total_mortality, 
        world_new_mortality, 
        korea_total_confirmed, 
        korea_new_confirmed, 
        korea_total_mortality, 
        korea_new_mortality
        ]
    return var_list

def upload_to_s3(variable_lists):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ['BEE_AWS_ID'],
        aws_secret_access_key=os.environ['BEE_AWS_PW']
        )

    response = s3.list_objects(
        Bucket = 'beetestawsbucket',
        Prefix = 'corona/'
    )

    s3_objects = []
    if 'Contents' in response:
        for obj in response['Contents']:
            s3_objects.append(obj['Key'])
    
    today = str(datetime.today().date()).replace('-','')
    print(today)

    count_list = {
            'update_time':variable_lists[0],
            'world_cases':variable_lists[1],
            'world_new_cases':variable_lists[2],
            'world_mortality':variable_lists[3],
            'world_new_mortality':variable_lists[4],
            'korea_cases':variable_lists[5],
            'korea_new_cases':variable_lists[6],
            'korea_mortality':variable_lists[7],
            'korea_new_mortality':variable_lists[8]
        }
    df = pd.DataFrame.from_dict([count_list])

    if f'corona/yyyymmdd={today}/{today}.parquet' not in s3_objects:

        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer)

        Bucket = 'beetestawsbucket'
        Key = f'corona/yyyymmdd={today}/{today}.parquet'

        s3.delete_object(
            Bucket = Bucket,
            Key = Key
            )
        
        # Upload the object
        s3.put_object(
            Bucket = Bucket,
            Key = Key, 
            Body=parquet_buffer.getvalue()
            )
    
    elif f'corona/yyyymmdd={today}/{today}.parquet' in s3_objects:
        Bucket = 'beetestawsbucket'
        Key = f'corona/yyyymmdd={today}/{today}.parquet'
        
        tempdir = tempfile.TemporaryDirectory(dir = '/Users/ihanbi/Desktop')

        s3.download_file(
            Bucket = Bucket,
            Key = Key,
            Filename = f'{tempdir.name}/tempfile.parquet'
        )


        tempdf = pd.read_parquet(f'{tempdir.name}/tempfile.parquet')
        new_df = pd.concat([df, tempdf])
        
        print(new_df)

        new_df.to_parquet(f'{tempdir.name}/tempfile2.parquet')

        s3.delete_object(
            Bucket = Bucket,
            Key = Key
            )
        
        # Upload the object
        s3.upload_file(
            Filename = f'{tempdir.name}/tempfile2.parquet' ,
            Bucket = Bucket,
            Key = Key
            )

  
if __name__ == "__main__":
    variable_lists = crawl_corona_cases()
    upload_to_s3(variable_lists)
