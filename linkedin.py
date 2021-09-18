import os
import csv
import glob
import time
import shutil
import pandas as pd
import mysql.connector
from selenium import webdriver
from selenium.webdriver.common import keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
options = Options()
rawdata = os.path.join('C:/Users/RDATS/Desktop/Projects/rawdata')
dataFloderPath = os.path.join('C:/Users/RDATS/Desktop/Projects/Data')
src = os.path.join('C:/Users/RDATS/Desktop/Projects/Data/')
dest = os.path.join('C:/Users/RDATS/Desktop/Projects/backupData/')
driver = webdriver.Chrome(executable_path=r"C:\\Users\\RDATS\\Desktop\\Projects\\driver\\chromedriver.exe")
email = "raoshankumarsaw@gmail.com"
password = "rdats4321"
keyword = open('keyword.txt')
files = keyword.readlines()
data = []
def login():
    url = "https://www.linkedin.com/"
    driver.get(url)
    driver.implicitly_wait(5)
    driver.maximize_window()
    driver.find_element_by_id('session_key').send_keys(email)
    driver.find_element_by_id('session_password').send_keys(password)
    driver.find_element_by_class_name('sign-in-form__submit-button').click()
    driver.implicitly_wait(5)
 
def extractData():
    for subcategory in files:
        '''details link extract data...............'''
        def GetDetailsOfItem(YPLink):
            print(YPLink)
            driver.get(YPLink)
            driver.implicitly_wait(10)
            try:
                state = driver.find_element_by_class_name('jobs-unified-top-card__bullet').text
                States =','.join(state.split(',')[1:-1]).strip()
                print(States)
            except:
                States = "Remote"    
            try:
                numberOfEmployees = driver.find_element_by_xpath("//div[@class='mt5 mb2']/div[2]/span").text
                print(numberOfEmployees)
            except:
                numberOfEmployees = ""    
            try:
                companiesDescription = driver.find_element_by_xpath("//p[@class='t-14 mt5']").text
                print(companiesDescription)
            except:
                companiesDescription = " "    
            driver.implicitly_wait(10)
            return pd.Series([companiesDescription, numberOfEmployees, States])

        '''Information_Technology_Software_SubCategory search one by one.......................'''
        url = "https://www.linkedin.com/jobs/"
        driver.get(url)
        driver.implicitly_wait(5)
        driver.find_element_by_css_selector("input[class='jobs-search-box__text-input jobs-search-box__keyboard-text-input']").send_keys(subcategory)
        driver.find_element_by_xpath('//*[@id="global-nav-search"]/div/div[2]/button[1]').click()
        driver.implicitly_wait(5)
        text = driver.find_element_by_xpath("//small[@class='display-flex t-12 t-black--light t-normal']").text.strip("results")
        driver.implicitly_wait(30)
        comma = text.replace(',', '')
        totalCount = int(comma)
        print(totalCount)
        loops = int(totalCount/25)+1
        print(loops)
        '''loop running................'''
        for lo in range(loops):
            actions = ActionChains(driver)
            time.sleep(5)
            '''Scraping data like jobPosition, companyofferingtheJob, Location and also detailsLink '''
            for prod in driver.find_elements_by_class_name("occludable-update"):
                actions.move_to_element(prod).perform()
                try:
                    Position = prod.find_element_by_class_name('job-card-list__title').text
                    print(Position)
                except:
                    Position = ""
                try:
                    companyName = prod.find_element_by_class_name('job-card-container__company-name').text
                    print(companyName)
                except: 
                    companyName = ""           
                try:
                    Location = prod.find_element_by_class_name('job-card-container__metadata-wrapper').find_element_by_class_name("job-card-container__metadata-item").text
                    print(Location)
                except: 
                    Location = "" 
                try:
                    detailsLink = prod.find_element_by_class_name("job-card-list__title").get_attribute('href')
                    print(detailsLink)
                except: 
                    detailsLink = "" 
             
                '''Data Append.........................................'''
                data.append([companyName, Position, Location, detailsLink])

            '''next page link or see more jobs link'''  
            print("Pagination==============================================================")
            try:
                current_page_number = driver.find_element_by_css_selector("li[class='artdeco-pagination__indicator artdeco-pagination__indicator--number active selected ember-view']").text
                pagenumber = int(current_page_number)
                print(f"Processing page {current_page_number}..")
                next_page_link = driver.find_element_by_css_selector("li[class='artdeco-pagination__indicator artdeco-pagination__indicator--number active selected ember-view']").find_element_by_xpath(f'//button[span = "{pagenumber + 1}"]')
                print(next_page_link)
                next_page_link.click()
            except NoSuchElementException:
                print(f"Exiting. Last page: {current_page_number}.")
                break
        if loops == 0:
            driver.close()
            pass
        else:
            testKeyword = driver.find_element_by_xpath("//h1[@class='t-16 truncate']").text
            searchKeyword = ''.join(testKeyword.split(' ')[:2])
            datadf = pd.DataFrame(data, columns=['companyName', 'Position', 'Location', 'detailsLink'])
            datadf.to_csv(os.path.join(rawdata, 'linkedin'+searchKeyword+'.csv'), index=False)  
            if len(datadf) == 0:
                driver.close()
            else:
                datadf[['companiesDescription','NumberOfEmployees', 'States']] = datadf[['detailsLink']].apply(lambda x: GetDetailsOfItem(x[0]), axis=1)
                datadf = datadf[['companyName', 'Position', 'Location', 'companiesDescription','NumberOfEmployees', 'States']]
                datadf.to_csv(os.path.join(dataFloderPath, 'linkedinDetails'+searchKeyword+'.csv'), index=False)
                
            '''Connect MySql....'''
            conn = mysql.connector.connect(host="localhost", user = "root", port=3306, password="rdats4321", database="raoshanks")
            cur = conn.cursor()
            csv_dir = "C:\\Users\\RDATS\\Desktop\\Projects\\Data\\"
            '''Any Name CSV File Inserted...............................'''
            for fcnt, csvfile in enumerate(glob.iglob(csv_dir + '*.csv')):
                with open(csvfile, encoding="utf8") as f:
                    csvreader = csv.DictReader(f)
                    next(csvreader)
                    for row in  csvreader:
                        # print(row['companyName'], row['Position'], row['Location'], row['companiesDescription'], row['NumberOfEmployees'], row['States'])
                        query = 'insert into linkedin(companyName, jobPosition, Location, Description, NumberOfEmployees, States) VALUES (%s, %s, %s, %s, %s, %s)'
                        cur.executemany(query,[(row['companyName'], row['Position'], row['Location'], row['companiesDescription'], row['NumberOfEmployees'], row['States'])])
                        conn.commit()
                        print("inserted into MySql Database...")

            '''EX..Insert data from folder and move another folder''' 
            files1 = os.listdir(src)
            print(files1)
            files1.sort()
            for file in files1:
                src1 = src + file
                dst = dest + file
                shutil.move(src1,dst)            
 
    '''Finally Driver close......'''
    driver.close()    
         
'''FunctionCall...................'''
login()
extractData()
