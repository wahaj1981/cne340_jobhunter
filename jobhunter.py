# Wahaj Al Obid
# CNE 340
# Date 02/08/2025

import mysql.connector
import time
import json
import requests
from datetime import date
import html2text


# Connect to database
def connect_to_sql():
    conn = mysql.connector.connect(
        user='root',
        password='',  # Add password if necessary
        host='127.0.0.1',
        database='job_hunter',
        charset='utf8mb4',
        collation='utf8mb4_unicode_ci'
    )
    return conn


# Create the table structure
def create_tables(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INT PRIMARY KEY AUTO_INCREMENT,
        job_id VARCHAR(50) UNIQUE,
        company VARCHAR(300),
        created_at DATE,
        url VARCHAR(3000),
        title TEXT CHARACTER SET utf8 COLLATE utf8_unicode_ci,
        salary VARCHAR(100),
        location VARCHAR(300),
        job_type VARCHAR(100),
        description TEXT CHARACTER SET utf8 COLLATE utf8_unicode_ci
    );''')


# Query the database.
# You should not need to edit anything in this function.

def query_sql(cursor, query, params=None):
    cursor.execute(query, params)  # Execute the query with params if provided
    return cursor


# Add a new job
def add_new_job(cursor, jobdetails):
    job_id = jobdetails.get('id')
    company = jobdetails.get('company_name', 'Unknown')
    created_at = jobdetails.get('publication_date', '')[:10]
    url = jobdetails.get('url', '')
    title = jobdetails.get('title', 'Unknown')
    salary = jobdetails.get('salary', 'Not specified')
    job_type = jobdetails.get('job_type', 'Not specified')
    location = jobdetails.get('candidate_required_location', 'Not specified')
    description = html2text.html2text(jobdetails.get('description', ''))

    query = '''
        INSERT INTO jobs (job_id, company, created_at, url, title, salary, job_type, location, description) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        company = VALUES(company),
        created_at = VALUES(created_at),
        url = VALUES(url),
        title = VALUES(title),
        salary = VALUES(salary),
        job_type = VALUES(job_type),
        location = VALUES(location),
        description = VALUES(description);
    '''
    values = (job_id, company, created_at, url, title, salary, job_type, location, description)

    query_sql(cursor, query, values)  # Pass values as parameters


# Check if new job exists
def check_if_job_exists(cursor, jobdetails):
    job_id = jobdetails.get('id')
    company = jobdetails.get('company')

    query = "SELECT 1 FROM jobs WHERE job_id = %s AND company = %s"
    query_sql(cursor, query, (job_id, company))  # Pass params as a tuple (job_id, company)

    return cursor.fetchone() is not None  # Returns True if job exists, False otherwise


# Deletes job
def delete_job(cursor, jobdetails):
    job_id = jobdetails.get('id')

    query = "DELETE FROM jobs WHERE job_id = %s"
    query_sql(cursor, query, (job_id,))


# Grab new jobs from a website, parse JSON code, and insert the data into a list of dictionaries.
def fetch_new_jobs():
    query = requests.get("https://remotive.com/api/remote-jobs")  # Updated API endpoint
    datas = json.loads(query.text)  # Parse the JSON response
    return datas.get('jobs', [])  # Return the list of jobs


# Main area of the code. Should not need to edit.

def jobhunt(cursor):
    jobpage = fetch_new_jobs()  # Fetch new jobs
    add_or_delete_job(jobpage, cursor)  # Add or delete jobs based on the current database


# Add or delete jobs based on API data
def add_or_delete_job(jobpage, cursor):
    for jobdetails in jobpage:  # Loop through each job in the list
        if check_if_job_exists(cursor, jobdetails):  # Check if the job already exists
            print(f"Job exists: {jobdetails['title']} at {jobdetails['company_name']}")
        else:
            print(f"New job found: {jobdetails['title']} at {jobdetails['company_name']}")
            add_new_job(cursor, jobdetails)


# Setup portion of the program. Take arguments and set up the script.

def main():
    conn = connect_to_sql()
    cursor = conn.cursor()
    create_tables(cursor)  # Create the necessary tables in the database

    while True:  # Infinite loop: Runs continuously as a background process
        try:
            jobhunt(cursor)  # Fetch jobs and process them
            conn.commit()  # Commit the changes to the database
        except Exception as e:
            print(f"Error occurred: {e}")
            conn.rollback()  # Rollback if an error occurs
        time.sleep(14400)  # Sleep for 4 hours


if __name__ == '__main__':
    main()
