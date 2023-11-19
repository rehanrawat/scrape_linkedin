import scrapy

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class GSheets:
    def __init__(self) -> None:
        self.creds = None
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

        if os.path.exists("token.json"):
            self.creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
                )

            self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(self.creds.to_json())
            
    def create(self, title):
        try:
            service = build("sheets", "v4", credentials=self.creds)
            spreadsheet = {
                'properties': {'title': title}
            }
            # Call the Sheets API
            spreadsheet = service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
            return spreadsheet.get('spreadsheetId')

        except HttpError as err:
            print(err)
            return err

    def batch_update_values(self, spreadsheet_id, range_name, value_input_options, values):
        try:
            service = build("sheets", "v4", credentials=self.creds)
            body={
                'valueInputOption':value_input_options,
                'data':[{
                    'range':range_name,
                    'values':values
                }]
            }
            result=service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
            print('{} cells updated.', format(result.get("totalUpdatedCells")))
            return result
        except HttpError as err:
            print(err)
            return err





class LinkedJobsSpider(scrapy.Spider):
    name = "linkedin"
    api_url = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=&location=Worldwide&locationId=&geoId=92000000&f_TPR=&f_JT=F%2CC&f_WT=2&start=' 

    def start_requests(self):
        first_job_on_page = 0
        first_url = self.api_url + str(first_job_on_page)
        yield scrapy.Request(url=first_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})

    def parse_job(self, response):
        first_job_on_page = response.meta['first_job_on_page']

        job_item = {}
        jobs = response.css("li")

        num_jobs_returned = len(jobs)
        print("******* Num Jobs Returned *******")
        print(num_jobs_returned)
        print('*****')

        sheet = GSheets()
        sid = sheet.create('scraped_data')  
        
        for job in jobs:
            company_location = job.css('.job-search-card__location::text').get(default='not-found').strip()
            if "United States" not in company_location or "Canada" not in company_location:
                job_item['job_title'] = job.css("h3::text").get(default='not-found').strip()
                job_item['job_detail_url'] = job.css(".base-card__full-link::attr(href)").get(default='not-found').strip()
                job_item['job_listed'] = job.css('time::text').get(default='not-found').strip()
                job_item['company_image'] = job.css('div img::attr(data-delayed-url)').get(default='not-found')
                job_item['company_name'] = job.css('h4 a::text').get(default='not-found').strip()
                job_item['company_link'] = job.css('h4 a::attr(href)').get(default='not-found')
                job_item['company_location'] = job.css('.job-search-card__location::text').get(default='not-found').strip()
                sheet.batch_update_values(sid, 'A1:A10', 'RAW', [[value for key, value in job_item.items()]])
                yield job_item
            
        
        #### REQUEST NEXT PAGE OF JOBS HERE ######
        if num_jobs_returned > 0:
            first_job_on_page = int(first_job_on_page) + 25
            next_url = self.api_url + str(first_job_on_page)
            yield scrapy.Request(url=next_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})
