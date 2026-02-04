#!/usr/bin/env python3
"""
COMPLETE Standalone Sequential University Data Scraper - FULL VERSION
-----------------------------------------------------------------------
This file contains ALL extraction code with EXACT functions and prompts from:
- Institution.py (all ~80+ functions)
- Department.py (complete extraction)
- All Programs sub-modules (graduate & undergraduate):
  * extract_programs_list.py
  * program_extra_fields.py
  * extract_test_scores_requirements.py
  * extract_application_requirements.py
  * extract_program_details_financial.py
  * merge_and_standardize.py
- merge_all.py (final merge logic)
- Programs.py (orchestration)
- sequential_scraper.py (main workflow)

NO MODIFICATIONS to original prompts - All code copied exactly as-is.

Usage:
    python sequential_scraper_complete.py "University Name"

Dependencies:
    pip install pandas google-genai python-dotenv openpyxl
"""

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================
import os
import sys
import json
import pandas as pd
import time
import random
import requests
from urllib.parse import urlparse
from google import genai
from google.genai import types
from dotenv import load_dotenv
import logging
import re
import csv
import queue
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

# ============================================================================
# INSTITUTION.PY - EXACT COPY OF ALL FUNCTIONS
# ============================================================================



# Wrapper for compatibility with existing code structure
class GeminiModelWrapper:
    def __init__(self, client, model_name):
        self.client = client
        self.model_name = model_name

    def generate_content(self, prompt, max_retries=5, base_delay=2):
        # Configure the search tool for every call to ensure live data
        google_search_tool = types.Tool(
            google_search=types.GoogleSearch()
        )

        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=[google_search_tool]
                    )
                )
                return response
            except Exception as e:
                # Check for 503 (Unavailable) or 429 (Resource Exhausted)
                # The google-genai SDK exceptions might vary, so we check broadly for now
                # and refine if needed. Common codes are 503 and 429.
                error_str = str(e)
                if "503" in error_str or "429" in error_str or "Too Many Requests" in error_str or "Overloaded" in error_str:
                    if attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        sleep_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        logger.warning(f"Attempt {attempt + 1} failed with error: {e}. Retrying in {sleep_time:.2f} seconds...")
                        time.sleep(sleep_time)
                        continue
                
                # If it's not a retryable error or we've run out of retries, raise it
                logger.error(f"Failed to generate content after {attempt + 1} attempts: {e}")
                raise e

# Initialize the model wrapper
model = GeminiModelWrapper(client, os.getenv("MODEL"))

# Helper functions for Institution extraction
def generate_text_safe(prompt):
    try:
        response = model.generate_content(prompt)
        
        # 1. Handle Safety/Empty blocks before accessing .text
        if not response.candidates or not response.candidates[0].content.parts:
            logger.warning("Model blocked the response or returned empty.")
            return "null"
            
        text = response.text
        
        # 2. Clean up specific artifacts while preserving structure
        # We keep it simple but ensure we don't return an empty string if we can help it
        clean_text = text.replace("```json", "").replace("```", "").strip()
        
        return clean_text if clean_text else "null"

    except Exception as e:
        # 3. Log the specific error to help with debugging the Scraper
        logger.error(f"Error generating content: {e}")
        return "null"

def extract_clean_value(response_text):
    if not response_text:
        return None
    
    # 1. Basic Cleanup
    text = response_text.replace("**", "").replace("```", "").strip()
    
    # 2. Split by common separators (Evidence, URLs, etc.)
    separators = ["\nEvidence:", "\nURL:", "\nSource:", "\nSnippet:", "\nQuote:"]
    for sep in separators:
        # Using a case-insensitive search
        idx = text.lower().find(sep.lower())
        if idx != -1:
            text = text[:idx].strip()
            break

    # 3. Get the first line
    text = text.split('\n')[0].strip()

    # 4. HANDLE KEY-VALUE PAIRS (NEW)
    # If the first line is "Allowed: True" or "Status: Required", 
    # we want to strip the "Allowed:" or "Status:" part.
    if ":" in text:
        parts = text.split(":", 1) # Split only on the first colon
        text = parts[1].strip()

    # 5. Handle "null"
    if text.lower() == "null" or not text:
        return None
        
    # 6. Fix incomplete URLs
    if text.startswith("//"):
        text = "https:" + text
    elif text.startswith("www."):
        text = "https://" + text
        
    return text

# Logic moved to process_institution_extraction

def get_academic_calender_url(website_url, university_name):
    prompt = (
        f"What is the academic calender URL for the university {university_name} on the website {website_url}. "
        f"Search query: site:{website_url} academic calender "
        "Return only the academic calender URL, no other text. "
        "No fabrication or guessing, just the academic calender URL. "
        "Only if the academic calender URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the academic calender URL is explicitly stated."
    )
    academic_calender_url = generate_text_safe(prompt)
    academic_calender_url = extract_clean_value(academic_calender_url)
    return academic_calender_url

def get_cost_of_attendance_url(website_url, university_name):
    prompt = (
        f"What is the cost of attendance URL for the university {university_name} on the website {website_url}. "
        f"Search query: site:{website_url} cost of attendance "
        "Return only the cost of attendance URL, no other text. "
        "No fabrication or guessing, just the cost of attendance URL. "
        "Only if the cost of attendance URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the cost of attendance URL is explicitly stated."
    )
    cost_of_attendance_url = generate_text_safe(prompt)
    cost_of_attendance_url = extract_clean_value(cost_of_attendance_url)
    return cost_of_attendance_url

def get_tuition_fee_url(website_url, university_name):
    prompt = (
        f"Find the tuition fee URL for the university {university_name} on the website {website_url}. "
        f"Search query: site:{website_url} tuition fees cost of attendance "
        "Return only the tuition fee URL, no other text. "
        "No fabrication or guessing, just the tuition fee URL. "
        "Only if the tuition fee URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the tuition fee URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_international_students_requirements_url(website_url, university_name):
    prompt = (
        f" What is the international students application requirements page url for the university {university_name} on the website {website_url}. "
        f"Search query: site:{website_url} international students application requirements "
        "Return only the international students application requirements page url, no other text. "
        "No fabrication or guessing, just the international students application requirements page url. "
        "Only if the international students application requirements page url is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the international students application requirements page url is explicitly stated."
    )
    return generate_text_safe(prompt)

############################################################################################################################################################


############################################################################################################################################################
                                                 # Functions to extract the data from the website #
############################################################################################################################################################
def get_womens_college(website_url, university_name):
    prompt = (
        f"Is the university {university_name}, {website_url} a women's college? "
        "Return only 'yes' or 'no', no other text. "
        "No fabrication or guessing, just yes or no. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_cost_of_living_min(website_url, university_name):
    prompt = (
        f"What is the minimum cost of living for students at the university {university_name} ,{website_url}? "
        "Return only the minimum cost of living amount, no other text. "
        "No fabrication or guessing, just the minimum cost of living. "
        "Only if the minimum cost of living is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the minimum cost of living is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_cost_of_living_max(website_url, university_name):
    prompt = (
        f"What is the maximum cost of living for students at the university {university_name}, {website_url}? "
        "Return only the maximum cost of living amount, no other text. "
        "No fabrication or guessing, just the maximum cost of living. "
        "Only if the maximum cost of living is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the maximum cost of living is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_orientation_available(website_url, university_name):
    prompt = (
        f"Is orientation available for students at the university {university_name}, {website_url}? "
        "Return only 'yes' or 'no', no other text. "
        "No fabrication or guessing, just yes or no. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_college_tour_after_admissions(website_url, university_name):
    prompt = (
        f"Does the university {university_name}, {website_url} offer in-person college tours after admissions? "
        "Return only 'yes' or 'no', no other text. "
        "No fabrication or guessing, just yes or no. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_university_name(website_url, university_name):
    prompt = (
        f"What is the name of the university {university_name} for the website {website_url}? "
        "Return only the name of the university, no other text. "
        "No fabrication or guessing, just the name of the university. "
        "Only if the name of the university is explicitly stated in the website, "
        "otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where "
        "the name of the university is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_college_setting(website_url, university_name):
    prompt = (
        f"What is the college setting for the university {university_name}, {website_url}? "
        "Search query: site:{website_url} college setting "
        "Example: urban, suburban, rural, etc. "
        "Return only the college setting, no other text. "
        "No fabrication or guessing, just the college setting. "
        "Only if the college setting is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the college setting is explicitly stated."
    )

    return generate_text_safe(prompt)

def get_type_of_institution(website_url, university_name):
    prompt = (
        f"What is the type of institution for the university {university_name}, {website_url}? "
        "Return only the type of institution, no other text. "
        "No fabrication or guessing, just the type of institution. "
        "Only if the type of institution is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the type of institution is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_student_faculty(website_url, university_name):
    prompt = (
        f"What is the student faculty ratio for the university {university_name}? "
        "Return only the student faculty ration, no other text. "
        "Example: 15:1, 16:1, etc. "
        "No extra text or explanation, just the student faculty ratio. "
        "No fabrication or guessing, just the student faculty ratio. "
        "Only if the student faculty ratio is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the student faculty ratio is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_number_of_campuses(website_url, university_name):
    prompt = (
        f"What is the number of campuses for the university {university_name}, {website_url}? "
        "Return only the number of campuses, no other text. "
        "No fabrication or guessing, just the number of campuses. "
        "Only if the number of campuses is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the number of campuses is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_total_faculty_available(website_url, university_name):
    prompt = (
        f"What is the total number of faculty available for the university {university_name}, {website_url}? "
        "Return only the total number of faculty available, no other text. "
        "No fabrication or guessing, just the total number of faculty available. "
        "Only if the total number of faculty available is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the total number of faculty available is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_total_programs_available(website_url, university_name):
    prompt = (
        f"What is the total number of programs available for the university {university_name}, {website_url}? "
        "Return only the total number of programs available, no other text. "
        "No fabrication or guessing, just the total number of programs available. "
        "Only if the total number of programs available is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the total number of programs available is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_total_students_enrolled(website_url, university_name):
    prompt = (
        f"What is the total number of students enrolled in the university {university_name}, {website_url} till date? "
        "Return only the total number of students enrolled, no other text. "
        "No fabrication or guessing, just the total number of students enrolled. "
        "Only if the total number of students enrolled is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the total number of students enrolled is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_total_graduate_programs(website_url, university_name):
    prompt = (
        f"What is the total number of graduate programs offered by the university {university_name}, {website_url}? "
        "Return only the total number of graduate programs, no other text. "
        "No fabrication or guessing, just the total number of graduate programs. "
        "Only if the total number of graduate programs is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the total number of graduate programs is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_total_international_students(website_url, university_name):
    prompt = (
        f"What is the total number of international students currently enrolled in the university {university_name}, {website_url}? "
        "Return only the total number of international students, no other text. "
        "No fabrication or guessing, just the total number of international students. "
        "Only if the total number of international students is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the total number of international students is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_total_students(website_url, university_name):
    prompt = (
        f"What is the total number of students enrolled in the university {university_name}, {website_url}? "
        "Return only the total number of students, no other text. "
        "No fabrication or guessing, just the total number of students. "
        "Only if the total number of students is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the total number of students is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_total_undergrad_majors(website_url, university_name):
    prompt = (
        f"What is the total number of undergrad majors offered by the university {university_name}, {website_url}? "
        "Return only the total number of undergrad majors, no other text. "
        "No fabrication or guessing, just the total number of undergrad majors. "
        "Only if the total number of undergrad majors is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the total number of undergrad majors is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_countries_represented(website_url, university_name):
    prompt = (
        f"How many countries students are represented by the university {university_name}, {website_url}? "
        "Return only the countries count, no other text. "
        "No fabrication or guessing, just the countries represented. "
        "Only if the countries represented is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the countries represented is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_street(website_url, university_name):
    prompt = (
        f"What is the street address for the university {university_name}, {website_url}? "
        "Return only just the street address, no other text. do not return extra address like city, state, country etc."
        "No fabrication or guessing, just the address. "
        "Only if the address is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the address is explicitly stated."
    )
    return generate_text_safe(prompt)



def get_county(website_url, university_name):
    prompt = (
        f"What county is the university {university_name}, {website_url} located in? "
        "Return only the county name, no other text. "
        "No fabrication or guessing, just the county name. "
        "Only if the county is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the county is explicitly stated."
    )
    return generate_text_safe(prompt)


def get_city(website_url, university_name):
    prompt = (
        f"What city is the university {university_name}, {website_url} located in? "
        "Return only the city name, no other text. "
        "No fabrication or guessing, just the city name. "
        "Only if the city is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the city is explicitly stated."
    )
    return generate_text_safe(prompt)


def get_state(website_url, university_name):
    prompt = (
        f"What state is the university {university_name}, {website_url} located in? "
        "Return only the state name, no other text. "
        "No fabrication or guessing, just the state name. "
        "Only if the state is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the state is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_country(website_url, university_name):
    prompt = (
        f"What country is the university {university_name}, {website_url} located in? "
        "Return only the country name, no other text. "
        "No fabrication or guessing, just the country name. "
        "Only if the country is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the country is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_zip_code(website_url, university_name):
    prompt = (
        f"What is the zip code for the university {university_name}, {website_url}? "
        "Return only the zip code, no other text. "
        "No fabrication or guessing, just the zip code. "
        "Only if the zip code is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the zip code is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_application_requirements(website_url, university_name):
    prompt = (
        f"What are the application requirements for the university {university_name}, {website_url}? "
        "Return only the application requirements, no other text. "
        "No fabrication or guessing, just the application requirements. "
        "Only if the application requirements is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the application requirements is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_contact_information(website_url, university_name):
    prompt = (
        f"What is the contact information for the university {university_name}, {website_url}? "
        "Return only the contact information, no other text. "
        "No fabrication or guessing, just the contact information. "
        "Only if the contact information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the contact information is explicitly stated."
    )
    return generate_text_safe(prompt)


"""
def get_grad_tuition(website_url, university_name, graduate_tuition_fee_urls=None, common_tuition_fee_urls=None):
    # Use specific URL if provided, else use common URL, else use website_url
    url_to_use = graduate_tuition_fee_urls if graduate_tuition_fee_urls else (common_tuition_fee_urls if common_tuition_fee_urls else website_url)
    prompt = (
        f"What is the average graduate tuition for the university {university_name} at {url_to_use}? "
        "Return only the graduate tuition, no other text. "
        "No fabrication or guessing, just the graduate tuition. "
        "Only if the graduate tuition is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the graduate tuition is explicitly stated."
    )
    return generate_text_safe(prompt)
"""

def get_grad_international_students(website_url, university_name):
    prompt = (
        f"What is the number of graduate international students for the university {university_name}, {website_url}? "
        "Return only the number of graduate international students, no other text. "
        "No fabrication or guessing, just the number of graduate international students. "
        "Only if the number of graduate international students is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the number of graduate international students is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_grad_scholarship_high(website_url, university_name, graduate_financial_aid_urls=None, common_financial_aid_urls=None):
    # Use specific URL if provided, else use common URL, else use website_url
    url_to_use = graduate_financial_aid_urls if graduate_financial_aid_urls else (common_financial_aid_urls if common_financial_aid_urls else website_url)
    prompt = (
        f"What is the highest graduate scholarship for the university {university_name} at {url_to_use}? "
        "Return only the highest graduate scholarship, no other text. "
        "No fabrication or guessing, just the highest graduate scholarship. "
        "Only if the highest graduate scholarship is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the highest graduate scholarship is explicitly stated."
    )
    return generate_text_safe(prompt)

#logopath is retrieved from Azure blob storage as it will be uploaded from the UI
"""
def get_logo_path(website_url, university_name):
    prompt = (
        f"What is the logo path or URL for the university {university_name}, {website_url}? "
        "Return only the logo path or URL, no other text. "
        "No fabrication or guessing, just the logo path. "
        "Only if the logo path is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the logo path is explicitly stated."
    )
    return generate_text_safe(prompt)
"""

def get_phone(website_url, university_name):
    prompt = (
        f"What is the main phone number for the university {university_name}, {website_url}? "
        "Return only the phone number, no other text. "
        "No fabrication or guessing, just the phone number. "
        "Only if the phone number is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the phone number is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_email(website_url, university_name):
    prompt = (
        f"What is the main contact email address for the university {university_name}, {website_url}? "
        " If there is no main contact email address, find the admissions email address."
        "Return only the email address, no other text. "
        "No fabrication or guessing, just the email address. "
        "Only if the email address is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the email address is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_secondary_email(website_url, university_name):
    prompt = (
        f"What is the secondary email address for the university {university_name}, {website_url}? "
        "Return only the secondary email address, no other text. "
        "No fabrication or guessing, just the secondary email address. "
        "Only if the secondary email address is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the secondary email address is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_website_url(website_url, university_name):
    prompt = (
        f"What is the official website URL for the university {university_name}, {website_url}? "
        "Return only the website URL, no other text. "
        "No fabrication or guessing, just the website URL. "
        "Only if the website URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the website URL is explicitly stated."
        "the return response should be http or https URL"
    )
    return generate_text_safe(prompt)

def get_admission_office_url(website_url, university_name):
    prompt = (
        f"What is the admission office URL for the university {university_name}, {website_url}? "
        "Return only the admission office URL, no other text. "
        "No fabrication or guessing, just the admission office URL. "
        "Only if the admission office URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the admission office URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_virtual_tour_url(website_url, university_name):
    prompt = (
        f"What is the virtual tour URL for the university {university_name}? "
        "Identify the url that is routed to the virtual tour page of {university_name} and not to the home page of the website"
        "Return only the virtual tour URL, no other text. "
        "if the direct url to the virtual tour page is not found then return the url of the page where the virtual tour is mentioned"
        "No fabrication or guessing, just the virtual tour URL. "
        "Only if the virtual tour URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the virtual tour URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_financial_aid_url(website_url, university_name):
    prompt = (
        f"What is the financial aid URL for the university {university_name}, {website_url}? "
        "Return only the financial aid URL, no other text. "
        "No fabrication or guessing, just the financial aid URL. "
        "Only if the financial aid URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the financial aid URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_application_fees(website_url, university_name):
    prompt = (
        f"Find the application fee for both domestic and international applicants for the university {university_name}, {website_url}? "
        "Return a line of text with the application fee for both domestic and international applicants, no other text. " 
        "Do not return the text like 'The application fee for graduate programs is not explicitly stated for domestic applicants on the university's website'. In this case just return what you find so far in the website. If you don't find something then don't explicitly mention in the return response."
        "No fabrication or guessing, just the application fee for both domestic and international applicants."
        "Example of the return response: 'The application fee for both domestic and international applicants is $amount. (or) The application fee for domestic applicants is $amount and for international applicants is $amount. '"
        "Only if the application fees are explicitly stated in the website, otherwise return null. "
        "Do not return [Cite] in the return response."
        "Only refer the {website_url} or the {university_name}.edu or it's sub domains or it's pages to find the application fees."
        "Do not refer any other third party websites to find the application fees."
        "Also provide the evidence for your answer with correct URL or page where the application fees are explicitly stated."
    )
    return generate_text_safe(prompt)

def get_test_policy(website_url, university_name):
    prompt = (
        f"Is {university_name} a test optional university? {website_url}? "
        "If ACT/SAT  scores submission is optional for the university, return 'Test Optional'. "
        "If ACT/SAT  scores submission is required for the university, return 'Test Required'. "
        "Return only the test policy, no other text. "
        "No fabrication or guessing, just a short line of text not a long paragraph. "
        "Do not return [Cite] in the return response."
        "The answers should be either 'Test Optional' or 'Test Required'"
        "Only return the test policy if it is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the test policy is explicitly stated."
    )
    return generate_text_safe(prompt)

"""
def get_courses_and_grades(website_url, university_name):
    prompt = (
        f"What are the courses and grades requirements for the university {university_name}, {website_url}? "
        "Return only the courses and grades requirements, no other text. "
        "No fabrication or guessing, just the courses and grades requirements. "
        "Only return the courses and grades requirements if they are explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the courses and grades requirements are explicitly stated."
    )
    return generate_text_safe(prompt)
"""

def get_recommendations(website_url, university_name):
    url_to_use = get_international_students_requirements_url(website_url, university_name)
    prompt = (
        f"How many letter of recommendations are required to apply for both undergraduate and graduate programs for the university {university_name}, {url_to_use}? "
        "Return only the count of letter of recommendations required, no other text. "
        "Go through the application requirements  using the {url_to_use} to find the count of letter of recommendations required. "
        "If the count is different for undergraduate and graduate programs, just return the count of letter of recommendations required for graduate programs. "
        "No fabrication or guessing, just the count of letter of recommendations required. "
        "Do not return [Cite] in the return response."
        "Example: 2, 3, 4, etc. "
        "Only return the count of letter of recommendations required if they are explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the count of letter of recommendations required are explicitly stated."
    )
    return generate_text_safe(prompt)

def get_personal_essay(website_url, university_name):
    prompt = (
        f"Investigate the undergraduate admissions requirements for {university_name} at {website_url}. "
        "I am looking specifically for 'Personal Essays' or 'Personal Statements'.\n\n"
        "Return only  Required or Not Required or null. "
        "No fabrication or guessing, just the personal essay requirements. "
        "Only if the personal essay requirements are explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the personal essay requirements are explicitly stated."
        "Critical: Except Required or Not Required or null, do not return any other text."
       
    )
    return generate_text_safe(prompt)

def get_writing_sample(website_url, university_name):
    prompt = (
        f"Does applying to the university {university_name}, {website_url} require a writing sample to submit as part of the application? "
        "If yes, return Required. if not, return Not Required. no extra text "
        "No fabrication or guessing, just the writing sample requirements. "
        "Only if the writing sample requirements are explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the writing sample requirements are explicitly stated."
        "Critical: Except Required or Not Required or null, do not return any other text."
    )
    return generate_text_safe(prompt)

"""
def get_additional_information(website_url, university_name):
    prompt = (
        f"Is there any additional information required to apply to the university {university_name}, {website_url}? "
        "If yes, return a short line of text about the additional information requirements. if not, return null. no extra text "
        "No fabrication or guessing, just the additional information requirements. "
        "Only if the additional information requirements are explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the additional information requirements are explicitly stated."
    )
    return generate_text_safe(prompt)
"""
def get_additional_deadlines(website_url, university_name):
    prompt = (
        f"Context: Researching {university_name} using {website_url}.\n"
        "Task: Identify specific non-application deadlines (scholarships, financial aid, housing, etc.).\n\n"
        "Constraint 1: Use ONLY information explicitly stated on the provided website. Do not use external knowledge.\n"
        "Constraint 2: If no specific dates are found, return exactly the word 'null' and nothing else.\n"
        "Constraint 3: Do not provide introductory text, explanations, or conversational fillers.\n\n"
        "Format your response exactly as follows:\n"
        "[Additional Deadlines] [Deadline Name]: [Date], [Deadline Name]: [Date]\n"
        "[Source URL] [Direct link to the page containing these dates]\n\n"
        "If no dates found, return: null"
    )
    return generate_text_safe(prompt)

def get_is_multiple_applications_allowed(website_url, university_name):
    requirements_url = get_international_students_requirements_url(website_url, university_name)
    
    prompt = (
        f"Context: {university_name} application policy ({website_url}, {requirements_url}).\n\n"
        "Task: Determine if an applicant can apply to more than one program for the same term.\n\n"
        "Return ONLY a valid JSON object. Do not include any other text, markdown formatting, or explanations.\n"
        "If the information is not explicitly found, return the JSON with null values.\n\n"
        "JSON Schema:\n"
        "{\n"
        "  \"allowed\": boolean or null,\n"
        "  \"restrictions\": \"string or null\",\n"
        "  \"evidence_url\": \"string or null\",\n"
        "  \"quote\": \"string or null\"\n"
        "}\n\n"
        "Constraint: The 'allowed' field must be true, false, or null based on the evidence."
    )
    return generate_text_safe(prompt)

def get_is_act_required(website_url, university_name):
    prompt = (
        f"Is ACT scorerequired for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_analytical_not_required(website_url, university_name):
    prompt = (
        f"Is analytical writing not required for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_analytical_optional(website_url, university_name):
    prompt = (
        f"Is analytical writing optional for the university {university_name}, {website_url}? "
        "Check through the website or its pages to find the answer. "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_duolingo_required(website_url, university_name):
    prompt = (
        f"Is Duolingo required for the university {university_name}, {website_url}? "
        "Check through the website or its pages to find the answer. "
        "Does international students need to take Duolingo?"
        "If the website explicitly states that the university does not require Duolingo, return 'False'. "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_els_required(website_url, university_name):
    prompt = (
        f"Is ELS required for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_english_not_required(website_url, university_name):
    prompt = (
        f"Is English proficiency not required for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_english_optional(website_url, university_name):
    prompt = (
        f"Is English proficiency test optional for the university {university_name}, {website_url}? "
        "if the website explicitly states the international student does not need to take English proficiency test, return 'True'. "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_gmat_or_gre_required(website_url, university_name):
    prompt = (
        f"Is GMAT or GRE required for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_gmat_required(website_url, university_name):
    prompt = (
        f"Is GMAT required for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_gre_required(website_url, university_name):
    prompt = (
        f"Is GRE score required for the university {university_name}, {website_url} to apply for any program for the international students? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_ielts_required(website_url, university_name):
    prompt = (
        f"Is IELTS score required for the university {university_name}, {website_url} to apply for any program for the international students? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_lsat_required(website_url, university_name):
    prompt = (
        f"Is LSAT scores are required to apply for the law school programs at the university {university_name}, {website_url}? "
        "If LSAT is mandatory then return 'True' otherwise return 'False'. "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_mat_required(website_url, university_name):
    prompt = (
        f"Context: Investigating graduate admission requirements for {university_name} using {website_url}.\n\n"
        "Task: Check if the Miller Analogies Test (MAT) is still listed as a requirement for any program.\n"
        "Note: The MAT was retired in late 2023. Look for whether the school explicitly accepts old scores or has replaced the requirement.\n\n"
        "Return ONLY a valid JSON object with the following keys:\n"
        "{\n"
        "  \"Allowed\": boolean or null,\n"
        "  \"status\": \"string (e.g., 'Required', 'Optional', 'Retired/No longer accepted', or 'null')\",\n"
        "  \"evidence_url\": \"string (The exact URL where this is mentioned)\",\n"
        "  \"quote\": \"string (The specific text from the site)\"\n"
        "}\n\n"
        "Constraint: If the information is missing or the site only mentions GRE/GMAT, set is_required to false and status to 'null'. Do not guess."
    )
    return generate_text_safe(prompt)

def get_is_mcat_required(website_url, university_name):
    prompt = (
        f"Is MCAT required for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_pte_required(website_url, university_name):
    prompt = (
        f"Is PTE required for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_sat_required(website_url, university_name):
    prompt = (
        f"Is SAT required for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_is_toefl_ib_required(website_url, university_name):
    prompt = (
        f"Is TOEFL iBT required for the university {university_name}, {website_url}? "
        "Return only 'True' or 'False', no other text. "
        "No fabrication or guessing, just True or False. "
        "Only if this information is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where this information is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_tuition_fees(website_url, university_name):
    # Use common URL if provided, else use website_url

    tuition_fee_url = get_tuition_fee_url(website_url, university_name)
    prompt = (
        f"Look for the tuition fees for the university {university_name} at {tuition_fee_url}. "
        "Please find the tuition fee for semester or year according to the website for the for both the undergraduate and graduate programs. "
        "The answer should be like this: 'Undergraduate (Full-Time): ~$7,438 per year (Resident), ~$19,318 (Non-Resident/Supplemental Tuition).Graduate (Full-Time): ~$8,872 per year (Resident), ~$18,952 (Non-Resident/Supplemental Tuition).' "
        "Return exactly how the above format is. "
        "Find for both the Graduate and Undergraduate tuition fees. "
        "No fabrication or guessing, just the answer you find in the website. or it's pages. "
        "Only if the tuition fees are explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the tuition fees are explicitly stated."
    )
    return generate_text_safe(prompt)

def get_facebook(website_url, university_name):
    prompt = (
        f"What is the Facebook URL for the university {university_name}, {website_url}? "
        "Return only the Facebook URL, no other text. "
        "No fabrication or guessing, just the Facebook URL. "
        "Only if the Facebook URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the Facebook URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_instagram(website_url, university_name):
    prompt = (
        f"What is the Instagram URL for the university {university_name}, {website_url}? "
        "Return only the Instagram URL, no other text. "
        "No fabrication or guessing, just the Instagram URL. "
        "Only if the Instagram URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the Instagram URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_twitter(website_url, university_name):
    prompt = (
        f"What is the Twitter URL for the university {university_name}, {website_url}? "
        "Return only the Twitter URL, no other text. "
        "No fabrication or guessing, just the Twitter URL. "
        "Only if the Twitter URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the Twitter URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_youtube(website_url, university_name):
    prompt = (
        f"What is the YouTube URL for the university {university_name}, {website_url}? "
        "Return only the YouTube URL, no other text. "
        "No fabrication or guessing, just the YouTube URL. "
        "Only if the YouTube URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the YouTube URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_tiktok(website_url, university_name):
    prompt = (
        f"What is the TikTok URL for the university {university_name}, {website_url}? "
        "Return only the TikTok URL, no other text. "
        "No fabrication or guessing, just the TikTok URL. "
        "Only if the TikTok URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the TikTok URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_linkedin(website_url, university_name):
    prompt = (
        f"What is the LinkedIn URL for the university {university_name}, {website_url}? "
        "Return only the LinkedIn URL, no other text. "
        "No fabrication or guessing, just the LinkedIn URL. "
        "Only if the LinkedIn URL is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the LinkedIn URL is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_grad_avg_tuition(website_url, university_name, graduate_tuition_fee_urls=None, common_tuition_fee_urls=None):
    # Establish a hierarchy of URLs to check
    coa_url = get_cost_of_attendance_url(website_url, university_name)
    url_to_use = coa_url or graduate_tuition_fee_urls or common_tuition_fee_urls or website_url
    
    prompt = (
        f"Identify the average annual graduate tuition for {university_name} using this source: {url_to_use}. "
        "\n\nInstructions:"
        "\n1. Look for 'Base Graduate Tuition', 'Standard Graduate Rate', or 'Master's/PhD Tuition'."
        "\n2. If different rates exist, prioritize the 'Out-of-State' or 'Non-Resident' annual rate for a full-time student."
        "\n3. If only a 'per credit hour' rate is found, multiply it by 18 (the standard annual full-time load) and provide that total."
        "\n4. Do NOT include 'Cost of Attendance' (which includes housing/food). Return ONLY the tuition portion."
        "\n\nStrict Output Format:"
        "\nLine 1: Return ONLY the numerical value with currency symbol (e.g., $15,400). If not found, return 'null'."
        "\nLine 2: Evidence: <URL to the specific tuition table> or the text snippet where the value is found"
        "\n\nConstraint: No guessing. If the page lists 10 different rates for 10 different programs and no 'base' rate, then  find the average of all the rates and provide that total."
        "\n\n Follow the same instructions as above and provide the answer in the same format.")
    return generate_text_safe(prompt)

def get_grad_scholarship_low(website_url, university_name, graduate_financial_aid_urls=None, common_financial_aid_urls=None):
    # Use specific URL if provided, else use common URL, else use website_url
    url_to_use = graduate_financial_aid_urls if graduate_financial_aid_urls else (common_financial_aid_urls if common_financial_aid_urls else website_url)
    prompt = (
        f"What is the lowest graduate scholarship for the university {university_name} at {url_to_use}? "
        "Return only the lowest graduate scholarship, no other text. "
        "No fabrication or guessing, just the lowest graduate scholarship. "
        "Only if the lowest graduate scholarship is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the lowest graduate scholarship is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_grad_total_students(website_url, university_name):
    prompt = (
        f"What is the total number of graduate students at the university {university_name}, {website_url}? "
        "Return only the total number of graduate students, no other text. "
        "No fabrication or guessing, just the total number of graduate students. "
        "Only if the total number of graduate students is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the total number of graduate students is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_ug_avg_tuition(website_url, university_name, undergraduate_tuition_fee_urls=None, common_tuition_fee_urls=None):
    # Establish a hierarchy of URLs to check
    coa_url = get_cost_of_attendance_url(website_url, university_name)
    url_to_use = coa_url or undergraduate_tuition_fee_urls or common_tuition_fee_urls or website_url
    prompt = (  
        f"Identify the average annual undergraduate tuition for {university_name} using this source: {url_to_use}. "
        "\n\nInstructions:"
        "\n1. Look for 'Base Undergraduate Tuition', 'Standard Undergraduate Rate', or 'Bachelor's Tuition'."
        "\n2. If different rates exist, prioritize the 'Out-of-State' or 'Non-Resident' annual rate for a full-time student."
        "\n3. If only a 'per credit hour' rate is found, multiply it by 30 (the standard annual full-time load) and provide that total."
        "\n4. Do NOT include 'Cost of Attendance' (which includes housing/food). Return ONLY the tuition portion."
        "\n\nStrict Output Format:"
        "\nLine 1: Return ONLY the numerical value with currency symbol (e.g., $15,400). If not found, return 'null'."
        "\nLine 2: Evidence: <URL to the specific tuition table> or the text snippet where the value is found"
        "\n\nConstraint: No guessing. If the page lists 10 different rates for 10 different programs and no 'base' rate, then  find the average of all the rates and provide that total."
        "\n\n Follow the same instructions as above and provide the answer in the same format."
    )

    return generate_text_safe(prompt)

def get_ug_international_students(website_url, university_name):
    prompt = (
        f"What is the number of undergraduate international students for the university {university_name}, {website_url}? "
        "Return only the number of undergraduate international students, no other text. "
        "No fabrication or guessing, just the number of undergraduate international students. "
        "Only if the number of undergraduate international students is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the number of undergraduate international students is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_ug_scholarship_high(website_url, university_name, undergraduate_financial_aid_urls=None, common_financial_aid_urls=None):
    # Use specific URL if provided, else use common URL, else use website_url
    url_to_use = undergraduate_financial_aid_urls if undergraduate_financial_aid_urls else (common_financial_aid_urls if common_financial_aid_urls else website_url)
    prompt = (
        f"What is the highest undergraduate scholarship for the university {university_name} at {url_to_use}? "
        "Return only the highest undergraduate scholarship, no other text. "
        "The value can be in percentage or amount. "
        "No fabrication or guessing, just the highest undergraduate scholarship. "
        "Only if the highest undergraduate scholarship is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the highest undergraduate scholarship is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_ug_scholarship_low(website_url, university_name, undergraduate_financial_aid_urls=None, common_financial_aid_urls=None):
    # Use specific URL if provided, else use common URL, else use website_url
    url_to_use = undergraduate_financial_aid_urls if undergraduate_financial_aid_urls else (common_financial_aid_urls if common_financial_aid_urls else website_url)
    prompt = (
        f"What is the lowest scholarship that can be awarded to undergraduate students at the university {university_name} at {url_to_use}? "
        "The value can be in percentage or amount. "
        "Return only the lowest undergraduate scholarship, no other text. "
        "No fabrication or guessing, just the lowest undergraduate scholarship. "
        "Only if the lowest undergraduate scholarship is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the lowest undergraduate scholarship is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_ug_total_students(website_url, university_name):
    prompt = (
        f"What is the total number of undergraduate students at the university {university_name}, {website_url}? "
        "Return only the total number of undergraduate students, no other text. "
        "No fabrication or guessing, just the total number of undergraduate students. "
        "Only if the total number of undergraduate students is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the total number of undergraduate students is explicitly stated."
    )
    return generate_text_safe(prompt)

def get_term_format(website_url, university_name):
    academic_calender_url = get_academic_calender_url(website_url, university_name)
    print(f"Academic Calendar URL: {academic_calender_url}")
    # Use the specific calendar URL if found, otherwise fall back to the main site
    search_context = academic_calender_url if academic_calender_url else website_url

    prompt = (
        f"Identify the academic calendar system (term format) for {university_name} using this source: {search_context}. "
        "Look for whether the school operates on a Semester, Quarter, or Trimester system. "
        "\n\nStrict Output Instructions:"
        "\n1. Return ONLY the single word: 'Semester', 'Quarter', 'Trimester', or 'null'."
        "\n2. Do not include sentences, introductory text, or explanations."
        "\n3. After the single word, on a new line, provide the URL used as 'Evidence: <URL>'."
        "\n\nSearch guidance: Focus on terms like 'Academic Calendar', 'Credit Hours', or 'Term System'."
    )
    return generate_text_safe(prompt)

def get_introduction(website_url, university_name):
    prompt = (
        f"Find 2-3 paragraphs of introduction for {university_name} at {website_url}? "
        "The introduction should be about the university, its history, mission, vision, and values. "
        "Return only the introduction, no other text. "
        "No fabrication or guessing, just the introduction. "
        "Only if the introduction is explicitly stated in the website, otherwise return null. "
        "Also provide the evidence for your answer with correct URL or page where the introduction is explicitly stated."
    )
    return generate_text_safe(prompt)

def process_institution_extraction(
    university_name, 
    undergraduate_tuition_fee_urls=None, 
    graduate_tuition_fee_urls=None, 
    undergraduate_financial_aid_urls=None, 
    graduate_financial_aid_urls=None,
    common_financial_aid_urls=None,
    common_tuition_fee_urls=None
):
    print(f"Processing {university_name}...")
    yield '{"status": "progress", "message": "Initializing extraction..."}'
    
    # 1. Get Website URL
    yield f'{{"status": "progress", "message": "Finding official website for {university_name}..."}}'
    prompt = f"What is the official university website for {university_name}?"
    website_url = generate_text_safe(prompt)
    print(f"Found Website URL: {website_url}")
    # 2. Get Tuition Fee URL
    yield f'{{"status": "progress", "message": "Finding tuition fee URL for {university_name}..."}}'
    # Use AI to find the tuition fee URLs
    ai_found_tuition_url = get_tuition_fee_url(website_url, university_name)
    
    print(f"Found Tuition Fee URL: {ai_found_tuition_url}")

    # New fields at the top
    yield '{"status": "progress", "message": "Extracting general information..."}'
    new_fields_data = {
        "womens_college": get_womens_college(website_url, university_name),
        "cost_of_living_min": get_cost_of_living_min(website_url, university_name),
        "cost_of_living_max": get_cost_of_living_max(website_url, university_name),
        "orientation_available": get_orientation_available(website_url, university_name),
        "college_tour_after_admissions": get_college_tour_after_admissions(website_url, university_name),
        "term_format": get_term_format(website_url, university_name),
        "introduction": get_introduction(website_url, university_name),
    }

    yield '{"status": "progress", "message": "Extracting application requirements..."}'
    application_data = {
        "application_requirements": get_application_requirements(website_url, university_name),
        "application_fees": get_application_fees(website_url, university_name),
        "test_policy": get_test_policy(website_url, university_name),
        "courses_and_grades": None,
        "recommendations": get_recommendations(website_url, university_name),
        "personal_essay": get_personal_essay(website_url, university_name),
        "writing_sample": get_writing_sample(website_url, university_name),
        "additional_information": None,
        "additional_deadlines": get_additional_deadlines(website_url, university_name),
        "tuition_fees": get_tuition_fees(website_url, university_name),
    }
    yield '{{ "status": "progress", "tuition_fees": "{tuition_fees}" }}'.format(tuition_fees=application_data["tuition_fees"])


    yield '{"status": "progress", "message": "Extracting university metrics..."}'
    university_data = {
        "university_name": get_university_name(website_url, university_name),
        "college_setting": get_college_setting(website_url, university_name),
        "type_of_institution": get_type_of_institution(website_url, university_name),
        "student_faculty": get_student_faculty(website_url, university_name),
        "number_of_campuses": get_number_of_campuses(website_url, university_name),
        "total_faculty_available": get_total_faculty_available(website_url, university_name),
        "total_programs_available": get_total_programs_available(website_url, university_name),
        "total_students_enrolled": get_total_students_enrolled(website_url, university_name),
        "total_graduate_programs": get_total_graduate_programs(website_url, university_name),
        "total_international_students": get_total_international_students(website_url, university_name),
        "total_students": get_total_students(website_url, university_name),
        "total_undergrad_majors": get_total_undergrad_majors(website_url, university_name),
        "countries_represented": get_countries_represented(website_url, university_name),
    }

    yield '{"status": "progress", "message": "Extracting address details..."}'
    address_data = {
        "street1": get_street(website_url, university_name),
        "street2": None,  # This would need a separate function if needed
        "county": get_county(website_url, university_name),
        "city": get_city(website_url, university_name),
        "state": get_state(website_url, university_name),
        "country": get_country(website_url, university_name),
        "zip_code": get_zip_code(website_url, university_name),
    }

    
    yield '{"status": "progress", "message": "Extracting contact information..."}'
    contact_data = {
        "contact_information": get_contact_information(website_url, university_name),
        "logo_path": None,
        "phone": get_phone(website_url, university_name),
        "email": get_email(website_url, university_name),
        "secondary_email": get_secondary_email(website_url, university_name),
        "website_url": get_website_url(website_url, university_name),
        "admission_office_url": get_admission_office_url(website_url, university_name),
        "virtual_tour_url": get_virtual_tour_url(website_url, university_name),
        "financial_aid_url": get_financial_aid_url(website_url, university_name),
    }

    yield '{"status": "progress", "message": "Extracting social media links..."}'
    social_media_data = {
        "facebook": get_facebook(website_url, university_name),
        "instagram": get_instagram(website_url, university_name),
        "twitter": get_twitter(website_url, university_name),
        "youtube": get_youtube(website_url, university_name),
        "tiktok": get_tiktok(website_url, university_name),
        "linkedin": get_linkedin(website_url, university_name),
    }

    yield '{"status": "progress", "message": "Extracting student statistics..."}'
    student_statistics_data = {
        "grad_avg_tuition": get_grad_avg_tuition(website_url, university_name, ai_found_tuition_url, common_tuition_fee_urls),
        "grad_international_students": get_grad_international_students(website_url, university_name),
        "grad_scholarship_high": get_grad_scholarship_high(website_url, university_name, graduate_financial_aid_urls, common_financial_aid_urls),
        "grad_scholarship_low": get_grad_scholarship_low(website_url, university_name, graduate_financial_aid_urls, common_financial_aid_urls),
        "grad_total_students": get_grad_total_students(website_url, university_name),
        "ug_avg_tuition": get_ug_avg_tuition(website_url, university_name, ai_found_tuition_url, common_tuition_fee_urls),
        "ug_international_students": get_ug_international_students(website_url, university_name),
        "ug_scholarship_high": get_ug_scholarship_high(website_url, university_name, undergraduate_financial_aid_urls, common_financial_aid_urls),
        "ug_scholarship_low": get_ug_scholarship_low(website_url, university_name, undergraduate_financial_aid_urls, common_financial_aid_urls),
        "ug_total_students": get_ug_total_students(website_url, university_name),
    }

    yield '{"status": "progress", "message": "Finalizing data..."}'
    raw_multiple = get_is_multiple_applications_allowed(website_url, university_name)
    raw_mat = get_is_mat_required(website_url, university_name)
    
    # Handle multiple applications parsing with error handling
    try:
        clean_multiple = raw_multiple.strip('`').replace('json', '').strip()
        if clean_multiple:
            data_multiple = json.loads(clean_multiple)
            value = str(data_multiple.get("allowed", "None"))
        else:
            value = "None"
    except (json.JSONDecodeError, AttributeError, Exception) as e:
        print(f"Error parsing multiple applications data: {e}")
        value = "None"


    # Handle MAT requirement parsing with error handling
    try:
        clean_mat = raw_mat.strip('`').replace('json', '').strip()
        if clean_mat:
            data_mat = json.loads(clean_mat)
            mat_value = str(data_mat.get("Allowed", "None")) if data_mat else "None"
        else:
            mat_value = "None"
    except (json.JSONDecodeError, AttributeError, Exception) as e:
        print(f"Error parsing MAT requirement data: {e}")
        mat_value = "None"
    boolean_fields_data = {
        "is_additional_information_available": "FALSE", 
        "is_multiple_applications_allowed": value,
        "is_act_required": get_is_act_required(website_url, university_name),
        "is_analytical_not_required": get_is_analytical_not_required(website_url, university_name),
        "is_analytical_optional": get_is_analytical_optional(website_url, university_name),
        "is_duolingo_required": get_is_duolingo_required(website_url, university_name),
        "is_els_required": get_is_els_required(website_url, university_name),
        "is_english_not_required": get_is_english_not_required(website_url, university_name),
        "is_english_optional": get_is_english_optional(website_url, university_name),
        "is_gmat_or_gre_required": get_is_gmat_or_gre_required(website_url, university_name),
        "is_gmat_required": get_is_gmat_required(website_url, university_name),
        "is_gre_required": get_is_gre_required(website_url, university_name),
        "is_ielts_required": get_is_ielts_required(website_url, university_name),
        "is_lsat_required": str(get_is_lsat_required(website_url, university_name)),
        "is_mat_required": mat_value,
        "is_mcat_required": get_is_mcat_required(website_url, university_name),
        "is_pte_required": get_is_pte_required(website_url, university_name),
        "is_sat_required": get_is_sat_required(website_url, university_name),
        "is_toefl_ib_required": get_is_toefl_ib_required(website_url, university_name),
        "is_import_verified": "FALSE",
        "is_imported": None,
        "is_enrolled": "FALSE",
    }

    #combine the data into one dict
    all_data = {
        "new_fields_data": new_fields_data,
        "university_data": university_data,
        "address_data": address_data,
        "application_data": application_data,
        "contact_data": contact_data,
        "social_media_data": social_media_data,
        "student_statistics_data": student_statistics_data,
        "boolean_fields_data": boolean_fields_data,
    }

    # Merge all dictionaries into one flat dictionary (without nesting) for CSV/Excel
    merged_data = {}
    merged_data.update(new_fields_data)
    merged_data.update(university_data)
    merged_data.update(address_data)
    merged_data.update(application_data)
    merged_data.update(contact_data)
    merged_data.update(social_media_data)
    merged_data.update(student_statistics_data)
    merged_data.update(boolean_fields_data)

    # Clean the values (remove evidence, URLs, etc.)
    def clean_data_values(data_dict):
        """
        Cleans values in a dictionary using extract_clean_value.
        Returns a dict with cleaned values (no evidence, URLs, or extra text).
        """
        cleaned = {}
        for k, v in data_dict.items():
            if isinstance(v, str):
                cleaned[k] = extract_clean_value(v)
            else:
                cleaned[k] = v
        return cleaned

    flat_data = clean_data_values(merged_data)

    # Define new fields that should be at the end
    new_fields_list = list(new_fields_data.keys())

    # Create ordered column list: university_name first, then others (excluding new fields), then new fields at end
    ordered_columns = []
    if 'university_name' in flat_data:
        ordered_columns.append('university_name')

    # Add all other columns except university_name and new fields
    for key in flat_data.keys():
        if key != 'university_name' and key not in new_fields_list:
            ordered_columns.append(key)

    # Add new fields at the end
    for key in new_fields_list:
        if key in flat_data:
            ordered_columns.append(key)

    # Sanitize university name for filename (replace spaces with underscores, remove special characters)
    safe_university_name = university_name.replace(" ", "_").replace("/", "_").replace("\\", "_")

    def rename_columns(df, flat_data):
        """
        Rename columns to match final required column names and ensure all required columns are present.
        Missing columns will be added as empty.
        """
        # Mapping from current column names to final column names
        column_mapping = {
            'university_name': 'CollegeName',
            'college_setting': 'CollegeSetting',
            'type_of_institution': 'InstitutionType',
            'student_faculty': 'Student_Faculty',
            'number_of_campuses': 'NumberOfCampuses',
            'total_faculty_available': 'TotalFacultyAvailable',
            'total_programs_available': 'TotalProgramsAvailable',
            'total_students_enrolled': 'TotalStudentsEnrolled',
            'total_graduate_programs': 'TotalGraduatePrograms',
            'total_international_students': 'TotalInternationalStudents',
            'total_students': 'TotalStudents',
            'total_undergrad_majors': 'TotalUndergradMajors',
            'countries_represented': 'CountriesRepresented',
            'street1': 'Street1',
            'street2': 'Street2',
            'county': 'County',
            'city': 'City',
            'state': 'State',
            'country': 'Country',
            'zip_code': 'ZipCode',
            'application_fees': 'ApplicationFees',
            'test_policy': 'TestPolicy',
            'courses_and_grades': 'CoursesAndGrades',
            'recommendations': 'Recommendations',
            'personal_essay': 'PersonalEssay',
            'writing_sample': 'WritingSample',
            'additional_information': 'AdditionalInformation',
            'additional_deadlines': 'AdditionalDeadlines',
            'tuition_fees': 'TuitionFees',
            'logo_path': 'LogoPath',
            'phone': 'Phone',
            'email': 'Email',
            'secondary_email': 'SecondaryEmail',
            'website_url': 'WebsiteUrl',
            'admission_office_url': 'AdmissionOfficeUrl',
            'virtual_tour_url': 'VirtualTourUrl',
            'financial_aid_url': 'FinancialAidUrl',
            'facebook': 'Facebook',
            'instagram': 'Instagram',
            'twitter': 'Twitter',
            'youtube': 'Youtube',
            'tiktok': 'Tiktok',
            'linkedin': 'LinkedIn',
            'introduction': 'Introduction',
            'grad_avg_tuition': 'GradAvgTuition',
            'grad_international_students': 'GradInternationalStudents',
            'grad_scholarship_high': 'GradScholarshipHigh',
            'grad_scholarship_low': 'GradScholarshipLow',
            'grad_total_students': 'GradTotalStudents',
            'ug_avg_tuition': 'UGAvgTuition',
            'ug_international_students': 'UGInternationalStudents',
            'ug_scholarship_high': 'UGScholarshipHigh',
            'ug_scholarship_low': 'UGScholarshipLow',
            'ug_total_students': 'UGTotalStudents',
            'is_additional_information_available': 'IsAdditionalInformationAvailable',
            'is_multiple_applications_allowed': 'IsMultipleApplicationsAllowed',
            'is_act_required': 'IsACTRequired',
            'is_analytical_not_required': 'IsAnalyticalNotRequired',
            'is_analytical_optional': 'IsAnalyticalOptional',
            'is_duolingo_required': 'IsDuoLingoRequired',
            'is_els_required': 'IsELSRequired',
            'is_english_not_required': 'IsEnglishNotRequired',
            'is_english_optional': 'IsEnglishOptional',
            'is_gmat_or_gre_required': 'IsGMATOrGreRequired',
            'is_gmat_required': 'IsGMATRequired',
            'is_gre_required': 'IsGRERequired',
            'is_ielts_required': 'IsIELTSRequired',
            'is_lsat_required': 'IsLSATRequired',
            'is_mat_required': 'IsMATRequired',
            'is_mcat_required': 'IsMCATRequired',
            'is_pte_required': 'IsPTERequired',
            'is_sat_required': 'IsSATRequired',
            'is_toefl_ib_required': 'IsTOEFLIBRequired',
            'is_import_verified': 'IsImportVerified',
            'is_imported': 'IsImported',
            'is_enrolled': 'IsEnrolled',
            'term_format': 'TermFormat',
        }
        
        # All required final column names
        final_columns = [
            'CollegeName', 'CollegeCode', 'LogoPath', 'Phone', 'Email', 'SecondaryEmail',
            'Street1', 'Street2', 'County', 'City', 'State', 'Country', 'ZipCode', 'WebsiteUrl',
            'AdmissionOfficeUrl', 'VirtualTourUrl', 'Facebook', 'Instagram', 'Twitter', 'Youtube',
            'Tiktok', 'ApplicationFees', 'TestPolicy', 'CoursesAndGrades', 'Recommendations',
            'PersonalEssay', 'WritingSample', 'FinancialAidUrl', 'AdditionalInformation',
            'AdditionalDeadlines', 'IsAdditionalInformationAvailable', 'Status',
            'IsMultipleApplicationsAllowed', 'MaximumApplicationsAllowed', 'CreatedBy',
            'CreatedDate', 'LiveDate', 'TuitionFees', 'UpdatedBy', 'UpdatedDate', 'CountryCode',
            'LinkedIn', 'IsACTRequired', 'IsAnalyticalNotRequired', 'IsAnalyticalOptional',
            'IsDuoLingoRequired', 'IsELSRequired', 'IsEnglishNotRequired', 'IsEnglishOptional',
            'IsGMATOrGreRequired', 'IsGMATRequired', 'IsGRERequired', 'IsIELTSRequired',
            'IsLSATRequired', 'IsMATRequired', 'IsMCATRequired', 'IsPTERequired', 'IsSATRequired',
            'IsTOEFLIBRequired', 'QsWorldRanking', 'UsRanking', 'BatchId', 'IsImportVerified',
            'IsImported', 'BannerImagePath', 'CollegeHtmlAdditionalInfo', 'Introduction',
            'NumberOfCampuses', 'TotalFacultyAvailable', 'TotalProgramsAvailable',
            'TotalStudentsEnrolled', 'CollegeSetting', 'TypeofInstitution', 'CountriesRepresented',
            'GradAvgTuition', 'GradInternationalStudents', 'GradScholarshipHigh',
            'GradScholarshipLow', 'GradTotalStudents', 'Student_Faculty', 'TotalGraduatePrograms',
            'TotalInternationalStudents', 'TotalStudents', 'TotalUndergradMajors', 'UGAvgTuition',
            'UGInternationalStudents', 'UGScholarshipHigh', 'UGScholarshipLow', 'UGTotalStudents',
            'InstitutionType', 'IsEnrolled','TermFormat', 'OGAEnrolledProgramLevels'
        ]
        
        # Rename existing columns
        df_renamed = df.rename(columns=column_mapping)
        
        # Add missing columns with empty values
        for col in final_columns:
            if col not in df_renamed.columns:
                df_renamed[col] = ''
        
        # Reorder columns to match final_columns order
        df_renamed = df_renamed[final_columns]
        
        return df_renamed

    # Create output directory if it doesn't exist
    # Use absolute path based on the script location to ensure consistency
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "Inst_outputs")
    os.makedirs(output_dir, exist_ok=True)

    # Create DataFrame from flat_data
    df = pd.DataFrame([flat_data])

    # Rename columns and ensure all required columns are present
    df_final = rename_columns(df, flat_data)

    # Write to CSV
    csv_filename = os.path.join(output_dir, f"{safe_university_name}_Institution.csv")
    df_final.to_csv(csv_filename, index=False, encoding='utf-8')

    # Write to Excel
    excel_filename = os.path.join(output_dir, f"{safe_university_name}_Institution.xlsx")
    try:
        df_final.to_excel(excel_filename, index=False, engine='openpyxl')
    except ImportError:
        print(f"Warning: openpyxl is not installed. Install it with: pip install openpyxl")
        print(f"Excel file {excel_filename} not created, but CSV is available.")
    except Exception as e:
        print(f"Error saving to Excel: {e}")
        print(f"Excel file {excel_filename} not created, but CSV is available.")


    # for the json, I want to save the data as a json file with all the fields like values, evidence, urls, etc.
    json_filename = os.path.join(output_dir, f"{safe_university_name}_Institution.json")
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)

    print(f"Saved cleaned {university_name} data to {csv_filename}, {excel_filename}, and {json_filename}.")
    yield f'{{"status": "complete", "files": {{"csv": "{csv_filename}", "excel": "{excel_filename}", "json": "{json_filename}"}}}}'


# ============================================================================
# DEPARTMENT.PY - EXACT COPY OF EXTRACTION FUNCTION
# ============================================================================



# Configure the client (using google-genai SDK)

# Define tools and model globally
def process_department_extraction(university_name):
    yield f'{{"status": "progress", "message": "Starting department extraction for {university_name}..."}}'
    
    # List of the fields that we need to extract from the website
    fields = [
        "DepartmentName", "Description", "Status", "CollegeId", "CreatedDate", 
        "CreatedBy", "UpdatedDate", "UpdatedBy", "City", "Country", "CountryCode", 
        "CountryName", "Email", "PhoneNumber", "PhoneType", "State", "Street1", 
        "Street2", "ZipCode", "StateName", "MaximumApplicationsPerTerm", 
        "IsRecommendationSystemOpted", "AdmissionUrl", "BuildingName", 
        "BatchId", "IsImportVerified", "IsImported", "CollegeName"
    ]

    # 1. Get Website URL
    yield f'{{"status": "progress", "message": "Finding official website for {university_name}..."}}'
    prompt = f"What is the official university website for {university_name}?"
    try:
        website_url = generate_text_safe(prompt)
        print(f"Website URL: {website_url}")
    except Exception as e:
         yield f'{{"status": "error", "message": "Failed to find website URL: {str(e)}"}}'
         return

    # 2. Extract Departments
    yield f'{{"status": "progress", "message": "Extracting admissions departments from {website_url}..."}}'
    
    # Improved prompt
    prompt = (
        f"You are extracting information about ADMISSIONS DEPARTMENTS ONLY from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website ({website_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official University of New Hampshire website, return null for that field.\n\n"
        f"Website URL: {website_url}\n\n"
        f"EXTRACTION SCOPE:\n"
        f"- Extract ONLY admissions-related departments and offices\n"
        f"- This includes: Undergraduate Admissions, Graduate Admissions, International Admissions, Transfer Admissions, any school specific admissions offices"
        f"  and any other admissions-specific offices\n"
        f"- DO NOT extract academic departments, student services, or any other non-admissions offices\n"
        f"- If no admissions departments are found, return an empty array []\n\n"
        f"For each admissions department/office found, extract the following fields ONLY if they are present on the official University of New Hampshire website:\n\n"
        f"1. Website_url: The official URL of the admissions office page on {website_url} or its subdomains. "
        f"   Must be from the {university_name} domain only. If not available, return null.\n"
        f"2. DepartmentName: The official name of the admissions office (e.g., 'Undergraduate Admissions', 'Graduate Admissions', etc.). "
        f"   Extract the exact name as it appears on the website. If not available, return null.\n"
        f"3. Email: The primary contact email address for the admissions office. "
        f"   Extract the complete email address. If not available, return null.\n"
        f"4. PhoneNumber: The primary contact phone number for admissions. Include area code and format as provided on the website. "
        f"   If not available, return null.\n"
        f"5. PhoneType: The type of phone number (e.g., 'Mobile', 'Landline', etc.). "
        f"   If not specified, return null.\n"
        f"6. AdmissionUrl: The URL specifically for admissions-related information and application process. "
        f"   Must be from the {university_name} domain only. If not available, return null.\n"
        f"7. BuildingName: The name of the building where the admissions office is located. "
        f"   Extract the exact building name as it appears on the website. If not available, return null.\n"
        f"8. Street1: The primary street address (street number and name) of the admissions office. "
        f"   Extract the complete street address line 1. If not available, return null.\n"
        f"9. Street2: Additional address information (suite number, room number, floor, etc.). "
        f"   If not available, return null.\n"
        f"10. City: The city where the admissions office is located. If not available, return null.\n"
        f"11. State: The state abbreviation (e.g., 'NY' for New York). Extract from the website. If not available, return null.\n"
        f"12. StateName: The full name of the state corresponding to the State abbreviation. "
        f"   You may derive this from the State abbreviation using standard US state mappings (e.g., 'CT' -> 'Connecticut', 'NY' -> 'New York'). "
        f"   If State is not available, return null.\n"
        f"13. Country: The country code or abbreviation (e.g., 'US', 'USA'). "
        f"   If the address is in the United States (based on State, City, or other address context), use 'US' or 'USA'. "
        f"   If the location context clearly indicates another country, use that country's code. If unclear, return null.\n"
        f"14. CountryCode: The ISO country code (e.g., 'US' for United States). "
        f"   If the address is in the United States, use 'US'. If the location context clearly indicates another country, use that country's ISO code. If unclear, return null.\n"
        f"15. CountryName: The full name of the country (e.g., 'United States'). "
        f"   If the address is in the United States, use 'United States'. If the location context clearly indicates another country, use that country's full name. If unclear, return null.\n"
        f"16. ZipCode: The postal/ZIP code. Extract the complete ZIP code including extension if provided. "
        f"    If not available, return null.\n"
        f"17. AirportPickup: Does the admissions office or university provide airport pickup service for international students? "
        f"    Return only 'yes' or 'no', no other text. "
        f"    No fabrication or guessing, just yes or no. "
        f"    Only if this information is explicitly stated in the website, otherwise return null. "
        f"    If not available, return null.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- Extract ONLY admissions departments/offices - ignore all other departments\n"
        f"- All data must be extracted ONLY from {website_url} or other official {university_name} pages\n"
        f"- For most fields: Do NOT infer, assume, or make up any information - extract verbatim from the website\n"
        f"- EXCEPTION for derived fields: StateName can be derived from State abbreviation using standard US state mappings. "
        f"  Country, CountryCode, and CountryName can be derived from location context (e.g., US address -> United States)\n"
        f"- If a field is not found on the official website and cannot be reasonably derived, return null for that field\n"
        f"- All URLs must be from the unh.edu domain or its subdomains\n"
        f"- Ensure all extracted text is accurate and verbatim from the source\n"
        f"- Extract ALL admissions departments/offices found on the website\n"
        f"- Return a JSON array of objects, where each object represents one admissions department/office\n"
        f"- Each object must contain all the fields listed above, using null for missing values\n\n"
        f"Return the data as a JSON array with the following exact keys for each admissions department/office: "
        f"'Website_url', 'DepartmentName', 'Email', 'PhoneNumber', 'PhoneType', 'AdmissionUrl', 'BuildingName', "
        f"'Street1', 'Street2', 'City', 'State', 'StateName', 'Country', 'CountryCode', 'CountryName', 'ZipCode', 'AirportPickup'. "
        f"Use null for any field where information is not available on the official website."
    )

    try:
        response_text = generate_text_safe(prompt)
        
        if not response_text:
            print("Error: Empty response from LLM")
            yield '{"status": "error", "message": "Empty response received from AI model"}'
            return

        print(f"Raw Response: {response_text[:200]}...") # Log start of response for debug

        # Remove markdown code blocks if present (handling residues)
        response_text = response_text.replace("json", "", 1) if response_text.startswith("json") else response_text
        response_text = response_text.replace("```", "").strip()
        
        # Parse the JSON response
        try:
             # Try to extract JSON from the response
             json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
             if json_match:
                 json_str = json_match.group(0)
                 departments_data = json.loads(json_str)
             else:
                 departments_data = json.loads(response_text)
                 
             if not isinstance(departments_data, list):
                 if isinstance(departments_data, dict):
                     departments_data = [departments_data]
                 else:
                     departments_data = []

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            yield f'{{"status": "error", "message": "Failed to parse AI response"}}'
            return

        yield f'{{"status": "progress", "message": "Successfully extracted {len(departments_data)} departments"}}'
        
        # Create DataFrame
        if departments_data:
            df = pd.DataFrame(departments_data)
            
            # Ensure all expected columns are present
            for field in fields:
                if field not in df.columns:
                    df[field] = None
            
            # Set specific default values
            df['IsImportVerified'] = False
            df['IsImported'] = False
            df['IsRecommendationSystemOpted'] = False
            df['CollegeName'] = university_name
    
            # Reorder columns
            df = df[fields]
            
            # Save to CSV and JSON
            
            # Use absolute path based on the script location for consistency with app.py
            script_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(script_dir, "Dept_outputs")
            os.makedirs(output_dir, exist_ok=True)
            
            safe_name = university_name.replace(" ", "_")
            csv_path = os.path.join(output_dir, f"{safe_name}_departments.csv")
            json_path = os.path.join(output_dir, f"{safe_name}_departments.json")
            
            df.to_csv(csv_path, index=False, encoding="utf-8")
            
            with open(json_path, "w", encoding="utf-8") as jf:
                json.dump(departments_data, jf, indent=4)
                
            yield f'{{"status": "complete", "files": {{"csv": "{csv_path}", "json": "{json_path}"}}}}'
            
        else:
            yield '{"status": "complete", "message": "No departments found", "files": {}}'

    except Exception as e:
        yield f'{{"status": "error", "message": "Error processing data: {str(e)}"}}'


# ============================================================================
# PROGRAMS EXTRACTION - GRADUATE PROGRAMS MODULES
# ============================================================================


# ----------------------------------------------------------------------------
# grad_step1 (graduate_programs)
# ----------------------------------------------------------------------------



# Add parent directories to sys.path to allow importing from Institution
current_dir = os.path.dirname(os.path.abspath(__file__))
# Go up 2 levels: University_Data/Programs/graduate_programs -> University_Data
# 1. .../Programs
# 2. .../University_Data
programs_dir = os.path.dirname(current_dir)
university_data_dir = os.path.dirname(programs_dir)
institution_dir = os.path.join(university_data_dir, 'Institution')
sys.path.append(institution_dir)


# Initialize the model using the wrapper (same as check.py)
model = GeminiModelWrapper(client, os.getenv("MODEL"))

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "Grad_prog_outputs")
# Create directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

def resolve_redirect(url):
    try:
        # Use HEAD request to follow redirects without downloading content
        response = requests.head(url, allow_redirects=True, timeout=5)
        return response.url
    except Exception:
        return url

def find_program_url(program_name, university_name):
    prompt = (
        f"Use Google Search to find the OFFICIAL '{program_name}' program page on the {university_name} website. "
        "1. Look at the search results. "
        "2. Identify the official '.edu' URL for this specific program. "
        "3. Do NOT return the 'vertexaisearch' or 'google.com' redirect links. "
        "4. Return ONLY the clean, direct official URL."
    )
    try:
        response = model.generate_content(prompt)
        
        # Check grounding metadata first for real URLs
        real_urls = []
        if response.candidates and response.candidates[0].grounding_metadata:
            for chunk in response.candidates[0].grounding_metadata.grounding_chunks:
                if chunk.web:
                    real_urls.append(resolve_redirect(chunk.web.uri))
        
        # Filter for .edu links
        edu_urls = [u for u in real_urls if ".edu" in u]
        
        if edu_urls:
            return edu_urls[0]
        elif real_urls:
            return real_urls[0]
        
        # Fallback to text
        text_url = response.text.replace("```", "").strip()
        # Basic clean
        match = re.search(r'https?://[^\s<>"]+|www\.[^\s<>"]+', text_url)
        if match:
             return match.group(0)
        return None
    except Exception:
        return None

def get_graduate_programs(url, university_name, existing_data=None):
    # Step 1: Extract just the names
    prompt_names = (
        f"Access the following URL: {url}\n"
        "Extract ALL graduate (Master's, PhD, Doctorate, Certificate) program NAMES listed on this page.\n"
        "When Extracting the programs names make sure the names are clear and full. which means not just the name i also want the full name like Master of Arts in Education, Master of Science in Computer Science, etc. not just Education, Computer Science, etc.\n"
        "So, get the full names of the programs.\n"
        "If the university uses 'Areas of Emphasis' or 'Concentrations' for graduate studies, include them.\n"
        "Only Look at the active and latest Programs. Do not include any expired or cancelled programs. or programs from older catalogs."
        "Return a JSON list of STRINGS (just the names).\n"
        "Example: [\"Master of Arts in Education\", \"PhD in Pharmacy\", \"Concentration in Public Administration\", ...]\n"
        "Exclude headers, categories, or navigation items."
    )
    
    program_names = []
    try:
        response = model.generate_content(prompt_names)
        text = response.text.replace("```json", "").replace("```", "").strip()
        start = text.find('[')
        end = text.rfind(']') + 1
        if start != -1 and end != -1:
             program_names = json.loads(text[start:end])
    except Exception as e:
        print(f"Error extracting names: {e}")
        yield f"Error extracting program names: {e}"
        yield [] # Return empty list on error
        return

    # Step 2: Iterate and find URLs
    results = existing_data if existing_data else []
    existing_urls = {p['Program name']: p['Program Page url'] for p in results if p.get('Program Page url') and p['Program Page url'] != url}
    
    total_programs = len(program_names)
    yield f"Found {total_programs} programs. Starting detailed URL search..."
    
    for i, name in enumerate(program_names):
        # Skip if already found with a valid URL
        if name in existing_urls:
            yield f"Skipping (already found) ({i+1}/{total_programs}): {name}"
            continue

        # Yield progress update
        yield f"Finding URL for ({i+1}/{total_programs}): {name}"
        
        found_url = find_program_url(name, university_name)
        program_entry = {
            "Program name": name,
            "Program Page url": found_url if found_url else url
        }
        
        # Save incrementally 
        # (We need to communicate this back to run())
        yield program_entry

def grad_step1_run(university_name_input):
    global university_name, institute_url
    university_name = university_name_input
    
    yield f'{{"status": "progress", "message": "Finding official website for {university_name}..."}}'
    
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Check if we already have the output
    csv_path = os.path.join(output_dir, f'{sanitized_name}_graduate_programs.csv')
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        count = len(pd.read_csv(csv_path))
        yield f'{{"status": "progress", "message": "Graduate programs list for {university_name} already exists. Skipping extraction."}}'
        yield f'{{"status": "complete", "message": "Found {count} graduate programs (using existing list)", "files": {{"grad_csv": "{csv_path}"}}}}'
        return

    prompt = f"What is the official university website for {university_name}?"
    try:
        website_url = model.generate_content(prompt).text.replace("**", "").replace("```", "").strip()
        institute_url = website_url
        yield f'{{"status": "progress", "message": "Website found: {website_url}"}}'
    except Exception as e:
        yield f'{{"status": "error", "message": "Failed to find website: {str(e)}"}}'
        return

    # Dynamic search for grad url
    yield f'{{"status": "progress", "message": "Finding graduate programs page..."}}'
    grad_url_prompt = (
        f"Use Google Search to find the OFFICIAL page listing all Graduate Degrees/Programs at {university_name}. "
        "The page should list specific majors/masters/phd programs. "
        "Return the URL. Do not generate a hypothetical URL."
    )
    try:
        response = model.generate_content(grad_url_prompt)
        
        # Check grounding metadata first for real URLs
        real_urls = []
        if response.candidates and response.candidates[0].grounding_metadata:
            for chunk in response.candidates[0].grounding_metadata.grounding_chunks:
                if chunk.web:
                    real_urls.append(resolve_redirect(chunk.web.uri))
        
        # Filter for .edu links
        edu_urls = [u for u in real_urls if ".edu" in u]
        
        if edu_urls:
            graduate_program_url = edu_urls[0]
        elif real_urls:
            graduate_program_url = real_urls[0]
        else:
            # Fallback to text
            graduate_program_url = response.text.strip()
            # clean url
            url_match = re.search(r'https?://[^\s<>"]+|www\.[^\s<>"]+', graduate_program_url)
            if url_match:
                graduate_program_url = url_match.group(0)
            
        yield f'{{"status": "progress", "message": "Graduate Page found: {graduate_program_url}"}}'
    except:
        graduate_program_url = website_url # Fallback

    yield f'{{"status": "progress", "message": "Extracting graduate programs list (this may take a while)..."}}'
    
    # Reload existing data just in case
    existing_programs = []
    json_path = os.path.join(output_dir, f'{sanitized_name}_graduate_programs.json')
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                existing_programs = json.load(f)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(existing_programs)} already found programs."}}'
        except:
            pass

    def save_progress(programs_list):
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(programs_list, f, indent=4, ensure_ascii=False)
        df = pd.DataFrame(programs_list)
        csv_path = os.path.join(output_dir, f'{sanitized_name}_graduate_programs.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8')

    # Process the generator
    current_programs = existing_programs.copy()
    existing_names = set(p['Program name'] for p in current_programs)
    
    for item in get_graduate_programs(graduate_program_url, university_name, existing_data=current_programs):
        if isinstance(item, str):
            # This is a progress message
            safe_msg = item.replace('"', "'")
            yield f'{{"status": "progress", "message": "{safe_msg}"}}'
        elif isinstance(item, dict):
            # This is a single program entry
            p_name = item.get('Program name')
            if p_name not in existing_names:
                current_programs.append(item)
                existing_names.add(p_name)
                save_progress(current_programs)
            else:
                # If name exists but we want to update URL (unlikely but safe)
                for p in current_programs:
                    if p['Program name'] == p_name:
                        p['Program Page url'] = item['Program Page url']
                        break
                save_progress(current_programs)

    if current_programs:
        yield f'{{"status": "complete", "message": "Found {len(current_programs)} graduate programs", "files": {{"grad_csv": "{os.path.join(output_dir, f"{sanitized_name}_graduate_programs.csv")}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No graduate programs found", "files": {{}}}}'




# ----------------------------------------------------------------------------
# grad_step2 (graduate_programs)
# ----------------------------------------------------------------------------



# Use GeminiModelWrapper (already initialized at top of file)
# model is already available globally

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "Grad_prog_outputs")
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, 'graduate_programs.csv')
json_path = os.path.join(output_dir, 'extra_fields_data.json')


def save_to_json(data, filepath):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def parse_json_from_response(text):
    """Parse JSON from Gemini response, handling markdown code blocks."""
    # Remove markdown formatting
    text = text.replace("**", "").replace("```json", "").replace("```", "").strip()
    
    # Try to extract JSON from the text
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # If no match, try parsing the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

def process_single_program(row, university_name):
    """Process a single program to extract extra fields."""
    program_name = row['Program name']
    program_page_url = row['Program Page url']
    
    prompt = (
        f"You are extracting information about the program '{program_name}' from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website. "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Program URL: {program_page_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website:\n"
        f"1. Concentration name: The specific concentration, specialization, or track name if the program offers concentrations. "
        f"   If no concentration is mentioned, return null.\n"
        f"2. Description: A comprehensive description of the program, its objectives, and what students will learn. "
        f"   Extract the full program description from the official page. If not available, return null.\n"
        f"3. Program website url: The official URL of the program page on {university_name} website. "
        f"   This should be a direct link to the program information page. Must be from official domain only.\n"
        f"4. Accreditation status: Any accreditation information mentioned for this specific program. "
        f"   Include the accrediting body name and status if available. If not mentioned, return null.\n\n"
        f"5. Level: The level of the program. The level can be either any of these and these are just examples : Masters, Doctoral, Associate,Certificate,MA,Minor,PhD,MBA,MFA."
        f"   This should be determined from the {program_page_url} when you are extracting there itself distingnuish the program level. If not mentioned, return null.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {program_page_url} or other official {university_name} pages\n"
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found on the official website, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n"
        f"- Ensure all extracted text is accurate and verbatim from the source\n\n"
        f"Return the data in a JSON format with the following exact keys: 'Concentration name', 'description', 'program website url', 'Accreditation status'. "
        f"Return a single JSON object, not an array. Use null for any field where information is not available on the official website."
    )
    
    try:
        print(f"[DEBUG] Generating content for program: {program_name} using model {model.model_name}")
        response = model.generate_content(prompt)
        print(f"[DEBUG] Received response for program: {program_name}")
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data:
            if isinstance(parsed_data, list) and len(parsed_data) > 0:
                parsed_data = parsed_data[0]
            
            parsed_data['Program name'] = program_name
            parsed_data['Program Page url'] = program_page_url
            return parsed_data
        else:
            return {
                'Program name': program_name, 'Program Page url': program_page_url,
                'Concentration name': None, 'description': None, 'program website url': None,
                'Accreditation status': None, 'error': 'Failed to parse JSON response'
            }
    
    except Exception as e:
        return {
            'Program name': program_name, 'Program Page url': program_page_url,
            'Concentration name': None, 'description': None, 'program website url': None,
            'Accreditation status': None, 'error': str(e)
        }

def grad_step2_run(university_name_input):
    global university_name
    university_name = university_name_input
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Update paths with university name
    csv_path = os.path.join(output_dir, f'{sanitized_name}_graduate_programs.csv')
    json_path = os.path.join(output_dir, f'{sanitized_name}_extra_fields_data.json')

    # Check if CSV file exists
    if not os.path.exists(csv_path):
        yield f'{{"status": "complete", "message": "CSV file not found: {csv_path}. Skipping Step 2.", "files": {{}}}}'
        return

    program_data = pd.read_csv(csv_path)

    if program_data.empty:
        yield f'{{"status": "error", "message": "CSV file is empty. Please check Step 1 results."}}'
        return

    # Check if required columns exist
    required_columns = ['Program name', 'Program Page url']
    missing_columns = [col for col in required_columns if col not in program_data.columns]
    if missing_columns:
        yield f'{{"status": "error", "message": "Missing columns: {", ".join(missing_columns)}"}}'
        return
        
    # Load existing data
    extra_fields_data = []
    processed_programs = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                extra_fields_data = json.load(f)
                for record in extra_fields_data:
                    program_name = record.get('Program name')
                    if program_name:
                        processed_programs.add(program_name)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(extra_fields_data)} existing records"}}'
        except Exception as e:
            pass

    # Filter out already processed programs
    programs_to_process = []
    for index, row in program_data.iterrows():
        if row['Program name'] not in processed_programs:
            programs_to_process.append(row)

    total_programs = len(program_data)
    processed_count = len(processed_programs)
    
    yield f'{{"status": "progress", "message": "Starting extraction for {total_programs} programs ({len(programs_to_process)} remaining)..."}}'

    for index, row in program_data.iterrows():
        program_name = row['Program name']
        program_page_url = row['Program Page url']
        
        if program_name in processed_programs:
            continue
        
        processed_count += 1
        yield f'{{"status": "progress", "message": "Processing [{processed_count}/{total_programs}]: {program_name}"}}'
        
        try:
            result = process_single_program(row, university_name)
            
            # Update shared data structures
            extra_fields_data.append(result)
            processed_programs.add(program_name)
            
            # Save progress (thread-safe due to lock in save_to_json)
            save_to_json(extra_fields_data, json_path)
            time.sleep(1) # Rate limit handling
            
        except Exception as e:
            yield f'{{"status": "warning", "message": "Error processing {program_name}: {str(e)}"}}'

    # Final save
    csv_output_path = os.path.join(output_dir, f'{sanitized_name}_extra_fields_data.csv')
    if extra_fields_data:
        df = pd.DataFrame(extra_fields_data)
        df.to_csv(csv_output_path, index=False, encoding='utf-8')
        yield f'{{"status": "complete", "message": "Completed extraction for {len(extra_fields_data)} programs", "files": {{"grad_extra_csv": "{csv_output_path}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No data extracted", "files": {{}}}}'


# ----------------------------------------------------------------------------
# grad_step3 (graduate_programs)
# ----------------------------------------------------------------------------



# Use GeminiModelWrapper (already initialized at top of file)
# model is already available globally

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
# output_dir = "/home/my-laptop/scraper/Quinnipiac_university/Programs/graduate_programs/Grad_prog_outputs"
output_dir = os.path.join(script_dir, "Grad_prog_outputs")
# Create directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, 'graduate_programs.csv')
json_path = os.path.join(output_dir, 'test_scores_requirements.json')

# Check if CSV file exists
# Logic moved to run()

# Check if CSV has data
# Logic moved to run()

# Check if required columns exist
# Logic moved to run()

# Institute level URL for fallback
institute_url = None # Will be set in run()
university_name = None # Will be set in run()

# Load existing data if the JSON file exists (for resuming)
# This part will be moved inside the run function

def save_to_json(data, filepath):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def parse_json_from_response(text):
    """Parse JSON from Gemini response, handling markdown code blocks."""
    # Remove markdown formatting
    text = text.replace("**", "").replace("```json", "").replace("```", "").strip()
    
    # Try to extract JSON from the text
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # If no match, try parsing the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

def extract_test_scores(program_name, program_url, institute_url):
    """Extract test scores and English requirements, first from program level, then institute level."""
    global university_name # Ensure university_name is accessible
    
    # First, try program level
    prompt_program = (
        f"You are extracting test score requirements and English language requirements for the program '{program_name}' "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Program URL: {program_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website for THIS SPECIFIC PROGRAM:\n\n"
        f"1. GreOrGmat: Whether GRE or GMAT is required, optional, or not required. Return 'GRE', 'GMAT', 'Either', 'Optional', 'Not Required', or null.\n"
        f"2. EnglishScore: Does international students need to submit English proficiency test scores? If yes then return REQUIRED else NOT REQUIRED or OPTIONAL.the field should only return one of these values.\n"
        f"3. IsDuoLingoRequired: MANDATORY BOOLEAN. Does duolingo score is accepted as an English proficiency test. If accepted then return TRUE else FALSE.\n"
        f"4. IsELSRequired: MANDATORY BOOLEAN. Is ELS (English Language Studies) score is accepted as an English proficiency test. If accepted then return TRUE else FALSE.\n"
        f"5. IsGMATOrGreRequired: MANDATORY BOOLEAN. Is either GMAT or GRE scores required to apply for this program? Return TRUE if yes, FALSE if no/optional.\n"
        f"6. IsGMATRequired: MANDATORY BOOLEAN. Is GMAT score required to apply for this program? Return TRUE or FALSE.\n"
        f"7. IsGRERequired: MANDATORY BOOLEAN. Is GRE score required to apply for this program? Return TRUE or FALSE.\n"
        f"8. IsIELTSRequired: MANDATORY BOOLEAN. Is IELTS Accepted as an English proficiency test. If accepted then return TRUE else FALSE.\n"
        f"9. IsLSATRequired: MANDATORY BOOLEAN. If this program is a law program then check if LSAT scores are required to apply for this program. if required then return TRUE else FALSE. if this is not a law program then return FALSE.\n"
        f"10. IsMATRequired: MANDATORY BOOLEAN. Is Miller Analogies Test (MAT) scores required to apply for this program? If required then return TRUE else FALSE.\n"
        f"11. IsMCATRequired: MANDATORY BOOLEAN. If this program is a medical program then check if MCAT test scores are required to apply for this program? If required then return TRUE else FALSE.\n"
        f"12. IsPTERequired: MANDATORY BOOLEAN. Is PTE (Pearson Test of English) accepted as an English proficiency test? Return TRUE or FALSE.\n"
        f"13. IsTOEFLIBRequired: MANDATORY BOOLEAN. Is TOEFL iBT (Internet-based Test) accepted as an English proficiency test? Return TRUE or FALSE.\n"
        f"14. IsTOEFLPBTRequired: MANDATORY BOOLEAN. Is TOEFL PBT (Paper-based Test) accepted as an English proficiency test? Return TRUE or FALSE.\n"
        f"15. IsEnglishNotRequired: MANDATORY BOOLEAN. Is English test explicitly NOT required? Return TRUE or FALSE.\n"
        f"16. IsEnglishOptional: MANDATORY BOOLEAN. If any English test scores are optional to submit in order to prove the English proficiency? Return TRUE or FALSE.\n"
        f"17. MinimumDuoLingoScore: Minimum required Duolingo score as a number. Return null if not specified.\n"
        f"18. MinimumELSScore: Minimum required ELS score as a number. Return null if not specified.\n"
        f"19. MinimumGMATScore: Minimum required GMAT score as a number. Return null if not specified.\n"
        f"20. MinimumGreScore: Minimum required GRE score. Can be total score or section scores. Return as string or number. Return null if not specified.\n"
        f"21. MinimumIELTSScore: Minimum required IELTS score as a number (typically 0-9). Return null if not specified.\n"
        f"22. MinimumMATScore: Minimum required MAT score as a number. Return null if not specified.\n"
        f"23. MinimumMCATScore: Minimum required MCAT score as a number. Return null if not specified.\n"
        f"24. MinimumPTEScore: Minimum required PTE score as a number. Return null if not specified.\n"
        f"25. MinimumTOEFLScore: Minimum required TOEFL score as a number. Return null if not specified.\n"
        f"26. MinimumLSATScore: Minimum required LSAT score as a number. Return null if not specified.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {program_url} or other official {university_name} pages\n"
        f"- Extract information SPECIFIC to this program '{program_name}'\n"
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found on the program page, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n"
        f"- Ensure all extracted text is accurate and verbatim from the source\n"
        f"- FOR MANDATORY BOOLEAN FIELDS: You MUST return true or false. Do not return null unless absolutely no information is available. If not mentioned as required, default to false.\n\n"
        f"Return the data in a JSON format with the following exact keys: "
        f"'GreOrGmat', 'EnglishScore', 'IsDuoLingoRequired', 'IsELSRequired', 'IsGMATOrGreRequired', "
        f"'IsGMATRequired', 'IsGRERequired', 'IsIELTSRequired', 'IsLSATRequired', 'IsMATRequired', "
        f"'IsMCATRequired', 'IsPTERequired', 'IsTOEFLIBRequired', 'IsTOEFLPBTRequired', "
        f"'IsEnglishNotRequired', 'IsEnglishOptional', 'MinimumDuoLingoScore', 'MinimumELSScore', "
        f"'MinimumGMATScore', 'MinimumGreScore', 'MinimumIELTSScore', 'MinimumMATScore', "
        f"'MinimumMCATScore', 'MinimumPTEScore', 'MinimumTOEFLScore', 'MinimumLSATScore'. "
        f"Return a single JSON object, not an array. Use null for non-boolean fields where information is not available."
    )
    
    try:
        response = model.generate_content(prompt_program)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data and isinstance(parsed_data, dict):
            # Check if we got any non-null values
            has_data = any(v is not None and v != "" for v in parsed_data.values())
            
            if has_data:
                parsed_data['extraction_level'] = 'program'
                return parsed_data
    except Exception as e:
        print(f"  Error extracting from program level: {str(e)}")
    
    # If no data found at program level, try institute level
    print(f"  No program-specific data found, trying institute level...")
    prompt_institute = (
        f"You are extracting general test score requirements and English language requirements "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Institute URL: {institute_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website as GENERAL/INSTITUTE-LEVEL requirements:\n\n"
        f"1. GreOrGmat: Whether GRE or GMAT is generally required, optional, or not required. Return 'GRE', 'GMAT', 'Either', 'Optional', 'Not Required', or null.\n"
        f"2. EnglishScore: Does international students need to submit English proficiency test scores? If yes then return REQUIRED else NOT REQUIRED or OPTIONAL. the field should only return one of these values.\n"
        f"3. IsDuoLingoRequired: MANDATORY BOOLEAN. Is Duolingo English test explicitly required? Return TRUE or FALSE.\n"
        f"4. IsELSRequired: MANDATORY BOOLEAN. Is ELS (English Language Services) required? Return TRUE or FALSE.\n"
        f"5. IsGMATOrGreRequired: MANDATORY BOOLEAN. Is either GMAT or GRE required? Return TRUE if yes, FALSE if no/optional.\n"
        f"6. IsGMATRequired: MANDATORY BOOLEAN. Is GMAT specifically required? Return TRUE or FALSE.\n"
        f"7. IsGRERequired: MANDATORY BOOLEAN. Is GRE specifically required to apply for the programs generally? If GRE is mandatory then return TRUE else FALSE.\n"
        f"8. IsIELTSRequired: MANDATORY BOOLEAN. Is IELTS Accepted as an English proficiency test. If accepted then return TRUE else FALSE.\n"
        f"9. IsLSATRequired: MANDATORY BOOLEAN. Check if LSAT scores are generally required for law programs. If required then return TRUE else FALSE.\n"
        f"10. IsMATRequired: MANDATORY BOOLEAN. Is Miller Analogies Test (MAT) scores required? If required then return TRUE else FALSE.\n"
        f"11. IsMCATRequired: MANDATORY BOOLEAN. Check if MCAT test scores are generally required for medical programs? If required then return TRUE else FALSE.\n"
        f"12. IsPTERequired: MANDATORY BOOLEAN. Is PTE (Pearson Test of English) accepted as an English proficiency test? Return TRUE or FALSE.\n"
        f"13. IsTOEFLIBRequired: MANDATORY BOOLEAN. Is TOEFL iBT (Internet-based Test) accepted as an English proficiency test? Return TRUE or FALSE.\n"
        f"14. IsTOEFLPBTRequired: MANDATORY BOOLEAN. Is TOEFL PBT (Paper-based Test) accepted as an English proficiency test? Return TRUE or FALSE.\n"
        f"15. IsEnglishNotRequired: MANDATORY BOOLEAN. Is English test explicitly NOT required? Return TRUE or FALSE.\n"
        f"16. IsEnglishOptional: MANDATORY BOOLEAN. Is English test optional? Return TRUE or FALSE.\n"
        f"17. MinimumDuoLingoScore: Minimum required Duolingo score as a number. Return null if not specified.\n"
        f"18. MinimumELSScore: Minimum required ELS score as a number. Return null if not specified.\n"
        f"19. MinimumGMATScore: Minimum required GMAT score as a number. Return null if not specified.\n"
        f"20. MinimumGreScore: Minimum required GRE score. Can be total score or section scores. Return as string or number. Return null if not specified.\n"
        f"21. MinimumIELTSScore: Minimum required IELTS score as a number (typically 0-9). Return null if not specified.\n"
        f"22. MinimumMATScore: Minimum required MAT score as a number. Return null if not specified.\n"
        f"23. MinimumMCATScore: Minimum required MCAT score as a number. Return null if not specified.\n"
        f"24. MinimumPTEScore: Minimum required PTE score as a number. Return null if not specified.\n"
        f"25. MinimumTOEFLScore: Minimum required TOEFL score as a number. Return null if not specified.\n"
        f"26. MinimumLSATScore: Minimum required LSAT score as a number. Return null if not specified.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {institute_url} or other official {university_name} pages\n"
        f"- Extract GENERAL/INSTITUTE-LEVEL requirements (not program-specific)\n"
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n\n"
        f"Return the data in a JSON format with the following exact keys: "
        f"'GreOrGmat', 'EnglishScore', 'IsDuoLingoRequired', 'IsELSRequired', 'IsGMATOrGreRequired', "
        f"'IsGMATRequired', 'IsGRERequired', 'IsIELTSRequired', 'IsLSATRequired', 'IsMATRequired', "
        f"'IsMCATRequired', 'IsPTERequired', 'IsTOEFLIBRequired', 'IsTOEFLPBTRequired', "
        f"'IsEnglishNotRequired', 'IsEnglishOptional', 'MinimumDuoLingoScore', 'MinimumELSScore', "
        f"'MinimumGMATScore', 'MinimumGREScore', 'MinimumIELTSScore', 'MinimumMATScore', "
        f"'MinimumMCATScore', 'MinimumPTEScore', 'MinimumTOEFLScore', 'MinimumLSATScore'. "
        f"Return a single JSON object, not an array. Use null for any field where information is not available on the official website."
    )
    
    try:
        response = model.generate_content(prompt_institute)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data and isinstance(parsed_data, dict):
            parsed_data['extraction_level'] = 'institute'
            return parsed_data
    except Exception as e:
        print(f"  Error extracting from institute level: {str(e)}")
    
    # Return empty dict with null values if nothing found
    return {
        'GreOrGmat': None, 'EnglishScore': None, 'IsDuoLingoRequired': None, 'IsELSRequired': None,
        'IsGMATOrGreRequired': None, 'IsGMATRequired': None, 'IsGreRequired': None, 'IsIELTSRequired': None,
        'IsLSATRequired': None, 'IsMATRequired': None, 'IsMCATRequired': None, 'IsPTERequired': None,
        'IsTOEFLIBRequired': None, 'IsTOEFLPBTRequired': None, 'IsEnglishNotRequired': None, 'IsEnglishOptional': None,
        'MinimumDuoLingoScore': None, 'MinimumELSScore': None, 'MinimumGMATScore': None, 'MinimumGreScore': None,
        'MinimumIELTSScore': None, 'MinimumMATScore': None, 'MinimumMCATScore': None, 'MinimumPTEScore': None,
        'MinimumTOEFLScore': None, 'MinimumLSATScore': None, 'extraction_level': 'none'
    }

def grad_step3_run(university_name_input):
    global university_name, institute_url
    university_name = university_name_input
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Update paths with university name
    csv_path = os.path.join(output_dir, f'{sanitized_name}_graduate_programs.csv')
    json_path = os.path.join(output_dir, f'{sanitized_name}_test_scores_requirements.json')

    # We need to find the institute URL first if not hardcoded, but for now we can rely on the previous steps or simple search if needed.
    # For now, let's just find it if we can, or pass it in. 
    # But to keep it simple and consistent with previous modification:
    yield f'{{"status": "progress", "message": "Initializing test score extraction for {university_name}..."}}'
    


    # Check if CSV file exists
    if not os.path.exists(csv_path):
        yield f'{{"status": "complete", "message": "CSV file not found: {csv_path}. Skipping Step.", "files": {{}}}}'
        return

    program_data = pd.read_csv(csv_path)

    if program_data.empty:
        yield f'{{"status": "error", "message": "CSV file is empty. Please check Step 1 results."}}'
        return

    # Check if required columns exist
    required_columns = ['Program name', 'Program Page url']
    missing_columns = [col for col in required_columns if col not in program_data.columns]
    if missing_columns:
        yield f'{{"status": "error", "message": "Missing columns: {", ".join(missing_columns)}"}}'
        return

    # Quick fetch of website url for context - LOCAL ONLY
    try:
        first_url = program_data.iloc[0]['Program Page url']
        domain = urlparse(first_url).netloc
        institute_url = f"https://{domain}"
    except:
        institute_url = f"https://www.google.com/search?q={university_name}"

    # Load existing data
    test_scores_data = []
    processed_programs = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                test_scores_data = json.load(f)
                for record in test_scores_data:
                    program_name = record.get('Program name')
                    if program_name:
                        processed_programs.add(program_name)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(test_scores_data)} existing records"}}'
        except Exception as e:
            pass

    # Filter out already processed programs
    programs_to_process = []
    for index, row in program_data.iterrows():
        if row['Program name'] not in processed_programs:
            programs_to_process.append(row)

    total_programs = len(program_data)
    processed_count = len(processed_programs)
    
    if not programs_to_process:
         yield f'{{"status": "progress", "message": "All {total_programs} programs already processed. Skipping extraction."}}'
    else:
         yield f'{{"status": "progress", "message": "Starting extraction for {total_programs} programs ({len(programs_to_process)} remaining)..."}}'

    for index, row in program_data.iterrows():
        program_name = row['Program name']
        program_page_url = row['Program Page url']
        
        if program_name in processed_programs:
            continue
        
        processed_count += 1
        yield f'{{"status": "progress", "message": "Processing [{processed_count}/{total_programs}]: {program_name}"}}'
        
        try:
            extracted_data = extract_test_scores(program_name, program_page_url, institute_url)
            
            extracted_data['Program name'] = program_name
            extracted_data['Program Page url'] = program_page_url
            test_scores_data.append(extracted_data)
            processed_programs.add(program_name)
            
            save_to_json(test_scores_data, json_path)
            time.sleep(1) # Rate limit handling
        
        except Exception as e:
            error_record = {
                'Program name': program_name, 'Program Page url': program_page_url,
                'GreOrGmat': None, 'EnglishScore': None, 'IsDuoLingoRequired': None, 'IsELSRequired': None,
                'IsGMATOrGreRequired': None, 'IsGMATRequired': None, 'IsGreRequired': None, 'IsIELTSRequired': None,
                'IsLSATRequired': None, 'IsMATRequired': None, 'IsMCATRequired': None, 'IsPTERequired': None,
                'IsTOEFLIBRequired': None, 'IsTOEFLPBTRequired': None, 'IsEnglishNotRequired': None, 'IsEnglishOptional': None,
                'MinimumDuoLingoScore': None, 'MinimumELSScore': None, 'MinimumGMATScore': None, 'MinimumGreScore': None,
                'MinimumIELTSScore': None, 'MinimumMATScore': None, 'MinimumMCATScore': None, 'MinimumPTEScore': None,
                'MinimumTOEFLScore': None, 'MinimumLSATScore': None, 'extraction_level': 'error', 'error': str(e)
            }
            test_scores_data.append(error_record)
            processed_programs.add(program_name)
            save_to_json(test_scores_data, json_path)

    # Final save
    csv_output_path = os.path.join(output_dir, f'{sanitized_name}_test_scores_requirements.csv')
    if test_scores_data:
        df = pd.DataFrame(test_scores_data)
        df.to_csv(csv_output_path, index=False, encoding='utf-8')
        yield f'{{"status": "complete", "message": "Completed extraction for {len(test_scores_data)} programs", "files": {{"grad_test_csv": "{csv_output_path}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No data extracted", "files": {{}}}}'




# ----------------------------------------------------------------------------
# grad_step4 (graduate_programs)
# ----------------------------------------------------------------------------



# Use GeminiModelWrapper (already initialized at top of file)
# model is already available globally

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
# output_dir = "/home/my-laptop/scraper/Quinnipiac_university/Programs/graduate_programs/Grad_prog_outputs"
output_dir = os.path.join(script_dir, "Grad_prog_outputs")
# Create directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, 'graduate_programs.csv')
json_path = os.path.join(output_dir, 'application_requirements.json')

university_name = None
institute_url = None

def save_to_json(data, filepath):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def parse_json_from_response(text):
    """Parse JSON from Gemini response, handling markdown code blocks."""
    # Remove markdown formatting
    text = text.replace("**", "").replace("```json", "").replace("```", "").strip()
    
    # Try to extract JSON from the text
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # If no match, try parsing the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

def extract_application_requirements(program_name, program_url, institute_url):
    """Extract application requirements and documents, first from program level, then institute level."""
    application_requirements_page_url = None
    prompt = """ Find the website url of the application requirements page for the program '{program_name}' from the official {university_name} website. Return the url if found, otherwise return null. """
    prompt_institute_level = """ Find the Application Requirements page url for the {university_name} website. Return the url if found, otherwise return null. """
    response = model.generate_content(prompt)
    response_text = response.text
    parsed_data = parse_json_from_response(response_text)
    if parsed_data and isinstance(parsed_data, dict):
        application_requirements_page_url = parsed_data.get('application_requirements_page_url')
    else:
        response = model.generate_content(prompt_institute_level)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        if parsed_data and isinstance(parsed_data, dict):
            application_requirements_page_url = parsed_data.get('application_requirements_page_url')
    # First, try program level
    prompt_program = (
        f"You are extracting application requirements and required documents for the program '{program_name}' "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website, {application_requirements_page_url} and ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Program URL: {program_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website for THIS SPECIFIC PROGRAM:\n\n"
        f"1. Resume: Is a resume/CV required? Return 'Required', 'Optional', 'Not Required', or null.\n"
        f"2. StatementOfPurpose: Is a statement of purpose required? Return 'Required', 'Optional', 'Not Required', or null.\n"
        f"3. Requirements: General application requirements text/description. Return null if not specified.\n"
        f"4. WritingSample: Is a writing sample required? Return 'Required', 'Optional', 'Not Required', or null.\n"
        f"5. IsAnalyticalNotRequired: MANDATORY BOOLEAN. Is analytical scores required to apply for this {program_name}? Return true or false.\n"
        f"6. IsAnalyticalOptional: MANDATORY BOOLEAN. Is analytical scores optional to apply for this {program_name}? Return true or false.\n"
        f"7. IsStemProgram: MANDATORY BOOLEAN. Is this a STEM(Science, Technology, Engineering, and Mathematics) program? Return true or false.\n"
        f"8. IsACTRequired: MANDATORY BOOLEAN. Return False.\n"
        f"9. IsSATRequired: MANDATORY BOOLEAN. Return False.\n"
        f"10. MinimumACTScore: Return null.\n"
        f"11. MinimumSATScore: Return null.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {program_url} or other official {university_name} pages\n"
        f"- Extract information SPECIFIC to this program '{program_name}'\n"
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found on the program page, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n"
        f"- Ensure all extracted text is accurate and verbatim from the source\n"
        f"- FOR MANDATORY BOOLEAN FIELDS: You MUST return true or false. Do not return null unless absolutely no information is available. If not mentioned as required, default to false.\n\n"
        f"Return the data in a JSON format with the following exact keys: "
        f"'Resume', 'StatementOfPurpose', 'Requirements', 'WritingSample', 'IsAnalyticalNotRequired', "
        f"'IsAnalyticalOptional', 'IsRecommendationSystemOpted', 'IsStemProgram', 'IsACTRequired', "
        f"'IsSATRequired', 'MinimumACTScore', 'MinimumSATScore'. "
        f"Return a single JSON object, not an array. Use null for non-boolean fields if info not available."
    )
    
    try:
        response = model.generate_content(prompt_program)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data and isinstance(parsed_data, dict):
            # Check if we got any non-null values
            has_data = any(v is not None and v != "" for v in parsed_data.values())
            
            if has_data:
                parsed_data['extraction_level'] = 'program'
                return parsed_data
    except Exception as e:
        print(f"  Error extracting from program level: {str(e)}")
    
    # If no data found at program level, try institute level
    print(f"  No program-specific data found, trying institute level...")
    prompt_institute = (
        f"You are extracting general application requirements and required documents "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Institute URL: {institute_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website as GENERAL/INSTITUTE-LEVEL requirements:\n\n"
        f"1. Resume: Is a resume/CV generally required? Return 'Required', 'Optional', 'Not Required', or null.\n"
        f"2. StatementOfPurpose: Is a statement of purpose generally required? Return 'Required', 'Optional', 'Not Required', or null.\n"
        f"3. Requirements: General application requirements text/description. Return null if not specified.\n"
        f"4. WritingSample: Is a writing sample generally required? Return 'Required', 'Optional', 'Not Required', or null.\n"
        f"5. IsAnalyticalNotRequired: MANDATORY BOOLEAN. Is analytical scores generally required to apply for graduate programs? Return true or false.\n"
        f"6. IsAnalyticalOptional: MANDATORY BOOLEAN. Is analytical scores generally optional to apply for graduate programs? Return true or false.\n"
        f"7. IsStemProgram: MANDATORY BOOLEAN. Return False.\n"
        f"8. IsACTRequired: MANDATORY BOOLEAN. Return False.\n"
        f"9. IsSATRequired: MANDATORY BOOLEAN. Return False.\n"
        f"10. MinimumACTScore: Return null.\n"
        f"11. MinimumSATScore: Return null.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {institute_url} or other official {university_name} pages\n"
        f"- Extract GENERAL/INSTITUTE-LEVEL requirements (not program-specific)\n"
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n\n"
        f"Return the data in a JSON format with the following exact keys: "
        f"'Resume', 'StatementOfPurpose', 'Requirements', 'WritingSample', 'IsAnalyticalNotRequired', "
        f"'IsAnalyticalOptional', 'IsRecommendationSystemOpted', 'IsStemProgram', 'IsACTRequired', "
        f"'IsSATRequired', 'MinimumACTScore', 'MinimumSATScore'. "
        f"Return a single JSON object, not an array. Use null for any field where information is not available on the official website."
    )
    
    try:
        response = model.generate_content(prompt_institute)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data and isinstance(parsed_data, dict):
            parsed_data['extraction_level'] = 'institute'
            return parsed_data
            #Setting a few default values for the parsed_data like ['IsAnalyticalNotRequired', 'IsAnalyticalOptional', 'IsRecommendationSystemOpted', 'IsStemProgram', 'IsACTRequired', 'IsSATRequired', 'MinimumACTScore', 'MinimumSATScore']
            
            parsed_data['IsAnalyticalNotRequired'] = True
            parsed_data['IsAnalyticalOptional'] = True
            parsed_data['IsRecommendationSystemOpted'] = False
            parsed_data['IsACTRequired'] = False
            parsed_data['IsSATRequired'] = False
            parsed_data['MinimumACTScore'] = None
            parsed_data['MinimumSATScore'] = None
            return parsed_data
            
    except Exception as e:
        print(f"  Error extracting from institute level: {str(e)}")
    
    # Return empty dict with null values if nothing found
    return {
        'Resume': None, 'StatementOfPurpose': None, 'Requirements': None, 'WritingSample': None,
        'IsAnalyticalNotRequired': False, 'IsAnalyticalOptional': False, 'IsRecommendationSystemOpted': False,
        'IsStemProgram': False, 'IsACTRequired': False, 'IsSATRequired': False,
        'MinimumACTScore': None, 'MinimumSATScore': None, 'extraction_level': 'none'
    }

# Institute level URL for fallback
def grad_step4_run(university_name_input):
    global university_name, institute_url
    university_name = university_name_input
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Update paths with university name
    csv_path = os.path.join(output_dir, f'{sanitized_name}_graduate_programs.csv')
    json_path = os.path.join(output_dir, f'{sanitized_name}_application_requirements.json')

    yield f'{{"status": "progress", "message": "Initializing application requirements extraction for {university_name}..."}}'
    


    # Check if CSV file exists
    if not os.path.exists(csv_path):
        yield f'{{"status": "complete", "message": "CSV file not found: {csv_path}. Skipping Step.", "files": {{}}}}'
        return

    program_data = pd.read_csv(csv_path)

    if program_data.empty:
        yield f'{{"status": "error", "message": "CSV file is empty. Please check Step 1 results."}}'
        return

    # Check if required columns exist
    required_columns = ['Program name', 'Program Page url']
    missing_columns = [col for col in required_columns if col not in program_data.columns]
    if missing_columns:
        yield f'{{"status": "error", "message": "Missing columns: {", ".join(missing_columns)}"}}'
        return

    # Quick fetch of website url for context - LOCAL ONLY
    try:
        first_url = program_data.iloc[0]['Program Page url']
        domain = urlparse(first_url).netloc
        institute_url = f"https://{domain}"
    except:
        institute_url = f"https://www.google.com/search?q={university_name}"

    # Load existing data
    application_data = []
    processed_programs = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                application_data = json.load(f)
                for record in application_data:
                    program_name = record.get('Program name')
                    if program_name:
                        processed_programs.add(program_name)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(application_data)} existing records"}}'
        except Exception as e:
            pass

    # Filter out already processed programs
    programs_to_process = []
    for index, row in program_data.iterrows():
        if row['Program name'] not in processed_programs:
            programs_to_process.append(row)

    total_programs = len(program_data)
    processed_count = len(processed_programs)
    
    if not programs_to_process:
         yield f'{{"status": "progress", "message": "All {total_programs} programs already processed. Skipping extraction."}}'
    else:
         yield f'{{"status": "progress", "message": "Starting extraction for {total_programs} programs ({len(programs_to_process)} remaining)..."}}'

    for index, row in program_data.iterrows():
        program_name = row['Program name']
        program_page_url = row['Program Page url']
        
        if program_name in processed_programs:
            continue
        
        processed_count += 1
        yield f'{{"status": "progress", "message": "Processing [{processed_count}/{total_programs}]: {program_name}"}}'
        
        try:
            extracted_data = extract_application_requirements(program_name, program_page_url, institute_url)
            
            extracted_data['Program name'] = program_name
            extracted_data['Program Page url'] = program_page_url
            application_data.append(extracted_data)
            processed_programs.add(program_name)
            
            save_to_json(application_data, json_path)
            time.sleep(1) # Rate limit handling
        
        except Exception as e:
            error_record = {
                'Program name': program_name, 'Program Page url': program_page_url,
                'Resume': None, 'StatementOfPurpose': None, 'Requirements': None, 'WritingSample': None,
                'IsAnalyticalNotRequired': None, 'IsAnalyticalOptional': None, 'IsRecommendationSystemOpted': None,
                'IsStemProgram': None, 'IsACTRequired': None, 'IsSATRequired': None,
                'MinimumACTScore': None, 'MinimumSATScore': None, 'extraction_level': 'error', 'error': str(e)
            }
            application_data.append(error_record)
            processed_programs.add(program_name)
            save_to_json(application_data, json_path)

    # Final save
    csv_output_path = os.path.join(output_dir, f'{sanitized_name}_application_requirements.csv')
    if application_data:
        df = pd.DataFrame(application_data)
        df.to_csv(csv_output_path, index=False, encoding='utf-8')
        yield f'{{"status": "complete", "message": "Completed extraction for {len(application_data)} programs", "files": {{"grad_app_req_csv": "{csv_output_path}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No data extracted", "files": {{}}}}'



# ----------------------------------------------------------------------------
# grad_step5 (graduate_programs)
# ----------------------------------------------------------------------------



# Use GeminiModelWrapper (already initialized at top of file)
# model is already available globally

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "Grad_prog_outputs")
# Create directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, 'graduate_programs.csv')
json_path = os.path.join(output_dir, 'program_details_financial.json')

def save_to_json(data, filepath):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def parse_json_from_response(text):
    """Parse JSON from Gemini response, handling markdown code blocks."""
    # Remove markdown formatting
    text = text.replace("**", "").replace("```json", "").replace("```", "").strip()
    
    # Try to extract JSON from the text
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # If no match, try parsing the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def extract_program_details(program_name, program_url, institute_url):
    global university_name
    
    prompt = (
        f"You are extracting program details and financial information for the program '{program_name}' "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Program URL: {program_url}\n"
        f"Institute URL: {institute_url}\n\n"
        f"Extract the following fields:\n\n"
        f"1. QsWorldRanking: QS World University Ranking (Instance Level). Return as string or number. Return null if not found.\n"
        f"2. School: The specific school or college offering the program (e.g. 'School of Business'). Return string or null.\n"
        f"3. MaxFails: Maximum number of failing grades allowed. Return number or null.\n"
        f"4. MaxGPA: Maximum GPA scale (e.g., 4.0). Return number or null.\n"
        f"5. MinGPA: Minimum GPA required for admission/graduation. Return number or null.\n"
        f"6. PreviousYearAcceptanceRates: Acceptance rate. Return string/number or null.\n"
        f"7. Term: Admission terms (e.g. 'Fall', 'Spring'). Return string or null.\n"
        f"8. LiveDate: Application opening date. Return string or null. look for fall 2026 application opening date\n"
        f"9. DeadlineDate: Application deadline. Return string or null. look for fall 2026 application deadline\n"
        f"10. Fees: Tuition fee for the program. Return a number. Look if the program specific tuition fee is mentioned in any cost of attendance page of the {program_url} website. sample output: $12,000/Semester or $18,000/Year\n"
        f"11. AverageScholarshipAmount: Average scholarship amount. Return string/number or null.\n"
        f"12. CostPerCredit: Cost per credit hour for the program. Return string/number or null.\n"
        f"13. ScholarshipAmount: General scholarship amount available. Return string/number or null.\n"
        f"14. ScholarshipPercentage: Scholarship percentage available. Return string/number or null.\n"
        f"15. ScholarshipType: Types of scholarships available (e.g. 'Merit-based'). Return string or null.\n"
        f"16. Program duration: Duration of the program. Return string or null.\n"
        f"Return data in JSON format with exact keys: 'QsWorldRanking', 'School', 'MaxFails', 'MaxGPA', 'MinGPA', "
        f"'PreviousYearAcceptanceRates', 'Term', 'LiveDate', 'DeadlineDate', 'Fees', 'AverageScholarshipAmount', 'CostPerCredit', "
        f"'ScholarshipAmount', 'ScholarshipPercentage', 'ScholarshipType', 'Program duration', 'Tuition fee'."
    )
    
    try:
        response = model.generate_content(prompt)
        parsed = parse_json_from_response(response.text)
        if parsed and isinstance(parsed, dict):
            return parsed
    except Exception as e:
        print(f"Error details extraction: {e}")
    
    # Return empty dict with nulls if fail
    return {
        'QsWorldRanking': None, 'School': None, 'MaxFails': None, 'MaxGPA': None, 'MinGPA': None,
        'PreviousYearAcceptanceRates': None, 'Term': None, 'LiveDate': None, 'DeadlineDate': None,
        'Fees': None, 'AverageScholarshipAmount': None, 'CostPerCredit': None,
        'ScholarshipAmount': None, 'ScholarshipPercentage': None, 'ScholarshipType': None,
        'Program duration': None, 'Tuition fee': None
    }

def process_single_program(row, institute_url):
    """Wrapper to process a single program."""
    program_name = row['Program name']
    program_page_url = row['Program Page url']
    
    try:
        extracted_data = extract_program_details(program_name, program_page_url, institute_url)
        
        extracted_data['Program name'] = program_name
        extracted_data['Program Page url'] = program_page_url
        return extracted_data
    
    except Exception as e:
        return {
            'Program name': program_name,
            'Program Page url': program_page_url,
            'QsWorldRanking': None, 'School': None, 'MaxFails': None, 'MaxGPA': None, 'MinGPA': None,
            'PreviousYearAcceptanceRates': None, 'Term': None, 'LiveDate': None, 'DeadlineDate': None,
            'Fees': None, 'AverageScholarshipAmount': None, 'CostPerCredit': None,
            'ScholarshipAmount': None, 'ScholarshipPercentage': None, 'ScholarshipType': None,
            'Program duration': None, 'Tuition fee': None, 'extraction_level': 'error', 'error': str(e)
        }

def grad_step5_run(university_name_input):
    global university_name, institute_url
    university_name = university_name_input
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Update paths with university name
    csv_path = os.path.join(output_dir, f'{sanitized_name}_graduate_programs.csv')
    json_path = os.path.join(output_dir, f'{sanitized_name}_program_details_financial.json')

    yield f'{{"status": "progress", "message": "Initializing program details & financial extraction for {university_name}..."}}'
    
    # Check if CSV file exists
    if not os.path.exists(csv_path):
        yield f'{{"status": "complete", "message": "CSV file not found: {csv_path}. Skipping Step.", "files": {{}}}}'
        return

    program_data = pd.read_csv(csv_path)

    if program_data.empty:
        yield f'{{"status": "error", "message": "CSV file is empty. Please check Step 1 results."}}'
        return

    # Check if required columns exist
    required_columns = ['Program name', 'Program Page url']
    missing_columns = [col for col in required_columns if col not in program_data.columns]
    if missing_columns:
        yield f'{{"status": "error", "message": "Missing columns: {", ".join(missing_columns)}"}}'
        return

    # Quick fetch of website url for context - LOCAL ONLY
    try:
        first_url = program_data.iloc[0]['Program Page url']
        domain = urlparse(first_url).netloc
        institute_url = f"https://{domain}"
    except:
        institute_url = f"https://www.google.com/search?q={university_name}"

    # Load existing data
    program_details_data = []
    processed_programs = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                program_details_data = json.load(f)
                for record in program_details_data:
                    program_name = record.get('Program name')
                    if program_name:
                        processed_programs.add(program_name)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(program_details_data)} existing records"}}'
        except Exception as e:
            pass

    # Filter out already processed programs
    programs_to_process = []
    for index, row in program_data.iterrows():
        if row['Program name'] not in processed_programs:
            programs_to_process.append(row)

    total_programs = len(program_data)
    processed_count = len(processed_programs)
    
    yield f'{{"status": "progress", "message": "Starting extraction for {total_programs} programs ({len(programs_to_process)} remaining)..."}}'

    for index, row in program_data.iterrows():
        program_name = row['Program name']
        program_page_url = row['Program Page url']
        
        if program_name in processed_programs:
            continue
        
        processed_count += 1
        yield f'{{"status": "progress", "message": "Processing [{processed_count}/{total_programs}]: {program_name}"}}'
        
        try:
            extracted_data = process_single_program(row, institute_url)
            
            program_details_data.append(extracted_data)
            processed_programs.add(program_name)
            
            save_to_json(program_details_data, json_path)
            time.sleep(1) # Rate limit handling
        
        except Exception as e:
            error_record = {
                'Program name': program_name,
                'Program Page url': program_page_url,
                'QsWorldRanking': None, 'School': None, 'MaxFails': None, 'MaxGPA': None, 'MinGPA': None,
                'PreviousYearAcceptanceRates': None, 'Term': None, 'LiveDate': None, 'DeadlineDate': None,
                'Fees': None, 'AverageScholarshipAmount': None, 'CostPerCredit': None,
                'ScholarshipAmount': None, 'ScholarshipPercentage': None, 'ScholarshipType': None,
                'Program duration': None, 'Tuition fee': None, 'extraction_level': 'error', 'error': str(e)
            }
            program_details_data.append(error_record)
            processed_programs.add(program_name)
            save_to_json(program_details_data, json_path)

    # Final save
    csv_output_path = os.path.join(output_dir, f'{sanitized_name}_program_details_financial.csv')
    if program_details_data:
        df = pd.DataFrame(program_details_data)
        df.to_csv(csv_output_path, index=False, encoding='utf-8')
        yield f'{{"status": "complete", "message": "Completed extraction for {len(program_details_data)} programs", "files": {{"grad_details_csv": "{csv_output_path}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No data extracted", "files": {{}}}}'



# ----------------------------------------------------------------------------
# grad_merge (graduate_programs)
# ----------------------------------------------------------------------------


# Define the target schema and column mapping
TARGET_COLUMNS = [
    'Id', 'ProgramName', 'ProgramCode', 'Status', 'CreatedDate', 'UpdatedDate', 'Level', 'Term',
    'TermCode', 'LiveDate', 'DeadlineDate', 'Resume', 'StatementOfPurpose', 'GreOrGmat',
    'EnglishScore', 'Requirements', 'WritingSample', 'CollegeId', 'IsAnalyticalNotRequired',
    'IsAnalyticalOptional', 'IsDuoLingoRequired', 'IsELSRequired', 'IsGMATOrGreRequired',
    'IsGMATRequired', 'IsGreRequired', 'IsIELTSRequired', 'IsLSATRequired', 'IsMATRequired',
    'IsMCATRequired', 'IsPTERequired', 'IsTOEFLIBRequired', 'IsTOEFLPBTRequired',
    'IsEnglishNotRequired', 'IsEnglishOptional', 'AcademicYear', 'AlternateProgram',
    'ApplicationType', 'Department', 'Fees', 'IsAvailable', 'ProgramType',
    'AdmissionDepartmentId', 'CreatedBy', 'UpdatedBy', 'Concentration', 'Description',
    'OtherConcentrations', 'ProgramWebsiteURL', 'Accredidation', 'AverageScholarshipAmount',
    'CostPerCredit', 'IsRecommendationSystemOpted', 'IsStemProgram', 'MaxFails', 'MaxGPA',
    'MinGPA', 'PreviousYearAcceptanceRates', 'QsWorldRanking', 'TotalAccepetedApplications',
    'TotalCredits', 'TotalDeniedApplications', 'TotalI20sIssued', 'TotalScholarshipsAwarded',
    'TotalSubmittedApplications', 'TotalVisasSecured', 'UsNewsRanking', 'CollegeApplicationFee',
    'IsCollegePaying', 'MEContractNegotiatedFee', 'MyGradAppFee', 'ProgramCategory',
    'IsCollegeApplicationFree', 'IsCouponAllowed', 'IsACTRequired', 'IsSATRequired',
    'SftpDestinationId', 'MinimumACTScore', 'MinimumDuoLingoScore', 'MinimumELSScore',
    'MinimumGMATScore', 'MinimumGreScore', 'MinimumIELTSScore', 'MinimumMATScore',
    'MinimumMCATScore', 'MinimumPTEScore', 'MinimumSATScore', 'MinimumTOEFLScore',
    'MLModelName', 'MinimumAnalyticalScore', 'MinimumEnglishScore', 'MinimumExperience',
    'MinimumSopRating', 'WeightAnalytical', 'WeightEnglish', 'WeightExperience', 'WeightGPA',
    'WeightSop', 'ScholarshipAmount', 'ScholarshipPercentage', 'ScholarshipType',
    'IsNewlyLaunched', 'BatchId', 'IsImported', 'IsImportVerified', 'Is_Recommendation_Sponser',
    'AnalyticalScore', 'MinimumLSATScore'
]

COLUMN_MAPPING = {
    # Base
    'Program name': 'ProgramName',
    'Level': 'Level',
    'Program Page url': 'ProgramWebsiteURL',
    
    # Financial
    'QsWorldRanking': 'QsWorldRanking',
    'School': 'Department', 
    'MaxFails': 'MaxFails',
    'MaxGPA': 'MaxGPA',
    'MinGPA': 'MinGPA',
    'PreviousYearAcceptanceRates': 'PreviousYearAcceptanceRates',
    'Term': 'Term',
    'LiveDate': 'LiveDate',
    'DeadlineDate': 'DeadlineDate',
    'Tuition fee': 'Fees',           # Mapping extracted 'Tuition fee' -> Fees column
    'AverageScholarshipAmount': 'AverageScholarshipAmount',
    'CostPerCredit': 'CostPerCredit',
    'ScholarshipAmount': 'ScholarshipAmount',
    'ScholarshipPercentage': 'ScholarshipPercentage',
    'ScholarshipType': 'ScholarshipType',
    
    # Test Scores
    'GreOrGmat': 'GreOrGmat',
    'EnglishScore': 'EnglishScore',
    'IsDuoLingoRequired': 'IsDuoLingoRequired',
    'IsELSRequired': 'IsELSRequired',
    'IsGMATOrGreRequired': 'IsGMATOrGreRequired',
    'IsGMATRequired': 'IsGMATRequired',
    'IsGreRequired': 'IsGreRequired',
    'IsIELTSRequired': 'IsIELTSRequired',
    'IsLSATRequired': 'IsLSATRequired',
    'IsMATRequired': 'IsMATRequired',
    'IsMCATRequired': 'IsMCATRequired',
    'IsPTERequired': 'IsPTERequired',
    'IsTOEFLIBRequired': 'IsTOEFLIBRequired',
    'IsTOEFLPBTRequired': 'IsTOEFLPBTRequired',
    'IsEnglishNotRequired': 'IsEnglishNotRequired',
    'IsEnglishOptional': 'IsEnglishOptional',
    'MinimumDuoLingoScore': 'MinimumDuoLingoScore',
    'MinimumELSScore': 'MinimumELSScore',
    'MinimumGMATScore': 'MinimumGMATScore',
    'MinimumGreScore': 'MinimumGreScore',
    'MinimumIELTSScore': 'MinimumIELTSScore',
    'MinimumMATScore': 'MinimumMATScore',
    'MinimumMCATScore': 'MinimumMCATScore',
    'MinimumPTEScore': 'MinimumPTEScore',
    'MinimumTOEFLScore': 'MinimumTOEFLScore',
    'MinimumLSATScore': 'MinimumLSATScore',
    
    # Application Requirements
    'Resume': 'Resume',
    'StatementOfPurpose': 'StatementOfPurpose',
    'Requirements': 'Requirements',
    'WritingSample': 'WritingSample',
    'IsAnalyticalNotRequired': 'IsAnalyticalNotRequired',
    'IsAnalyticalOptional': 'IsAnalyticalOptional',
    'IsRecommendationSystemOpted': 'IsRecommendationSystemOpted',
    'IsStemProgram': 'IsStemProgram',
    'IsACTRequired': 'IsACTRequired',
    'IsSATRequired': 'IsSATRequired',
    'MinimumACTScore': 'MinimumACTScore',
    'MinimumSATScore': 'MinimumSATScore',
    
    # Extra Fields
    'Concentration name': 'Concentration',
    'description': 'Description',
    'Accreditation status': 'Accredidation'
}

def load_json_data(filepath):
    if not os.path.exists(filepath):
        print(f"Warning: File not found: {filepath}")
        return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return []

def grad_merge_run(university_name=None):
    yield f'{{"status": "progress", "message": "Starting data merge and standardization..."}}'
    
    if not university_name:
        yield f'{{"status": "error", "message": "University name not provided for merge step."}}'
        return

    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "Grad_prog_outputs")
    os.makedirs(output_dir, exist_ok=True)
    
    # File paths
    base_csv_path = os.path.join(output_dir, f'{sanitized_name}_graduate_programs.csv')
    financial_json_path = os.path.join(output_dir, f'{sanitized_name}_program_details_financial.json')
    test_scores_json_path = os.path.join(output_dir, f'{sanitized_name}_test_scores_requirements.json')
    app_req_json_path = os.path.join(output_dir, f'{sanitized_name}_application_requirements.json')
    extra_fields_json_path = os.path.join(output_dir, f'{sanitized_name}_extra_fields_data.json')
    
    # 1. Load Base Data
    if not os.path.exists(base_csv_path):
        yield f'{{"status": "complete", "message": "Base CSV not found at {base_csv_path}. Skipping merge step.", "files": {{}}}}'
        return
        
    df_base = pd.read_csv(base_csv_path)
    yield f'{{"status": "progress", "message": "Loaded {len(df_base)} programs from base CSV"}}'
    
    # 2. Load and Prepare Merge Data
    financial_data = load_json_data(financial_json_path)
    test_scores_data = load_json_data(test_scores_json_path)
    app_req_data = load_json_data(app_req_json_path)
    extra_fields_data = load_json_data(extra_fields_json_path)
    
    # Convert to DataFrames
    df_fin = pd.DataFrame(financial_data) if financial_data else pd.DataFrame()
    df_test = pd.DataFrame(test_scores_data) if test_scores_data else pd.DataFrame()
    df_app = pd.DataFrame(app_req_data) if app_req_data else pd.DataFrame()
    df_extra = pd.DataFrame(extra_fields_data) if extra_fields_data else pd.DataFrame()
    
    # Merge Key
    merge_key = 'Program name'
    
    # Ensure merge key exists in all DFs before merging
    dfs_to_merge = [df_fin, df_test, df_app, df_extra]
    final_df = df_base.copy()
    
    for i, df in enumerate(dfs_to_merge):
        if not df.empty and merge_key in df.columns:
            # Drop duplicates in join tables if any
            df = df.drop_duplicates(subset=[merge_key])
            # Drop Program Page url from merge tables to avoid suffixes, keep it from base
            if 'Program Page url' in df.columns:
                df = df.drop(columns=['Program Page url'])
            
            final_df = pd.merge(final_df, df, on=merge_key, how='left')
            yield f'{{"status": "progress", "message": "Merged dataset {i+1}..."}}'
        else:
            yield f'{{"status": "progress", "message": "Skipping dataset {i+1} (empty or missing key)"}}'

    # 3. Rename Columns
    # Rename columns that exist in the mapping
    final_df = final_df.rename(columns=COLUMN_MAPPING)
    
    # 4. Add Missing Columns
    for col in TARGET_COLUMNS:
        if col not in final_df.columns:
            final_df[col] = ""  # Initialize with empty string
            
    # 5. Select and Reorder Columns
    # Only keep columns that are in TARGET_COLUMNS
    final_df = final_df[TARGET_COLUMNS]
    #qs_ranking set to null for entire column 
    final_df['QsWorldRanking'] = ""
    levels_map = {
        "Doctoral": ["phd", "edd", "dpt", "pharmd", "otd", "doctor"],
        "Graduate-Certificate": ["certificate", "certification", "cert"]
    }

    # Determine level logic:
    # Default to 'Graduate'
    final_df['Level'] = final_df['ProgramName'].apply(lambda x: next((k for k, v in levels_map.items() if any(keyword in str(x).lower() for keyword in v)), 'Graduate'))
    
    # 6. Save Final CSV
    output_csv_path = os.path.join(output_dir, f'{sanitized_name}_graduate_programs_final.csv')
    final_df.to_csv(output_csv_path, index=False, encoding='utf-8')
    
    yield f'{{"status": "complete", "message": "Successfully merged and standardized data", "files": {{"grad_final_csv": "{output_csv_path}"}}}}'




# ============================================================================
# PROGRAMS EXTRACTION - UNDERGRADUATE PROGRAMS MODULES
# ============================================================================


# ----------------------------------------------------------------------------
# undergrad_step1 (undergraduate_programs)
# ----------------------------------------------------------------------------




# Add parent directories to sys.path to allow importing from Institution
current_dir = os.path.dirname(os.path.abspath(__file__))
# Go up 2 levels: University_Data/Programs/undergraduate_programs -> University_Data
# 1. .../Programs
# 2. .../University_Data
programs_dir = os.path.dirname(current_dir)
university_data_dir = os.path.dirname(programs_dir)
institution_dir = os.path.join(university_data_dir, 'Institution')
sys.path.append(institution_dir)


# Initialize the model using the wrapper (consistent with check.py)
model = GeminiModelWrapper(client, os.getenv("MODEL"))

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "Undergrad_prog_outputs")
# Create directory if it doesn't exist
# Create directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

def resolve_redirect(url):
    try:
        # Use HEAD request to follow redirects without downloading content
        response = requests.head(url, allow_redirects=True, timeout=5)
        return response.url
    except Exception:
        return url



def find_program_url(program_name, university_name):
    prompt = (
        f"Use Google Search to find the OFFICIAL '{program_name}' program page on the {university_name} website. "
        "1. Look at the search results. "
        "2. Identify the official '.edu' URL for this specific program. "
        "3. Do NOT return the 'vertexaisearch' or 'google.com' redirect links. "
        "4. Return ONLY the clean, direct official URL."
    )
    try:
        response = model.generate_content(prompt)
        
        # Check grounding metadata first for real URLs
        real_urls = []
        if response.candidates and response.candidates[0].grounding_metadata:
            for chunk in response.candidates[0].grounding_metadata.grounding_chunks:
                if chunk.web:
                    real_urls.append(resolve_redirect(chunk.web.uri))
        
        # Filter for .edu links
        edu_urls = [u for u in real_urls if ".edu" in u]
        
        if edu_urls:
            return edu_urls[0]
        elif real_urls:
            return real_urls[0]
        
        # Fallback to text
        if not response.text:
             return None
        text_url = response.text.replace("```", "").strip()
        # Basic clean
        match = re.search(r'https?://[^\s<>"]+|www\.[^\s<>"]+', text_url)
        if match:
             return match.group(0)
        return None
    except Exception:
        return None

def get_undergraduate_programs(url, university_name, existing_data=None):
    # Step 1: Extract just the names
    prompt_names = (
        f"I am providing you with the URL of the official undergraduate programs listing for {university_name}: {url}\n\n"
        "Your task is to identify and extract the names of ALL undergraduate programs (Majors, Bachelors, Associates, and Minors) listed on that page.\n"
        "1. Carefully identify every program name.\n"
        "2. Include the full degree designation if available (e.g., 'Bachelor of Science in Biology' instead of just 'Biology').\n"
        "3. Only include active programs.\n"
        "4. If the university uses 'Concentrations', 'Areas of Study', or 'Fields of Study', treat those as the program names.\n\n"
        "RETURN ONLY A JSON LIST OF STRINGS.\n"
        "Example format: [\"Bachelor of Science in Computer Science\", \"Associate of Applied Science in Nursing\"]\n\n"
        "DO NOT explain your limitations or mention your search tools. Just return the JSON list based on your knowledge of this page's structure or by searching for its content."
    )
    
    program_names = []
    max_attempts = 2
    for attempt_num in range(1, max_attempts + 1):
        try:
            # yield f'{{"status": "progress", "message": "DEBUG: Prompting for names with URL: {url}"}}'
            response = model.generate_content(prompt_names)
            if not response.text:
                if attempt_num < max_attempts: continue
                yield f'{{"status": "error", "message": "Error extracting names: Model returned empty response (text is None)"}}'
                yield []
                return
                
            text = response.text.replace("```json", "").replace("```", "").strip()
            
            # Escape quotes for JSON safety in the message
            safe_text = text.replace('"', "'").replace('\n', ' ')
            yield f'{{"status": "progress", "message": "DEBUG: Raw response text: {safe_text}"}}'
            
            start = text.find('[')
            end = text.rfind(']') + 1
            if start != -1 and end != -1:
                 program_names = json.loads(text[start:end])
            else:
                # Fallback: Try to parse bulleted list
                print("DEBUG: JSON not found, attempting fallback parsing for bulleted list.")
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Match lines starting with *, -, or numbers 1.
                    if line.startswith(('*', '-', '•')) or (len(line) > 0 and line[0].isdigit() and line[1] == '.'):
                        # Clean up the line
                        clean_name = re.sub(r'^[\*\-•\d\.]+\s*', '', line).strip()
                        if clean_name:
                            program_names.append(clean_name)
                
            if program_names:
                break # Success
            elif attempt_num < max_attempts:
                yield f'{{"status": "warning", "message": "Attempt {attempt_num} failed to parse program names. Retrying with refined focus..."}}'
                # Slightly refine prompt for retry
                prompt_names += "\n\nCRITICAL: You must return a list of at least 5-10 programs. Do not return an empty list."
                
        except Exception as e:
            if attempt_num < max_attempts: continue
            yield f'{{"status": "error", "message": "Error extracting names: {str(e)}"}}'
            yield [] # Return empty list on error
            return

    if not program_names:
        yield f'{{"status": "warning", "message": "DEBUG: Could not find any program names after {max_attempts} attempts."}}'

    # Step 2: Iterate and find URLs
    results = existing_data if existing_data else []
    existing_urls = {p['Program name']: p['Program Page url'] for p in results if p.get('Program Page url') and p['Program Page url'] != url}
    existing_names = set(p['Program name'] for p in results)

    total_programs = len(program_names)
    yield f"Found {total_programs} programs. Starting detailed URL search..."
    
    for i, name in enumerate(program_names):
        # Skip if already found with a valid URL
        if name in existing_urls:
            yield f"Skipping (already found) ({i+1}/{total_programs}): {name}"
            continue

        # Yield progress update
        if name in existing_names:
             yield f"Skipping existing program: {name}"
             continue

        yield f"Finding URL for ({i+1}/{total_programs}): {name}"
        
        found_url = find_program_url(name, university_name)
        program_entry = {
            "Program name": name,
            "Program Page url": found_url if found_url else url
        }
        
        # Yield the individual result
        yield program_entry
            
def undergrad_step1_run(university_name_input):
    global university_name, institute_url
    university_name = university_name_input
    
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Define output files
    json_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs.json')
    csv_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs.csv')

    # Early check for completed list
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        count = len(pd.read_csv(csv_path))
        yield f'{{"status": "progress", "message": "Undergraduate programs list for {university_name} already exists. Skipping extraction."}}'
        yield f'{{"status": "complete", "message": "Found {count} undergraduate programs (using existing list)", "files": {{"undergrad_csv": "{csv_path}"}}}}'
        return

    # Load existing data to handle resuming/appending
    existing_programs = []
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                existing_programs = json.load(f)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(existing_programs)} already found programs."}}'
        except:
            pass
    
    # Helper to save progress
    def save_progress(programs_list):
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(programs_list, f, indent=4, ensure_ascii=False)
        df = pd.DataFrame(programs_list)
        df.to_csv(csv_path, index=False, encoding='utf-8')

    
    prompt = f"What is the official university website for {university_name}?"
    try:
        resp = model.generate_content(prompt)
        if resp.text:
            website_url = resp.text.replace("**", "").replace("```", "").strip()
            institute_url = website_url
            yield f'{{"status": "progress", "message": "Website found: {website_url}"}}'
        else:
             raise Exception("Model returned empty text")
    except Exception as e:
        yield f'{{"status": "error", "message": "Failed to find website: {str(e)}"}}'
        return

    # Dynamic search for undergrad url
    yield f'{{"status": "progress", "message": "Finding undergraduate programs page..."}}'
    undergrad_url_prompt = (
        f"Use Google Search to find the OFFICIAL page listing all Undergraduate Degrees/Programs (Majors) at {university_name}. "
        "Only Look at the active and latest Programs page urls. Do not include any expired or cancelled programs pages urls. or programs page urls from older catalogs."
        "The page should list specific bachelors/associate degrees. "
        "the page should belong to the official university domain."
        "Return the URL. Do not generate a hypothetical URL."
    )
    try:
        response = model.generate_content(undergrad_url_prompt)
        
        # Check grounding metadata first for real URLs
        real_urls = []
        if response.candidates and response.candidates[0].grounding_metadata:
            for chunk in response.candidates[0].grounding_metadata.grounding_chunks:
                if chunk.web:
                    real_urls.append(resolve_redirect(chunk.web.uri))
        
        # Filter for .edu links
        edu_urls = [u for u in real_urls if ".edu" in u]
        
        if edu_urls:
            undergraduate_program_url = edu_urls[0]
        elif real_urls:
            undergraduate_program_url = real_urls[0]
        else:
             # Fallback to text
            if response.text:
                undergraduate_program_url = response.text.strip()
            else:
                 undergraduate_program_url = ""
            # clean url
            url_match = re.search(r'https?://[^\s<>"]+|www\.[^\s<>"]+', undergraduate_program_url)
            if url_match:
                undergraduate_program_url = url_match.group(0)
            
        yield f'{{"status": "progress", "message": "Undergraduate Page found: {undergraduate_program_url}"}}'
    except:
        undergraduate_program_url = website_url # Fallback

    yield f'{{"status": "progress", "message": "Extracting undergraduate programs list (this may take a while)..."}}'
    
    # Define output files
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    json_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs.json')
    csv_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs.csv')

    # Load existing data to handle resuming/appending
    existing_programs = []
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                existing_programs = json.load(f)
        except:
            pass
            
    # Process the generator
    current_programs = existing_programs.copy()
    existing_names = set(p['Program name'] for p in current_programs)
    
    for item in get_undergraduate_programs(undergraduate_program_url, university_name, existing_data=current_programs):
        if isinstance(item, str):
            # This is a progress message
            safe_msg = item.replace('"', "'")
            yield f'{{"status": "progress", "message": "{safe_msg}"}}'
        elif isinstance(item, dict):
            # This is a single program entry
            p_name = item.get('Program name')
            if p_name not in existing_names:
                current_programs.append(item)
                existing_names.add(p_name)
                save_progress(current_programs)
                # yield f'{{"status": "progress", "message": "Saved: {p_name}"}}'
        
    
    undergraduate_programs = current_programs

    if undergraduate_programs:
        # Final save is handled by loop, but we ensure output message is correct
        yield f'{{"status": "complete", "message": "Found {len(undergraduate_programs)} undergraduate programs", "files": {{"undergrad_csv": "{csv_path}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No undergraduate programs found", "files": {{}}}}'




# ----------------------------------------------------------------------------
# undergrad_step2 (undergraduate_programs)
# ----------------------------------------------------------------------------



# Use GeminiModelWrapper (already initialized at top of file)
# model is already available globally

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "Undergrad_prog_outputs")
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, 'undergraduate_programs.csv')
json_path = os.path.join(output_dir, 'extra_fields_data.json')


def save_to_json(data, filepath):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def parse_json_from_response(text):
    """Parse JSON from Gemini response, handling markdown code blocks."""
    # Remove markdown formatting
    text = text.replace("**", "").replace("```json", "").replace("```", "").strip()
    
    # Try to extract JSON from the text
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # If no match, try parsing the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

def process_single_program(row, university_name):
    """Process a single program to extract extra fields."""
    program_name = row['Program name']
    program_page_url = row['Program Page url']
    
    prompt = (
        f"You are extracting information about the program '{program_name}' from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website. "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Program URL: {program_page_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website:\n"
        f"1. Concentration name: The specific concentration, specialization, or track name if the program offers concentrations. "
        f"   If no concentration is mentioned, return null.\n"
        f"2. Description: A comprehensive description of the program, its objectives, and what students will learn. "
        f"   Extract the full program description from the official page. If not available, return null.\n"
        f"3. Program website url: The official URL of the program page on {university_name} website. "
        f"   This should be a direct link to the program information page. Must be from official domain only.\n"
        f"4. Accreditation status: Any accreditation information mentioned for this specific program. "
        f"   Include the accrediting body name and status if available. If not mentioned, return null.\n\n"
        f"5. Level: The level of the program. The level can be either any of these and these are just examples : BA,Bachelor's,BS,BSc,BFA,Minor."
        f"   This should be determined from the {program_page_url} when you are extracting there itself distingnuish the program level. If not mentioned, return null.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {program_page_url} or other official {university_name} pages\n"
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found on the official website, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n"
        f"- Ensure all extracted text is accurate and verbatim from the source\n\n"
        f"Return the data in a JSON format with the following exact keys: 'Concentration name', 'description', 'program website url', 'Accreditation status'. "
        f"Return a single JSON object, not an array. Use null for any field where information is not available on the official website."
    )
    
    try:
        print(f"[DEBUG] Generating content for program: {program_name} using model {model.model_name}")
        response = model.generate_content(prompt)
        print(f"[DEBUG] Received response for program: {program_name}")
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data:
            if isinstance(parsed_data, list) and len(parsed_data) > 0:
                parsed_data = parsed_data[0]
            
            parsed_data['Program name'] = program_name
            parsed_data['Program Page url'] = program_page_url
            return parsed_data
        else:
            return {
                'Program name': program_name, 'Program Page url': program_page_url,
                'Concentration name': None, 'description': None, 'program website url': None,
                'Accreditation status': None, 'error': 'Failed to parse JSON response'
            }
    
    except Exception as e:
        return {
            'Program name': program_name, 'Program Page url': program_page_url,
            'Concentration name': None, 'description': None, 'program website url': None,
            'Accreditation status': None, 'error': str(e)
        }

def undergrad_step2_run(university_name_input):
    global university_name
    university_name = university_name_input
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Update paths with university name
    csv_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs.csv')
    json_path = os.path.join(output_dir, f'{sanitized_name}_extra_fields_data.json')

    # Check if CSV file exists
    if not os.path.exists(csv_path):
        yield f'{{"status": "complete", "message": "CSV file not found: {csv_path}. Skipping Step 2.", "files": {{}}}}'
        return

    program_data = pd.read_csv(csv_path)

    if program_data.empty:
        yield f'{{"status": "error", "message": "CSV file is empty. Please check Step 1 results."}}'
        return

    # Check if required columns exist
    required_columns = ['Program name', 'Program Page url']
    missing_columns = [col for col in required_columns if col not in program_data.columns]
    if missing_columns:
        yield f'{{"status": "error", "message": "Missing columns: {", ".join(missing_columns)}"}}'
        return
        
    # Load existing data
    extra_fields_data = []
    processed_programs = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                extra_fields_data = json.load(f)
                for record in extra_fields_data:
                    program_name = record.get('Program name')
                    if program_name:
                        processed_programs.add(program_name)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(extra_fields_data)} existing records"}}'
        except Exception as e:
            pass

    # Filter out already processed programs
    programs_to_process = []
    for index, row in program_data.iterrows():
        if row['Program name'] not in processed_programs:
            programs_to_process.append(row)

    total_programs = len(program_data)
    processed_count = len(processed_programs)
    
    yield f'{{"status": "progress", "message": "Starting extraction for {total_programs} programs ({len(programs_to_process)} remaining)..."}}'

    for index, row in program_data.iterrows():
        program_name = row['Program name']
        program_page_url = row['Program Page url']
        
        if program_name in processed_programs:
            continue
        
        processed_count += 1
        yield f'{{"status": "progress", "message": "Processing [{processed_count}/{total_programs}]: {program_name}"}}'
        
        try:
            result = process_single_program(row, university_name)
            
            # Update shared data structures
            extra_fields_data.append(result)
            processed_programs.add(program_name)
            
            # Save progress (thread-safe due to lock in save_to_json)
            save_to_json(extra_fields_data, json_path)
            time.sleep(1) # Rate limit handling
            
        except Exception as e:
            yield f'{{"status": "warning", "message": "Error processing {program_name}: {str(e)}"}}'

    # Final save
    csv_output_path = os.path.join(output_dir, f'{sanitized_name}_extra_fields_data.csv')
    if extra_fields_data:
        df = pd.DataFrame(extra_fields_data)
        df.to_csv(csv_output_path, index=False, encoding='utf-8')
        yield f'{{"status": "complete", "message": "Completed extraction for {len(extra_fields_data)} programs", "files": {{"undergrad_extra_csv": "{csv_output_path}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No data extracted", "files": {{}}}}'


# ----------------------------------------------------------------------------
# undergrad_step3 (undergraduate_programs)
# ----------------------------------------------------------------------------



# Use GeminiModelWrapper (already initialized at top of file)
# model is already available globally

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
# output_dir = "/home/my-laptop/scraper/Quinnipiac_university/Programs/graduate_programs/Grad_prog_outputs"
output_dir = os.path.join(script_dir, "Undergrad_prog_outputs")
# Create directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, 'undergraduate_programs.csv')
json_path = os.path.join(output_dir, 'test_scores_requirements.json')

# Check if CSV file exists
# Logic moved to run()

# Check if CSV has data
# Logic moved to run()

# Check if required columns exist
# Logic moved to run()

# Institute level URL for fallback
institute_url = None # Will be set in run()
university_name = None # Will be set in run()

# Load existing data if the JSON file exists (for resuming)
# This part will be moved inside the run function

def save_to_json(data, filepath):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def parse_json_from_response(text):
    """Parse JSON from Gemini response, handling markdown code blocks."""
    # Remove markdown formatting
    text = text.replace("**", "").replace("```json", "").replace("```", "").strip()
    
    # Try to extract JSON from the text
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # If no match, try parsing the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

def extract_test_scores(program_name, program_url, institute_url):
    """Extract test scores and English requirements, first from program level, then institute level."""
    global university_name # Ensure university_name is accessible
    
    # First, try program level
    prompt_program = (
        f"You are extracting test score requirements and English language requirements for the program '{program_name}' "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Program URL: {program_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website for THIS SPECIFIC PROGRAM:\n\n"
        f"1. GreOrGmat: Whether GRE or GMAT is required, optional, or not required. Return 'GRE', 'GMAT', 'Either', 'Optional', 'Not Required', or null.\n"
        f"2. EnglishScore: Is English proficiency test such as TOEFL, IELTS, Duolingo, ELS, PTE required? If required then return Required else return Optional or Not Required.\n"
        f"3. IsDuoLingoRequired: MANDATORY BOOLEAN. Is Duolingo English test explicitly required? Return true or false.\n"
        f"4. IsELSRequired: MANDATORY BOOLEAN. Is ELS (English Language Services) required? If required then return true else return false.\n"
        f"5. IsGMATOrGreRequired: MANDATORY BOOLEAN. Is either GMAT or GRE required? Return true if yes, false if no/optional.\n"
        f"6. IsGMATRequired: MANDATORY BOOLEAN. Is GMAT specifically required? Return true or false.\n"
        f"7. IsGRERequired: MANDATORY BOOLEAN. Is GRE specifically required? Return true or false.\n"
        f"8. IsIELTSRequired: MANDATORY BOOLEAN. Is IELTS score is accepted as ? Return true or false.\n"
        f"9. IsLSATRequired: MANDATORY BOOLEAN. Is LSAT required? Return true or false.\n"
        f"10. IsMATRequired: MANDATORY BOOLEAN. Is MAT required? Return true or false.\n"
        f"11. IsMCATRequired: MANDATORY BOOLEAN. Is MCAT required? Return true or false.\n"
        f"12. IsPTERequired: MANDATORY BOOLEAN. Is PTE (Pearson Test of English) required? Return true or false.\n"
        f"13. IsTOEFLIBRequired: MANDATORY BOOLEAN. Is TOEFL iBT (Internet-based Test) required? Return true or false.\n"
        f"14. IsTOEFLPBTRequired: MANDATORY BOOLEAN. Is TOEFL PBT (Paper-based Test) required? Return true or false.\n"
        f"15. IsEnglishNotRequired: MANDATORY BOOLEAN. Is English test explicitly NOT required? Return true or false.\n"
        f"16. IsEnglishOptional: MANDATORY BOOLEAN. Is English test optional? Return true or false.\n"
        f"17. MinimumDuoLingoScore: Minimum required Duolingo score as a number. Return null if not specified.\n"
        f"18. MinimumELSScore: Minimum required ELS score as a number. Return null if not specified.\n"
        f"19. MinimumGMATScore: Minimum required GMAT score as a number. Return null if not specified.\n"
        f"20. MinimumGreScore: Minimum required GRE score. Can be total score or section scores. Return as string or number. Return null if not specified.\n"
        f"21. MinimumIELTSScore: Minimum required IELTS score as a number (typically 0-9). Return null if not specified.\n"
        f"22. MinimumMATScore: Minimum required MAT score as a number. Return null if not specified.\n"
        f"23. MinimumMCATScore: Minimum required MCAT score as a number. Return null if not specified.\n"
        f"24. MinimumPTEScore: Minimum required PTE score as a number. Return null if not specified.\n"
        f"25. MinimumTOEFLScore: Minimum required TOEFL score as a number. Return null if not specified.\n"
        f"26. MinimumLSATScore: Minimum required LSAT score as a number. Return null if not specified.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {program_url} or other official {university_name} pages\n"
        f"- Extract information SPECIFIC to this program '{program_name}'\n"
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found on the program page, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n"
        f"- Ensure all extracted text is accurate and verbatim from the source\n"
        f"- FOR MANDATORY BOOLEAN FIELDS: You MUST return true or false. Do not return null unless absolutely no information is available. If not mentioned as required, default to false.\n\n"
        f"Return the data in a JSON format with the following exact keys: "
        f"'GreOrGmat', 'EnglishScore', 'IsDuoLingoRequired', 'IsELSRequired', 'IsGMATOrGreRequired', "
        f"'IsGMATRequired', 'IsGRERequired', 'IsIELTSRequired', 'IsLSATRequired', 'IsMATRequired', "
        f"'IsMCATRequired', 'IsPTERequired', 'IsTOEFLIBRequired', 'IsTOEFLPBTRequired', "
        f"'IsEnglishNotRequired', 'IsEnglishOptional', 'MinimumDuoLingoScore', 'MinimumELSScore', "
        f"'MinimumGMATScore', 'MinimumGreScore', 'MinimumIELTSScore', 'MinimumMATScore', "
        f"'MinimumMCATScore', 'MinimumPTEScore', 'MinimumTOEFLScore', 'MinimumLSATScore'. "
        f"Return a single JSON object, not an array. Use null for non-boolean fields where information is not available."
    )
    
    try:
        response = model.generate_content(prompt_program)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data and isinstance(parsed_data, dict):
            # Check if we got any non-null values
            has_data = any(v is not None and v != "" for v in parsed_data.values())
            
            if has_data:
                parsed_data['extraction_level'] = 'program'
                return parsed_data
    except Exception as e:
        print(f"  Error extracting from program level: {str(e)}")
    
    # If no data found at program level, try institute level
    print(f"  No program-specific data found, trying institute level...")
    prompt_institute = (
        f"You are extracting general test score requirements and English language requirements "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Institute URL: {institute_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website as GENERAL/INSTITUTE-LEVEL requirements:\n\n"
        f"1. GreOrGmat: Whether GRE or GMAT is generally required, optional, or not required. Return 'GRE', 'GMAT', 'Either', 'Optional', 'Not Required', or null.\n"
        f"2. EnglishScore: Does International students require to submit English language proficiency test scores? Return 'Required', 'Optional', 'Not Required', or null.The field should only return Required , Optional or Not Required. no extra text should be added.\n"
        f"3. IsDuoLingoRequired: Boolean (true/false) - Is Duolingo English test required? Return true, false, or null.\n"
        f"4. IsIELTSRequired: MANDATORY BOOLEAN. Is IELTS score Accepted as an proof of English proficiency test. If accepted then return TRUE else FALSE.\n"
        f"5. IsGMATOrGreRequired: Boolean (true/false) - Is either GMAT or GRE required? Return true, false, or null.\n"
        f"6. IsGMATRequired: Boolean (true/false) - Is GMAT specifically required? Return true, false, or null.\n"
        f"7. IsGRERequired: Boolean (true/false) - Is GRE specifically required? Return true, false, or null.\n"
        f"8. IsIELTSRequired: Boolean (true/false) - Is IELTS required? Return true, false, or null.\n"
        f"9. IsLSATRequired: Boolean (true/false) - If this program is law school program then does the applicant need to submit LSAT scores to be considered. if LSAT is mandatory then return TRUE else return FALSE.\n"
        f"10. IsMATRequired: Boolean (true/false) - Is Millers Analogies Test required by the applicant to be considered. if it is mandatory then return TRUE else return FALSE.\n"
        f"11. IsMCATRequired: Boolean (true/false) - If this program is medical school program then does the applicant need to submit MCAT scores to be considered. if MCAT is mandatory then return TRUE else return FALSE.\n"
        f"12. IsPTERequired: MANDATORY BOOLEAN. Is PTE (Pearson Test of English) accepted as an English proficiency test? Return TRUE or FALSE.\n"
        f"13. IsTOEFLIBRequired: MANDATORY BOOLEAN. Is TOEFL iBT (Internet-based Test) accepted as an English proficiency test? Return TRUE or FALSE.\n"
        f"14. IsTOEFLPBTRequired: MANDATORY BOOLEAN. Is TOEFL PBT (Paper-based Test) accepted as an English proficiency test? Return TRUE or FALSE.\n"
        f"15. IsEnglishNotRequired: MANDATORY BOOLEAN. Is English test explicitly NOT required? Return TRUE or FALSE.\n"
        f"16. IsEnglishOptional: MANDATORY BOOLEAN. If any English test scores are optional to submit in order to prove the English proficiency? Return TRUE or FALSE.\n"
        f"17. MinimumDuoLingoScore: Minimum required Duolingo score as a number. Return null if not specified.\n"
        f"18. MinimumELSScore: Minimum required ELS score as a number. Return null if not specified.\n"
        f"19. MinimumGMATScore: Minimum required GMAT score as a number. Return null if not specified.\n"
        f"20. MinimumGreScore: Minimum required GRE score. Can be total score or section scores. Return as string or number. Return null if not specified.\n"
        f"21. MinimumIELTSScore: Minimum required IELTS score as a number (typically 0-9). Return null if not specified.\n"
        f"22. MinimumMATScore: Minimum required MAT score as a number. Return null if not specified.\n"
        f"23. MinimumMCATScore: Minimum required MCAT score as a number. Return null if not specified.\n"
        f"24. MinimumPTEScore: Minimum required PTE score as a number. Return null if not specified.\n"
        f"25. MinimumTOEFLScore: Minimum required TOEFL score as a number. Return null if not specified.\n"
        f"26. MinimumLSATScore: Minimum required LSAT score as a number. Return null if not specified.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {institute_url} or other official {university_name} pages\n"
        f"- Extract GENERAL/INSTITUTE-LEVEL requirements (not program-specific)\n"
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n\n"
        f"Return the data in a JSON format with the following exact keys: "
        f"'GreOrGmat', 'EnglishScore', 'IsDuoLingoRequired', 'IsELSRequired', 'IsGMATOrGreRequired', "
        f"'IsGMATRequired', 'IsGRERequired', 'IsIELTSRequired', 'IsLSATRequired', 'IsMATRequired', "
        f"'IsMCATRequired', 'IsPTERequired', 'IsTOEFLIBRequired', 'IsTOEFLPBTRequired', "
        f"'IsEnglishNotRequired', 'IsEnglishOptional', 'MinimumDuoLingoScore', 'MinimumELSScore', "
        f"'MinimumGMATScore', 'MinimumGreScore', 'MinimumIELTSScore', 'MinimumMATScore', "
        f"'MinimumMCATScore', 'MinimumPTEScore', 'MinimumTOEFLScore', 'MinimumLSATScore'. "
        f"Return a single JSON object, not an array. Use null for any field where information is not available on the official website."
    )
    
    try:
        response = model.generate_content(prompt_institute)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data and isinstance(parsed_data, dict):
            parsed_data['extraction_level'] = 'institute'
            return parsed_data
    except Exception as e:
        print(f"  Error extracting from institute level: {str(e)}")
    
    # Return empty dict with null values if nothing found
    return {
        'GreOrGmat': None, 'EnglishScore': None, 'IsDuoLingoRequired': None, 'IsELSRequired': None,
        'IsGMATOrGreRequired': None, 'IsGMATRequired': None, 'IsGRERequired': None, 'IsIELTSRequired': None,
        'IsLSATRequired': None, 'IsMATRequired': None, 'IsMCATRequired': None, 'IsPTERequired': None,
        'IsTOEFLIBRequired': None, 'IsTOEFLPBTRequired': None, 'IsEnglishNotRequired': None, 'IsEnglishOptional': None,
        'MinimumDuoLingoScore': None, 'MinimumELSScore': None, 'MinimumGMATScore': None, 'MinimumGreScore': None,
        'MinimumIELTSScore': None, 'MinimumMATScore': None, 'MinimumMCATScore': None, 'MinimumPTEScore': None,
        'MinimumTOEFLScore': None, 'MinimumLSATScore': None, 'extraction_level': 'none'
    }

def undergrad_step3_run(university_name_input):
    global university_name, institute_url
    university_name = university_name_input
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Update paths with university name
    csv_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs.csv')
    json_path = os.path.join(output_dir, f'{sanitized_name}_test_scores_requirements.json')

    # We need to find the institute URL first if not hardcoded, but for now we can rely on the previous steps or simple search if needed.
    # For now, let's just find it if we can, or pass it in. 
    # But to keep it simple and consistent with previous modification:
    yield f'{{"status": "progress", "message": "Initializing test score extraction for {university_name}..."}}'
    
    # Check if CSV file exists
    if not os.path.exists(csv_path):
        yield f'{{"status": "complete", "message": "CSV file not found: {csv_path}. Skipping Step.", "files": {{}}}}'
        return

    program_data = pd.read_csv(csv_path)

    if program_data.empty:
        yield f'{{"status": "error", "message": "CSV file is empty. Please check Step 1 results."}}'
        return

    # Check if required columns exist
    required_columns = ['Program name', 'Program Page url']
    missing_columns = [col for col in required_columns if col not in program_data.columns]
    if missing_columns:
        yield f'{{"status": "error", "message": "Missing columns: {", ".join(missing_columns)}"}}'
        return

    # Quick fetch of website url for context - LOCAL ONLY
    try:
        first_url = program_data.iloc[0]['Program Page url']
        domain = urlparse(first_url).netloc
        institute_url = f"https://{domain}"
    except:
        institute_url = f"https://www.google.com/search?q={university_name}"

    # Load existing data
    test_scores_data = []
    processed_programs = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                test_scores_data = json.load(f)
                for record in test_scores_data:
                    program_name = record.get('Program name')
                    if program_name:
                        processed_programs.add(program_name)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(test_scores_data)} existing records"}}'
        except Exception as e:
            pass

    # Filter out already processed programs
    programs_to_process = []
    for index, row in program_data.iterrows():
        if row['Program name'] not in processed_programs:
            programs_to_process.append(row)

    total_programs = len(program_data)
    processed_count = len(processed_programs)
    
    if not programs_to_process:
         yield f'{{"status": "progress", "message": "All {total_programs} programs already processed. Skipping extraction."}}'
    else:
         yield f'{{"status": "progress", "message": "Starting extraction for {total_programs} programs ({len(programs_to_process)} remaining)..."}}'

    for index, row in program_data.iterrows():
        program_name = row['Program name']
        program_page_url = row['Program Page url']
        
        if program_name in processed_programs:
            continue
        
        processed_count += 1
        yield f'{{"status": "progress", "message": "Processing [{processed_count}/{total_programs}]: {program_name}"}}'
        
        try:
            extracted_data = extract_test_scores(program_name, program_page_url, institute_url)
            
            extracted_data['Program name'] = program_name
            extracted_data['Program Page url'] = program_page_url
            test_scores_data.append(extracted_data)
            processed_programs.add(program_name)
            
            save_to_json(test_scores_data, json_path)
            time.sleep(1) # Rate limit handling
        
        except Exception as e:
            error_record = {
                'Program name': program_name, 'Program Page url': program_page_url,
                'GreOrGmat': None, 'EnglishScore': None, 'IsDuoLingoRequired': None, 'IsELSRequired': None,
                'IsGMATOrGreRequired': None, 'IsGMATRequired': None, 'IsGRERequired': None, 'IsIELTSRequired': None,
                'IsLSATRequired': None, 'IsMATRequired': None, 'IsMCATRequired': None, 'IsPTERequired': None,
                'IsTOEFLIBRequired': None, 'IsTOEFLPBTRequired': None, 'IsEnglishNotRequired': None, 'IsEnglishOptional': None,
                'MinimumDuoLingoScore': None, 'MinimumELSScore': None, 'MinimumGMATScore': None, 'MinimumGreScore': None,
                'MinimumIELTSScore': None, 'MinimumMATScore': None, 'MinimumMCATScore': None, 'MinimumPTEScore': None,
                'MinimumTOEFLScore': None, 'MinimumLSATScore': None, 'extraction_level': 'error', 'error': str(e)
            }
            test_scores_data.append(error_record)
            processed_programs.add(program_name)
            save_to_json(test_scores_data, json_path)

    # Final save
    csv_output_path = os.path.join(output_dir, f'{sanitized_name}_test_scores_requirements.csv')
    if test_scores_data:
        df = pd.DataFrame(test_scores_data)
        df.to_csv(csv_output_path, index=False, encoding='utf-8')
        yield f'{{"status": "complete", "message": "Completed extraction for {len(test_scores_data)} programs", "files": {{"undergrad_test_csv": "{csv_output_path}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No data extracted", "files": {{}}}}'




# ----------------------------------------------------------------------------
# undergrad_step4 (undergraduate_programs)
# ----------------------------------------------------------------------------



# Use GeminiModelWrapper (already initialized at top of file)
# model is already available globally

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
# output_dir = "/home/my-laptop/scraper/Quinnipiac_university/Programs/graduate_programs/Grad_prog_outputs"
output_dir = os.path.join(script_dir, "Undergrad_prog_outputs")
# Create directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, 'undergraduate_programs.csv')
json_path = os.path.join(output_dir, 'application_requirements.json')

university_name = None
institute_url = None

def save_to_json(data, filepath):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def parse_json_from_response(text):
    """Parse JSON from Gemini response, handling markdown code blocks."""
    # Remove markdown formatting
    text = text.replace("**", "").replace("```json", "").replace("```", "").strip()
    
    # Try to extract JSON from the text
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # If no match, try parsing the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

def extract_application_requirements(program_name, program_url, institute_url):
    """Extract application requirements and documents, first from program level, then institute level."""
    application_requirements_page_url = None
    prompt = """ Find the website url of the application requirements page for the program '{program_name}' from the official {university_name} website. Return the url if found, otherwise return null. """
    prompt_institute_level = """ Find the Application Requirements page url for the {university_name} website. Return the url if found, otherwise return null. """
    response = model.generate_content(prompt)
    response_text = response.text
    parsed_data = parse_json_from_response(response_text)
    if parsed_data and isinstance(parsed_data, dict):
        application_requirements_page_url = parsed_data.get('application_requirements_page_url')
    else:
        response = model.generate_content(prompt_institute_level)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        if parsed_data and isinstance(parsed_data, dict):
            application_requirements_page_url = parsed_data.get('application_requirements_page_url')

    # First, try program level
    prompt_program = (
        f"You are extracting application requirements and required documents for the program '{program_name}' "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website,{application_requirements_page_url} ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Program URL: {program_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website for THIS SPECIFIC PROGRAM:\n\n"
        f"1. Resume: Is a resume/CV required to apply for {program_name}? Return 'Required', 'Optional', 'Not Required', or null. the field should only return either 'Required' or 'Not Required' or null.\n"
        f"2. StatementOfPurpose: Is a statement of purpose required to apply for {program_name}? Return 'Required', 'Optional', 'Not Required', or null. the field should only return either 'Required' or 'Not Required' or null.\n"
        f"3. Requirements: General application requirements text/description. Return null if not specified.\n"
        f"4. WritingSample: Is a writing sample required to apply for {program_name}? Return 'Required', 'Optional', 'Not Required', or null. the field should only return either 'Required' or 'Not Required' or null.\n"
        f"5. IsAnalyticalNotRequired: MANDATORY BOOLEAN. Is analytical scores are not required to apply for {program_name}? Return true or false.\n"
        f"6. IsAnalyticalOptional: MANDATORY BOOLEAN. Is analytical scores are optional if it's optional to apply for {program_name}? Return true or false.\n"
        f"7. IsStemProgram: MANDATORY BOOLEAN. Is this a STEM program? Return true or false.\n"
        f"8. IsACTRequired: MANDATORY BOOLEAN. Is ACT scores are required to apply for {program_name}? Return true if required, false if not required, or null if not specified.\n"
        f"9. IsSATRequired: MANDATORY BOOLEAN. Is SAT scores are required to apply for {program_name}? Return true if required, false if not required, or null if not specified.\n"
        f"10. MinimumACTScore: Minimum required ACT score required to apply for {program_name} as a number. Return null if not specified.\n"
        f"11. MinimumSATScore: Minimum required SAT score required to apply for {program_name} as a number. Return null if not specified.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {program_url} or other official {university_name} pages\n"
        f"- Extract information SPECIFIC to this program '{program_name}'\n"
        f"- Browse all "
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found on the program page, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n"
        f"- Ensure all extracted text is accurate and verbatim from the source\n"
        f"- FOR MANDATORY BOOLEAN FIELDS: You MUST return true or false. Do not return null unless absolutely no information is available. If not mentioned as required, default to false.\n\n"
        f"Return the data in a JSON format with the following exact keys: "
        f"'Resume', 'StatementOfPurpose', 'Requirements', 'WritingSample', 'IsAnalyticalNotRequired', "
        f"'IsAnalyticalOptional', 'IsStemProgram', 'IsACTRequired', "
        f"'IsSATRequired', 'MinimumACTScore', 'MinimumSATScore'. "
        f"Return a single JSON object, not an array. Use null for non-boolean fields if info not available."
    )
    
    try:
        response = model.generate_content(prompt_program)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data and isinstance(parsed_data, dict):
            # Check if we got any non-null values
            has_data = any(v is not None and v != "" for v in parsed_data.values())
            
            if has_data:
                parsed_data['extraction_level'] = 'program'
                return parsed_data
    except Exception as e:
        print(f"  Error extracting from program level: {str(e)}")
    
    # If no data found at program level, try institute level
    print(f"  No program-specific data found, trying institute level...")
    prompt_institute = (
        f"You are extracting general application requirements and required documents "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Institute URL: {institute_url}\n\n"
        f"Extract the following fields ONLY if they are present on the official {university_name} website as GENERAL/INSTITUTE-LEVEL requirements:\n\n"
        f"1. Resume: Is a resume/CV generally required? Return 'Required', 'Optional', 'Not Required', or null. the field should only return either 'Required' or 'Not Required' or null.\n"
        f"2. StatementOfPurpose: Is a statement of purpose generally required? Return 'Required', 'Optional', 'Not Required', or null. the field should only return either 'Required' or 'Not Required' or null.\n"
        f"3. Requirements: General application requirements text/description. Return null if not specified.\n"
        f"4. WritingSample: Is a writing sample generally required? Return 'Required', 'Optional', 'Not Required', or null. the field should only return either 'Required' or 'Not Required' or null.\n"
        f"5. IsAnalyticalNotRequired: MANDATORY BOOLEAN. Does analytical scores such as ACT/SAT are not required to submit? Return true if not required, false if required.\n"
        f"6. IsAnalyticalOptional: MANDATORY BOOLEAN. Is analytical writing section optional if it's optional? Return true, false, or null.\n"
        f"7. IsStemProgram: MANDATORY BOOLEAN. This field should be null at institute level (program-specific). Return null.\n"
        f"8. IsACTRequired: MANDATORY BOOLEAN. Is ACT exam required for this program to apply? Return true, false, or null.\n"
        f"9. IsSATRequired: MANDATORY BOOLEAN. Is SAT exam required for this program to apply? Return true, false, or null.\n"
        f"10. MinimumACTScore: Minimum required ACT score as a number. Return null if not specified.\n"
        f"11. MinimumSATScore: Minimum required SAT score as a number. Return null if not specified.\n\n"
        f"CRITICAL REQUIREMENTS:\n"
        f"- All data must be extracted ONLY from {institute_url} or other official {university_name} pages\n"
        f"- Extract GENERAL/INSTITUTE-LEVEL requirements (not program-specific)\n"
        f"- Do NOT infer, assume, or make up any information\n"
        f"- If a field is not found, return null for that field\n"
        f"- All URLs must be from the {university_name} domain or its subdomains\n\n"
        f"Return the data in a JSON format with the following exact keys: "
        f"'Resume', 'StatementOfPurpose', 'Requirements', 'WritingSample', 'IsAnalyticalNotRequired', "
        f"'IsAnalyticalOptional', 'IsStemProgram', 'IsACTRequired', "
        f"'IsSATRequired', 'MinimumACTScore', 'MinimumSATScore'. "
        f"Return a single JSON object, not an array. Use null for any field where information is not available on the official website."
    )
    
    try:
        response = model.generate_content(prompt_institute)
        response_text = response.text
        parsed_data = parse_json_from_response(response_text)
        
        if parsed_data and isinstance(parsed_data, dict):
            parsed_data['extraction_level'] = 'institute'
            return parsed_data
    except Exception as e:
        print(f"  Error extracting from institute level: {str(e)}")
    
    # Return empty dict with null values if nothing found
    return {
        'Resume': None, 'StatementOfPurpose': None, 'Requirements': None, 'WritingSample': None,
        'IsAnalyticalNotRequired': False, 'IsAnalyticalOptional': False, 'IsRecommendationSystemOpted': False,
        'IsStemProgram': False, 'IsACTRequired': False, 'IsSATRequired': False,
        'MinimumACTScore': None, 'MinimumSATScore': None, 'extraction_level': 'none'
    }

# Institute level URL for fallback
def undergrad_step4_run(university_name_input):
    global university_name, institute_url
    university_name = university_name_input
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Update paths with university name
    csv_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs.csv')
    json_path = os.path.join(output_dir, f'{sanitized_name}_application_requirements.json')

    yield f'{{"status": "progress", "message": "Initializing application requirements extraction for {university_name}..."}}'
    
    # Quick fetch of website url for context
    try:
        website_url_prompt = f"What is the official university website for {university_name}?"
        institute_url = model.generate_content(website_url_prompt).text.replace("**", "").replace("```", "").strip()
    except:
        institute_url = f"https://www.google.com/search?q={university_name}"

    # Check if CSV file exists
    if not os.path.exists(csv_path):
        yield f'{{"status": "complete", "message": "CSV file not found: {csv_path}. Skipping Step.", "files": {{}}}}'
        return

    program_data = pd.read_csv(csv_path)

    if program_data.empty:
        yield f'{{"status": "error", "message": "CSV file is empty. Please check Step 1 results."}}'
        return

    # Check if required columns exist
    required_columns = ['Program name', 'Program Page url']
    missing_columns = [col for col in required_columns if col not in program_data.columns]
    if missing_columns:
        yield f'{{"status": "error", "message": "Missing columns: {", ".join(missing_columns)}"}}'
        return

    # Load existing data
    application_data = []
    processed_programs = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                application_data = json.load(f)
                for record in application_data:
                    program_name = record.get('Program name')
                    if program_name:
                        processed_programs.add(program_name)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(application_data)} existing records"}}'
        except Exception as e:
            pass

    # Filter out already processed programs
    programs_to_process = []
    for index, row in program_data.iterrows():
        if row['Program name'] not in processed_programs:
            programs_to_process.append(row)

    total_programs = len(program_data)
    processed_count = len(processed_programs)
    
    if not programs_to_process:
         yield f'{{"status": "progress", "message": "All {total_programs} programs already processed. Skipping extraction."}}'
    else:
         yield f'{{"status": "progress", "message": "Starting extraction for {total_programs} programs ({len(programs_to_process)} remaining)..."}}'

    for index, row in program_data.iterrows():
        program_name = row['Program name']
        program_page_url = row['Program Page url']
        
        if program_name in processed_programs:
            continue
        
        processed_count += 1
        yield f'{{"status": "progress", "message": "Processing [{processed_count}/{total_programs}]: {program_name}"}}'
        
        try:
            extracted_data = extract_application_requirements(program_name, program_page_url, institute_url)
            
            extracted_data['Program name'] = program_name
            extracted_data['Program Page url'] = program_page_url
            application_data.append(extracted_data)
            processed_programs.add(program_name)
            
            save_to_json(application_data, json_path)
            time.sleep(1) # Rate limit handling
        
        except Exception as e:
            error_record = {
                'Program name': program_name, 'Program Page url': program_page_url,
                'Resume': None, 'StatementOfPurpose': None, 'Requirements': None, 'WritingSample': None,
                'IsAnalyticalNotRequired': None, 'IsAnalyticalOptional': None, 'IsRecommendationSystemOpted': None,
                'IsStemProgram': None, 'IsACTRequired': None, 'IsSATRequired': None,
                'MinimumACTScore': None, 'MinimumSATScore': None, 'extraction_level': 'error', 'error': str(e)
            }
            application_data.append(error_record)
            processed_programs.add(program_name)
            save_to_json(application_data, json_path)

    # Final save
    csv_output_path = os.path.join(output_dir, f'{sanitized_name}_application_requirements.csv')
    if application_data:
        df = pd.DataFrame(application_data)
        df.to_csv(csv_output_path, index=False, encoding='utf-8')
        yield f'{{"status": "complete", "message": "Completed extraction for {len(application_data)} programs", "files": {{"undergrad_app_req_csv": "{csv_output_path}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No data extracted", "files": {{}}}}'



# ----------------------------------------------------------------------------
# undergrad_step5 (undergraduate_programs)
# ----------------------------------------------------------------------------



# Use GeminiModelWrapper (already initialized at top of file)
# model is already available globally

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "Undergrad_prog_outputs")
# Create directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, 'undergraduate_programs.csv')
json_path = os.path.join(output_dir, 'program_details_financial.json')

def save_to_json(data, filepath):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def parse_json_from_response(text):
    """Parse JSON from Gemini response, handling markdown code blocks."""
    # Remove markdown formatting
    text = text.replace("**", "").replace("```json", "").replace("```", "").strip()
    
    # Try to extract JSON from the text
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # If no match, try parsing the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def extract_program_details(program_name, program_url, institute_url):
    global university_name
    
    prompt = (
        f"You are extracting program details and financial information for the program '{program_name}' "
        f"from the official {university_name} website.\n\n"
        f"IMPORTANT: You MUST ONLY use information from the official {university_name} website ({institute_url} and its subdomains). "
        f"Do NOT use information from any other sources. If the information is not available on the official {university_name} website, return null for that field.\n\n"
        f"Program URL: {program_url}\n"
        f"Institute URL: {institute_url}\n\n"
        f"Extract the following fields:\n\n"
        f"1. QsWorldRanking: QS World University Ranking (Instance Level). Return as string or number. Return null if not found.\n"
        f"2. School: The specific school or college offering the program (e.g. 'School of Business'). Return string or null.\n"
        f"3. MaxFails: Maximum number of failing grades allowed. Return number or null.\n"
        f"4. MaxGPA: Maximum GPA scale (e.g., 4.0). Return number or null.\n"
        f"5. MinGPA: Minimum GPA required for admission/graduation. Return number or null.\n"
        f"6. PreviousYearAcceptanceRates: Acceptance rate. Return string/number or null.\n"
        f"7. Term: Fall 2026. Return string or null.\n"
        f"8. LiveDate: Application opening date. Return string or null. look for fall 2026 application opening date\n"
        f"9. DeadlineDate: Application deadline. Return string or null. look for fall 2026 application deadline\n"
        f"10. Fees: Tuition fee for the program. Return a number. Look if the program specific tuition fee is mentioned in any cost of attendance page of the {program_url} website. sample output: $12,000/Semester or $18,000/Year\n"
        f"11. AverageScholarshipAmount: Average scholarship amount. Return string/number or null.\n"
        f"12. CostPerCredit: Cost per credit hour for the program. Return string/number or null.\n"
        f"13. ScholarshipAmount: General scholarship amount available. Return string/number or null.\n"
        f"14. ScholarshipPercentage: Scholarship percentage available. Return string/number or null.\n"
        f"15. ScholarshipType: Types of scholarships available (e.g. 'Merit-based'). Return string or null.\n"
        f"16. Program duration: Duration of the program. Return string or null.\n"
        f"Return data in JSON format with exact keys: 'QsWorldRanking', 'School', 'MaxFails', 'MaxGPA', 'MinGPA', "
        f"'PreviousYearAcceptanceRates', 'Term', 'LiveDate', 'DeadlineDate', 'Fees', 'AverageScholarshipAmount', 'CostPerCredit', "
        f"'ScholarshipAmount', 'ScholarshipPercentage', 'ScholarshipType', 'Program duration', 'Tuition fee'."
    )
    
    try:
        response = model.generate_content(prompt)
        parsed = parse_json_from_response(response.text)
        if parsed and isinstance(parsed, dict):
            return parsed
    except Exception as e:
        print(f"Error details extraction: {e}")
    
    # Return empty dict with nulls if fail
    return {
        'QsWorldRanking': None, 'School': None, 'MaxFails': None, 'MaxGPA': None, 'MinGPA': None,
        'PreviousYearAcceptanceRates': None, 'Term': None, 'LiveDate': None, 'DeadlineDate': None,
        'Fees': None, 'AverageScholarshipAmount': None, 'CostPerCredit': None,
        'ScholarshipAmount': None, 'ScholarshipPercentage': None, 'ScholarshipType': None,
        'Program duration': None, 'Tuition fee': None
    }

def process_single_program(row, institute_url):
    """Wrapper to process a single program."""
    program_name = row['Program name']
    program_page_url = row['Program Page url']
    
    try:
        extracted_data = extract_program_details(program_name, program_page_url, institute_url)
        
        extracted_data['Program name'] = program_name
        extracted_data['Program Page url'] = program_page_url
        return extracted_data
    
    except Exception as e:
        return {
            'Program name': program_name,
            'Program Page url': program_page_url,
            'QsWorldRanking': None, 'School': None, 'MaxFails': None, 'MaxGPA': None, 'MinGPA': None,
            'PreviousYearAcceptanceRates': None, 'Term': None, 'LiveDate': None, 'DeadlineDate': None,
            'Fees': None, 'AverageScholarshipAmount': None, 'CostPerCredit': None,
            'ScholarshipAmount': None, 'ScholarshipPercentage': None, 'ScholarshipType': None,
            'Program duration': None, 'Tuition fee': None, 'extraction_level': 'error', 'error': str(e)
        }

def undergrad_step5_run(university_name_input):
    global university_name, institute_url
    university_name = university_name_input
    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    # Update paths with university name
    csv_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs.csv')
    json_path = os.path.join(output_dir, f'{sanitized_name}_program_details_financial.json')

    yield f'{{"status": "progress", "message": "Initializing program details & financial extraction for {university_name}..."}}'
    


    # Check if CSV file exists
    if not os.path.exists(csv_path):
        yield f'{{"status": "complete", "message": "CSV file not found: {csv_path}. Skipping Step.", "files": {{}}}}'
        return

    program_data = pd.read_csv(csv_path)

    if program_data.empty:
        yield f'{{"status": "error", "message": "CSV file is empty. Please check Step 1 results."}}'
        return

    # Check if required columns exist
    required_columns = ['Program name', 'Program Page url']
    missing_columns = [col for col in required_columns if col not in program_data.columns]
    if missing_columns:
        yield f'{{"status": "error", "message": "Missing columns: {", ".join(missing_columns)}"}}'
        return

    # Quick fetch of website url for context - LOCAL ONLY
    try:
        first_url = program_data.iloc[0]['Program Page url']
        domain = urlparse(first_url).netloc
        institute_url = f"https://{domain}"
    except:
        institute_url = f"https://www.google.com/search?q={university_name}"

    # Load existing data
    program_details_data = []
    processed_programs = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                program_details_data = json.load(f)
                for record in program_details_data:
                    program_name = record.get('Program name')
                    if program_name:
                        processed_programs.add(program_name)
            yield f'{{"status": "progress", "message": "Resuming: Loaded {len(program_details_data)} existing records"}}'
        except Exception as e:
            pass

    # Filter out already processed programs
    programs_to_process = []
    for index, row in program_data.iterrows():
        if row['Program name'] not in processed_programs:
            programs_to_process.append(row)

    total_programs = len(program_data)
    processed_count = len(processed_programs)
    
    yield f'{{"status": "progress", "message": "Starting extraction for {total_programs} programs ({len(programs_to_process)} remaining)..."}}'

    for index, row in program_data.iterrows():
        program_name = row['Program name']
        program_page_url = row['Program Page url']
        
        if program_name in processed_programs:
            continue
        
        processed_count += 1
        yield f'{{"status": "progress", "message": "Processing [{processed_count}/{total_programs}]: {program_name}"}}'
        
        try:
            extracted_data = process_single_program(row, institute_url)
            
            program_details_data.append(extracted_data)
            processed_programs.add(program_name)
            
            save_to_json(program_details_data, json_path)
            time.sleep(1) # Rate limit handling
        
        except Exception as e:
            error_record = {
                'Program name': program_name,
                'Program Page url': program_page_url,
                'QsWorldRanking': None, 'School': None, 'MaxFails': None, 'MaxGPA': None, 'MinGPA': None,
                'PreviousYearAcceptanceRates': None, 'Term': None, 'LiveDate': None, 'DeadlineDate': None,
                'Fees': None, 'AverageScholarshipAmount': None, 'CostPerCredit': None,
                'ScholarshipAmount': None, 'ScholarshipPercentage': None, 'ScholarshipType': None,
                'Program duration': None, 'Tuition fee': None, 'extraction_level': 'error', 'error': str(e)
            }
            program_details_data.append(error_record)
            processed_programs.add(program_name)
            save_to_json(program_details_data, json_path)

    # Final save
    csv_output_path = os.path.join(output_dir, f'{sanitized_name}_program_details_financial.csv')
    if program_details_data:
        df = pd.DataFrame(program_details_data)
        df.to_csv(csv_output_path, index=False, encoding='utf-8')
        yield f'{{"status": "complete", "message": "Completed extraction for {len(program_details_data)} programs", "files": {{"undergrad_details_csv": "{csv_output_path}"}}}}'
    else:
        yield f'{{"status": "complete", "message": "No data extracted", "files": {{}}}}'



# ----------------------------------------------------------------------------
# undergrad_merge (undergraduate_programs)
# ----------------------------------------------------------------------------


# Define the target schema and column mapping
TARGET_COLUMNS = [
    'Id', 'ProgramName', 'ProgramCode', 'Status', 'CreatedDate', 'UpdatedDate', 'Level', 'Term',
    'TermCode', 'LiveDate', 'DeadlineDate', 'Resume', 'StatementOfPurpose', 'GreOrGmat',
    'EnglishScore', 'Requirements', 'WritingSample', 'CollegeId', 'IsAnalyticalNotRequired',
    'IsAnalyticalOptional', 'IsDuoLingoRequired', 'IsELSRequired', 'IsGMATOrGreRequired',
    'IsGMATRequired', 'IsGRERequired', 'IsIELTSRequired', 'IsLSATRequired', 'IsMATRequired',
    'IsMCATRequired', 'IsPTERequired', 'IsTOEFLIBRequired', 'IsTOEFLPBTRequired',
    'IsEnglishNotRequired', 'IsEnglishOptional', 'AcademicYear', 'AlternateProgram',
    'ApplicationType', 'Department', 'Fees', 'IsAvailable', 'ProgramType',
    'AdmissionDepartmentId', 'CreatedBy', 'UpdatedBy', 'Concentration', 'Description',
    'OtherConcentrations', 'ProgramWebsiteURL', 'Accredidation', 'AverageScholarshipAmount',
    'CostPerCredit', 'IsRecommendationSystemOpted', 'IsStemProgram', 'MaxFails', 'MaxGPA',
    'MinGPA', 'PreviousYearAcceptanceRates', 'QsWorldRanking', 'TotalAccepetedApplications',
    'TotalCredits', 'TotalDeniedApplications', 'TotalI20sIssued', 'TotalScholarshipsAwarded',
    'TotalSubmittedApplications', 'TotalVisasSecured', 'UsNewsRanking', 'CollegeApplicationFee',
    'IsCollegePaying', 'MEContractNegotiatedFee', 'MyGradAppFee', 'ProgramCategory',
    'IsCollegeApplicationFree', 'IsCouponAllowed', 'IsACTRequired', 'IsSATRequired',
    'SftpDestinationId', 'MinimumACTScore', 'MinimumDuoLingoScore', 'MinimumELSScore',
    'MinimumGMATScore', 'MinimumGreScore', 'MinimumIELTSScore', 'MinimumMATScore',
    'MinimumMCATScore', 'MinimumPTEScore', 'MinimumSATScore', 'MinimumTOEFLScore',
    'MLModelName', 'MinimumAnalyticalScore', 'MinimumEnglishScore', 'MinimumExperience',
    'MinimumSopRating', 'WeightAnalytical', 'WeightEnglish', 'WeightExperience', 'WeightGPA',
    'WeightSop', 'ScholarshipAmount', 'ScholarshipPercentage', 'ScholarshipType',
    'IsNewlyLaunched', 'BatchId', 'IsImported', 'IsImportVerified', 'Is_Recommendation_Sponser',
    'AnalyticalScore', 'MinimumLSATScore'
]

COLUMN_MAPPING = {
    # Base
    'Program name': 'ProgramName',
    'Level': 'Level',
    'Program Page url': 'ProgramWebsiteURL',
    
    # Financial
    'QsWorldRanking': 'QsWorldRanking',
    'School': 'Department', 
    'MaxFails': 'MaxFails',
    'MaxGPA': 'MaxGPA',
    'MinGPA': 'MinGPA',
    'PreviousYearAcceptanceRates': 'PreviousYearAcceptanceRates',
    'Term': 'Term',
    'LiveDate': 'LiveDate',
    'DeadlineDate': 'DeadlineDate',
    'Fees': 'CollegeApplicationFee', # Mapping extracted 'Fees' (which are usually app fees) to CollegeApplicationFee
    'Tuition fee': 'Fees',           # Mapping extracted 'Tuition fee' -> Fees column
    'AverageScholarshipAmount': 'AverageScholarshipAmount',
    'CostPerCredit': 'CostPerCredit',
    'ScholarshipAmount': 'ScholarshipAmount',
    'ScholarshipPercentage': 'ScholarshipPercentage',
    'ScholarshipType': 'ScholarshipType',
    
    # Test Scores
    'GreOrGmat': 'GreOrGmat',
    'EnglishScore': 'EnglishScore',
    'IsDuoLingoRequired': 'IsDuoLingoRequired',
    'IsELSRequired': 'IsELSRequired',
    'IsGMATOrGreRequired': 'IsGMATOrGreRequired',
    'IsGMATRequired': 'IsGMATRequired',
    'IsGRERequired': 'IsGRERequired',
    'IsIELTSRequired': 'IsIELTSRequired',
    'IsLSATRequired': 'IsLSATRequired',
    'IsMATRequired': 'IsMATRequired',
    'IsMCATRequired': 'IsMCATRequired',
    'IsPTERequired': 'IsPTERequired',
    'IsTOEFLIBRequired': 'IsTOEFLIBRequired',
    'IsTOEFLPBTRequired': 'IsTOEFLPBTRequired',
    'IsEnglishNotRequired': 'IsEnglishNotRequired',
    'IsEnglishOptional': 'IsEnglishOptional',
    'MinimumDuoLingoScore': 'MinimumDuoLingoScore',
    'MinimumELSScore': 'MinimumELSScore',
    'MinimumGMATScore': 'MinimumGMATScore',
    'MinimumGreScore': 'MinimumGreScore',
    'MinimumIELTSScore': 'MinimumIELTSScore',
    'MinimumMATScore': 'MinimumMATScore',
    'MinimumMCATScore': 'MinimumMCATScore',
    'MinimumPTEScore': 'MinimumPTEScore',
    'MinimumTOEFLScore': 'MinimumTOEFLScore',
    'MinimumLSATScore': 'MinimumLSATScore',
    
    # Application Requirements
    'Resume': 'Resume',
    'StatementOfPurpose': 'StatementOfPurpose',
    'Requirements': 'Requirements',
    'WritingSample': 'WritingSample',
    'IsAnalyticalNotRequired': 'IsAnalyticalNotRequired',
    'IsAnalyticalOptional': 'IsAnalyticalOptional',
    'IsRecommendationSystemOpted': 'IsRecommendationSystemOpted',
    'IsStemProgram': 'IsStemProgram',
    'IsACTRequired': 'IsACTRequired',
    'IsSATRequired': 'IsSATRequired',
    'MinimumACTScore': 'MinimumACTScore',
    'MinimumSATScore': 'MinimumSATScore',
    
    # Extra Fields
    'Concentration name': 'Concentration',
    'description': 'Description',
    'Accreditation status': 'Accredidation'
}

def load_json_data(filepath):
    if not os.path.exists(filepath):
        print(f"Warning: File not found: {filepath}")
        return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return []

def undergrad_merge_run(university_name=None):
    yield f'{{"status": "progress", "message": "Starting data merge and standardization..."}}'
    
    if not university_name:
        yield f'{{"status": "error", "message": "University name not provided for merge step."}}'
        return

    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "Undergrad_prog_outputs")
    os.makedirs(output_dir, exist_ok=True)
    
    # File paths
    base_csv_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs.csv')
    financial_json_path = os.path.join(output_dir, f'{sanitized_name}_program_details_financial.json')
    test_scores_json_path = os.path.join(output_dir, f'{sanitized_name}_test_scores_requirements.json')
    app_req_json_path = os.path.join(output_dir, f'{sanitized_name}_application_requirements.json')
    extra_fields_json_path = os.path.join(output_dir, f'{sanitized_name}_extra_fields_data.json')
    
    # 1. Load Base Data
    if not os.path.exists(base_csv_path):
        yield f'{{"status": "complete", "message": "Base CSV not found at {base_csv_path}. Skipping merge step.", "files": {{}}}}'
        return
        
    df_base = pd.read_csv(base_csv_path)
    yield f'{{"status": "progress", "message": "Loaded {len(df_base)} programs from base CSV"}}'
    
    # 2. Load and Prepare Merge Data
    financial_data = load_json_data(financial_json_path)
    test_scores_data = load_json_data(test_scores_json_path)
    app_req_data = load_json_data(app_req_json_path)
    extra_fields_data = load_json_data(extra_fields_json_path)
    
    # Convert to DataFrames
    df_fin = pd.DataFrame(financial_data) if financial_data else pd.DataFrame()
    df_test = pd.DataFrame(test_scores_data) if test_scores_data else pd.DataFrame()
    df_app = pd.DataFrame(app_req_data) if app_req_data else pd.DataFrame()
    df_extra = pd.DataFrame(extra_fields_data) if extra_fields_data else pd.DataFrame()
    
    # Merge Key
    merge_key = 'Program name'
    
    # Ensure merge key exists in all DFs before merging
    dfs_to_merge = [df_fin, df_test, df_app, df_extra]
    final_df = df_base.copy()
    
    for i, df in enumerate(dfs_to_merge):
        if not df.empty and merge_key in df.columns:
            # Drop duplicates in join tables if any
            df = df.drop_duplicates(subset=[merge_key])
            # Drop Program Page url from merge tables to avoid suffixes, keep it from base
            if 'Program Page url' in df.columns:
                df = df.drop(columns=['Program Page url'])
            
            final_df = pd.merge(final_df, df, on=merge_key, how='left')
            yield f'{{"status": "progress", "message": "Merged dataset {i+1}..."}}'
        else:
            yield f'{{"status": "progress", "message": "Skipping dataset {i+1} (empty or missing key)"}}'

    # 3. Rename Columns
    # Rename columns that exist in the mapping
    final_df = final_df.rename(columns=COLUMN_MAPPING)
    
    # 4. Add Missing Columns
    for col in TARGET_COLUMNS:
        if col not in final_df.columns:
            final_df[col] = ""  # Initialize with empty string
            
    # 5. Select and Reorder Columns
    # Only keep columns that are in TARGET_COLUMNS
    final_df = final_df[TARGET_COLUMNS]
    
    # Define keywords for undergraduate levels
    # Using lowercase for case-insensitive matching
    levels_map = {
        "Undergraduate-Certificate": ["certificate", "certification", "cert"],
        "Associate": ["associate", "aa", "as", "aas"],
         
    }

    # Determine level logic:
    # Default to 'Undergraduate' (which covers general Bachelors if not explicitly matched, or we can default to Bachelor)
    # The user asked for specific logic for certs, but we should make it robust for undergrad.
    final_df['Level'] = final_df['ProgramName'].apply(lambda x: next((k for k, v in levels_map.items() if any(keyword in str(x).lower() for keyword in v)), 'Undergraduate'))


    # 6. Save Final CSV
    output_csv_path = os.path.join(output_dir, f'{sanitized_name}_undergraduate_programs_final.csv')
    final_df.to_csv(output_csv_path, index=False, encoding='utf-8')
    
    yield f'{{"status": "complete", "message": "Successfully merged and standardized data", "files": {{"undergrad_final_csv": "{output_csv_path}"}}}}'




# ============================================================================
# MERGE_ALL.PY - FINAL MERGE LOGIC
# ============================================================================


def merge_all_run(university_name=None):
    yield f'{{"status": "progress", "message": "Starting final merge of Graduate and Undergraduate programs..."}}'
    
    if not university_name:
        yield f'{{"status": "error", "message": "University name not provided for final merge."}}'
        return

    sanitized_name = university_name.replace(" ", "_").replace("/", "_")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Paths to the final CSVs
    grad_csv_path = os.path.join(script_dir, 'graduate_programs', 'Grad_prog_outputs', f'{sanitized_name}_graduate_programs_final.csv')
    undergrad_csv_path = os.path.join(script_dir, 'undergraduate_programs', 'Undergrad_prog_outputs', f'{sanitized_name}_undergraduate_programs_final.csv')
    
    output_csv_path = os.path.join(script_dir, f'{sanitized_name}_Final.csv')
    
    dfs = []
    
    # Load Graduate Programs
    if os.path.exists(grad_csv_path):
        df_grad = pd.read_csv(grad_csv_path)
        yield f'{{"status": "progress", "message": "Loaded {len(df_grad)} graduate programs"}}'
        dfs.append(df_grad)
    else:
        yield f'{{"status": "progress", "message": "Graduate programs file not found at {grad_csv_path}"}}'
        
    # Load Undergraduate Programs
    if os.path.exists(undergrad_csv_path):
        df_undergrad = pd.read_csv(undergrad_csv_path)
        yield f'{{"status": "progress", "message": "Loaded {len(df_undergrad)} undergraduate programs"}}'
        dfs.append(df_undergrad)
    else:
        yield f'{{"status": "progress", "message": "Undergraduate programs file not found at {undergrad_csv_path}"}}'
        
    if not dfs:
        yield f'{{"status": "error", "message": "No data found to merge."}}'
        return



    # Merge
    yield f'{{"status": "progress", "message": "Merging datasets..."}}'
    final_df = pd.concat(dfs, ignore_index=True)
    final_df['QsWorldRanking'] = ""
    final_df['CollegeApplicationFee'] = ""
    final_df['IsNewlyLaunched'] = "FALSE"
    final_df['IsImportVerified'] = "FALSE"
    final_df['Is_Recommendation_Sponser'] = "FALSE"
    final_df['IsRecommendationSystemOpted'] = "FALSE"
    final_df['Term']="Fall 2026"
    final_df['LiveDate']=""
    final_df['DeadlineDate']=""
    final_df['PreviousYearAcceptanceRates']=""
    final_df['IsStemProgram']= final_df['IsStemProgram'].fillna(False)
    final_df['IsStemProgram']= final_df['IsStemProgram'].astype(bool)
    final_df['IsACTRequired']= final_df['IsACTRequired'].fillna(False)
    final_df['IsACTRequired']= final_df['IsACTRequired'].astype(bool)
    final_df['IsSATRequired']= final_df['IsSATRequired'].fillna(False)
    final_df['IsSATRequired']= final_df['IsSATRequired'].astype(bool)
    final_df['IsAnalyticalNotRequired'] = final_df['IsAnalyticalNotRequired'].fillna(True)
    final_df['IsAnalyticalNotRequired'] = final_df['IsAnalyticalNotRequired'].astype(bool)
    final_df['IsAnalyticalOptional'] = final_df['IsAnalyticalOptional'].fillna(True)
    final_df['IsAnalyticalOptional'] = final_df['IsAnalyticalOptional'].astype(bool)

    final_df['ProgramName'] = final_df['ProgramName'].apply(standardize_program_name)

    
    ###############
    final_df.to_csv(output_csv_path, index=False, encoding='utf-8')
    yield f'{{"status": "complete", "message": "Successfully merged {len(final_df)} programs", "files": {{"final_csv": "{output_csv_path}"}}}}'


def standardize_program_name(name):
    name_str = str(name).strip()
    # Mapping of suffix to prefix
    mappings = {
        " MS": "Master of Science in",
        " MFA": "Master of Fine Arts in",
        " BS": "Bachelor of Science in",
        " BA": "Bachelor of Arts in",
        " MA": "Master of Arts in",
        "AAS": "Associate of Applied Science in",
        "AS": "Associate of Science in",
        "AA": "Associate of Arts in",
        "BFA": "Bachelor of Fine Arts in",
        "MBA": "Master of Business Administration in",
        "AOS": "Associate of Science in",
        " (MS)": "Master of Science in",
        " (MFA)": "Master of Fine Arts in",
        " (BS)": "Bachelor of Science in",
        " (BA)": "Bachelor of Arts in",
        " (MA)": "Master of Arts in",
        " (AAS)": "Associate of Applied Science in",
        " (AS)": "Associate of Science in",
        " (AA)": "Associate of Arts in",
        " (BFA)": "Bachelor of Fine Arts in",
        " (MBA)": "Master of Business Administration in",
        "(BA, BS)": "Bachelor of Arts in"

    }
    
    for suffix, prefix in mappings.items():
        if name_str.endswith(suffix):
            # Remove the suffix (e.g. " MS") and prepend the prefix
            # Original: "Program MS" -> "Program" -> "Master of Science in Program"
            clean_name = name_str[:-len(suffix)]
            return f"{prefix} {clean_name}"
            
    return name_str


# ============================================================================
# MODULE WRAPPERS - Allow Programs.py orchestration to work
# ============================================================================

class ModuleWrapper:
    """Wrapper to make a run function look like an imported module"""
    def __init__(self, run_func):
        self.run = run_func

# Create module references for graduate programs
grad_step1 = ModuleWrapper(grad_step1_run)
grad_step2 = ModuleWrapper(grad_step2_run)
grad_step3 = ModuleWrapper(grad_step3_run)
grad_step4 = ModuleWrapper(grad_step4_run)
grad_step5 = ModuleWrapper(grad_step5_run)
grad_merge = ModuleWrapper(grad_merge_run)

# Create module references for undergraduate programs
undergrad_step1 = ModuleWrapper(undergrad_step1_run)
undergrad_step2 = ModuleWrapper(undergrad_step2_run)
undergrad_step3 = ModuleWrapper(undergrad_step3_run)
undergrad_step4 = ModuleWrapper(undergrad_step4_run)
undergrad_step5 = ModuleWrapper(undergrad_step5_run)
undergrad_merge = ModuleWrapper(undergrad_merge_run)

# Create merge_all module reference
class MergeAllWrapper:
    """Wrapper for merge_all module"""
    @staticmethod
    def run(*args, **kwargs):
        return merge_all_run(*args, **kwargs)

merge_all = MergeAllWrapper()


# ============================================================================
# PROGRAMS.PY - PROGRAMS ORCHESTRATION
# ============================================================================


# Add current directory to path to allow imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import Graduate Scripts

# Import Undergraduate Scripts


# Import Final Merge Script

def process_programs_extraction(university_name, step):
    """
    Orchestrate the extraction process based on the step.
    Generator that yields JSON strings with updates.
    """
    
    # Map steps to modules
    # Each step list contains [grad_module, undergrad_module]
    steps_map = {
        1: [grad_step1, undergrad_step1],
        2: [grad_step2, undergrad_step2],
        3: [grad_step3, undergrad_step3],
        4: [grad_step4, undergrad_step4],
        5: [grad_step5, undergrad_step5],
        6: [grad_merge, undergrad_merge]  # This is the standardize step
    }

    try:
        step = int(step)
    except ValueError:
        yield f'{{"status": "error", "message": "Invalid step number: {step}"}}'
        return

    if step == 7: # Special step for Final Merge
        yield f'{{"status": "progress", "message": "Starting Step 7: Final Merge..."}}'
        try:
            for update in merge_all.run(university_name):
                yield update
        except Exception as e:
            yield f'{{"status": "error", "message": "Error in Final Merge: {str(e)}"}}'
        return

    if step == 8: # Special step for Concurrent Execution (Steps 2, 3, 4, 5)
        yield f'{{"status": "progress", "message": "Starting Concurrent Extraction for Steps 2, 3, 4, 5..."}}'
        
        modules_to_run = [
            (grad_step2, "[Grad] Step 2"),
            (grad_step3, "[Grad] Step 3"),
            (grad_step4, "[Grad] Step 4"),
            (grad_step5, "[Grad] Step 5"),
            (undergrad_step2, "[Undergrad] Step 2"),
            (undergrad_step3, "[Undergrad] Step 3"),
            (undergrad_step4, "[Undergrad] Step 4"),
            (undergrad_step5, "[Undergrad] Step 5")
        ]
        
        msg_queue = queue.Queue()
        accumulated_files = {}
        
        def run_module(module, name, q):
            try:
                if module and hasattr(module, 'run'):
                    for update in module.run(university_name):
                        try:
                            # Parse JSON to inject prefix in message
                            try:
                                data = json.loads(update)
                            except:
                                data = None
                            
                            if isinstance(data, dict):
                                if 'message' in data:
                                    data['message'] = f"[{name}] {data['message']}"
                                
                                # Intercept complete status from sub-modules
                                if data.get('status') == 'complete':
                                    # Collect files
                                    if 'files' in data:
                                        data['files_update'] = data['files']
                                    
                                    # Change status to progress so frontend doesn't disconnect
                                    data['status'] = 'progress'
                                    data['message'] = f"[{name}] Sub-task completed."
                                    
                                q.put(json.dumps(data))
                            else:
                                # Not a dict (e.g. string or list), treat clearly
                                safe_msg = str(update)
                                msg_obj = {
                                    "status": "progress",
                                    "message": f"[{name}] {safe_msg}"
                                }
                                q.put(json.dumps(msg_obj))

                        except Exception as parse_error:
                             # Fallback for any other errors
                             safe_msg = str(update)
                             msg_obj = {
                                "status": "error",
                                "message": f"[{name}] Error processing update: {safe_msg}"
                             }
                             q.put(json.dumps(msg_obj))
                else:
                    q.put(f'{{"status": "warning", "message": "{name} module not available"}}')
            except Exception as e:
                q.put(f'{{"status": "error", "message": "Error in {name}: {str(e)}"}}')

        threads = []
        for mod, name in modules_to_run:
            t = threading.Thread(target=run_module, args=(mod, name, msg_queue))
            t.start()
            threads.append(t)
            
        # Monitor threads and queue
        alive_threads = len(threads)
        while alive_threads > 0:
            try:
                # Wait for message with timeout
                msg = msg_queue.get(timeout=0.1)
                
                # Check for file updates to accumulate
                try:
                    data = json.loads(msg)
                    if 'files_update' in data:
                        accumulated_files.update(data['files_update'])
                        del data['files_update'] # Remove before yielding
                        # Include all current files in the update
                        data['files'] = accumulated_files
                        msg = json.dumps(data)
                except:
                    pass
                    
                yield msg
            except queue.Empty:
                # Check threads status
                alive_threads = sum(1 for t in threads if t.is_alive())
        
        # Drain remaining messages
        while not msg_queue.empty():
            msg = msg_queue.get()
            try:
                data = json.loads(msg)
                if 'files_update' in data:
                    accumulated_files.update(data['files_update'])
                    del data['files_update']
                    msg = json.dumps(data)
            except:
                pass
            yield msg
            
        yield json.dumps({
            "status": "complete", 
            "message": "Concurrent extraction completed for Steps 2, 3, 4, 5", 
            "files": accumulated_files
        })
        return

    if step == 9: # Combined Flow (Step 1 + Step 8)
        yield f'{{"status": "progress", "message": "Starting Automated Combined Flow for {university_name}..."}}'
        
        # Phase 1: Step 1 (Extract List) with Retry
        max_retries = 5
        grad_count = 0
        undergrad_count = 0
        accumulated_files = {}

        for attempt in range(1, max_retries + 1):
            yield f'{{"status": "progress", "message": "--- Step 1: Program Extraction Attempt {attempt}/{max_retries} ---"}}'
            
            # Run Grad Step 1
            yield f'{{"status": "progress", "message": "Extracting Graduate programs..."}}'
            try:
                for update in grad_step1.run(university_name):
                    try:
                        data = json.loads(update)
                        if data.get('status') == 'complete':
                            if 'files' in data:
                                accumulated_files.update(data['files'])
                            msg = data.get('message', '')
                            # Extract count
                            match = re.search(r'Found (\d+) graduate', msg)
                            if match:
                                grad_count = int(match.group(1))
                            yield json.dumps({"status": "progress", "message": f"[Grad] {msg}", "files": accumulated_files})
                        else:
                            yield update
                    except:
                        yield update
            except Exception as e:
                yield f'{{"status": "error", "message": "Error in Grad Step 1: {str(e)}"}}'

            # Run Undergrad Step 1
            yield f'{{"status": "progress", "message": "Extracting Undergraduate programs..."}}'
            try:
                for update in undergrad_step1.run(university_name):
                    try:
                        data = json.loads(update)
                        if data.get('status') == 'complete':
                            if 'files' in data:
                                accumulated_files.update(data['files'])
                            msg = data.get('message', '')
                            # Extract count
                            match = re.search(r'Found (\d+) undergraduate', msg)
                            if match:
                                undergrad_count = int(match.group(1))
                            yield json.dumps({"status": "progress", "message": f"[Undergrad] {msg}", "files": accumulated_files})
                        else:
                            yield update
                    except:
                        yield update
            except Exception as e:
                yield f'{{"status": "error", "message": "Error in Undergrad Step 1: {str(e)}"}}'

            if grad_count > 0 and undergrad_count > 0:
                yield f'{{"status": "progress", "message": "Success! Found {grad_count} Grad and {undergrad_count} Undergrad programs. Proceeding to enrichment."}}'
                break
            elif attempt < max_retries:
                missing = []
                if grad_count == 0: missing.append("Graduate")
                if undergrad_count == 0: missing.append("Undergraduate")
                yield f'{{"status": "warning", "message": "Missing {', '.join(missing)} programs list on attempt {attempt}. Retrying Step 1..."}}'
                time.sleep(2) 
            else:
                yield f'{{"status": "error", "message": "Max retries reached. Could not find both Grad and Undergrad lists. (Grad: {grad_count}, Undergrad: {undergrad_count}). Automation stopped."}}'
                return

        # Phase 2: Step 8 (Parallel)
        # We only reach here if both counts > 0 due to the 'return' in the else block above
        yield f'{{"status": "progress", "message": "--- Transitioning to Parallel Extraction (Steps 2-5) ---"}}'
            # Reuse Step 8 logic by calling recursively or just inline
            # For simplicity, I'll yield from process_programs_extraction(university_name, 8)
            # But we need to handle the 'complete' status of Step 8 carefully
        # Phase 2 Step 8 logic
        step8_gen = process_programs_extraction(university_name, 8)
        for update in step8_gen:
            try:
                data = json.loads(update)
                if data.get('status') == 'complete':
                    if 'files' in data:
                        accumulated_files.update(data['files'])
                    # Don't yield 'complete' yet
                    yield f'{{"status": "progress", "message": "Parallel extraction completed. Finalizing..."}}'
                else:
                    yield update
            except:
                yield update

        yield json.dumps({
            "status": "complete", 
            "message": "Automated combined flow completed successfully.", 
            "files": accumulated_files
        })
        return

    if step not in steps_map:
        yield f'{{"status": "error", "message": "Unknown step: {step}"}}'
        return

    # Track files from both executions
    accumulated_files = {}

    grad_module, undergrad_module = steps_map[step]
    
    yield f'{{"status": "progress", "message": "Starting Step {step} for {university_name}..."}}'

    # Execute Graduate Script
    yield f'{{"status": "progress", "message": "--- Processing Graduate Programs ---"}}'
    try:
        if step == 6: # Standardize step
            if hasattr(grad_module, 'run'):
                for update in grad_module.run(university_name):
                    try:
                        data = json.loads(update)
                        if data.get('status') == 'complete':
                            if 'files' in data:
                                accumulated_files.update(data['files'])
                            data['status'] = 'progress'
                            data['message'] = "[Grad] " + data.get('message', '')
                            yield json.dumps(data)
                        else:
                            yield update
                    except:
                        yield update
        elif hasattr(grad_module, 'run'):
            for update in grad_module.run(university_name):
                try:
                    data = json.loads(update)
                    if data.get('status') == 'complete':
                        if 'files' in data:
                            accumulated_files.update(data['files'])
                        data['status'] = 'progress'
                        data['message'] = "[Grad] " + data.get('message', '')
                        yield json.dumps(data)
                    else:
                        yield update
                except:
                    yield update
        else:
            yield f'{{"status": "warning", "message": "Graduate script for Step {step} does not have a run function"}}'
    except Exception as e:
        yield f'{{"status": "error", "message": "Error in Graduate Step {step}: {str(e)}"}}'
        # Continue to Undergrad even if Grad fails to ensure robustness? 
        # Yes, let's try Undergrad.

    # Execute Undergraduate Script
    if undergrad_module:
        yield f'{{"status": "progress", "message": "--- Processing Undergraduate Programs ---"}}'
        try:
             if step == 6: # Standardize step
                 if hasattr(undergrad_module, 'run'):
                    for update in undergrad_module.run(university_name):
                        try:
                            data = json.loads(update)
                            if data.get('status') == 'complete':
                                if 'files' in data:
                                    accumulated_files.update(data['files'])
                                data['status'] = 'progress'
                                data['message'] = "[Undergrad] " + data.get('message', '')
                                yield json.dumps(data)
                            else:
                                yield update
                        except:
                            yield update
             elif hasattr(undergrad_module, 'run'):
                for update in undergrad_module.run(university_name):
                    try:
                        data = json.loads(update)
                        if data.get('status') == 'complete':
                            if 'files' in data:
                                accumulated_files.update(data['files'])
                            data['status'] = 'progress'
                            data['message'] = "[Undergrad] " + data.get('message', '')
                            yield json.dumps(data)
                        else:
                            yield update
                    except:
                         yield update
             else:
                yield f'{{"status": "warning", "message": "Undergraduate script for Step {step} does not have a run function"}}'
        except Exception as e:
            yield f'{{"status": "error", "message": "Error in Undergraduate Step {step}: {str(e)}"}}'
    else:
         yield f'{{"status": "warning", "message": "Undergraduate module for Step {step} not found or disabled."}}'

    # If this was Step 6, we also auto-run the final merge (Step 7 logic)
    if step == 6:
        yield f'{{"status": "progress", "message": "--- Running Final Merge ---"}}'
        try:
            for update in merge_all.run(university_name):
                try:
                    data = json.loads(update)
                    if data.get('status') == 'complete':
                        if 'files' in data:
                            accumulated_files.update(data['files'])
                        data['status'] = 'progress'
                        data['message'] = "[Merge] " + data.get('message', '')
                        yield json.dumps(data)
                    else:
                        yield update
                except:
                    yield update
        except Exception as e:
            yield f'{{"status": "error", "message": "Error in Final Merge: {str(e)}"}}'

    # Final Complete Message
    yield json.dumps({
        "status": "complete", 
        "message": f"Step {step} completed for both Program levels", 
        "files": accumulated_files
    })



# ============================================================================
# SEQUENTIAL ORCHESTRATION - FROM Uniscraper.py
# ============================================================================



# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'Institution'))
sys.path.insert(0, os.path.join(current_dir, 'Departments'))
sys.path.insert(0, os.path.join(current_dir, 'Programs'))

# Import extraction functions from existing modules


def run_sequential_extraction(university_name):
    """
    Run complete sequential extraction for a university.
    
    This function orchestrates the extraction of:
    1. Institution data (all university-level information)
    2. Department data (admissions offices and contacts)
    3. Programs data (graduate and undergraduate programs with full details)
    
    Args:
        university_name (str): The name of the university to extract data for
        
    Yields:
        str: JSON-formatted status updates throughout the extraction process.
             Each update contains:
             - status: 'progress', 'complete', or 'error'
             - message: Human-readable status message
             - files: Dictionary of output files (when available)
             
    Example:
        >>> for update in run_sequential_extraction("SUNY Brockport"):
        ...     data = json.loads(update)
        ...     print(data['message'])
    """
    
    yield json.dumps({
        "status": "progress",
        "message": f"Starting sequential extraction for {university_name}...",
        "phase": "initialization"
    })
    
    # Track all output files across all phases
    all_files = {}
    
    # ========================================================================
    # PHASE 1: INSTITUTION EXTRACTION
    # ========================================================================
    yield json.dumps({
        "status": "progress",
        "message": "[PHASE 1/3] Starting Institution Extraction...",
        "phase": "institution"
    })
    
    try:
        for update in process_institution_extraction(university_name):
            try:
                # Parse the update to add phase information
                data = json.loads(update)
                data['phase'] = 'institution'
                
                # Collect files if this is a completion update
                if data.get('status') == 'complete' and 'files' in data:
                    all_files.update(data['files'])
                    # Change to progress so we can continue
                    data['status'] = 'progress'
                    data['message'] = f"[PHASE 1/3] Institution extraction completed. Files saved."
                
                yield json.dumps(data)
            except json.JSONDecodeError:
                # If update is not JSON, wrap it
                yield json.dumps({
                    "status": "progress",
                    "message": f"[PHASE 1/3] {update}",
                    "phase": "institution"
                })
    except Exception as e:
        yield json.dumps({
            "status": "error",
            "message": f"[PHASE 1/3] Error in institution extraction: {str(e)}",
            "phase": "institution",
            "error": str(e)
        })
        # Continue to next phase despite error
    
    yield json.dumps({
        "status": "progress",
        "message": "[PHASE 1/3] Institution extraction completed.",
        "phase": "institution"
    })
    
    # ========================================================================
    # PHASE 2: DEPARTMENT EXTRACTION
    # ========================================================================
    yield json.dumps({
        "status": "progress",
        "message": "[PHASE 2/3] Starting Department Extraction...",
        "phase": "department"
    })
    
    try:
        for update in process_department_extraction(university_name):
            try:
                # Parse the update to add phase information
                data = json.loads(update)
                data['phase'] = 'department'
                
                # Collect files if this is a completion update
                if data.get('status') == 'complete' and 'files' in data:
                    all_files.update(data['files'])
                    # Change to progress so we can continue
                    data['status'] = 'progress'
                    data['message'] = f"[PHASE 2/3] Department extraction completed. Files saved."
                
                yield json.dumps(data)
            except json.JSONDecodeError:
                # If update is not JSON, wrap it
                yield json.dumps({
                    "status": "progress",
                    "message": f"[PHASE 2/3] {update}",
                    "phase": "department"
                })
    except Exception as e:
        yield json.dumps({
            "status": "error",
            "message": f"[PHASE 2/3] Error in department extraction: {str(e)}",
            "phase": "department",
            "error": str(e)
        })
        # Continue to next phase despite error
    
    yield json.dumps({
        "status": "progress",
        "message": "[PHASE 2/3] Department extraction completed.",
        "phase": "department"
    })
    
    # ========================================================================
    # PHASE 3: PROGRAMS EXTRACTION (Graduate + Undergraduate)
    # ========================================================================
    yield json.dumps({
        "status": "progress",
        "message": "[PHASE 3/3] Starting Programs Extraction...",
        "phase": "programs"
    })
    
    try:
        # Step 9 runs the automated combined flow:
        # - Step 1 (extract program lists) with retry
        # - Steps 2-5 in parallel (extra fields, test scores, requirements, financial)
        for update in process_programs_extraction(university_name, step=9):
            try:
                # Parse the update to add phase information
                data = json.loads(update)
                data['phase'] = 'programs'
                
                # Collect files if present
                if 'files' in data:
                    all_files.update(data['files'])
                
                # Modify complete status to progress since we want to finalize
                if data.get('status') == 'complete':
                    data['status'] = 'progress'
                    data['message'] = f"[PHASE 3/3] Programs extraction completed."
                
                yield json.dumps(data)
            except json.JSONDecodeError:
                # If update is not JSON, wrap it
                yield json.dumps({
                    "status": "progress",
                    "message": f"[PHASE 3/3] {update}",
                    "phase": "programs"
                })
    except Exception as e:
        yield json.dumps({
            "status": "error",
            "message": f"[PHASE 3/3] Error in programs extraction: {str(e)}",
            "phase": "programs",
            "error": str(e)
        })
    
    yield json.dumps({
        "status": "progress",
        "message": "[PHASE 3/3] Programs extraction completed.",
        "phase": "programs"
    })
    
    # ========================================================================
    # FINAL COMPLETION
    # ========================================================================
    yield json.dumps({
        "status": "complete",
        "message": f"Successfully completed all extraction phases for {university_name}!",
        "phase": "complete",
        "files": all_files,
        "summary": {
            "university": university_name,
            "phases_completed": ["institution", "department", "programs"],
            "total_files": len(all_files)
        }
    })


def main():
    """
    Command-line interface for the sequential scraper.
    
    Usage:
        python Uniscraper.py "University Name"
    """
    if len(sys.argv) < 2:
        print("Usage: python Uniscraper.py \"University Name\"")
        print("Example: python Uniscraper.py \"SUNY Brockport\"")
        sys.exit(1)
    
    university_name = sys.argv[1]
    
    print(f"\n{'='*80}")
    print(f"Sequential University Data Extraction")
    print(f"University: {university_name}")
    print(f"{'='*80}\n")
    
    files_collected = {}
    
    for update_json in run_sequential_extraction(university_name):
        try:
            update = json.loads(update_json)
            
            # Print status messages
            status = update.get('status', 'unknown')
            message = update.get('message', '')
            phase = update.get('phase', '')
            
            # Color-code the output
            if status == 'error':
                print(f"❌ ERROR: {message}")
            elif status == 'complete':
                print(f"✅ {message}")
            elif status == 'progress':
                print(f"⏳ {message}")
            else:
                print(f"ℹ️  {message}")
            
            # Collect files
            if 'files' in update:
                files_collected.update(update['files'])
                
        except json.JSONDecodeError:
            print(f"ℹ️  {update_json}")
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"Extraction Complete!")
    print(f"{'='*80}")
    if files_collected:
        print(f"\nOutput files ({len(files_collected)}):")
        for file_type, file_path in files_collected.items():
            print(f"  - {file_type}: {file_path}")
    else:
        print("\nNo output files were generated.")
    print()





if __name__ == "__main__":
    main()
