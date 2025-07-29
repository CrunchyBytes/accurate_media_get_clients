import requests
import csv
import os
import datetime
from dotenv import load_dotenv
import shutil
from time import sleep

# === Load API Keys from .env ===
load_dotenv()
APOLLO_API_ORG_AND_PEOPLE_SEARCH_KEY = os.getenv("APOLLO_API_ORG_AND_PEOPLE_SEARCH_KEY")
APOLLO_API_PEOPLE_ENRICHMENT_SEARCH_KEY = os.getenv("APOLLO_API_PEOPLE_ENRICHMENT_SEARCH_KEY")

PIPEDREAM_API_KEY = os.getenv("PIPEDREAM_API_KEY")
PIPEDREAM_SOURCE_ID = os.getenv("PIPEDREAM_SOURCE_ID")
PIPEDREAM_WEBHOOK_URL = os.getenv("PIPEDREAM_WEBHOOK_URL")

UPNIFY_API_KEY = os.getenv("UPNIFY_API_KEY", "")
UPNIFY_API_TOKEN = os.getenv("UPNIFY_API_TOKEN", "")

# === Configuration ===
TEST_MODE = True  # Set to False in production

# "Organization Search" endpoint parameters
ORGANIZATION_LOCATIONS = ["mexico, mexico city", "guadalajara", "monterrey", "querétaro", "puebla"]
EMPLOYEE_RANGES = ["500,9999999"]
REVENUE_RANGE_MIN = 50000000

# "People Search" endpoint parameters 
PERSON_TITLES = ["sales", "marketing", "media", "communication", "advertising", "advertisement", "branding", "brands"]
PERSON_SENIORITIES = ["owner", "founder", "c_suite", "partner", "vp", "head", "director", "manager", "senior", "entry", "intern"]

# General parameters
ORGANIZATIONS_PER_PAGE = 5 if TEST_MODE else 100
CONTACTS_PER_PAGE = 5 if TEST_MODE else 100

# Constants
LOCKED_EMAIL = "email_not_unlocked@domain.com"
WEBHOOK_RESPONSE_TIME = 180


timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"apollo_contacts_{timestamp}.csv"
master_csv = "apollo_contacts_master.csv"

headers = {
    "accept": "application/json",
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "X-Api-Key": APOLLO_API_ORG_AND_PEOPLE_SEARCH_KEY
}

# === Load previous emails for deduplication ===
existing_emails = set()
if os.path.exists(master_csv):
    with open(master_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        existing_emails = set(row['Email'].lower() for row in reader)

# === Helper to make safe POST requests ===
def safe_post(url, payload={}, json={}):
    try:
        response = requests.post(url, headers=headers, params=payload, json=json)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[Error] {e}")
        return {}

# === Step 1: Get Companies ===
print("🔎 Retrieving companies...")

# === Step 1.25: Get Companies with Number of Employees filter === 
payload = {
    "organization_num_employees_ranges[]": EMPLOYEE_RANGES,
    "organization_locations[]": ORGANIZATION_LOCATIONS,
    "per_page": ORGANIZATIONS_PER_PAGE
}

result = safe_post("https://api.apollo.io/api/v1/mixed_companies/search", payload)

if result:
    companies = result.get("accounts", [])

# === Step 1.5: Get Companies with Revenue filter === 
payload = {
    "organization_locations[]": ORGANIZATION_LOCATIONS,
    "revenue_range[min]": REVENUE_RANGE_MIN,
    "per_page": ORGANIZATIONS_PER_PAGE
}

result = safe_post("https://api.apollo.io/api/v1/mixed_companies/search", payload)

if result:
    companies_revenue_filtered = result.get("accounts", [])

companies += companies_revenue_filtered
organization_ids = []

for company in companies:
    organization_id = company.get("organization_id", "")
    organization_ids.append(organization_id)

print(f"✅ Retrieved {len(organization_ids)} companies.")

# === Step 2: Get Contacts per Company ===
print("👥 Retrieving contacts...")
contacts_found = []

payload = {
    "person_titles[]": PERSON_TITLES,
    "person_locations[]": ORGANIZATION_LOCATIONS,
    "person_seniorities[]": PERSON_SENIORITIES,
    "organization_locations[]": ORGANIZATION_LOCATIONS,
    "organization_ids[]": organization_ids,
    "per_page": CONTACTS_PER_PAGE
}

result = safe_post("https://api.apollo.io/api/v1/mixed_people/search", payload)
people = result.get("people", [])

for person in people:
    email = person.get("email", "").lower()
    
    if email in existing_emails and email != LOCKED_EMAIL:
        continue

    id = person.get("id", "")
    name = person.get("first_name", "") + " " + person.get("last_name", "")
    location = person.get("city", "") + ", " + person.get("state", "") + ", " + person.get("country", "")

    try:
        organization =  person.get("organization").get("name", "")
    except AttributeError as error:
        print(f"⚠️ {name} does not have an organization. Error: {error}")
        organization = ""

    try:
        organization_phone = person.get("account").get("phone", "")
    except AttributeError as error:
        if organization != "":
            print(f"⚠️ {organization} does not have a phone. Error: {error}")
        else:
           print(f"⚠️ {name} does not have a phone. Error: {error}") 
        
        organization_phone = "" 


    contact_data = {
        "Person ID": id,
        "Name": name,
        "WhatsApp": "",
        "LinkedIn": person.get("linkedin_url", ""),
        "Organization ID": person.get("organization_id", ""),
        "Organization": organization,
        "Title": person.get("title", ""),
        "Email": email,
        "Organization Phone": organization_phone,
        "Location": location
    }

    contacts_found.append(contact_data)
    existing_emails.add(email)

print(f"✅ Retrieved {len(contacts_found)} new contacts.")

# === Step 2.5: Enrich e-mails ===
if contacts_found:
    # Redefine header
    headers = {
        "accept": "application/json",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "X-Api-Key": APOLLO_API_PEOPLE_ENRICHMENT_SEARCH_KEY
    }

    # Chunk contacts_found into groups of 10
    person_id_chunks = [contacts_found[i:i + 10] for i in range(0, len(contacts_found), 10)]

    for chunk in person_id_chunks:
        # Collect person IDs from this chunk
        person_ids = [contact["Person ID"] for contact in chunk if "Person ID" in contact and contact["Person ID"]]

        if not person_ids:
            continue

        # Construct the payload in Apollo's expected format
        payload = {
            "reveal_personal_emails": "true",
            "reveal_phone_number": "true",
            "webhook_url": PIPEDREAM_WEBHOOK_URL
        }

        json = {
            "details": [{"id": pid} for pid in person_ids]
        }

        # Send bulk enrichment request
        response = safe_post("https://api.apollo.io/api/v1/people/bulk_match", payload, json)
        
        if response:
            # Extract email info from response
            enriched_people = response.get("matches", [])

            # Map each person ID to their real email
            enriched_map = {person.get("id"): person.get("email") for person in enriched_people}
            
            # Update the corresponding contact's Email field with the enriched email
            for contact in chunk:
                pid = contact.get("Person ID")
                if pid in enriched_map and enriched_map[pid]:
                    contact["Email"] = enriched_map[pid]

# === Step 3: Save CSV ===
# Sleep while awaiting webhook response
print(f"📞 Waiting {WEBHOOK_RESPONSE_TIME} seconds to retrieve phone numbers...")
sleep(WEBHOOK_RESPONSE_TIME)

# Define request headers
headers = {
    "Authorization": f"Bearer {PIPEDREAM_API_KEY}"
}

# Poll webhook events from Pipedream
response = requests.get(
    f"https://api.pipedream.com/v1/sources/{PIPEDREAM_SOURCE_ID}/events",
    headers=headers
)

if response.status_code != 200:
    print("❌ Failed to retrieve events.")
    print(f"Status code: {response.status_code}")
    exit()

events = response.json().get("data", [])

id_to_phone = {}
for event in events:
    try:
        payload_body = event.get("e").get("body", {})
        
        if payload_body:
            payload_body_status = payload_body.get("status")

            if payload_body_status == "success":
                people = payload_body.get("people")

                for person in people:
                    person_status = person.get("status")

                    if person_status == "success":
                        person_id = person.get("id")
                        person_phone_numbers = person.get("phone_numbers", [])

                        raw_number = ""
                        for person_phone_number in person_phone_numbers:
                            if raw_number:
                                raw_number += ", " + person_phone_number.get("raw_number")
                            else:
                                raw_number = person_phone_number.get("raw_number")

                        if person_id and raw_number:
                            id_to_phone[person_id] = raw_number
            else:
                print(f"⚠️ Webhook's status was {payload_body_status}; not 'success'")
        else:
            print("⚠️ Webhook's body is empty")
    except KeyError as e:
        print(f"❌ Failed to retrieve webhook's events: {e}")

if id_to_phone:
    print(f"✅ Retrieved {len(id_to_phone)} phone numbers.")
    for id_phone in id_to_phone:
        print(f"{id_phone} : {id_to_phone[id_phone]}")
else:
    print(f"⚠️ Could not retrieve phone numbers")


# Fill in contacts' "WhatsApp" field
for contact in contacts_found:
    pid = contact.get("Person ID")
    
    if pid in id_to_phone:
        phone_number = id_to_phone[pid]

        if "ext" in phone_number:
            organization_phone = contact["Organization Phone"]

            if organization_phone:
                contact["Organization Phone"] += ", " + phone_number
            else:
                contact["Organization Phone"] = phone_number
        else:
            contact["WhatsApp"] = phone_number


# === Step 4: Save CSV ===
print(f"💾 Saving results to {csv_filename}...")
if contacts_found:
    with open(csv_filename, "w", newline="", encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=contacts_found[0].keys())
        writer.writeheader()
        writer.writerows(contacts_found)
    
    try:
        file_exists = os.path.exists(master_csv)

        with open(master_csv, "a", newline="", encoding="utf-8") as master_file:
            writer = csv.DictWriter(master_file, fieldnames=contacts_found[0].keys())

            if not file_exists:
                writer.writeheader()  # solo si no existe

            writer.writerows(contacts_found)

        print(f"✅ Contacts appended to {master_csv}")
    except PermissionError as permission_error:
        print(f"⚠️ PermissionError when attempting to copy {csv_filename} to {master_csv}: {permission_error}.")
        print(f"⚠️ Please ensure that {master_csv} isn't open")
else:
    print("⚠️ No new contacts to save.")

# === Step 4: Optional Upload to Upnify CRM ===
if UPNIFY_API_KEY and UPNIFY_API_TOKEN and contacts_found:
    print("☁️ Uploading to Upnify CRM...")
    upnify_url = "https://api.upnify.com/v1/contacts"
    upnify_headers = {
        "X-API-KEY": UPNIFY_API_KEY,
        "X-API-TOKEN": UPNIFY_API_TOKEN,
        "Content-Type": "application/json"
    }

    for contact in contacts_found:
        payload = {
            "name": f"{contact['First Name']} {contact['Last Name']}",
            "email": contact["Email"],
            "phone": contact["Phone"],
            "company": contact["Organization"],
            "title": contact["Title"],
            "location": contact["Location"]
        }
        try:
            response = requests.post(upnify_url, headers=headers, params=payload, json={})
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"[Upnify Error] {e}")

    print("✅ Upload complete.")
else:
    print("ℹ️ Upnify upload skipped (no credentials or no contacts).")

print("✅ Script finished.")
