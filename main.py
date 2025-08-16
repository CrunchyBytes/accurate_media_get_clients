import requests
import csv
import os
import datetime
from dotenv import load_dotenv
from time import sleep
from json import load, dump

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

# General parameters
ORGANIZATIONS_PER_PAGE = 5 if TEST_MODE else 50     # Despite its request accepting a maximum of 100 organizations, split in half due to querying it twice
CONTACTS_PER_PAGE = 5 if TEST_MODE else 100

# Constants
LOCKED_EMAIL = "email_not_unlocked@domain.com"
WEBHOOK_RESPONSE_TIME = 300

# Files
ORG_CACHE_FILE = "cached_organizations.json"
STATE_FILE = "pagination_state.json"

# Apollo API Endpoints
ORG_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_companies/search"
PEOPLE_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/search"


# Functions
def load_state():
    if not os.path.exists(STATE_FILE):
        return {"organization_page": 1, "people_page": 1}
    with open(STATE_FILE, "r") as f:
        return load(f)

def save_state(state):
    with open(STATE_FILE, "w") as f:
        dump(state, f, indent=2)

def load_cached_organizations():
    if os.path.exists(ORG_CACHE_FILE):
        with open(ORG_CACHE_FILE, "r") as f:
            return load(f)
    return []

def save_cached_organizations(orgs):
    with open(ORG_CACHE_FILE, "w") as f:
        dump(orgs, f, indent=2)

def refresh_cached_organizations(org_page, increase_page):
    companies = companies_revenue_filtered = []

    print("🔎 Retrieving companies...")

    payload = {
        "organization_num_employees_ranges[]": EMPLOYEE_RANGES,
        "organization_locations[]": ORGANIZATION_LOCATIONS,
        "page": org_page,
        "per_page": ORGANIZATIONS_PER_PAGE
    }

    result = safe_post(ORG_SEARCH_URL, payload)
    companies = result.get("organizations", []) + result.get("accounts", [])

    # === Step 1.5: Get Companies with Revenue filter ===
    payload = {
        "organization_locations[]": ORGANIZATION_LOCATIONS,
        "revenue_range[min]": REVENUE_RANGE_MIN,
        "page": org_page,
        "per_page": ORGANIZATIONS_PER_PAGE
    }

    result = safe_post(ORG_SEARCH_URL, payload)
    companies_revenue_filtered = result.get("organizations", []) + result.get("accounts", [])


    companies += companies_revenue_filtered
    print(f"✅ Retrieved {len(companies)} companies.")

    # "organization_id" is the Organization's ID for accounts, whereas "id" is the Organization's ID for organizations
    # "organization_id" does not exist within organizations, but "id" does exist for both organizations and accounts, but refer to different things
    # That's why it's important to first read by "organization_id", in case the current company (o) is actually an account.
    # If not, then (o) must be an organization, hence it's read by "id"
    new_orgs = [{"id": o.get("organization_id", o.get("id")), "name": o.get("name")} for o in companies]
    save_cached_organizations(new_orgs)

    # Update org_page
    next_org_page = org_page + 1 if increase_page else 1
    return new_orgs, next_org_page

def fetch_people(org_ids, people_page):
    print(f"👥 Retrieving contacts... (page {people_page}) from {len(org_ids)} orgs...")

    payload = {
        "person_titles[]": PERSON_TITLES,
        "person_locations[]": ORGANIZATION_LOCATIONS,
        "organization_ids[]": org_ids,
        "page": people_page,
        "per_page": CONTACTS_PER_PAGE
    }

    result = safe_post(PEOPLE_SEARCH_URL, payload)
    return result.get("people", []) + result.get("contacts", [])

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"⏰ Program started running at: {timestamp}!")

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

# Load current state
state = load_state()
people_page = state.get("people_page", 1)
org_page = state.get("organization_page", 1)

cached_orgs = load_cached_organizations()

# === If no orgs cached, fetch them
if not cached_orgs:
    cached_orgs, org_page = refresh_cached_organizations(org_page, False)
    state["organization_page"] = org_page
    state["people_page"] = people_page = 1
    save_state(state)

org_ids = [org["id"] for org in cached_orgs]

# === Step 1: Try current people_page for cached orgs
people = fetch_people(org_ids, people_page)

# === Step 2: If no people, try next people page
if len(people) == 0:
    print("⚠️ No people found. Trying next page...")
    people_page += 1
    people = fetch_people(org_ids, people_page)

# === Step 3: Still no people → refresh orgs and try again
if len(people) == 0:
    print("⚠️ Still no people after next page. Refreshing organizations...")
    cached_orgs, org_page = refresh_cached_organizations(org_page, True)
    save_cached_organizations(cached_orgs)
    people_page = 1
    org_ids = [org["id"] for org in cached_orgs]
    people = fetch_people(org_ids, people_page)

# === Step 4: If this run fetched people, advance the people_page
if len(people) > 0:
    print(f"✅ Retrieved {len(people)} people.")
    state["people_page"] = people_page + 1 if not TEST_MODE else 1
    state["organization_page"] = org_page
    save_state(state)
else:
    print("❌ No people found after all fallbacks.")

# === Step 1: Get Companies ===
contacts_found = []

for person in people:
    email = person.get("email")

    if email:
        email = email.lower()
    
    if email in existing_emails and email != LOCKED_EMAIL:
        continue

    # "person_id" is the Person's ID for contacts, whereas "id" is the Person's ID for people
    # "person_id" does not exist within people, but "id" does exist for both people and contacts, but refer to different things
    # That's why it's important to first read by "person_id", in case the current enriched person (person) is actually a contact.
    # If not, then (person) must be an person, hence it's read by "id"
    id = person.get("person_id", person.get("id"))
    name = person.get("name", "")

    try:
        location = person.get("city", "") + ", " + person.get("state", "") + ", " + person.get("country", "")
    except TypeError as error:
        print(f"⚠️ {name} does not have city, state, or country. Error: {error}")

    try:
        organization =  person.get("organization").get("name", "")
    except AttributeError as error:
        print(f"⚠️ {name} does not have an organization. Error: {error}")
        organization = ""

    try:
        organization_phone = person.get("organization").get("phone", "")
    except AttributeError as error:
        if organization != "":
            print(f"⚠️ {organization} does not have a phone. Error: {error}")
        else:
           print(f"⚠️ {name} does not have a phone. Error: {error}") 
        
        organization_phone = "" 


    contact_data = {
        "Person ID": id,
        "First Name": person.get("first_name", ""),
        "Last Name": person.get("last_name", ""),
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
            try:
                enriched_map = {person.get("id"): person.get("email") for person in enriched_people if person["id"] and person["email"]}
            except (TypeError, AttributeError) as error:
                print(f'❌ An exception occurred when mapping enriched people\'s IDs to e-mails: {error}')
            
            # Update the corresponding contact's Email field with the enriched email
            for contact in chunk:
                pid = contact.get("Person ID")
                if pid in enriched_map and enriched_map[pid]:
                    contact["Email"] = enriched_map[pid]


# === Step 3: Filter contacts with repeated e-mails
# Remove contacts from contacts_found if their Email already exists in existing_emails
try:
    contacts_found = [
        contact for contact in contacts_found
        if contact["Email"] and contact.get("Email").lower() not in existing_emails
    ]
except AttributeError as error:
    print(f'❌ An exception occurred when filtering contacts with repeated e-mails: {error}')

if not contacts_found:
    print("⚠️ No new contacts to save.")
else:
    print(f"✅ Retrieved {len(contacts_found)} new contacts.")


    # === Step 4: Retrieve personal phone numbers ===
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


    print("✅ Script finished.")