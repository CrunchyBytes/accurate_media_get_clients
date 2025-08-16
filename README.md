# Get clients
Python script that obtains client information from Apollo's database, which is then exported to a CSV file. Said file may be later uploaded manually to a CRM of your choice (i.e., Upnify, Salesforce).

## Prerequisites
1. Have an [Apollo](https://www.apollo.io/) and [Pipedream](https://pipedream.com/) account, with API Keys configured for each and a Pipedream Source ID (where events are registered).
2. Install [Git](https://git-scm.com/downloads) in your local machine.
3. Clone the Github repository to your local machine using the following command from your terminal:
`git clone https://github.com/CrunchyBytes/accurate_media_get_clients.git`
4. Rename file **.env.sample** to just **.env**
5. Fill in Apollo's and Pipedream's API Keys and Source ID in **.env**:
- _APOLLO_API_ORG_AND_PEOPLE_SEARCH_KEY_ = Apollo's "Organization Search" and "People Search" API Key
- _APOLLO_API_PEOPLE_ENRICHMENT_SEARCH_KEY_ = Apollo's "People Enrichment" and "Bulk People Enrichment" API Key
- _PIPEDREAM_API_KEY_ = Pipedream's API Key
- _PIPEDREAM_SOURCE_ID_ = Pipedream's Source ID
6. Execute the following commands from the terminal:
`python -m pip install requests`
to install the [Python "requests" library](https://docs.python-requests.org/en/latest/index.html), which is used to:
- Send requests to Apollo's API endpoints to query its organizations, contacts, and to enrich said contacts (i.e., unlock their e-mails and request their phone numbers)
- Send requests to Pipedream's API to retrieve said contacts' phone numbers

## How to run the script
From the directory in which you've cloned the Github repository, execute the following command from the terminal to execute the script:
`py main.py <n>`
, where _\<n\>_ is the number of iterations you would like to run the script.


Please note that at least 4 files will be created when executing the script for the first time:
1. **apollo_contacts_master.csv:** Contains every retrieved contact, in order to guarantee that only new contacts will be saved with each execution. One file; **updated per execution**.
2. **apollo_contacts_\<timestamp\>.csv:** Contains the retrieved contacts for a specific execution, which is denoted by a timestamp in the following format: YYYYMMDD_HHmmss. **One file generated per execution**.
3. **cached_organizations.json:** A list of the organizations whose employees contact information is queried. Important in order to avoid querying organizations with each script execution, and thus reduce Credit spending.
4. **pagination_state.json:** A registry of the current Organization and People page to query, in order to ensure that the same information will not be queried indefinitely.


## Resources
### Apollo
- [Apollo's Home Page](https://www.apollo.io/)
- [Apollo's API Keys:](https://developer.apollo.io/keys/) To view an account's API Keys and remaining Credits
#### Apollo API Documentation
- [Organization Search](https://docs.apollo.io/reference/organization-search)
- [People Search](https://docs.apollo.io/reference/people-search)
- [Bulk People Enrichment](https://docs.apollo.io/reference/bulk-people-enrichment)
### Pipedream
- [Pipedream Sources:](https://pipedream.com/sources/) Where contacts' retrieved phone numbers are loggeed