import streamlit as st
import pandas as pd
import requests
import streamlit_authenticator as stauth
import yaml
import logging
from yaml.loader import SafeLoader

# Set up logging
logging.basicConfig(level=logging.INFO)

# Function to process the file
def process_file(file):
    df = pd.read_csv(file)
    url = "https://api.fullcontact.com/v3/person.enrich"
    api_key = st.secrets["fullcontact_api_key"]
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    def safe_get(dictionary, keys, default=None):
        for key in keys:
            try:
                dictionary = dictionary[key]
            except (KeyError, IndexError, TypeError):
                return default
        return dictionary

    def write(data):
        details = data.get('details', {})
        enriched_df = pd.DataFrame({
            'Current Organization Job Title': [safe_get(details, ['employment', 0, 'title'])],
            'Current Organization Name': [safe_get(details, ['employment', 0, 'name'])],
            'Current Organization Start Year': [safe_get(details, ['employment', 0, 'start', 'year'])],
            'Current Organization Start Month': [safe_get(details, ['employment', 0, 'start', 'month'])],
            'Current Organization Domain': [safe_get(details, ['employment', 0, 'domain'])],
            'Business Email': [safe_get(details, ['emails', 0, 'value'])],
            'Business Phone': [safe_get(details, ['emails', 0, 'phone'])],
            'Current Organization City': [safe_get(details, ['locations', 0, 'city'])],
            'Current Organization Region': [safe_get(details, ['locations', 0, 'region'])],
            'Current Organization Region Code': [safe_get(details, ['locations', 0, 'regionCode'])],
            'Current Organization Country': [safe_get(details, ['locations', 0, 'country'])]
        })
        return enriched_df

    for index, row in df.iterrows():
        data = {
            "email": row['email'],
            "dataFilter": ['professional']
        }
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Request failed for index {index}: {e}")
            continue

        if response.status_code == 200:
            enriched_data = write(response.json())
            for column in enriched_data.columns:
                df.at[index, column] = enriched_data.at[0, column]
        else:
            logging.error(f"Failed to fetch data for index {index}: {response.json()}")

    return df

def main():
    st.title("VisitorIQ Pro : Profile Enhancement")

    with open("config.yaml") as file:
        config = yaml.load(file, Loader=SafeLoader)

    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['pre-authorized']
    )
    name, authentication_status, username = authenticator.login("Login", "main")

    if authentication_status:
        authenticator.logout("Logout", "main")
        st.write(f'Welcome *{name}*')
        uploaded_file = st.file_uploader("Choose a file", type=["csv"])

        if uploaded_file is not None:
            processed_df = process_file(uploaded_file)
            st.write("Processed Data:")
            st.dataframe(processed_df)

            st.download_button(
                label="Download Processed File",
                data=processed_df.to_csv(index=False).encode('utf-8'),
                file_name='enriched_report.csv',
                mime='text/csv'
            )
    elif authentication_status is False:
        st.error('Username/password is incorrect')
    elif authentication_status is None:
        st.warning('Please enter your username and password')

if __name__ == "__main__":
    main()
