import streamlit as st
import pandas as pd
import requests

# Function to process the file
def process_file(file):
    # Assuming the file is a CSV, you can process it with pandas
    df = pd.read_csv(file)
    url = "https://api.fullcontact.com/v3/person.enrich"
    api_key = "gAUBIGSXPTPMzdLaALN4PgW2sWP0Bo7B"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Function to safely get a nested dictionary value
    def safe_get(dictionary, keys, default=None):
        """
        Safely get a nested dictionary value.
        
        :param dictionary: The dictionary to get the value from.
        :param keys: A list of keys representing the nested path.
        :param default: The default value to return if any key is not found.
        :return: The value or the default.
        """
        for key in keys:
            try:
                dictionary = dictionary[key]
            except (KeyError, IndexError, TypeError):
                return default
        return dictionary
    
    # Function to flatten the JSON response
    def write(data):
        details = data.get('details', {})
        df = pd.DataFrame({
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
        return df
    
    # Iterate over the DataFrame and enrich data
    for index, row in df.iterrows():
        data = {
            "email": row['email'],
            "dataFilter": ['professional']
        }
        try:
            response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 200:
            enriched_data = write(response.json())
            for column in enriched_data.columns:
                df.at[index, column] = enriched_data.at[0, column]
        else:
            print(f"Failed to fetch data for index {index}: {response.json()}")
    
    # Save the updated DataFrame to a CSV file if needed
    # df.to_csv('updated_file.csv', index=False)
    
            
    
    # Save the updated DataFrame to a CSV file if needed
    # enriched_df.to_csv('updated_file.csv', index=False)

    return df

def main():
    st.title("VisitorIQ Pro : Profile Enhancement")

    uploaded_file = st.file_uploader("Choose a file", type=["csv"])

    if uploaded_file is not None:
        # Process the file
        processed_df = process_file(uploaded_file)
        
        # Display the processed dataframe
        st.write("Processed Data:")
        st.dataframe(processed_df)

        # Download the processed file
        st.download_button(
            label="Download Processed File",
            data=processed_df.to_csv(index=False).encode('utf-8'),
            file_name='enriched_report.csv',
            mime='text/csv',
        )

if __name__ == "__main__":
    main()
