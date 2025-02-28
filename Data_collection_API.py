
'''
### Complete the Data Collection API Lab ###

Objective: To predict if the Falcon 9 rocket lands successfully based on various features.
Data is collected from the SpaceX API to analyze Falcon 9 rocket launches.
'''

# Import necessary libraries
import requests 
import pandas as pd 
import numpy as np 
import datetime

# Print all columns of a data frame
pd.set_option('display.max_columns', None)
# Print all of the data in a feature
pd.set_option('display.max_colwidth', None)

# FUNCTIONS FOR DATA COLLECTION
# Function that fetches the names of the rocket boosters for each launch and appends them to a global list.
def getBoosterVersion(data):
    for x in data['rocket']: # Iterate through the list of rocket IDs in the 'rocket' field
        if x:
            # Fetch rocket details from SpaceX API
            response = requests.get("https://api.spacexdata.com/v4/rockets/"+str(x)).json()
             # Append the Booster Version (name) to the global list
            BoosterVersion.append(response["name"])
                  
# Retrieves the launch site details (name, longitude, latitude) for each launch and appends them to respective global lists.
def getLaunchSite(data):
    for x in data["launchpad"]:
        if x: 
            response = requests.get("https://api.spacexdata.com/v4/launchpads/"+str(x)).json()
            Longitude.append(response["longitude"])
            Latitude.append(response["latitude"])
            LaunchSite.append(response["name"])
                
# Collects payload data (mass and orbit) for each launch and appends them to global lists.
def getPayloadData(data):
    for load in data['payloads']:
       if load:
        response = requests.get("https://api.spacexdata.com/v4/payloads/"+load).json()
        PayloadMass.append(response['mass_kg'])
        Orbit.append(response['orbit'])      

# Gathers core data (landing success, reuse count, etc.) for each launch and appends them to global lists.
def getCoreData(data):
    for core in data['cores']:
            if core['core'] != None:
                response = requests.get("https://api.spacexdata.com/v4/cores/"+core['core']).json()
                Block.append(response['block'])
                ReusedCount.append(response['reuse_count'])
                Serial.append(response['serial'])
            else:
                Block.append(None)
                ReusedCount.append(None)
                Serial.append(None)
            Outcome.append(str(core['landing_success'])+' '+str(core['landing_type']))
            Flights.append(core['flight'])
            GridFins.append(core['gridfins'])
            Reused.append(core['reused'])
            Legs.append(core['legs'])
            LandingPad.append(core['landpad'])
            

# DATA RETRIEVAL
# The script makes a request to the SpaceX API to get past launch data.
spacex_url="https://api.spacexdata.com/v4/launches/past"
response = requests.get(spacex_url)
#print(response.content)

# Decode the response as a Json using .json() and turn it into a Pandas df using .json_normalize
data = response.json()

# Normalizes the JSON response into a DataFrame and selects relevant columns.
data = pd.json_normalize(data)
#print(data.head())


# Select a subset from the dataframe and keep only some the features and the flight number, and date_utc.
data = data[['rocket', 'payloads', 'launchpad', 'cores', 'flight_number', 'date_utc']]

# Remove rows with multiple cores because those are falcon rockets with 2 extra rocket boosters and rows that have multiple payloads in a single rocket.
data = data[data['cores'].map(len)==1]
data = data[data['payloads'].map(len)==1]
print(data.head())

# Filter out launches with multiple cores or payloads, ensuring that only single-core, single-payload launches are analyzed.
data['cores'] = data['cores'].map(lambda x : x[0])
data['payloads'] = data['payloads'].map(lambda x : x[0])

# Convert the date_utc to a datetime datatype and extract the date leaving the time
data['date'] = pd.to_datetime(data['date_utc']).dt.date

# Using the date we will restrict the dates of the launches
data = data[data['date'] <= datetime.date(2020, 11, 13)]

# DATA COLLECTION EXECUTION
# The script initializes global lists to store collected data.
# It calls the previously defined functions to populate these lists with data from the API

BoosterVersion = []
PayloadMass = []
Orbit = []
LaunchSite = []
Outcome = []
Flights = []
GridFins = []
Reused = []
Legs = []
LandingPad = []
Block = []
ReusedCount = []
Serial = []
Longitude = []
Latitude = []

# A dictionary is created to combine all the collected data into a structured format.
getBoosterVersion(data)
BoosterVersion[0:5]

# Apply the rest of the functions here
getLaunchSite(data)
getPayloadData(data)
getCoreData(data)

# DATAFRAME CONSTRUCTION
# A pandas DataFrame (launch_df) is created from this dictionary, 
# which can be used for further analysis.
launch_dict = {'FlightNumber': list(data['flight_number']),
'Date': list(data['date']),
'BoosterVersion':BoosterVersion,
'PayloadMass':PayloadMass,
'Orbit':Orbit,
'LaunchSite':LaunchSite,
'Outcome':Outcome,
'Flights':Flights,
'GridFins':GridFins,
'Reused':Reused,
'Legs':Legs,
'LandingPad':LandingPad,
'Block':Block,
'ReusedCount':ReusedCount,
'Serial':Serial,
'Longitude': Longitude,
'Latitude': Latitude}

# Create a data frame from the dictionary launch_dict
launch_df = pd.DataFrame(launch_dict)
print(launch_df.head())


# After constructing the launch_df DataFrame
print(launch_df.columns)  # Check if 'BoosterVersion' exists

# Filter for Falcon 9 launches
if 'BoosterVersion' in launch_df.columns:
    falcon9_launches = launch_df[launch_df['BoosterVersion'] != 'Falcon 1']
    print(falcon9_launches.head())  # Display the first few rows of the df
else:
    print("BoosterVersion column does not exist in the DataFrame.")

# Filter for Falcon 9 launches and count launches
data_falcon9 = launch_df[launch_df['BoosterVersion'] != 'Falcon 1']
num_falcon9_launches = len(data_falcon9)
num_falcon9_launches_shape = data_falcon9.shape[0]
# print(num_falcon9_launches, num_falcon9_launches_shape) #90

# Reset the FlightNumber column
data_falcon9.loc[:, 'FlightNumber'] = list(range(1, data_falcon9.shape[0] + 1))
print(data_falcon9.head())

# DATA WRANGLING
# Check the total missing values
data_falcon9.isnull().sum()

missing_count = data_falcon9["LandingPad"].isnull().sum()
print("Missing values for column LandingPad:{}".format(missing_count))

# Calculate the mean value of the PayloadMass column
mean_payload_mass = data_falcon9['PayloadMass'].mean()

# Print the mean value for verification
print(f"Mean PayloadMass: {mean_payload_mass}")

# Replace the np.nan values in the PayloadMass column with the mean value
data_falcon9['PayloadMass'] = data_falcon9['PayloadMass'].replace(np.nan, mean_payload_mass)
print(data_falcon9.head())

# Export to a CSV file for further analysis in part 2.
data_falcon9.to_csv('dataset_part_1.csv', index=False)
df = pd.read_csv('dataset_part_1.csv')
print(df.head())

# Convert the rows to an HTML table and save the file
df.to_html('table1.html', index=False)