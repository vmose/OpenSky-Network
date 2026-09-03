import requests

try:
    response = requests.get(
        "https://opensky-network.org/api/states/all"
    )

    if response.status_code == 200:
        data = response.json()

        print(f"Timestamp: {data['time']}")
        print(f"Number of aircraft: {len(data['states'])}")
    else:
        print(f"Error: Received status code {response.status_code}")

except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
