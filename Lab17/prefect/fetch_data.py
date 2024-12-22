import requests

url = 'https://api.spacexdata.com/v4/launches'
response = requests.get(url)
data = response.json()

# Перевірка даних
for launch in data:
    if "DemoSat" in launch.get("name", ""):
        print(launch)
