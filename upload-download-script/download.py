import requests

url = "http://185.96.69.234:8000/parsed?id=4"

payload = {}
headers = {
    'X-API-KEY': '92f023f8b3ea67d455b9398c3f51fc48'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
