import requests

url = "http://185.96.69.234:8000/upload"

payload = {}
files = [
    ('files', ('CV_DATA.pdf', open('/home/abdelrahman/Downloads/CV_DATA.pdf', 'rb'), 'application/pdf'))
]
headers = {
    'X-API-KEY': '92f023f8b3ea67d455b9398c3f51fc48'
}

response = requests.request("POST", url, headers=headers, data=payload, files=files)

print(response.text)
