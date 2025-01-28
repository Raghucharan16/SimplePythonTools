import requests
import threading
from bs4 import BeautifulSoup

urls = ['url1','url2','url3']

threads=[]

def fetch_content():
  response = requests.get(url)
  soup = BeautifulSoup(response.content,'html.parser')
  print(soup.text)


for url in urls:
  thread=threading.Thread(fetch_content, args=(url,))
  threads.append(thread)
  thread.start()

for thread in threads:
  thread.join()

print("data Fetched simultaneously")
