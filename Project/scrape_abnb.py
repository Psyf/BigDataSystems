import urllib.request
import re
import wget

with urllib.request.urlopen('http://insideairbnb.com/get-the-data.html') as response:
   html = response.read().decode("utf-8")
   zip_patterns = re.compile(r'href=\"(.*\.csv\.gz)\"')
   urls = zip_patterns.findall(html)

for url in urls: 
    wget.download(url, url[6:].replace('/', '_'))