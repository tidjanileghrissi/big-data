import requests
from bs4 import BeautifulSoup
import pandas as pd

base_url = "https://www.gutenberg.org/ebooks/search/?query=&submit_search=Go%21&page="
books = []
max_pages = 500

for page in range(1, max_pages + 1):
    url = base_url + str(page)
    try:

        response.raise_for_status() 
    
        soup = BeautifulSoup(response.text, "html.parser")
        book_items = soup.find_all("li", class_="booklink")

        for book in book_items:
            try:
                title = book.find("span", class_="title").text.strip()
                author = book.find("span", class_="subtitle").text.strip() 
                book_link = "https://www.gutenberg.org" + book.find("a")["href"]

                books.append({
                    "Title": title,
                    "Author": author,
                    "Link": book_link
                })
            except Exception as e:
                print(f" Error while processing a book: {e}")  

        print(f" Data has been extracted from the page{page}")  

    except requests.exceptions.RequestException as e:
        print(f" Page {page} was skipped due to a connection error: {e}")
    
    time.sleep(1)

df = pd.DataFrame(books)
df.to_csv("gutenberg_books.csv", index=False, encoding="utf-8")
print(" The data was saved in'gutenberg_books.csv'")
