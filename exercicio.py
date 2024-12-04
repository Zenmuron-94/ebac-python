import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup
from threading import Lock

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}
MAX_THREADS = 15
file_lock = Lock()

def save_to_csv(data):
    with file_lock:
        with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
            movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            movie_writer.writerow(data)

def extract_movie_details(movie_link):
    time.sleep(random.uniform(0.5, 1.5))
    response = requests.get(movie_link, headers=headers)

    if response.status_code != 200:
        print(f"Erro ao acessar {movie_link}: {response.status_code}")
        return

    movie_soup = BeautifulSoup(response.content, 'html.parser')

    title, date, rating, plot_text = None, None, None, None
    page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})

    if page_section:
        divs = page_section.find_all('div', recursive=False)
        if len(divs) > 1:
            target_div = divs[1]
            title_tag = target_div.find('h1')
            if title_tag:
                title = title_tag.get_text().strip()
            date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
            if date_tag:
                date = date_tag.get_text().strip()
        
        rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
        rating = rating_tag.get_text().strip() if rating_tag else None

        plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
        plot_text = plot_tag.get_text().strip() if plot_tag else None

        if all([title, date, rating, plot_text]):
            print(title, date, rating, plot_text)
            save_to_csv([title, date, rating, plot_text])

def extract_movies(soup):
    movie_links = []
    main_column = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'})
    if main_column:
        movies_table = main_column.find('ul')
        if movies_table:
            movies_table_rows = movies_table.find_all('li')
            movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows if movie.find('a')]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)

def main():
    start_time = time.time()
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)

    if response.status_code != 200:
        print(f"Erro ao acessar a URL principal: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    extract_movies(soup)

    end_time = time.time()
    print('Total time taken:', end_time - start_time)

if __name__ == '__main__':
    main()
