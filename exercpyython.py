import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}
MAX_THREADS = 10

def extract_movie_details(movie_link):
    try:
        time.sleep(random.uniform(0, 0.2))
        response = requests.get(movie_link, headers=headers)
        response.raise_for_status()
        movie_soup = BeautifulSoup(response.content, 'html.parser')

        if movie_soup is not None:
            title = None
            date = None
            movie_data = movie_soup.find('div', attrs={'class': 'sc-94726ce4-1 iNShGo'})
            if movie_data is not None:
                title = movie_data.find('h1').get_text()
                date = movie_data.find('span', attrs={'class': 'sc-8c396aa2-2 itZqyK'}).get_text().strip()

            rating = movie_soup.find('span', attrs={'class': 'sc-bde20123-1 iZlgcd'}).get_text() if movie_soup.find(
                'span', attrs={'class': 'sc-bde20123-1 iZlgcd'}) else None
            plot_text = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'}).get_text().strip() if movie_soup.find(
                'span', attrs={'data-testid': 'plot-xs_to_m'}) else None

            with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
                movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                if all([title, date, rating, plot_text]):
                    print(title, date, rating, plot_text)
                    movie_writer.writerow([title, date, rating, plot_text])
    except Exception as e:
        print(f"Failed to process {movie_link}: {e}")

def extract_movies(soup):
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)

def main():
    start_time = time.time()

    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')


    with open('movies.csv', mode='w', newline='', encoding='utf-8') as file:
        movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        movie_writer.writerow(['Title', 'Date', 'Rating', 'Plot'])


    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)

if __name__ == '__main__':
    main()
