import csv, requests
from time import time, sleep
from threading import Thread
from bs4 import BeautifulSoup
from os import path, remove, system

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


url = "https://aragge.ch/cgi-bin/data_frame.pl"

months = {
    "Janvier": "Mars",
    "Avril": "Juin",
    "Juillet": "Septembre",
    "Octobre": "Novembre"
}
years = [2019, 2020, 2021, 2022]

running = True


def timer(func):
    def wrapper(*args, **kwargs):
        start = time()
        func(*args, **kwargs)
        end = time()
        print(f'Time elapsed: {end - start - 1} seconds.')
    return wrapper


def create_status_dict():
    screen_dict = {}
    for y in years:
        screen_dict[y] = {}
        for m in range(len(months)):
            screen_dict[y][m] = {
                "status": "STARTING"
            }
        screen_dict[y]["status"] = "----------"
    return screen_dict
    
screen_dict = create_status_dict()


def change_status():
    s = ""
    for y in years:
        s += f"""{y} :
\tJanvier - Mars: {screen_dict[y][0]["status"]}\t
Avril - Juin: {screen_dict[y][1]["status"]}\t
Juillet - Septembre: {screen_dict[y][2]["status"]}\t
Octobre - Novembre: {screen_dict[y][3]["status"]}\t
{screen_dict[y]["status"]}\n\r"""
    return s


def update_screen(year, month, status):
    if month == -1:
        screen_dict[year]["status"] = status
    else:
        screen_dict[year][month]["status"] = status


def html_to_rows(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    rows = []
    for row in table.find_all('tr'):
        cols = row.find_all('td')
        # handle empty td cells
        cols = [res if (res := ele.text.strip()) else None for ele in cols]
        rows.append(cols)
    return rows


def rows_to_csv(rows, year):
    with open(f'data/flights_{year}.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in rows:
            writer.writerow([ele for ele in row])
    return


def merge_csv(c1, c2):
    with open(c1, "r") as c1:
        with open(c2, "a") as c2:
            c2.write(c1.read())


def bytes_to_universal(bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024:
            return f'{bytes:.2f} {unit}'
        bytes /= 1024


def thread_month_function(year, k, v, result, idx, pidx):
    update_screen(year, idx, f"{bcolors.FAIL}FETCHING{bcolors.ENDC}")
    payload = {
        "start_hour": "06",
        "hour_count": "24",
        "start_day": "01",
        "end_day": "31",
        "year": year,
        "start_month_name": k,
        "end_month_name": v
    }
    files = []
    headers = {}
    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    update_screen(year, idx, f"{bcolors.WARNING}CONVERTING{bcolors.ENDC} (size: {bytes_to_universal(len(response.text))})")

    rows = html_to_rows(response.text)
    result[idx] = rows
    
    update_screen(year, idx, f"{bcolors.OKGREEN}DONE{bcolors.ENDC}")
    return


def thread_year_function(year, result, idx):
    idx_month = 0
    t = []
    for k, v in months.items():
        t.append(Thread(target=thread_month_function, args=(year, k, v, result[idx], idx_month, idx)))
        t[-1].start()
        idx_month += 1

    for thread in t:
        thread.join()
    
    update_screen(year, -1, f"Writing to file data/flights_{year}.csv ...")
    
    for r in result[idx]:
        rows_to_csv(r, year)
    merge_csv(f'data/Dec{year}.csv', f'data/flights_{year}.csv')
    update_screen(year, -1, f"{bcolors.BOLD}Year {year} completed.{bcolors.ENDC}")


def thread_screen_function():
    while running:
        screen = change_status()
        system('clear')
        print(f'{screen}', end='')
        sleep(0.5)


def clean():
    for year in years:
        csv_path = f'data/flights_{year}.csv'
        if path.exists(csv_path):
            remove(csv_path)


@timer
def main():
    clean()
    t_year = [None] * len(years)
    t_results = [[None] * len(months)] * len(years)
    
    t_screen = Thread(target=thread_screen_function)
    t_screen.start()
    
    for i, year in enumerate(years):
        t_year[i] = Thread(target=thread_year_function, args=(year, t_results, i))
        t_year[i].start()

    for thread in t_year:
        thread.join()

    sleep(1) # allow screen to finish refreshing

    global running
    running = False
    t_screen.join()
    
    print('Done.', end='\n\r')


if __name__ == '__main__':
    main()