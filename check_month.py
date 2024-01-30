
import time
import traceback

from dotenv import dotenv_values
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options

from sqlalchemy import create_engine

cfg = dotenv_values(".env")

engine = create_engine(f"mysql+pymysql://{cfg['USR']}:{cfg['PWD']}@{cfg['HOST']}/{cfg['DB']}")
conn = engine.connect()


def db_exec(eng, this_sql):
    # print(f"sql: {sql}")
    if this_sql.strip().startswith('select'):
        return [dict(row) for row in eng.execute(this_sql).fetchall()]
    else:
        # print(f"sql: {this_sql}")
        return eng.execute(this_sql)


def scrape_links():

    opts = Options()
    opts.add_argument("--headless")

    br = webdriver.Firefox(options=opts)

    br.set_window_position(50, 50)
    br.set_window_size(900, 900)

    br.implicitly_wait(10)

    url = 'https://procurement.sccgov.org/doing-business-county/active-contracts'

    br.get(url)

    time.sleep(5)

    newer = list()

    try:
        for link in br.find_elements(By.TAG_NAME, 'a'):
            href = link.get_attribute('href')
            if href.endswith('.pdf') and not href.endswith("Report.pdf"):
                newer.append(href)

    except:
        traceback.print_exc()
    finally:
        br.quit()

    return newer


def last_saved():
    
    sql = "select source_url from sources where month_pk = (select max(pk) from months where approved is not NULL);"
    sources = [r['source_url'] for r in db_exec(conn, sql)]

    return sources


if __name__ == '__main__':

    newer = scrape_links()
    # print(f"newer: {newer}")

    found = last_saved()
    # print(f"found: {found}")

    for url in found:
        try:
            newer.remove(url)
        except ValueError (e):
            pass

    if len(newer) == 0:
        print(f"found NOTHING new")
    else:
        print(f"After checking, newer: {newer}")


