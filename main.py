import requests
from bs4 import BeautifulSoup
import re
import time
import concurrent.futures
import pandas as pd
from tqdm import tqdm
import pygsheets


headers2= {
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7", 
#     "Accept-Encoding": "gzip, deflate, br", 
#     "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7", 
    "Cache-Control": "max-age=0", 
    "Cookie": "datr=J9gPY2PidPg_JoPmoWRrdmPi; sb=A0qqYwH7KDzZqQEk7eT2pnEY; c_user=100000266768500; m_page_voice=100000266768500; m_pixel_ratio=2; dpr=1; wd=1699x1018; xs=26%3APhksew7PoPRkyQ%3A2%3A1672193063%3A-1%3A11326%3A%3AAcVYXBLRTwBWdQHv1to47JCN4FhaVGTwGm88ITHdMsF7; fr=00oCOSYdLPuk6hVmn.AWVcWJIL1bWY5xnuSUFRluC2cBY.BktAQb.WA.AAA.0.0.BktAQb.AWU7e8fHmqI; presence=C%7B%22t3%22%3A%5B%5D%2C%22utc3%22%3A1689519135605%2C%22v%22%3A1%7D",
    "Dnt":"1", 
    "Sec-Ch-Ua":'"Not.A/Brand";v="8", "Chromium";v="114"',
    "Sec-Ch-Ua-Full-Version-List":'"Not.A/Brand";v="8.0.0.0", "Chromium";v="114.0.5735.198"',
    "Sec-Ch-Ua-Mobile": "?0", 
    "Sec-Fetch-Dest": "document", 
    "Sec-Fetch-Mode": "navigate", 
    "Sec-Fetch-Site": "none", 
    "Upgrade-Insecure-Requests": "1", 
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
  }

def scrape_page(url, headers):
    response = requests.get(url, headers=headers)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, features="xml")
    return soup


def process_post(post, headers, progress):
    ptext = post.text
    isMore = post.find('a', string='更多')
    if isMore:
        more_url = isMore.get('href')
        ptext = scrape_more(more_url, headers)  # Assuming scrape_more is defined elsewhere
    footer = post.parent.next_sibling   
    try:
        likes = int(footer.select_one('a').text.replace(',', ''))
    except:
        likes = 0
    post_url = footer.find('a', string='完整動態').get('href')
    pheonix =  "鳳凰電波" in ptext and likes > 100
    progress.update(1)
    return {'text': ptext, 'likes': likes, 'links': post_url, 'pheonix': pheonix}

def scrape_more(url, headers):
    response = requests.get(url, headers=headers)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, features="xml")
    header = soup.select_one('header')
    post_text = header.next_sibling.text
    return post_text

def get_nextPageUrl(soup_page):
    nextPageDiv = soup_page.select_one('#m_group_stories_container section').next_sibling
    nextPageUrl = 'https://mbasic.facebook.com' + nextPageDiv.find('a').get('href')
    return nextPageUrl

def scrape(base_url, num_posts):
    progress = tqdm(total=num_posts)
    next_page_url = base_url
    all_post = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        while len(all_post) < num_posts:
            soup_future = executor.submit(scrape_page, next_page_url, headers2)
            soup = soup_future.result()
            posts = soup.select('section > article > .dg > .dm')
            postslist = list(executor.map(process_post, posts, [headers2] * len(posts), [progress] * len(posts)))
            all_post += postslist

            next_page_future = executor.submit(get_nextPageUrl, soup)
            next_page_url = next_page_future.result()
            # print(len(all_post), 'posts done')
#             time.sleep(1)
    return pd.DataFrame(all_post)

def main():
    try:
        gc = pygsheets.authorize(client_secret='client_secret.json')
    except:
        print('no client_secret.json')
    
    # Open the Google Sheet by title
    sheet_title = 'bevenus 3000 posts with Requests'
    # Find the Google Sheets file by name
    try:
        sheet = gc.open(sheet_title)
        print("Found the sheet:", sheet.title)
    except pygsheets.SpreadsheetNotFound:
        print("The sheet with the name '{}' was not found. Creating a new one.".format(sheet_title))
        
        # Create a new spreadsheet with the given name
        sheet = gc.create(sheet_title)
        print("New sheet created with the title:", sheet.title)

    base_url = 'https://mbasic.facebook.com/groups/686552765061367'
    df = scrape(base_url=base_url, num_posts=3000)
    
    worksheet = sheet[0]
    # Write the DataFrame to the Google Sheet starting from cell 'A1'
    worksheet.set_dataframe(df, start='A1')

if __name__ == '__main__':
    main()