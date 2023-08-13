from bs4 import BeautifulSoup
import requests
import datetime
import logging
import time
import pandas as pd
import datetime
new_list=[]
def setLogger():
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='logs_file',
                    filemode='w')
    console = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def getEconomicCalendar(startlink):

    # write to console current status
    logging.info("Scraping data for link: {}".format(startlink))

    # get the page and make the soup
    baseURL = "https://www.forexfactory.com//calendar?day="
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    link=startlink.strftime("%b%d.%Y")
    r = requests.get(baseURL + link, headers=headers)
    data = r.text
    soup = BeautifulSoup(data, "html.parser")

    # get and parse table data, ignoring details and graph
    table = soup.find("table", class_="calendar__table")
    # print(table)
    # do not use the ".calendar__row--grey" css selector (reserved for historical data)
    trs = table.select("tr.calendar__row")
    fields = ["date","time","currency","impact","event","actual","forecast","previous"]
    # some rows do not have a date (cells merged)
    curr_year = link[-4:]
    curr_date = ""
    curr_time = ""

    for tr in trs:
        # fields may mess up sometimes, see Tue Sep 25 2:45AM French Consumer Spending
        # in that case we append to errors.csv the date time where the error is
        try:
            new_dict={
                "datetime":None,
                "currency":None,
                "impact":None,
                "event":None,
                "actual":None,
                "forecast":None,
                "previous":None
            }
            for field in fields:
                data = tr.select("td.calendar__cell.calendar__{}".format(field,field))[0]
                if field=="date" and data.text.strip()!="":
                    curr_date = data.text.strip()
                elif field=="time" and data.text.strip()!="":
                    if data.text.strip().find("Day")!=-1:
                        curr_time = "12:00am"
                    else:
                        curr_time = data.text.strip()
                elif field=="currency":
                    new_dict['currency'] = data.text.strip()
                elif field=="impact":
                    impact_span = data.find('span', class_='icon--ff-impact-red')
                    if impact_span:
                        new_dict['impact'] = "High Impact Expected"
                    impact_span = data.find('span', class_='icon--ff-impact-ora')
                    if impact_span:
                        new_dict['impact'] = "Medium Impact Expected"
                    impact_span = data.find('span', class_='icon--ff-impact-yel')
                    if impact_span:
                        new_dict['impact'] = "low Impact Expected"
                elif field=="event":
                    new_dict['event'] = data.text.strip()
                elif field=="actual":
                    new_dict['actual'] = data.text.strip()
                elif field=="forecast":
                    new_dict['forecast'] = data.text.strip()
                elif field=="previous":
                    new_dict['previous'] = data.text.strip()

            dt = datetime.datetime.strptime(",".join([curr_year,curr_date,curr_time]),
                                            "%Y,%a %b %d,%I:%M%p")
            new_dict['datetime']=dt
            new_list.append(new_dict)
        except:
            # print(traceback.format_exc())
            pass
    

if __name__ == "__main__":
    """
    Run this using the command "python `script_name`.py >> `output_name`.csv"
    """
    setLogger()
    startlink=datetime.datetime(2017,1,1)
    endlink=datetime.datetime(2023,8,18)
    while True:
        getEconomicCalendar(startlink)
        if startlink==endlink:
            logging.info("Successfully retrieved data")
            break

    # get the link for the next week and follow
        startlink+=datetime.timedelta(days=1)
        time.sleep(1)

    df=pd.DataFrame(new_list)

    df.to_csv("Newsdata.csv",index=False)
    print(df)