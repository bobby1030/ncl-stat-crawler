import re
import requests
from bs4 import BeautifulSoup
import pandas as pd


class NCLCrawler:
    def __init__(self, county):
        self.baseurl = "https://stat.ncl.edu.tw/searchResult.jsp"
        self.county = county

    def lsFiles(self):
        payload = {
            "q_txt1": self.county,
            "q_field1": "ti",
            "terms1": "and",
            "q_txt2": "議員",
            "q_field2": "ti",
            "terms2": "and",
            "q_txt3": "選舉",
            "q_field3": "ti",
            "eachpage": "100",  # Results per page
            "c5_flag": "Y",  # 直轄市改制前資料
            "qdate_field": "pd",
            "year1": "35",  # Starting year
            "month1": "10",  # Starting month
            "year2": "110",  # Ending year
            "month2": "12",  # Ending month
            "dtd_id": "11",  # Unknown purpose but required
            "s_flag": "2",  # Unknown purpose but required
            # "sort_by": "pd",
            # "sort_order": "1",
            # "qry_mode": "1",
            # "Search": "查詢",
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0",
            "Origin": "https://stat.ncl.edu.tw",
            "Referer": "https://stat.ncl.edu.tw/searchResult.jsp",
        }

        response = requests.request("POST", self.baseurl, data=payload, headers=headers)
        soup = BeautifulSoup(response.text, "lxml")
        rows = soup.select("div.table_list tbody > tr")

        def rowToObject(row):
            title = row.select('td[data-title="篇目"] > a')[0].text
            pdflink = row.select('td[data-title="電子檔"] > a')[0]["href"]
            date = row.select('td[data-title="出版日期"]')[0].text

            if "https://" not in pdflink:
                pdflink = "https://stat.ncl.edu.tw/" + pdflink

            try:
                records = re.findall(r"(\d{4})", title)
                earliestRec, latestRec = int(records[0]), int(records[-1])
            except IndexError:
                earliestRec, latestRec = None, None

            year = int(re.match(r"民國(\d+)年\d+月", date).group(1)) + 1911

            return {
                "county": self.county,
                "title": title,
                "pdflink": pdflink,
                "year": year,
                "earliestRec": earliestRec,
                "latestRec": latestRec,
            }

        self.filelist = pd.DataFrame([rowToObject(row) for row in rows])
        self.filelist = self.filelist[
            self.filelist["title"].str.contains("[省|省]議[會|員]") != True
        ]  # Filter out 省議員選舉 (Contains CJK Compatibility Ideographs)

        return self.filelist
