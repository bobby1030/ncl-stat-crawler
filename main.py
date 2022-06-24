import threading
from queue import Queue
import os
import pandas as pd
from modules.NCLCrawler import NCLCrawler
from modules.NCLDownloader import NCLDownloader

# fmt: off
counties = ["臺北市","臺北縣","臺中市","臺中縣","臺南市","臺南縣","桃園市","桃園縣","高雄市","高雄縣","連江縣","宜蘭縣","彰化縣","南投縣","雲林縣","基隆市","苗栗縣","嘉義市","嘉義縣","金門縣","臺東縣","花蓮縣","澎湖縣","新竹市","新竹縣","屏東縣"]
# fmt: on

"""
    Scrap election-related files from 'stats.ncl.edu.tw'（國家圖書館 政府統計資訊網）
"""

crawlers = [NCLCrawler(county) for county in counties]
threads = []
filelist = [None] * len(crawlers)  # Storage for responses


def doFetchJobs(crawler, index):
    filelist[index] = crawler.lsFiles()
    print(f"[{index} {counties[index]}] done")


# Initiate threads
for i in range(len(crawlers)):
    threads.append(threading.Thread(target=doFetchJobs, args=(crawlers[i], i)))
    threads[-1].start()

# Wait for threads finishing
for thread in threads:
    thread.join()

print("Done!")

# Concat responses
filelist = pd.DataFrame(pd.concat(filelist))

# Keep only newest record for each election
filelist = (
    filelist.sort_values(["county", "latestRec"], axis=0)
    .groupby(["county", "latestRec"])
    .head(1)
)

try:
    os.mkdir("./output")
except FileExistsError:
    pass

filelist.to_csv("./output/filelist.csv", index=False)


"""
Download PDFs from filtered filelist
"""
downloadjobs = Queue()

# Define Download Task
def doDownloadJob(queue):
    while queue.empty() is False:
        job = queue.get()
        NCLDownloader(job[0], job[1])
        queue.task_done()

# Push jobs into download queue
for file in filelist.to_dict("records"):
    pdfurl = file["pdflink"]
    basedir = f"./output"
    filepath = f"{file['county']}/{file['county']}{file['latestRec']:.0f} - {file['title']}.pdf"
    fullpath = os.path.join(basedir, filepath)

    downloadjobs.put((pdfurl, fullpath))

# Initiate Download Worker (Threads = 5)
for i in range(5):
    worker = threading.Thread(target=doDownloadJob, args=(downloadjobs,))
    worker.start()

# Wait until all jobs in queue are done
downloadjobs.join()

print("Done!")