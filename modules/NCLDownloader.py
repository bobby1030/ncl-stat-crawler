import os
import requests


def NCLDownloader(url: str, path: str):

    dir = os.path.dirname(path)
    filename = os.path.basename(path)
    fullpath = os.path.join(dir, filename)

    try:
        os.makedirs(dir)
    except FileExistsError:
        pass

    print(f"Downloading {filename}")
    response = requests.get(url)

    with open(fullpath, "wb") as f:
        print(f"Writing to {fullpath}")
        f.write(response.content)
