import argparse
import re
from dataclasses import dataclass

import matplotlib.pyplot as plt
import pandas as pd

"""
log_format main '$time_iso8601 $remote_addr $request_time $status "$request" '
                '$body_bytes_sent "$http_referer" "$http_user_agent" $http_x_forwarded_for $remote_user';
"""
PATTERN = '([^ ]+) ([\d.]+) ([\d.]+) ([\d]+) "([^"]+)" ([\d]+) "([^"]+)" "([^"]+)" ([\d.,-]+) (.*)'


@dataclass
class NginxLog:
    time: str
    remote_addr: str
    request_time: float
    status: int
    request: str
    body_bytes_sent: int
    http_referer: str
    http_user_agent: str
    http_x_forwarded_for: str
    remote_user: str
    method: str
    protocol: str
    product: str
    sid: str


def exclude(log: NginxLog):
    if "35.74.201.143" in log.http_x_forwarded_for.lower():  # prometheus
        return True
    if "elb-healthchecker" in log.http_user_agent.lower():
        return True
    if "hetrixtools" in log.http_user_agent.lower():
        return True
    fext = [
        ".html",
        ".css",
        ".js",
        ".webp",
        ".jpg",
        ".png",
        ".svg",
        ".ico",
        ".mp3",
        ".mp4",
        ".bin",
        "audio-editor",
        "photo-editor",
    ]
    if any(x in log.request.lower() for x in fext):
        return True
    return False


def parse_sid(uri: str):
    data = uri.split("/")
    if len(data) < 4:
        return "-"
    elif data[2] != "effect":
        return "-"
    match = re.match("^(.+)-(\d+)-(\d+)$", data[3])
    if not match:
        return "-"
    return match.group(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    args = parser.parse_args()

    fname = args.file
    data = []
    with open(fname, "r") as fp:
        for line in fp.readlines():
            match = re.match(PATTERN, line.strip())
            if match:
                request = match.group(5)
                method, uri, protocol = request.split(" ")
                sid = parse_sid(uri)
                product = match.group(8).split(" ")[0].split("/")[0]
                log = NginxLog(*match.groups(), method, protocol, product, sid)
                # print(log)
                if not exclude(log):
                    data.append(log)
            else:
                print(f"Not Found: {line}")

    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])
    # print(df.head())

    sid_total = df.groupby([df["time"].dt.minute]).sid.nunique()
    print(sid_total)
    sid_total.plot(xticks=range(0, 60))
    plt.show()

    method_total = (
        df[~df["method"].isin(("DELETE", "GET", "HEAD", "OPTIONS", "PUT"))]
        .groupby([df["time"].dt.minute, "method"])
        .size()
    )
    print(method_total)
    method_total.unstack().plot(kind="bar", stacked=False)
    plt.show()

    product_total = (
        df.groupby(["product"]).size().sort_values(ascending=False).nlargest(10)
    )
    print(product_total)
    product_total.plot(kind="bar")
    plt.show()


if __name__ == "__main__":
    main()
