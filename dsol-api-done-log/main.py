import argparse
import math
import re
from dataclasses import dataclass
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

"""
L: GotUploadLink
W: Waiting
D: Downloading
A: Applying
U: Uploading
E: Done
"""
PATTERN = (
    "^(?P<datetime>.*) \[(?P<sid>\d+)\]\[(?P<category>\w+)\]\[(.*)\] "
    'Done: "(.*)", "(.*)", "(.*)", '
    "I: ([-]?\d+) bytes\((\d+) files\), (\d+) ms, (?P<effect>\w+), "
    "L: (\d+)\((?P<l_duration>\d+), (\d+) KB\/s\), "
    "W: (\d+)\((?P<w_duration>\d+)\), "
    "D: (\d+)\((?P<d_duration>\d+), (?P<d_kbps>\d+) KB\/s\), "
    "A: (\d+)\((?P<a_duration>\d+), (\d*\.?\d*)x\), "
    "O: (\d+) bytes\((\d+) files\), "
    "U: (\d+)\((?P<u_duration>[-]?\d+), (?P<u_kbps>\d+) KB\/s\), "
    "E: (\d+)\((\d*\.?\d*)x, (?P<duration>\d+), (\d*\.?\d*)x\) "
    "(.*)$"
)


@dataclass
class DoneLog:
    datetime: datetime
    session_id: int
    category: str
    effect: str
    duration: int
    l_duration: int
    w_duration: int
    d_duration: int
    a_duration: int
    u_duration: int
    d_kbps: int
    u_kbps: int


def analyze_arrival_process(ax0, ax1, event_times):
    event_times = event_times.sort_values()
    event_times = event_times - event_times.iloc[0]

    inter_arrival_times = np.diff(event_times)
    num_events = len(event_times)

    mean = np.mean(inter_arrival_times)
    std = np.std(inter_arrival_times)

    ax0.step(event_times, np.arange(1, num_events + 1), where="post", color="blue")
    ax0.set_xlabel("Time")
    ax0.set_ylabel("Event Number")
    ax0.set_title(f"Event Times\nTotal: {num_events} events\n")
    ax0.grid(True, alpha=0.5)

    ax1.hist(inter_arrival_times, bins=20, color="green", alpha=0.5)
    ax1.set_xlabel("Inter-Arrival Time")
    ax1.set_ylabel("Frequency")
    ax1.set_title(f"Inter-Arrival Times\nMEAN: {mean:.2f} | STD: {std:.2f}\n")
    ax1.grid(True, alpha=0.5)

    return mean, std


def analyze_service_process(ax, durations):
    mean = np.mean(durations)
    std = np.std(durations)

    min_ = math.floor(np.min(durations))
    max_ = math.ceil(np.max(durations))
    bins = min(max(math.ceil(max_ - min_), 2), 20)

    ax.hist(durations, bins=bins, color="green", alpha=0.5)
    ax.set_xlabel("Service Time")
    ax.set_ylabel("Frequency")
    ax.set_title(f"Service Times\nMEAN: {mean:.2f} | STD: {std:.2f}\n")
    ax.grid(True, alpha=0.5)

    return mean, std


def analyze_queuing_time(ax, queuing_times):
    queuing_times = queuing_times.sort_values()
    mean = np.mean(queuing_times)

    q = (50, 90, 95, 99)
    percentiles = tuple(map(int, np.percentile(queuing_times, q)))

    min_ = math.floor(np.min(queuing_times))
    max_ = math.ceil(np.max(queuing_times))
    bins = min(max(math.ceil((max_ - min_) / 200), 5), 20)

    ax.hist(queuing_times, bins=bins, color="green", alpha=0.5)
    ax.set_xlabel("Queuing Time")
    ax.set_ylabel("Frequency")
    ax.set_title(f"Queuing Times    \nMEAN: {mean:.2f}\n")
    for d, p in zip(q, percentiles):
        ax.axvline(x=p, color="tab:orange", linestyle="--", label=f"{d}%: {p}")
    ax.grid(True, alpha=0.5)

    return mean, tuple(zip(q, percentiles))


def analyze_processing_time(df):
    w = pd.to_numeric(df["w_duration"])
    d = pd.to_numeric(df["d_duration"])
    a = pd.to_numeric(df["a_duration"])
    u = pd.to_numeric(df["u_duration"])
    e = pd.to_numeric(df["duration"])
    return tuple((np.mean(x), np.std(x)) for x in (w, d, a, u, e))


def analyze_s3_bandwidth(df):
    d = pd.to_numeric(df["d_kbps"])
    u = pd.to_numeric(df["u_kbps"])
    return tuple((np.mean(x), np.std(x)) for x in (d, u))


def load_done_log(fname: str) -> list[DoneLog]:
    data = []
    with open(fname, "r") as fp:
        for line in fp.readlines():
            line = line.strip()
            match = re.match(PATTERN, line)
            if match:
                m = match
                log = DoneLog(
                    datetime.strptime(m.group("datetime"), "%Y/%m/%d %H:%M:%S.%f"),
                    m.group("sid"),
                    m.group("category"),
                    m.group("effect"),
                    m.group("duration"),
                    m.group("l_duration"),
                    m.group("w_duration"),
                    m.group("d_duration"),
                    m.group("a_duration"),
                    m.group("u_duration"),
                    m.group("d_kbps"),
                    m.group("u_kbps"),
                )
                data.append(log)
                # print(log)
            else:
                print(f"not-found: {line}")
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    parser.add_argument("effect", nargs="+")
    args = parser.parse_args()

    fname = args.file
    data = load_done_log(fname)

    df = pd.DataFrame(data)

    category_total = df.groupby("category").size().sort_values(ascending=False)
    print(f"{category_total}\n")

    effect_total = df.groupby("effect").size().sort_values(ascending=False)
    print(f"{effect_total}\n")

    effect = args.effect
    # df_effect = df[~df["category"].isin(("Fres", "Photo", "Audio", "Ttsp", "Fas", "Vani", "Txt", "Tran", "Avatar", "Hds"))]
    df_effect = df[df["effect"].isin(effect)]
    # print(df_fs.head())

    fig, ax = plt.subplots(2, 2, figsize=(16, 9))

    event_times = pd.to_numeric(df_effect["datetime"]) / 10**9
    mean, std = analyze_arrival_process(ax[0][0], ax[0][1], event_times)
    print(f"{effect} Arrival Process\nMEAN: {mean} | STD: {std}\n")

    durations = pd.to_numeric(df_effect["duration"]) / 10**3
    mean, std = analyze_service_process(ax[1][0], durations)
    print(f"{effect} Service Process\nMEAN: {mean} | STD: {std}\n")

    queuing_times = pd.to_numeric(df_effect["w_duration"])
    mean, percentiles = analyze_queuing_time(ax[1][1], queuing_times)
    print(f"{effect} Queuing Times\nMEAN: {mean} | PERCENTILE: {percentiles}\n")

    print(f"{effect} Processing Times")
    for i, d in zip(("W", "D", "A", "U", "E"), analyze_processing_time(df_effect)):
        print(f"[{i}] MEAN: {d[0]:8.2f} | STD: {d[1]:8.2f}")
    print(",".join([str(x[0]) for x in analyze_processing_time(df_effect)]))
    print("")

    print(f"{effect} S3 Bandwidth")
    for i, d in zip(("D", "U"), analyze_s3_bandwidth(df_effect)):
        print(f"[{i}] MEAN: {d[0]:8.2f} KB/s | STD: {d[1]:8.2f}")
    print("")

    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
