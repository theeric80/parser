import argparse
import re
import statistics

AI_END_PATTERN = "^(.*)\[apply\]\[(\d+)\] \((\d+)\) AI End \((\d+) ~ (\d+), (?P<duration>\d+) ms\)(.+)$"
FUN_END_PATTERN = "^(.*)\[apply\] Function End \((?P<duration>\d+) ms\)$"


def load_done_log(fname: str):
    ai_end = []
    func_end = []
    with open(fname, "r") as fp:
        for line in fp.readlines():
            line = line.strip()
            if "AI End" in line:
                match = re.match(AI_END_PATTERN, line)
                if match:
                    m = match
                    ai_end.append(int(m.group("duration")))
                else:
                    print(f"not-found: {line}")
            elif "Function End" in line:
                match = re.match(FUN_END_PATTERN, line)
                if match:
                    m = match
                    func_end.append(int(m.group("duration")))
                else:
                    print(f"not-found: {line}")
    return ai_end, func_end


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    args = parser.parse_args()

    fname = args.file
    ai_end, func_end = load_done_log(fname)
    mean_ai_end = statistics.mean(ai_end)
    mean_func_end = statistics.mean(func_end)
    overhead = mean_func_end - mean_ai_end

    print(f"AI End: {mean_ai_end:.3f} ms")
    print(f"Function End: {mean_func_end:.3f} ms")
    print(f"Overhead: {overhead:.3f} ms")


if __name__ == "__main__":
    main()
