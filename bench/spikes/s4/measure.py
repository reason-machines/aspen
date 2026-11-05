#!/usr/bin/env python3
"""Run a shell command and report wall time, CPU time and peak memory as JSON.

Memory is sampled every 20 ms from /proc/<pid>/status for the whole process
tree (so `cat pack | git index-pack --stdin` counts both sides) and summed per
sample. RssAnon is heap; RssFile is page cache mapped by the process (pack
mmaps), which the kernel can reclaim; RssShmem is tmpfs/shared memory.
Linux only.

The command's stdout passes through; the JSON report is the last line on
stderr.

usage: measure.py <label> <shell command>
"""
import json, os, resource, subprocess, sys, time

KEYS = ("RssAnon", "RssFile", "RssShmem", "VmRSS")


def tree(pid):
    out, todo = [], [pid]
    while todo:
        p = todo.pop()
        out.append(p)
        try:
            for t in os.listdir(f"/proc/{p}/task"):
                with open(f"/proc/{p}/task/{t}/children") as f:
                    todo.extend(int(c) for c in f.read().split())
        except OSError:
            pass
    return out


def sample(pids):
    total = dict.fromkeys(KEYS, 0)
    for p in pids:
        try:
            with open(f"/proc/{p}/status") as f:
                for line in f:
                    k, _, v = line.partition(":")
                    if k in total:
                        total[k] += int(v.split()[0])
        except OSError:
            pass
    return total


def main():
    label, cmd = sys.argv[1], sys.argv[2]
    load_before = os.getloadavg()[0]
    t0 = time.monotonic()
    proc = subprocess.Popen(["bash", "-o", "pipefail", "-c", cmd])
    peak = dict.fromkeys(KEYS, 0)
    while proc.poll() is None:
        for k, v in sample(tree(proc.pid)).items():
            peak[k] = max(peak[k], v)
        time.sleep(0.02)
    wall = time.monotonic() - t0
    ru = resource.getrusage(resource.RUSAGE_CHILDREN)
    mib = lambda kib: round(kib / 1024, 1)
    print(json.dumps({
        "label": label,
        "exit": proc.returncode,
        "wall_s": round(wall, 2),
        "user_s": round(ru.ru_utime, 2),
        "sys_s": round(ru.ru_stime, 2),
        "peak_rss_anon_mib": mib(peak["RssAnon"]),
        "peak_rss_file_mib": mib(peak["RssFile"]),
        "peak_rss_shmem_mib": mib(peak["RssShmem"]),
        "peak_rss_sum_mib": mib(peak["VmRSS"]),
        "max_single_process_rss_mib": mib(ru.ru_maxrss),
        # Load from other processes skews wall time; CPU time is the steadier number.
        "loadavg_1m_before": round(load_before, 2),
        "loadavg_1m_after": round(os.getloadavg()[0], 2),
    }), file=sys.stderr)
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
