"""Plot the sink's k-anonymous aggregates. Works only from /analysis/* responses (no install ids exist there)."""
import argparse
import json
import os
import urllib.request

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ORDER = ["0", "1-49", "50-199", "200-499", "500+", "suppressed"]
COLOR = "#2f6f9f"


def get(sink, path):
    with urllib.request.urlopen(sink.rstrip("/") + path) as r:
        return json.load(r)


def finish(fig, ax, title, k, path, note):
    ax.set_title(title, loc="left", fontsize=13, fontweight="bold", pad=26)
    ax.text(0, 1.03, f"MOCKED DATA, k={k} (groups under k installs suppressed)", transform=ax.transAxes,
            fontsize=9, color="#a33")
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    fig.text(0.01, 0.01, note, fontsize=7.5, color="#444", va="bottom", wrap=True)
    fig.savefig(path, dpi=130)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sink", default="http://127.0.0.1:8000")
    ap.add_argument("--out", default="./out")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    mods = get(a.sink, "/analysis/modules")
    cmds = get(a.sink, "/analysis/commands")
    k = mods["k"]

    b = sorted(mods["buckets"], key=lambda x: ORDER.index(x["bucket"]) if x["bucket"] in ORDER else 99)
    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar([x["bucket"] for x in b], [x["installs"] for x in b], color=COLOR)
    ax.bar_label(bars, padding=2)
    ax.set_xlabel("modules per build (bucket)")
    ax.set_ylabel("installs")
    finish(fig, ax, "Installs by module count", k, os.path.join(a.out, "module_count_distribution.png"),
           "Groups under k installs are hidden or merged into 'suppressed'.\nAn install counts once per bucket it has builds in, so bars can sum above the install count.")

    top = mods["names"][:15][::-1]
    fig, ax = plt.subplots(figsize=(7, 5.5))
    bars = ax.barh([x["name"] for x in top], [x["installs"] for x in top], color=COLOR)
    ax.bar_label(bars, padding=2)
    ax.set_xlabel("installs reporting the module")
    finish(fig, ax, "Top 15 module names", k, os.path.join(a.out, "top_modules.png"),
           "Groups under k installs are hidden or merged into 'suppressed'.\nInstalls whose collector removed module names are absent from this figure.")

    c = cmds["commands"]
    # One row per command: runs and runs-weighted mean over statuses.
    agg = {}
    for x in c:
        g = agg.setdefault(x["command"], [0, 0.0])
        g[0] += x["runs"]
        g[1] += x["runs"] * (x["mean_secs"] or 0)
    names = sorted(agg, key=lambda n: -agg[n][0])
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 4.5))
    r = a1.bar(names, [agg[n][0] for n in names], color=COLOR)
    a1.bar_label(r, padding=2)
    a1.set_ylabel("runs")
    m = a2.bar(names, [agg[n][1] / agg[n][0] for n in names], color="#c9772b")
    a2.bar_label(m, fmt="%.0f", padding=2)
    a2.set_ylabel("mean seconds")
    for ax in (a1, a2):
        ax.tick_params(axis="x", rotation=30)
    a2.set_title("Mean elapsed time", loc="left", fontsize=11)
    a2.spines[["top", "right"]].set_visible(False)
    finish(fig, a1, "Runs per command", k, os.path.join(a.out, "commands.png"),
           "Groups under k installs are hidden or merged into 'suppressed'; a 'suppressed' bar is a merge, not a command.\nRuns are summed over statuses.")
    print(f"wrote 3 figures to {a.out} (k={k})")


if __name__ == "__main__":
    main()
