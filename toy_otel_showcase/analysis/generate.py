"""Mock telemetry generator for the sink analysis demo.

THE DATA IS MOCKED. It is drawn from a seeded random generator and says nothing
about real kw users, their hardware, or their kernels. Only the record shapes
(OTLP/JSON logs) follow what the kw client exports.
"""
import argparse
import hashlib
import json
import random
import urllib.request
import uuid

RARE_K = 5  # the sink's default k; rare names are placed on fewer installs than this

COMMON = [
    "ext4", "btrfs", "xfs", "vfat", "nfs", "usbcore", "usb_storage", "xhci_hcd", "ahci", "nvme",
    "snd_hda_intel", "snd_usb_audio", "i915", "amdgpu", "nouveau", "drm", "drm_kms_helper", "iwlwifi",
    "cfg80211", "mac80211", "bluetooth", "btusb", "e1000e", "r8169", "igb", "tun", "bridge", "veth",
    "overlay", "br_netfilter", "nf_tables", "nf_conntrack", "iptable_filter", "ip_tables", "x_tables",
    "kvm", "kvm_intel", "kvm_amd", "vhost_net", "virtio_net", "virtio_blk", "virtio_pci", "loop",
    "dm_mod", "dm_crypt", "raid1", "md_mod", "fuse", "cuse", "hid_generic", "usbhid", "evdev",
    "atkbd", "psmouse", "uvcvideo", "videodev", "thermal", "acpi_cpufreq", "intel_rapl", "coretemp",
    "watchdog", "rtc_cmos",
]
RARE = [f"vendor_{n}" for n in ("acme_dsp", "zeta_fpga", "orion_pcie", "helix_can", "quark_nic", "lumen_isp")]
PROFILES = [  # name, probability, module_count range, chance a common module is loaded
    ("minimal", 0.30, (10, 40), 0.25),
    ("desktop", 0.40, (120, 200), 0.60),
    ("server", 0.22, (250, 400), 0.80),
    ("full", 0.08, (500, 650), 0.95),
]
COMMANDS = [  # command, weight, mean seconds
    ("build", 0, 240), ("deploy", 0, 35), ("config", 4, 3), ("explore", 3, 2), ("diff", 2, 2), ("backup", 1, 20),
]
FILTERED_SHARE = 0.12  # installs whose collector dropped module_names
EPOCH_NS = 1_700_000_000 * 10**9


def _s(v):
    return {"stringValue": v}


def _i(v):
    return {"intValue": str(v)}


def _record(kind, event_id, t, attrs):
    kv = [{"key": "kw.event.id", "value": _i(event_id)}, {"key": "kw.event.kind", "value": _s(kind)}]
    kv += [{"key": k, "value": v} for k, v in attrs.items()]
    return {"timeUnixNano": str(t), "observedTimeUnixNano": str(t), "severityNumber": 9,
            "severityText": "INFO", "body": _s(kind), "attributes": kv}


def _install(rng, rare_for):
    iid = str(uuid.UUID(int=rng.getrandbits(128), version=4))
    r = rng.random()
    acc = 0
    for name, p, (lo, hi), load in PROFILES:
        acc += p
        if r < acc:
            break
    count = rng.randint(lo, hi)
    names = [m for m in COMMON if rng.random() < load * (1.2 - COMMON.index(m) / len(COMMON))]
    names += rare_for
    # The name list is only the named subset; module_count is the total, as the real kw reports it.
    names_str = "" if rng.random() < FILTERED_SHARE else ",".join(sorted(set(names)))
    records, t = [], EPOCH_NS + rng.randrange(10**6) * 10**9
    eid = 0
    for _ in range(rng.randint(1, 40)):
        t += rng.randint(60, 86400) * 10**9
        kind = "build" if rng.random() < 0.55 else None
        if kind:
            eid += 1
            secs = max(1, int(rng.gauss(240 * count / 150, 60)))
            status = "success" if rng.random() < 0.9 else "failure"
            records.append(_record("executed", eid, t, {
                "kw.executed.command": _s("build"), "kw.executed.status": _s(status),
                "kw.executed.elapsed_time_in_secs": _i(secs)}))
            records.append(_record("build_modules", eid, t, {
                "kw.build_modules.module_count": _i(count),
                "kw.build_modules.module_names": _s(names_str)}))
            if rng.random() < 0.5:
                eid += 1
                records.append(_record("executed", eid, t + 10**9, {
                    "kw.executed.command": _s("deploy"),
                    "kw.executed.status": _s("success" if rng.random() < 0.95 else "failure"),
                    "kw.executed.elapsed_time_in_secs": _i(max(1, int(rng.gauss(35, 10))))}))
        else:
            others = COMMANDS[2:]
            cmd, _, mean = rng.choices(others, weights=[w for _, w, _ in others])[0]
            eid += 1
            records.append(_record("executed", eid, t, {
                "kw.executed.command": _s(cmd), "kw.executed.status": _s("success"),
                "kw.executed.elapsed_time_in_secs": _i(max(0, int(rng.expovariate(1 / mean))))}))
    return {"resourceLogs": [{
        "resource": {"attributes": [{"key": "service.name", "value": _s("kworkflow")},
                                    {"key": "service.instance.id", "value": _s(iid)}]},
        "scopeLogs": [{"scope": {"name": "kw_tele", "version": "1"}, "logRecords": records}]}]}


def build_payloads(seed=42, installs=300):
    rng = random.Random(seed)
    rare_owner = {}
    # Each rare name goes to 1-3 installs, always below RARE_K.
    for name in RARE:
        for idx in rng.sample(range(installs), rng.randint(1, 3)):
            rare_owner.setdefault(idx, []).append(name)
    return [_install(rng, rare_owner.get(n, [])) for n in range(installs)]


def post(sink, payloads, batch=25):
    for n in range(0, len(payloads), batch):
        merged = {"resourceLogs": [rl for p in payloads[n:n + batch] for rl in p["resourceLogs"]]}
        req = urllib.request.Request(sink.rstrip("/") + "/v1/logs", json.dumps(merged).encode(),
                                     {"content-type": "application/json"})
        urllib.request.urlopen(req).read()


def digest(payloads):
    return hashlib.sha256(json.dumps(payloads, sort_keys=True).encode()).hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sink", default="http://127.0.0.1:8000")
    ap.add_argument("--installs", type=int, default=300)
    a = ap.parse_args()
    payloads = build_payloads(a.seed, a.installs)
    post(a.sink, payloads)
    n = sum(len(sl["logRecords"]) for p in payloads for rl in p["resourceLogs"] for sl in rl["scopeLogs"])
    print(f"MOCKED: seed={a.seed} installs={len(payloads)} records={n} sha256={digest(payloads)[:12]} -> {a.sink}")


if __name__ == "__main__":
    main()
