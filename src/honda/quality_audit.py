"""Independent panel scan, comparison keys, and canonical hashes for the map."""
import hashlib
from collections import Counter
from statistics import median

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .quality_engine import KEY, SORT, TABLES
from .quality_inputs import log_rows
from .run import write_json
from .source import BIN


def canonical(path, name):
    schema = pa.schema([(c, pa.string() if c in ("meter_id", "reason") else pa.int64())
                        for c in TABLES[name]])
    table = pq.read_table(path, columns=schema.names).cast(schema).combine_chunks()
    return table.sort_by([(key, "ascending") for key in SORT[name]])


def digest(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table.combine_chunks())
    return hashlib.sha256(memoryview(sink.getvalue())).hexdigest()


def signature(output):
    result = {}
    for name in TABLES:
        table = canonical(output / f"{name}.parquet", name)
        result[name] = dict(rows=table.num_rows, sha256=digest(table))
    return result


def assert_rows(run, checks, label, name, output, rows):
    path = output / f"{name}.parquet"
    table = canonical(path, name)
    expected = pa.Table.from_pylist([dict(zip(TABLES[name], r)) for r in sorted(rows)],
                                   schema=table.schema)
    if not expected.equals(table):
        actual = table.to_pylist()
        wanted = expected.to_pylist()
        for index in range(max(len(actual), len(wanted))):
            a = actual[index] if index < len(actual) else None
            e = wanted[index] if index < len(wanted) else None
            if a != e:
                write_json(run.path / "first_mismatch.json",
                           dict(table=name, row=index, expected=e, actual=a))
                break
        checks.check(label, digest(expected), digest(table), "first_mismatch.json")
    checks.check(label, len(rows), table.num_rows, path)


def compare_maps(run, checks, label, left, right, expected, actual):
    for name in TABLES:
        if actual[name] != expected[name]:
            reference = canonical(left / f"{name}.parquet", name)
            result = canonical(right / f"{name}.parquet", name)
            for index in range(max(reference.num_rows, result.num_rows)):
                a = reference.slice(index, 1).to_pylist()
                b = result.slice(index, 1).to_pylist()
                if a != b:
                    write_json(run.path / "first_mismatch.json",
                               dict(comparison=label, table=name, row=index, expected=a, actual=b))
                    break
            checks.check(f"{label}/{name}", expected[name], actual[name], "first_mismatch.json")
    checks.check(label, expected, actual, right)


class PanelAudit:
    """Use indexed NumPy windows over the original ordered UTC cell grid."""

    def __init__(self, panel, specs, catalog, reasons):
        self.meters = sorted(specs)
        self.origin = min(s["start_us"] for s in specs.values())
        size = (max(s["end_us"] for s in specs.values()) - self.origin) // BIN
        scoped = np.zeros(size, np.int32)
        valid = np.zeros(size, np.int32)
        self.episodes = {}
        self.reasons = {}
        self.prefixes = {}
        self.logs = {}
        for meter, index, left, right in log_rows(catalog, self.meters):
            self.logs.setdefault(meter, []).append((index, left, right))
        for meter in self.meters:
            path = panel / f"{meter}.parquet"
            table = pq.read_table(path, columns=["bin_us", "valid_ref", "reason"]).sort_by("bin_us")
            ticks = table["bin_us"].to_numpy()
            flags = table["valid_ref"].to_numpy()
            labels = table["reason"].combine_chunks().dictionary_encode()
            names = labels.dictionary.to_pylist()
            indices = labels.indices.to_numpy()
            # Q01 proves each meter is a dense aligned grid between the registered bounds.
            offset = (int(ticks[0]) - self.origin) // BIN
            scoped[offset:offset + len(ticks)] += 1
            valid[offset:offset + len(ticks)] += flags
            self.prefixes[meter] = (offset, np.r_[0, np.cumsum(flags, dtype=np.int32)])
            change = np.diff(np.r_[False, ~flags, False].astype(np.int8))
            starts = np.flatnonzero(change == 1)
            stops = np.flatnonzero(change == -1)
            keys = [(meter, int(ticks[a]), int(ticks[b - 1]) + BIN) for a, b in zip(starts, stops)]
            self.episodes[meter] = [(*key, int(b - a)) for key, a, b in zip(keys, starts, stops)]
            for reason in reasons:
                if reason == "valid" or reason not in names:
                    continue
                cumulative = np.r_[0, np.cumsum(indices == names.index(reason), dtype=np.int32)]
                counts = cumulative[stops] - cumulative[starts]
                for index in np.flatnonzero(counts):
                    key = keys[index]
                    self.reasons.setdefault(key, []).append([reason, int(counts[index])])
        self.any_prefix = np.r_[0, np.cumsum(valid > 0, dtype=np.int32)]
        self.timeline = [(self.origin + int(i) * BIN, int(scoped[i]), int(valid[i]), int(scoped[i]-valid[i]))
                         for i in np.flatnonzero(scoped)]

    def channels(self, key):
        meter, start, end = key
        lo, hi = (start - self.origin) // BIN, (end - self.origin) // BIN
        channels = []
        for other, (offset, prefix) in self.prefixes.items():
            if other == meter or lo >= offset + len(prefix) - 1 or hi <= offset:
                continue
            bins = int(prefix[min(hi - offset, len(prefix) - 1)] - prefix[max(0, lo - offset)])
            if bins:
                channels.append([other, bins])
        return channels

    def supported(self, key):
        _, start, end = key
        return int(self.any_prefix[(end - self.origin) // BIN] -
                   self.any_prefix[(start - self.origin) // BIN])

    def detail(self, key):
        meter, start, end = key
        bins = (end - start) // BIN
        channels = self.channels(key)
        continuous = sum(n == bins for _, n in channels)
        return dict(key=list(key), support=[bins, self.supported(key), continuous],
                    reasons=sorted(self.reasons[key]), channels=channels,
                    logs=[[index, a, b] for index, a, b in self.logs.get(meter, []) if a < end and b >= start])

    def audit_keys(self):
        keys = []
        for rows in self.episodes.values():
            if rows:
                longest = min(rows, key=lambda row: (-row[3], row[:3]))
                keys.extend(dict.fromkeys([rows[0][:3], rows[-1][:3], longest[:3]]))
        return keys

    def links(self):
        for meter, episodes in self.episodes.items():
            if not episodes:
                continue
            starts = np.array([row[1] for row in episodes])
            ends = np.array([row[2] for row in episodes])
            logs = self.logs.get(meter, [])
            lower = ends.searchsorted([row[1] for row in logs], side="right")
            upper = starts.searchsorted([row[2] for row in logs], side="right")
            for log, i, j in zip(logs, lower, upper):
                for ep in episodes[i:j]:
                    yield (*ep[:3], *log)


def sample_keys(episodes):
    def key(row):
        return tuple(row[k] for k in KEY)

    if not episodes:
        return []
    midpoint = median(row["bins"] for row in episodes)
    picked = [min(episodes, key=lambda r: (r["bins"], key(r))),
              min(episodes, key=lambda r: (-r["bins"], key(r))),
              min(episodes, key=lambda r: (abs(r["bins"] - midpoint), key(r)))]
    return list(dict.fromkeys(map(key, picked)))


def verify_map(run, checks, label, output, audit, counts):
    episodes = [row for rows in audit.episodes.values() for row in rows]
    reasons = [(*key, reason, count) for key, rs in audit.reasons.items() for reason, count in rs]
    assert_rows(run, checks, f"{label} exact episode partition of all invalid cells", "episodes", output, episodes)
    assert_rows(run, checks, f"{label} all reasons per episode", "episode_reasons", output, reasons)
    assert_rows(run, checks, f"{label} per-tick denominator and state accounting", "timeline", output, audit.timeline)
    assert_rows(run, checks, f"{label} inclusive/exclusive log overlaps", "log_links", output, list(audit.links()))

    cells_by_reason = Counter()
    for _, _, _, reason, cells in reasons:
        cells_by_reason[reason] += cells
    expected = {r: n for r, n in counts.items() if r != "valid"}
    checks.check(f"{label} Q01 invalid cells per reason", expected, dict(cells_by_reason), output)
    checks.check(f"{label} total episode cells", sum(expected.values()), sum(r[3] for r in episodes), output)

    support = canonical(output / "support.parquet", "support").to_pylist()
    wanted = [(*row, audit.supported(row[:3])) for row in sorted(episodes)]
    actual = [tuple(row[k] for k in TABLES["support"][:-1]) for row in support]
    if wanted != actual:
        first = next((i for i, (a, b) in enumerate(zip(wanted, actual)) if a != b),
                     min(len(wanted), len(actual)))
        write_json(run.path / "first_mismatch.json",
                   dict(table="support", expected=wanted[first:first+1], actual=actual[first:first+1]))
    checks.check(f"{label} support keys and exact supported bins", True, wanted == actual, output)
    checks.check(f"{label} support bounds", True,
                 all(0 <= r["supported_bins"] <= r["bins"] and
                     0 <= r["continuous_channels"] < len(audit.meters) and
                     (not r["continuous_channels"] or r["supported_bins"] == r["bins"])
                     for r in support), output)

    by_key = {tuple(r[k] for k in KEY): r["continuous_channels"] for r in support}
    crosschecks = []
    for key in audit.audit_keys():
        independent = audit.detail(key)
        observed = by_key[key]
        if independent["support"][2] != observed:
            write_json(run.path / "first_mismatch.json", dict(expected=independent, actual=observed))
        checks.check(f"{label} continuous support {key}", independent["support"][2], observed, output)
        crosschecks.append(independent)

    query_keys = sample_keys([dict(zip(TABLES["episodes"], row)) for row in episodes])
    expected_queries = [audit.detail(key) for key in query_keys]
    return dict(episodes=len(episodes), invalid_bins=sum(expected.values()),
                valid_bins=sum(row[2] for row in audit.timeline),
                channels_with_episodes=sum(bool(rows) for rows in audit.episodes.values()),
                episodes_with_continuous_support=sum(r["continuous_channels"] > 0 for r in support),
                duration_bins=sorted(Counter(r[3] for r in episodes).items()),
                crosschecks=crosschecks, queries=expected_queries, signature=signature(output))
