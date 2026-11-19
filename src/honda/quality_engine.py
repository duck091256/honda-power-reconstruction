"""Shared relational map plan and adapters for DuckDB and Spark."""
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from .quality_inputs import log_rows
from .source import BIN

DAY = 86_400_000_000
KEY = ("meter_id", "start_us", "end_us")
TABLES = {
    "episodes": (*KEY, "bins"),
    "episode_reasons": (*KEY, "reason", "bins"),
    "timeline": ("bin_us", "in_scope", "valid", "invalid"),
    "support": (*KEY, "bins", "supported_bins", "continuous_channels"),
    "log_links": (*KEY, "interval_index", "log_start_us", "log_end_us"),
}
SORT = {"episodes": KEY, "episode_reasons": (*KEY, "reason"),
        "timeline": ("bin_us",), "support": KEY, "log_links": (*KEY, "interval_index")}
PANEL_COLUMNS = ("meter_id", "bin_us", "valid_ref", "reason")
LOG_SCHEMA = pa.schema([("meter_id", pa.string()), ("interval_index", pa.int64()),
                        ("log_start_us", pa.int64()), ("log_end_us", pa.int64())])


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def day(column):
    return f"CAST(FLOOR(({column}) / {DAY}) AS BIGINT)"


class Duck:
    def __init__(self, conn, codec):
        self.conn = conn
        self.codec = codec

    def read(self, name, paths, columns):
        names = ", ".join(sql_string(path) for path in paths)
        self.conn.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS "
                          f"SELECT {', '.join(columns)} FROM read_parquet([{names}])")

    def stage(self, name, sql):
        self.conn.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS {sql}")

    def save(self, path, sql):
        self.conn.execute(f"COPY ({sql}) TO {sql_string(path)} (FORMAT parquet, COMPRESSION {self.codec})")

    def rows(self, sql):
        return self.conn.execute(sql).fetchall()

    def days(self, relation, start, last):
        return f"FROM {relation}, UNNEST(generate_series({day(start)}, {day(last)})) AS d(bucket)"


class Spark:
    def __init__(self, session, codec, scratch):
        self.session = session
        self.codec = codec
        self.scratch = scratch

    def read(self, name, paths, columns):
        self.session.read.parquet(*map(str, paths)).select(*columns).createOrReplaceTempView(name)

    def stage(self, name, sql):
        path = self.scratch / name
        self.session.sql(sql).write.parquet(str(path), compression=self.codec)
        self.session.read.parquet(str(path)).createOrReplaceTempView(name)

    def save(self, path, sql):
        with TemporaryDirectory(dir=self.scratch) as folder:
            self.session.sql(sql).coalesce(1).write.parquet(folder, mode="overwrite", compression=self.codec)
            parts = list(Path(folder).glob("*.parquet"))
            if len(parts) != 1:
                raise ValueError(f"Expected one Parquet data file for {path}, found {parts}")
            parts[0].replace(path)

    def rows(self, sql):
        return [tuple(row) for row in self.session.sql(sql).collect()]

    def days(self, relation, start, last):
        return f"FROM {relation} LATERAL VIEW explode(sequence({day(start)}, {day(last)})) d AS bucket"


@contextmanager
def engine(name, config, scratch):
    scratch.mkdir(parents=True, exist_ok=True)
    if name == "duckdb":
        import duckdb
        with duckdb.connect(config={"threads": str(config["threads"]),
                                    "memory_limit": config["duckdb_memory_limit"],
                                    "preserve_insertion_order": "false",
                                    "max_temp_directory_size": f'{config["scratch_limit_bytes"]}B',
                                    "temp_directory": str(scratch / "duck")}) as conn:
            yield Duck(conn, config["codec"])
    elif name == "spark":
        from pyspark.sql import SparkSession

        session = (
            SparkSession.builder.master(f'local[{config["threads"]}]')
            .appName("Honda quality map")
            .config("spark.driver.memory", config["spark_heap"])
            .config("spark.sql.shuffle.partitions", str(config["spark_shuffle_partitions"]))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.local.dir", str(scratch / "spark_local"))
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        try:
            yield Spark(session, config["codec"], scratch)
        finally:
            session.stop()
    else:
        raise ValueError(f"Unknown engine: {name}")


def load_panel(db, panel, meters):
    db.read("panel", [panel / f"{m}.parquet" for m in meters], PANEL_COLUMNS)


def load_map(db, output):
    for name, columns in TABLES.items():
        db.read(name, [output / f"{name}.parquet"], columns)


def build(db, panel, meters, catalog, output, scratch):
    output.mkdir(parents=True)
    load_panel(db, panel, meters)
    intervals = log_rows(catalog, meters)
    log_path = scratch / "intervals.parquet"
    pq.write_table(pa.Table.from_pylist([dict(zip(LOG_SCHEMA.names, row)) for row in intervals],
                                       schema=LOG_SCHEMA), log_path, compression=db.codec)
    db.read("intervals", [log_path], LOG_SCHEMA.names)

    # Subtract the position within each meter/validity stream. The difference
    # is constant exactly while neighboring cells in that stream are adjacent.
    db.stage("segments", f"""
        WITH assigned AS (
          SELECT *, bin_us-CAST({BIN} AS BIGINT)*row_number() OVER
                 (PARTITION BY meter_id, valid_ref ORDER BY bin_us) AS streak
          FROM panel
        )
        SELECT meter_id, streak, valid_ref, reason, min(bin_us) AS reason_start_us,
               max(bin_us)+{BIN} AS reason_end_us, count(*) AS bins
        FROM assigned GROUP BY meter_id, streak, valid_ref, reason
    """)
    db.stage("invalid_episodes", """
        SELECT meter_id, streak, min(reason_start_us) AS start_us, max(reason_end_us) AS end_us,
               CAST(sum(bins) AS BIGINT) AS bins
        FROM segments WHERE NOT valid_ref GROUP BY meter_id, streak
    """)
    db.save(output / "episodes.parquet", """
        SELECT meter_id, start_us, end_us, bins FROM invalid_episodes
    """)
    db.read("episodes", [output / "episodes.parquet"], TABLES["episodes"])
    db.save(output / "episode_reasons.parquet", """
        SELECT e.meter_id, e.start_us, e.end_us, s.reason, s.bins
        FROM invalid_episodes e JOIN segments s ON e.meter_id=s.meter_id AND e.streak=s.streak
        WHERE NOT s.valid_ref
    """)
    db.save(output / "timeline.parquet", """
        SELECT bin_us, count(*) AS in_scope, count(CASE WHEN valid_ref THEN 1 END) AS valid,
               count(CASE WHEN NOT valid_ref THEN 1 END) AS invalid
        FROM panel GROUP BY bin_us
    """)
    db.read("timeline", [output / "timeline.parquet"], TABLES["timeline"])
    db.stage("covered", """
        SELECT bin_us, CAST(sum(CASE WHEN valid>0 THEN 1 ELSE 0 END) OVER (ORDER BY bin_us
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BIGINT) AS prefix,
          CASE WHEN valid>0 THEN 1 ELSE 0 END AS any_valid
        FROM timeline
    """)

    # Index streaks by UTC day. Only the bucket of the episode start can produce
    # matches, and only a single valid streak of a given meter can contain it.
    db.stage("valid_streaks_by_day", f"""
        SELECT meter_id, reason_start_us AS start_us, reason_end_us AS end_us, bucket
        {db.days("(SELECT * FROM segments WHERE valid_ref) s", "reason_start_us", f"reason_end_us-{BIN}")}
    """)
    db.save(output / "support.parquet", f"""
        WITH continuous AS (
          SELECT e.meter_id, e.start_us, e.end_us, count(*) AS channels
          FROM episodes e JOIN valid_streaks_by_day v
            ON {day("e.start_us")}=v.bucket AND e.meter_id<>v.meter_id
            AND v.start_us<=e.start_us AND v.end_us>=e.end_us
          GROUP BY e.meter_id, e.start_us, e.end_us
        )
        SELECT e.meter_id, e.start_us, e.end_us, e.bins,
               z.prefix-a.prefix+a.any_valid AS supported_bins,
               coalesce(c.channels, 0) AS continuous_channels
        FROM episodes e JOIN covered a ON a.bin_us=e.start_us
        JOIN covered z ON z.bin_us=e.end_us-{BIN}
        LEFT JOIN continuous c ON e.meter_id=c.meter_id AND e.start_us=c.start_us AND e.end_us=c.end_us
    """)

    # Intervals can meet in several buckets. Retain only the bucket of their
    # later start, which exists exactly once when the intersection is nonempty.
    db.save(output / "log_links.parquet", f"""
        WITH ep AS (
          SELECT meter_id, start_us, end_us, bucket {db.days("episodes", "start_us", "end_us-1")}
        ), ls AS (
          SELECT meter_id, interval_index, log_start_us, log_end_us, bucket
          {db.days("intervals", "log_start_us", "log_end_us")}
        )
        SELECT e.meter_id, e.start_us, e.end_us, l.interval_index, l.log_start_us, l.log_end_us
        FROM ep e JOIN ls l ON e.meter_id=l.meter_id AND e.bucket=l.bucket
          AND e.bucket={day("greatest(e.start_us,l.log_start_us)")}
          AND l.log_start_us<e.end_us AND l.log_end_us>=e.start_us
    """)
    return len(intervals)


def streak_channel_names(db, keys, scratch):
    if not keys:
        return []
    path = scratch / "audit_keys.parquet"
    schema = pa.schema([("meter_id", pa.string()), ("start_us", pa.int64()), ("end_us", pa.int64())])
    pq.write_table(pa.Table.from_pylist([dict(zip(KEY, row)) for row in keys], schema=schema), path)
    db.read("audit_keys", [path], KEY)
    return db.rows(f"""
        SELECT e.meter_id, e.start_us, e.end_us, v.meter_id
        FROM audit_keys e JOIN valid_streaks_by_day v
          ON {day("e.start_us")}=v.bucket AND e.meter_id<>v.meter_id
          AND v.start_us<=e.start_us AND v.end_us>=e.end_us
        ORDER BY e.meter_id, e.start_us, e.end_us, v.meter_id
    """)


def detail(db, key):
    meter, start, end = key
    where = f"meter_id={sql_string(meter)} AND start_us={int(start)} AND end_us={int(end)}"
    # Query all tables under the same identity and materialize all rows returned.
    episode = db.rows(f"SELECT bins, supported_bins, continuous_channels FROM support WHERE {where}")
    if len(episode) != 1:
        raise ValueError(f"Expected exactly one episode for {key}")
    return dict(key=list(key), support=list(episode[0]),
                reasons=[list(row) for row in db.rows(
                    f"SELECT reason, bins FROM episode_reasons WHERE {where} ORDER BY reason")],
                channels=[list(row) for row in db.rows(f"""
                    SELECT meter_id, count(*) AS bins FROM panel WHERE valid_ref
                      AND meter_id<>{sql_string(meter)} AND bin_us>={int(start)} AND bin_us<{int(end)}
                    GROUP BY meter_id ORDER BY meter_id
                """)],
                logs=[list(row) for row in db.rows(
                    f"SELECT interval_index, log_start_us, log_end_us FROM log_links "
                    f"WHERE {where} ORDER BY interval_index")])
