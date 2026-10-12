# Owned RPKI fixtures

These CSVs are authored by this project, not RIPE data. Documentation prefixes
and private ASNs exercise one duplicate tuple, IPv6 removal, IPv4 addition,
source-row retries and repeated observations. Compression/checksums are produced
in pytest temporary storage. No external records or third-party scripts execute.

Expected memberships: day 1 has 4 source rows / 3 unique tuples; day 2 has 3 / 3.
Boundary: add AS64503/203.0.113.0/24/24, remove AS64502/2001:db8::/32/48;
two unchanged tuples. Expected sets/actions are independently authored, not
computed by Spark. The independent oracle uses stdlib SQLite/IP parsing; optional
Parquet export is only enabled in Spark/real-data tests.
