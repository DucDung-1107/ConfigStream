# Number verification

Recomputed from `workloads/results/**` and `docs/phase12-acceptance.md`; compared with `docs/configstream_paper.md`.

**82 / 82 checks pass.**

| ID | Quantity | Recomputed | Paper | Status |
|---|---|---:|---:|---|
| T1 | TerraDS file incarnations | 6505 | 6505 | PASS |
| T2 | TerraDS versions (revisions) | 10188 | 10188 | PASS |
| T3 | TerraDS logical UTF-8 bytes | 12278085 | 12278085 | PASS |
| T4 | TerraDS adjacent pairs = diff.added.count | 3683 | 3683 | PASS |
| T5 | pairs = versions - incarnations | 3683 | 3683 | PASS |
| T6 | sum_f (n_f - 1) from histogram | 3683 | 3683 | PASS |
| T7 | sum_f n_f from histogram | 10188 | 10188 | PASS |
| T8 | share of files with one version (%) | 77.48 | 77.48 | PASS |
| M1 | successful matched trials | 27 | 27 | PASS |
| M2 | failed matched trials in corrected campaign | 0 | 0 | PASS |
| M3 | all trials exact PASS | 27 | 27 | PASS |
| M4 | combined attempt elapsed (s) | 502.981 | 502.981 | PASS |
| M5-rpki-B0-n | rpki B0 repetitions | 3 | 3 | PASS |
| M6-rpki-B0-med | rpki B0 median (s) | 6.297 | 6.297 | PASS |
| M6-rpki-B0-min | rpki B0 min (s) | 6.282 | 6.282 | PASS |
| M6-rpki-B0-max | rpki B0 max (s) | 12.577 | 12.577 | PASS |
| M5-rpki-B2-n | rpki B2 repetitions | 3 | 3 | PASS |
| M6-rpki-B2-med | rpki B2 median (s) | 16.84 | 16.84 | PASS |
| M6-rpki-B2-min | rpki B2 min (s) | 14.729 | 14.729 | PASS |
| M6-rpki-B2-max | rpki B2 max (s) | 21.08 | 21.08 | PASS |
| M5-rpki-B3-n | rpki B3 repetitions | 3 | 3 | PASS |
| M6-rpki-B3-med | rpki B3 median (s) | 25.417 | 25.417 | PASS |
| M6-rpki-B3-min | rpki B3 min (s) | 23.144 | 23.144 | PASS |
| M6-rpki-B3-max | rpki B3 max (s) | 25.424 | 25.424 | PASS |
| M5-ris-B0-n | ris B0 repetitions | 3 | 3 | PASS |
| M6-ris-B0-med | ris B0 median (s) | 10.485 | 10.485 | PASS |
| M6-ris-B0-min | ris B0 min (s) | 8.437 | 8.437 | PASS |
| M6-ris-B0-max | ris B0 max (s) | 12.636 | 12.636 | PASS |
| M5-ris-B2-n | ris B2 repetitions | 3 | 3 | PASS |
| M6-ris-B2-med | ris B2 median (s) | 21.167 | 21.167 | PASS |
| M6-ris-B2-min | ris B2 min (s) | 18.967 | 18.967 | PASS |
| M6-ris-B2-max | ris B2 max (s) | 23.186 | 23.186 | PASS |
| M5-ris-B3-n | ris B3 repetitions | 3 | 3 | PASS |
| M6-ris-B3-med | ris B3 median (s) | 29.644 | 29.644 | PASS |
| M6-ris-B3-min | ris B3 min (s) | 23.233 | 23.233 | PASS |
| M6-ris-B3-max | ris B3 max (s) | 29.726 | 29.726 | PASS |
| M5-dns-B0-n | dns B0 repetitions | 3 | 3 | PASS |
| M6-dns-B0-med | dns B0 median (s) | 2.089 | 2.089 | PASS |
| M6-dns-B0-min | dns B0 min (s) | 2.087 | 2.087 | PASS |
| M6-dns-B0-max | dns B0 max (s) | 6.302 | 6.302 | PASS |
| M5-dns-B2-n | dns B2 repetitions | 3 | 3 | PASS |
| M6-dns-B2-med | dns B2 median (s) | 10.536 | 10.536 | PASS |
| M6-dns-B2-min | dns B2 min (s) | 10.486 | 10.486 | PASS |
| M6-dns-B2-max | dns B2 max (s) | 12.584 | 12.584 | PASS |
| M5-dns-B3-n | dns B3 repetitions | 3 | 3 | PASS |
| M6-dns-B3-med | dns B3 median (s) | 16.866 | 16.866 | PASS |
| M6-dns-B3-min | dns B3 min (s) | 12.624 | 12.624 | PASS |
| M6-dns-B3-max | dns B3 max (s) | 16.924 | 16.924 | PASS |
| M7-rpki | RPKI rows per trial | 360226 | 360226 | PASS |
| M7-ris | RIS occurrences per trial | 370141 | 370141 | PASS |
| M7-dns | DNS raw measurements per trial | 4096 | 4096 | PASS |
| M8-rpki | RPKI rows over the 3 B3 trials | 1080678 | 1080678 | PASS |
| M8-ris | RIS occurrences over the 3 B3 trials | 1110423 | 1110423 | PASS |
| M8-dns | DNS rows over the 3 B3 trials | 12288 | 12288 | PASS |
| M9 | sum parquet bytes over 27 trials | 1622766994 | 1622766994 | PASS |
| M10 | sum MinIO bytes over 27 trials | 1623023143 | 1623023143 | PASS |
| M11 | B3 trials with zero final lag | 9 | 9 | PASS |
| R1 | retained growth (bytes) | 3869485576 | 3869485576 | PASS |
| R2 | peak sampled process-tree RSS (bytes) | 1612824576 | 1612824576 | PASS |
| R3 | min guest RAM (bytes) | 4191440896 | 4191440896 | PASS |
| P1 | R1 analytical bytes | 47756423 | 47756423 | PASS |
| P2 | R2 analytical bytes | 3555516 | 3555516 | PASS |
| P3 | R3 analytical bytes | 6954890 | 6954890 | PASS |
| P4 | R2 saving vs R1 (%) | 92.55 | 92.55 | PASS |
| P5 | R3 saving vs R1 (%) | 85.44 | 85.44 | PASS |
| P6 | R2 worst query median ratio <= 2 | True | True | PASS |
| P7 | R3 worst query median ratio <= 2 | True | True | PASS |
| P8 | R2/R3 status PREFERRED | ('PREFERRED', 'PREFERRED') | ('PREFERRED', 'PREFERRED') | PASS |
| P9 | all query outputs exact PASS | True | True | PASS |
| H1 | H1 decision | SUPPORTED_WITHIN_TESTED_SCOPE | SUPPORTED_WITHIN_TESTED_SCOPE | PASS |
| H2 | H2 decision | SUPPORTED_WITHIN_TESTED_SCOPE | SUPPORTED_WITHIN_TESTED_SCOPE | PASS |
| H3 | H3 decision | NOT_RUN | NOT_RUN | PASS |
| Q1 | publisher seconds / rate | 20.375 / 500.023 | 500.023 | PASS |
| Q2 | streaming incl. drain / logical rate | 68.742 s / 148.205 | 68.742 / 148.205 | PASS |
| Q3 | startup / drain / total | 6.422 / 48.367 / 76.071 | - | PASS |
| Q4 | Parquet bytes | 29402997 | 29,402,997 | PASS |
| Q5 | repeat Parquet bytes | 29346912 | 29,346,912 | PASS |
| Q6 | repeat drain rate | 72.998 s / 139.565 | 139.565 | PASS |
| Q7 | replay version cap 75, retention 1000000 h | Watermark and timeout 1000000 | cap 75 | PASS |
| Q8 | accepted logical rate = 10188/68.742 | 148.206 | 148.205 | PASS |
| Q9 | headroom rate = 10188/72.998 | 139.565 | 139.565 | PASS |
| Q10 | publisher rate = 10188/20.375 | 500.025 | 500.023 | PASS |
