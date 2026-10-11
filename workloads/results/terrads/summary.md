# Measured evidence comparison

Units differ: static files versus observed/controlled states; estimates are not observations.
CIs and rejection assumptions: `population-estimate.json`.

| Dataset | Evidence | Assets/repos | Files/states | Logical bytes |
| --- | --- | ---: | ---: | ---: |
| terraform | observed | 6505 | 10188 | 12278085 |
| frr-nginx | observed | 1906 | 2525 | 1133998 |
| frr | observed | 1900 | 2516 | 1109458 |
| nginx | observed | 6 | 9 | 24540 |
| controlled-reference | controlled | 100 | 10000 | 12542677 |
| phase11-calibrated | calibrated | 76240 | 100518 | 45002724 |
| phase10-1m | stress/semi-synthetic | 10000 | 1000000 | 1144675896 |
| batfish | real baseline, not temporal | 324 | 324 | 361177 |
| terrads-static | real static, not temporal | 82749 | 1513378 | 2601501820 |
| terrads-population-estimate | inferred, NOT observed | 62068.64966452637 | 1684251.0484092056 | 2029291960.3685882 |

Temporal distributions: `temporal-comparison.csv`; sampler/funnel remain unchanged.
