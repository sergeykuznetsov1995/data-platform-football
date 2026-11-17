# FotMob #2256 — frozen Bronze completion audit

Counts below are current-equivalent at the exact Iceberg snapshots in snapshots.json; they are not live source measurements.

Primary #2258 required cohort: **603039 known scoped IDs**, **563878 finished**, **555006 finished with current payload**, **8872 finished without current payload** (literal #1294 rule). Actionable applicable gap: **8825**.

Supplementary union including marker-only/historical scopes: 606056 known scoped IDs, 565058 finished, **556186 finished with current payload**, **8872 finished without current payload** (literal #1294 rule). Actionable applicable gap: **8825**.

Cohort flags overlap; scope-summary.csv contains each physical scope exactly once.

All current calendar rows: 556188 scoped payload rows. Global distinct known IDs: 603080; current payload IDs: 556188; finished IDs: 562171; IDs with any scoped literal gap: 8872. Shared IDs can be present in one scope and absent in another.

## Snapshot and evidence identity

| Table | Snapshot | Made current at |
| --- | --- | --- |
| c1 | 2953056259902045724 | 2026-11-15 20:09:50.847000+00:00 |
| manifest | 3196714190286422442 | 2026-11-14 14:31:55.609000+00:00 |
| matches | 3224980456284593238 | 2026-11-14 20:32:55.991000+00:00 |
| payloads | 153801965908223392 | 2026-11-14 13:29:02.835000+00:00 |

| ID set | SHA256 |
| --- | --- |
| known_global | 2748d7fbf4c413cd11d5934f9960681e176d83ee29307998ca65bfba8789c9d2 |
| known_scoped | 0ea94bb97a6b739abb7f334d48db40d88bc383fa5147e76402ff89c98fee0842 |
| literal_gap_global | 0dd1fac1254b1deb0f27a5b1f58599282bcd5104929d0902ad7fa401c6f82a45 |
| literal_gap_scoped | 3e1eb0d4eaf5214c546650df55ed8c300364394136b56bff636e5e98beb95d1e |
| payload_global | 43cb10a125cc37cc031ea3a0e485b6233ff7f7a79ced41dce981bc91d37d468b |
| payload_scoped | 63d0b0528e706a23a78b2a7c4cecbf23f8c49d477a4b08763e2ddc33f618034f |
| required_known_global | 9b0c6e8cc23a6b7cd227fb01d4068080b0c9828dcecf03f8e33fb87965bdf7db |
| required_known_scoped | d74ce4054acb81f721cc016551dbe044744ac59bfa8bf522829b8f41e61a754c |
| required_literal_gap_global | 0dd1fac1254b1deb0f27a5b1f58599282bcd5104929d0902ad7fa401c6f82a45 |
| required_literal_gap_scoped | 3e1eb0d4eaf5214c546650df55ed8c300364394136b56bff636e5e98beb95d1e |
| required_payload_global | 9555cf08d3bc031b411ce2928d355d505b1b04e0a14d6351eac2e009f0ee26d2 |
| required_payload_scoped | b3598d91fdd5509b0045fff6a71b1b783e400871651bd28f0dc69dce0a6abed4 |

Full local evidence: `/root/data-platform-football/.local/tasks/2256-completion-audit/run-pass-c/passes/pass1/current-id-evidence.csv`; SHA256 `3ef238d46d71837edf51aa926066d8b6485159654d7d95a252f51be485205e34`. This CSV preserves every matching and missing scoped ID, including global IDs, batch identities, flags and observation times. Preserve the run directory with this report; the report alone is not a backup of full local evidence.

Full local section evidence: `/root/data-platform-football/.local/tasks/2256-completion-audit/run-pass-c/passes/pass1/section-evidence.csv`; SHA256 `6219196feb7c67e69c0ccbbdb6360f05d96d71e96d83b031b6381124f010b634`. It contains all six comparisons per selected payload and must be preserved with the frozen run. Future applicability uses utc_time + 3 hours against the single frozen captured_at `2026-11-16T18:24:44.943480+00:00`.

## Supplementary competition breakdown (union of cohorts)

| ID | Name | Scopes | Known | Finished | With payload | Literal gap | Actionable gap |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 38 | Bundesliga | 17 | 1873 | 1813 | 1812 | 1 | 0 |
| 40 | First Division A | 17 | 3140 | 2925 | 2925 | 0 | 0 |
| 41 | First Division A Qualification | 10 | 70 | 70 | 58 | 12 | 12 |
| 42 | Champions League | 17 | 2266 | 2194 | 2175 | 19 | 19 |
| 44 | Copa America | 6 | 182 | 172 | 172 | 0 | 0 |
| 45 | Copa Libertadores | 17 | 1282 | 1281 | 1281 | 0 | 0 |
| 46 | Superligaen | 17 | 2031 | 1983 | 1983 | 0 | 0 |
| 47 | Premier League | 17 | 6461 | 6181 | 6070 | 111 | 111 |
| 48 | Championship | 17 | 5569 | 5195 | 5195 | 0 | 0 |
| 50 | EURO | 10 | 330 | 279 | 279 | 0 | 0 |
| 51 | Veikkausliiga | 17 | 1712 | 1712 | 1712 | 0 | 0 |
| 52 | Veikkausliiga Qualification | 12 | 22 | 22 | 22 | 0 | 0 |
| 53 | Ligue 1 | 17 | 3508 | 3191 | 3190 | 1 | 0 |
| 54 | Bundesliga | 17 | 3060 | 2835 | 2835 | 0 | 0 |
| 55 | Serie A | 17 | 3801 | 3531 | 3531 | 0 | 0 |
| 57 | Eredivisie | 17 | 3097 | 2825 | 2825 | 0 | 0 |
| 58 | Eredivisie Qualification | 15 | 130 | 130 | 130 | 0 | 0 |
| 59 | Eliteserien | 17 | 2401 | 2369 | 2369 | 0 | 0 |
| 60 | Eliteserien Qualification | 16 | 49 | 48 | 48 | 0 | 0 |
| 61 | Liga Portugal | 17 | 3060 | 2852 | 2852 | 0 | 0 |
| 63 | Premier League | 17 | 4136 | 3978 | 3898 | 80 | 80 |
| 64 | Premiership | 17 | 2221 | 2079 | 2079 | 0 | 0 |
| 66 | Summer Olympics | 4 | 129 | 129 | 129 | 0 | 0 |
| 67 | Allsvenskan | 17 | 2430 | 2384 | 2384 | 0 | 0 |
| 68 | Allsvenskan Qualification | 17 | 22 | 20 | 20 | 0 | 0 |
| 69 | Super League | 17 | 1898 | 1844 | 1843 | 1 | 0 |
| 70 | Super League Qualification | 10 | 18 | 18 | 18 | 0 | 0 |
| 71 | Süper Lig | 17 | 3394 | 3187 | 3187 | 0 | 0 |
| 73 | Europa League | 17 | 1964 | 1888 | 1888 | 0 | 0 |
| 74 | UEFA Super Cup | 16 | 10 | 10 | 10 | 0 | 0 |
| 77 | World Cup | 10 | 656 | 656 | 656 | 0 | 0 |
| 78 | FIFA Club World Cup | 15 | 137 | 133 | 133 | 0 | 0 |
| 85 | 1. Division | 17 | 1879 | 1830 | 1830 | 0 | 0 |
| 86 | Serie B | 17 | 3940 | 3662 | 3662 | 0 | 0 |
| 87 | LaLiga | 17 | 3800 | 3540 | 3540 | 0 | 0 |
| 108 | League One | 17 | 5570 | 5024 | 5024 | 0 | 0 |
| 109 | League Two | 17 | 5573 | 5073 | 5071 | 2 | 0 |
| 110 | Ligue 2 | 17 | 3688 | 3280 | 3280 | 0 | 0 |
| 111 | Eerste Divisie | 17 | 3804 | 3469 | 3469 | 0 | 0 |
| 112 | Liga Profesional | 30 | 4906 | 4853 | 3893 | 960 | 960 |
| 113 | A-League | 17 | 1579 | 1446 | 1446 | 0 | 0 |
| 114 | Friendlies | 1 | 432 | 368 | 368 | 0 | 0 |
| 116 | Cymru Premier | 17 | 2019 | 1847 | 1846 | 1 | 0 |
| 117 | National League | 17 | 5530 | 4974 | 4971 | 3 | 0 |
| 118 | Premier League | 17 | 1534 | 1348 | 1346 | 2 | 2 |
| 119 | 2. Liga | 17 | 2346 | 2172 | 2171 | 1 | 0 |
| 120 | Super League | 17 | 2561 | 2322 | 2322 | 0 | 0 |
| 121 | Primera Division | 17 | 2456 | 2443 | 2443 | 0 | 0 |
| 122 | 1. Liga | 17 | 2721 | 2586 | 2586 | 0 | 0 |
| 123 | Championship | 17 | 1759 | 1605 | 1604 | 1 | 0 |
| 124 | League One | 17 | 1780 | 1584 | 1582 | 2 | 0 |
| 125 | League Two | 17 | 1775 | 1572 | 1572 | 0 | 0 |
| 126 | Premier Division | 17 | 1819 | 1729 | 1694 | 35 | 34 |
| 127 | Ligat ha'Al | 17 | 2342 | 2230 | 2230 | 0 | 0 |
| 128 | Leumit League | 17 | 2935 | 2767 | 2766 | 1 | 1 |
| 129 | Premiership | 17 | 2251 | 2128 | 2125 | 3 | 0 |
| 130 | MLS | 17 | 4858 | 4598 | 4598 | 0 | 0 |
| 131 | Liga 1 | 17 | 3292 | 3167 | 3167 | 0 | 0 |
| 132 | FA Cup | 17 | 1472 | 1449 | 1449 | 0 | 0 |
| 133 | EFL Cup | 17 | 921 | 917 | 916 | 1 | 0 |
| 134 | Coupe de France | 17 | 3717 | 3622 | 3279 | 343 | 343 |
| 135 | Super League 1 | 17 | 2334 | 2221 | 2221 | 0 | 0 |
| 136 | 1. Division | 17 | 2260 | 2111 | 2111 | 0 | 0 |
| 137 | Scottish Cup | 17 | 1035 | 1013 | 1009 | 4 | 3 |
| 138 | Copa del Rey | 17 | 1228 | 1221 | 1221 | 0 | 0 |
| 139 | Supercopa de España | 16 | 27 | 24 | 24 | 0 | 0 |
| 140 | LaLiga2 | 17 | 4674 | 4352 | 4352 | 0 | 0 |
| 141 | Coppa Italia | 17 | 587 | 569 | 569 | 0 | 0 |
| 142 | EFL Trophy | 17 | 1239 | 1231 | 1231 | 0 | 0 |
| 143 | Suomen Cup | 17 | 657 | 653 | 652 | 1 | 1 |
| 144 | Primera Division | 18 | 2779 | 2722 | 2722 | 0 | 0 |
| 145 | Greece Cup | 17 | 880 | 853 | 799 | 54 | 54 |
| 146 | 2. Bundesliga | 17 | 3062 | 2855 | 2853 | 2 | 0 |
| 147 | Serie C | 17 | 11688 | 10380 | 10377 | 3 | 3 |
| 149 | Belgian Cup | 17 | 368 | 360 | 360 | 0 | 0 |
| 151 | Turkish Cup | 17 | 1487 | 1459 | 1425 | 34 | 33 |
| 161 | Liga AUF Uruguaya | 17 | 2918 | 2911 | 2911 | 0 | 0 |
| 163 | Challenge League | 17 | 1711 | 1686 | 1686 | 0 | 0 |
| 164 | Swiss Cup | 17 | 596 | 588 | 588 | 0 | 0 |
| 165 | 1. Lig | 17 | 3399 | 3158 | 3158 | 0 | 0 |
| 166 | Super Cup | 15 | 12 | 12 | 12 | 0 | 0 |
| 168 | Superettan | 17 | 2430 | 2400 | 2400 | 0 | 0 |
| 169 | Ettan | 17 | 4716 | 4686 | 4686 | 0 | 0 |
| 171 | Svenska Cupen | 17 | 1139 | 1135 | 1135 | 0 | 0 |
| 172 | Superettan Qualification | 17 | 44 | 40 | 40 | 0 | 0 |
| 173 | Prva Liga | 17 | 1801 | 1677 | 1677 | 0 | 0 |
| 174 | Slovenia Cup | 17 | 182 | 168 | 168 | 0 | 0 |
| 176 | 1. liga | 17 | 1878 | 1800 | 1799 | 1 | 1 |
| 177 | Slovak Cup | 17 | 369 | 365 | 365 | 0 | 0 |
| 179 | Challenge Cup | 16 | 696 | 684 | 684 | 0 | 0 |
| 180 | League Cup | 17 | 950 | 949 | 949 | 0 | 0 |
| 181 | Premiership playoff | 12 | 60 | 60 | 60 | 0 | 0 |
| 182 | Super Liga | 17 | 2887 | 2809 | 2809 | 0 | 0 |
| 183 | Kup Srbije | 16 | 77 | 77 | 77 | 0 | 0 |
| 185 | Liga Portugal 2 | 17 | 3135 | 2828 | 2828 | 0 | 0 |
| 186 | Taça de Portugal | 17 | 1010 | 993 | 968 | 25 | 25 |
| 187 | League Cup | 17 | 303 | 294 | 294 | 0 | 0 |
| 188 | Super Cup | 16 | 10 | 10 | 10 | 0 | 0 |
| 189 | Liga I | 17 | 2945 | 2818 | 2818 | 0 | 0 |
| 190 | Cupa României | 17 | 264 | 252 | 252 | 0 | 0 |
| 192 | Supercupa | 16 | 10 | 10 | 10 | 0 | 0 |
| 193 | Russian Cup | 17 | 659 | 645 | 644 | 1 | 1 |
| 195 | Super Cup | 17 | 11 | 10 | 10 | 0 | 0 |
| 196 | Ekstraklasa | 17 | 2965 | 2783 | 2783 | 0 | 0 |
| 197 | I Liga | 17 | 3081 | 2901 | 2901 | 0 | 0 |
| 198 | Puchar Polski | 17 | 656 | 648 | 628 | 20 | 20 |
| 199 | Division Profesional | 51 | 4202 | 4142 | 2215 | 1927 | 1927 |
| 200 | Superpuchar Polski | 15 | 10 | 10 | 10 | 0 | 0 |
| 203 | OBOS-ligaen | 17 | 2400 | 2400 | 2400 | 0 | 0 |
| 204 | 2. divisjon | 17 | 3682 | 3469 | 3468 | 1 | 1 |
| 205 | Norsk Tipping-ligaen | 17 | 10399 | 9282 | 9282 | 0 | 0 |
| 206 | Norgesmesterskapet | 18 | 1089 | 1078 | 1078 | 0 | 0 |
| 207 | Trophée des champions | 16 | 10 | 10 | 10 | 0 | 0 |
| 208 | 3. Liga | 17 | 3802 | 3514 | 3514 | 0 | 0 |
| 209 | DFB Pokal | 17 | 623 | 615 | 615 | 0 | 0 |
| 212 | Nemzeti Bajnokság I | 17 | 1981 | 1860 | 1859 | 1 | 0 |
| 213 | Magyar Kupa | 17 | 214 | 206 | 206 | 0 | 0 |
| 215 | Besta deildin | 17 | 1470 | 1445 | 1445 | 0 | 0 |
| 216 | 1. Deild | 17 | 1362 | 1327 | 1327 | 0 | 0 |
| 217 | Icelandic Cup | 17 | 309 | 307 | 307 | 0 | 0 |
| 218 | First Division | 17 | 1559 | 1512 | 1512 | 0 | 0 |
| 219 | FAI Cup | 17 | 358 | 357 | 357 | 0 | 0 |
| 221 | Premier Division Qualification | 16 | 14 | 14 | 14 | 0 | 0 |
| 222 | Supercoppa | 16 | 17 | 16 | 16 | 0 | 0 |
| 223 | J. League | 18 | 3250 | 3010 | 3010 | 0 | 0 |
| 224 | J. League Cup | 17 | 669 | 631 | 631 | 0 | 0 |
| 225 | Premier League | 17 | 1942 | 1828 | 1828 | 0 | 0 |
| 226 | Virsliga | 17 | 1605 | 1499 | 1499 | 0 | 0 |
| 228 | Toplyga | 17 | 1562 | 1485 | 1485 | 0 | 0 |
| 229 | National Division | 17 | 2229 | 2022 | 2022 | 0 | 0 |
| 230 | Liga MX | 33 | 2865 | 2780 | 2780 | 0 | 0 |
| 231 | National Division | 17 | 1075 | 1050 | 1050 | 0 | 0 |
| 232 | 1. CFL | 17 | 1715 | 1668 | 1668 | 0 | 0 |
| 235 | KNVB Cup | 17 | 628 | 602 | 602 | 0 | 0 |
| 237 | Super Cup | 15 | 10 | 10 | 10 | 0 | 0 |
| 239 | 2. Division | 17 | 2548 | 2446 | 2446 | 0 | 0 |
| 240 | 3. Division | 17 | 3326 | 3022 | 3021 | 1 | 0 |
| 241 | Danmarksserien | 5 | 2687 | 2608 | 2608 | 0 | 0 |
| 242 | DBU Pokalen | 17 | 1062 | 1052 | 1052 | 0 | 0 |
| 246 | Serie A | 17 | 2624 | 2599 | 2599 | 0 | 0 |
| 247 | Community Shield | 16 | 10 | 10 | 10 | 0 | 0 |
| 248 | Regular Season | 17 | 1836 | 1743 | 1743 | 0 | 0 |
| 249 | Prva Liga | 17 | 1792 | 1695 | 1695 | 0 | 0 |
| 250 | Premier League | 17 | 1351 | 1351 | 1351 | 0 | 0 |
| 251 | Ykkosliiga | 17 | 1437 | 1435 | 1432 | 3 | 0 |
| 252 | HNL | 17 | 1806 | 1690 | 1690 | 0 | 0 |
| 253 | FNL | 17 | 2343 | 2214 | 2214 | 0 | 0 |
| 254 | Czech Cup | 17 | 645 | 628 | 627 | 1 | 1 |
| 256 |  | 1 | 90 | 10 | 10 | 0 | 0 |
| 260 | Kategoria Superiore | 17 | 1724 | 1689 | 1689 | 0 | 0 |
| 262 | Premier League | 17 | 1497 | 1331 | 1331 | 0 | 0 |
| 263 | Premier League | 17 | 2430 | 2353 | 2353 | 0 | 0 |
| 264 | First Division B | 17 | 1792 | 1592 | 1592 | 0 | 0 |
| 266 | Super Cup | 15 | 10 | 10 | 10 | 0 | 0 |
| 267 | Premier League | 17 | 1938 | 1756 | 1756 | 0 | 0 |
| 268 | Série A | 17 | 3800 | 3759 | 3759 | 0 | 0 |
| 270 | First Professional League | 17 | 2496 | 2411 | 2410 | 1 | 1 |
| 271 | Bulgarian Cup | 17 | 325 | 313 | 313 | 0 | 0 |
| 272 | Super Cup | 15 | 10 | 10 | 10 | 0 | 0 |
| 273 | Liga de Primera | 32 | 2382 | 2312 | 2312 | 0 | 0 |
| 274 | Primera A | 34 | 3459 | 3251 | 3251 | 0 | 0 |
| 275 | Croatian Cup | 17 | 209 | 206 | 206 | 0 | 0 |
| 276 | Super Cup | 6 | 6 | 5 | 5 | 0 | 0 |
| 278 | Austrian Cup | 17 | 628 | 624 | 624 | 0 | 0 |
| 289 | Africa Cup of Nations | 9 | 336 | 336 | 336 | 0 | 0 |
| 290 | Asian Cup | 5 | 217 | 166 | 137 | 29 | 29 |
| 297 | CONCACAF Champions Cup | 17 | 391 | 390 | 390 | 0 | 0 |
| 298 | CONCACAF Gold Cup | 8 | 230 | 225 | 217 | 8 | 8 |
| 299 | Copa Sudamericana | 17 | 1252 | 1250 | 1202 | 48 | 48 |
| 300 | East Asian Championship | 7 | 79 | 79 | 45 | 34 | 34 |
| 305 | Maurice Revello Tournament | 15 | 245 | 235 | 235 | 0 | 0 |
| 329 | Gulf Cup | 8 | 122 | 122 | 122 | 0 | 0 |
| 330 | Cyprus Cup | 17 | 344 | 332 | 332 | 0 | 0 |
| 331 |  | 1 | 132 | 68 | 68 | 0 | 0 |
| 332 |  | 1 | 132 | 70 | 70 | 0 | 0 |
| 333 |  | 1 | 51 | 50 | 50 | 0 | 0 |
| 335 | Primera Division | 34 | 2316 | 2170 | 2170 | 0 | 0 |
| 336 | Liga Nacional | 17 | 2566 | 2454 | 2453 | 1 | 1 |
| 337 | Liga Nacional | 33 | 1701 | 1663 | 1663 | 0 | 0 |
| 338 | 1. Division | 17 | 3590 | 3249 | 3249 | 0 | 0 |
| 339 | Primera Division | 17 | 2901 | 2748 | 2746 | 2 | 1 |
| 342 | Finland Cup | 12 | 336 | 336 | 335 | 1 | 1 |
| 439 | Erovnuli Liga | 17 | 1800 | 1690 | 1690 | 0 | 0 |
| 440 | Super Cup | 15 | 10 | 10 | 10 | 0 | 0 |
| 441 | Premier League | 17 | 2283 | 1955 | 1953 | 2 | 0 |
| 442 | Ukraine Cup | 16 | 167 | 163 | 163 | 0 | 0 |
| 461 | Premier League | 17 | 1049 | 936 | 936 | 0 | 0 |
| 489 | Club Friendlies | 1 | 1506 | 1435 | 1431 | 4 | 4 |
| 490 | Super Cup | 1 | 1 | 1 | 0 | 1 | 1 |
| 491 | Recopa Sudamericana | 17 | 20 | 20 | 20 | 0 | 0 |
| 492 | Prva Liga Qualification | 15 | 20 | 16 | 16 | 0 | 0 |
| 512 | Regionalliga | 18 | 14201 | 12400 | 12399 | 1 | 0 |
| 516 | Ligue 1 | 17 | 2590 | 2365 | 2365 | 0 | 0 |
| 519 | Premier League | 17 | 2879 | 2769 | 2769 | 0 | 0 |
| 521 | Super Cup | 15 | 10 | 10 | 10 | 0 | 0 |
| 522 | Premier League | 12 | 2862 | 2646 | 1927 | 719 | 719 |
| 523 | Persian Gulf | 17 | 2275 | 2205 | 2205 | 0 | 0 |
| 524 | Stars League | 17 | 3536 | 3207 | 3204 | 3 | 2 |
| 525 | AFC Champions League Elite | 17 | 1298 | 1175 | 1175 | 0 | 0 |
| 526 | CAF Champions League | 18 | 666 | 617 | 617 | 0 | 0 |
| 529 | Premier League | 18 | 1083 | 996 | 996 | 0 | 0 |
| 530 | Botola Pro | 17 | 2455 | 2455 | 2455 | 0 | 0 |
| 533 | Professional Football League | 17 | 3515 | 2976 | 2976 | 0 | 0 |
| 535 | Qatar Stars League | 17 | 1254 | 1242 | 1242 | 0 | 0 |
| 536 | Saudi Pro League | 17 | 2606 | 2404 | 2404 | 0 | 0 |
| 537 | Premier Soccer League | 17 | 2439 | 2238 | 2237 | 1 | 0 |
| 538 | Pro League | 17 | 1707 | 1588 | 1588 | 0 | 0 |
| 540 | Superliga | 1 | 240 | 215 | 215 | 0 | 0 |
| 544 | Ligue I | 17 | 1854 | 1788 | 1786 | 2 | 2 |
| 8814 | Série B | 17 | 3804 | 3793 | 3793 | 0 | 0 |
| 8815 | Super League 2 | 17 | 3268 | 3069 | 3069 | 0 | 0 |
| 8816 | Super Cup | 2 | 2 | 2 | 2 | 0 | 0 |
| 8870 | Championship | 16 | 737 | 707 | 611 | 96 | 96 |
| 8924 | Super Cup | 16 | 10 | 10 | 10 | 0 | 0 |
| 8935 | II Liga | 17 | 3185 | 2945 | 2945 | 0 | 0 |
| 8938 | Australia Championship | 2 | 1351 | 1338 | 1338 | 0 | 0 |
| 8944 | National North & South | 17 | 10163 | 8683 | 8680 | 3 | 0 |
| 8947 | Premier Division | 17 | 18284 | 15096 | 15091 | 5 | 0 |
| 8965 | Primera B Nacional | 18 | 5213 | 5057 | 5057 | 0 | 0 |
| 8968 | Primera Federación | 17 | 10681 | 9627 | 9627 | 0 | 0 |
| 8969 | Ykkonen | 17 | 3398 | 3253 | 3250 | 3 | 0 |
| 8970 | Ligue 3 | 17 | 3067 | 2680 | 2679 | 1 | 0 |
| 8971 | Série C | 17 | 2074 | 2074 | 2074 | 0 | 0 |
| 8972 | USL Championship | 16 | 4257 | 3840 | 3840 | 0 | 0 |
| 8973 | 2. Liga | 17 | 2353 | 2114 | 2114 | 0 | 0 |
| 8974 | J. League 2 | 17 | 4395 | 4175 | 4175 | 0 | 0 |
| 8976 | Liga de Expansión MX | 17 | 2477 | 2445 | 2445 | 0 | 0 |
| 8978 | League Cup | 16 | 208 | 200 | 189 | 11 | 11 |
| 8979 | Premiership Qualification | 12 | 20 | 20 | 20 | 0 | 0 |
| 8982 | Indian Football League | 17 | 1136 | 1065 | 1065 | 0 | 0 |
| 8983 | Super League | 17 | 2764 | 2258 | 2258 | 0 | 0 |
| 8984 | Thai League | 17 | 2533 | 2355 | 2355 | 0 | 0 |
| 8985 | Liga Super | 17 | 1352 | 1255 | 1255 | 0 | 0 |
| 9011 | Emperor's Cup | 17 | 816 | 806 | 738 | 68 | 68 |
| 9039 | LPF | 11 | 1952 | 1946 | 735 | 1211 | 1211 |
| 9066 | Premier League | 16 | 2750 | 2590 | 2042 | 548 | 548 |
| 9067 | Copa do Brasil | 17 | 1246 | 1244 | 1244 | 0 | 0 |
| 9069 | Super Cup | 16 | 10 | 10 | 10 | 0 | 0 |
| 9080 | K League 1 | 17 | 2280 | 2196 | 2196 | 0 | 0 |
| 9081 | Bundesliga Qualification | 16 | 20 | 20 | 20 | 0 | 0 |
| 9088 | V-League | 18 | 1516 | 1467 | 1467 | 0 | 0 |
| 9089 |  | 1 | 182 | 86 | 86 | 0 | 0 |
| 9091 | Copa Chile | 17 | 568 | 561 | 547 | 14 | 14 |
| 9096 | Second Professional League | 17 | 3075 | 2635 | 2635 | 0 | 0 |
| 9097 | State Cup | 17 | 399 | 382 | 382 | 0 | 0 |
| 9098 | Toto Cup Leumit | 16 | 86 | 86 | 85 | 1 | 1 |
| 9100 | 2. Division | 17 | 2423 | 2281 | 2191 | 90 | 90 |
| 9112 | Liga 3 | 17 | 6572 | 6007 | 5869 | 138 | 138 |
| 9113 | Liga II | 17 | 3122 | 2770 | 2770 | 0 | 0 |
| 9116 | K League 2 | 17 | 2153 | 2090 | 2090 | 0 | 0 |
| 9117 | NB II | 2 | 480 | 344 | 344 | 0 | 0 |
| 9122 | Segunda Division | 17 | 1909 | 1907 | 1907 | 0 | 0 |
| 9123 | Second League | 21 | 9156 | 8787 | 8787 | 0 | 0 |
| 9125 | Primera B | 17 | 2910 | 2786 | 2786 | 0 | 0 |
| 9126 | Liga de Ascenso | 18 | 2386 | 2322 | 2322 | 0 | 0 |
| 9134 |  | 1 | 240 | 139 | 139 | 0 | 0 |
| 9136 | J. League 3 | 17 | 3231 | 2975 | 2975 | 0 | 0 |
| 9137 | China League One | 17 | 2431 | 2431 | 2431 | 0 | 0 |
| 9138 | Segunda Federación | 13 | 9435 | 8249 | 7810 | 439 | 439 |
| 9143 | Singapore Cup | 14 | 187 | 187 | 187 | 0 | 0 |
| 9166 | Welsh Cup | 15 | 29 | 27 | 27 | 0 | 0 |
| 9170 | Primera B Nacional Qualification | 3 | 8 | 8 | 8 | 0 | 0 |
| 9173 | Superiore Qualification | 9 | 28 | 28 | 24 | 4 | 4 |
| 9174 | National Division Qualification | 14 | 15 | 15 | 15 | 0 | 0 |
| 9178 | 1. CFL Qualification | 16 | 40 | 40 | 40 | 0 | 0 |
| 9195 | Tweede Divisie | 17 | 3086 | 2536 | 2528 | 8 | 8 |
| 9213 | Primera B Metropolitana & Torneo Federal A | 20 | 8065 | 7872 | 7871 | 1 | 1 |
| 9227 |  | 1 | 182 | 0 | 0 | 0 | 0 |
| 9253 | FA Trophy | 16 | 1226 | 1180 | 1152 | 28 | 28 |
| 9255 | Premier League Qualification | 12 | 20 | 20 | 20 | 0 | 0 |
| 9265 | ASEAN Championship | 9 | 202 | 202 | 202 | 0 | 0 |
| 9294 |  | 1 | 132 | 0 | 0 | 0 | 0 |
| 9296 | USL League One | 16 | 2561 | 2478 | 2478 | 0 | 0 |
| 9305 | Copa Argentina | 12 | 630 | 628 | 628 | 0 | 0 |
| 9310 | Erovnuli Qualification | 11 | 37 | 37 | 37 | 0 | 0 |
| 9333 | Premier League Qualification | 11 | 44 | 44 | 40 | 4 | 4 |
| 9334 | Primera Division Qualification | 11 | 25 | 23 | 19 | 4 | 4 |
| 9372 | Azadegan League | 13 | 3114 | 3032 | 2886 | 146 | 146 |
| 9381 | Supercopa Argentina | 11 | 10 | 9 | 9 | 0 | 0 |
| 9382 |  | 1 | 2 | 2 | 2 | 0 | 0 |
| 9389 | Irish Cup | 12 | 167 | 166 | 166 | 0 | 0 |
| 9407 | Super Cup | 13 | 12 | 12 | 12 | 0 | 0 |
| 9422 | K League Qualification | 11 | 28 | 28 | 28 | 0 | 0 |
| 9428 | African Nations Championship | 6 | 204 | 204 | 172 | 32 | 32 |
| 9429 | Copa do Nordeste | 12 | 815 | 814 | 810 | 4 | 4 |
| 9431 | Super Cup | 12 | 10 | 9 | 9 | 0 | 0 |
| 9441 | Open Cup | 14 | 776 | 746 | 745 | 1 | 0 |
| 9464 | Serie D | 2 | 1120 | 1120 | 1120 | 0 | 0 |
| 9467 | CAF Super Cup | 14 | 11 | 11 | 11 | 0 | 0 |
| 9468 | CAF Confed Cup | 14 | 747 | 690 | 690 | 0 | 0 |
| 9469 | AFC Champions League Two | 13 | 1084 | 926 | 926 | 0 | 0 |
| 9470 | AFC Challenge League | 4 | 140 | 126 | 104 | 22 | 22 |
| 9471 | Australia Cup | 13 | 289 | 289 | 289 | 0 | 0 |
| 9473 | Nedbank Cup | 12 | 311 | 311 | 311 | 0 | 0 |
| 9474 | South Africa 8 Cup | 13 | 90 | 90 | 90 | 0 | 0 |
| 9478 | Indian Super League | 13 | 1182 | 1114 | 1114 | 0 | 0 |
| 9483 | Paulista A2 | 3 | 418 | 418 | 418 | 0 | 0 |
| 9486 | Super Cup | 4 | 4 | 3 | 3 | 0 | 0 |
| 9487 | Hazfi Cup | 12 | 304 | 304 | 282 | 22 | 22 |
| 9490 | Copa Colombia | 12 | 806 | 797 | 739 | 58 | 58 |
| 9491 | Super Cup | 10 | 10 | 9 | 9 | 0 | 0 |
| 9493 | Super Cup | 12 | 10 | 10 | 10 | 0 | 0 |
| 9498 | Thai League 2 | 3 | 934 | 681 | 681 | 0 | 0 |
| 9500 |  | 1 | 132 | 0 | 0 | 0 | 0 |
| 9504 | Super Cup | 12 | 13 | 13 | 13 | 0 | 0 |
| 9514 | The Atlantic Cup | 12 | 107 | 103 | 103 | 0 | 0 |
| 9521 | Belarusian Cup | 12 | 38 | 38 | 38 | 0 | 0 |
| 9523 | Løgmanssteypið | 12 | 10 | 10 | 10 | 0 | 0 |
| 9527 | Coupe de Luxembourg | 11 | 9 | 9 | 8 | 1 | 1 |
| 9528 | Macedonian Cup | 12 | 9 | 9 | 9 | 0 | 0 |
| 9530 | Cupa Moldova | 12 | 10 | 10 | 10 | 0 | 0 |
| 9537 | K League 3 | 14 | 1862 | 1824 | 1824 | 0 | 0 |
| 9545 | Highland / Lowland | 2 | 1432 | 973 | 973 | 0 | 0 |
| 9550 | Chinese FA Cup | 12 | 579 | 575 | 575 | 0 | 0 |
| 9551 | Cup | 12 | 568 | 568 | 568 | 0 | 0 |
| 9584 | First Professional League Qualification | 11 | 15 | 15 | 15 | 0 | 0 |
| 9587 | Liga I Qualification | 11 | 32 | 32 | 32 | 0 | 0 |
| 9628 | V-League Qualification | 6 | 6 | 6 | 5 | 1 | 1 |
| 9632 | A Lyga Qualification | 9 | 18 | 16 | 16 | 0 | 0 |
| 9634 | Ettan Qualification | 8 | 32 | 28 | 24 | 4 | 4 |
| 9658 | Super Cup | 10 | 10 | 10 | 10 | 0 | 0 |
| 9661 | Qatar Stars League Qualification | 10 | 10 | 10 | 10 | 0 | 0 |
| 9666 | Ligue 1 Qualification | 8 | 30 | 30 | 30 | 0 | 0 |
| 9667 | Ligue 2 Qualification | 7 | 14 | 14 | 14 | 0 | 0 |
| 9668 | Liga Portugal 2 Qualification | 6 | 18 | 18 | 18 | 0 | 0 |
| 9669 | Ligue I Qualification | 6 | 16 | 14 | 14 | 0 | 0 |
| 9677 |  | 1 | 132 | 0 | 0 | 0 | 0 |
| 9682 | CONCACAF Central American Cup | 10 | 439 | 435 | 435 | 0 | 0 |
| 9690 | Southeast Asian Games | 5 | 119 | 119 | 119 | 0 | 0 |
| 9734 | 2. Bundesliga Qualification | 16 | 20 | 20 | 20 | 0 | 0 |
| 9735 | Leumit League Qualification | 13 | 15 | 15 | 15 | 0 | 0 |
| 9737 | Championship Qualification | 15 | 60 | 60 | 60 | 0 | 0 |
| 9738 | League One Qualification | 15 | 60 | 60 | 60 | 0 | 0 |
| 9739 | League Two Qualification | 11 | 36 | 36 | 34 | 2 | 2 |
| 9754 | OBOS-ligaen Qualification | 10 | 38 | 37 | 36 | 1 | 1 |
| 9806 | UEFA Nations League A | 5 | 240 | 228 | 228 | 0 | 0 |
| 9807 | UEFA Nations League B | 5 | 216 | 194 | 194 | 0 | 0 |
| 9808 | UEFA Nations League C | 5 | 240 | 228 | 228 | 0 | 0 |
| 9809 | UEFA Nations League D | 5 | 108 | 104 | 104 | 0 | 0 |
| 9821 | CONCACAF Nations League | 5 | 532 | 505 | 505 | 0 | 0 |
| 9829 | Premier League Qualification | 6 | 27 | 27 | 26 | 1 | 1 |
| 9833 | Asian Games | 5 | 186 | 181 | 74 | 107 | 107 |
| 9837 | Canadian Championship | 9 | 140 | 131 | 123 | 8 | 8 |
| 9845 | 1. liga Qualification | 9 | 18 | 16 | 15 | 1 | 1 |
| 9849 | COSAFA Cup | 1 | 22 | 22 | 22 | 0 | 0 |
| 9851 | Tweede Divisie Qualification | 5 | 78 | 76 | 76 | 0 | 0 |
| 9862 | Super Cup | 9 | 9 | 9 | 9 | 0 | 0 |
| 9875 | Campeones Cup | 9 | 8 | 8 | 8 | 0 | 0 |
| 9876 | SAFF Championship | 3 | 35 | 32 | 32 | 0 | 0 |
| 9906 |  | 1 | 126 | 9 | 9 | 0 | 0 |
| 9907 |  | 1 | 240 | 0 | 0 | 0 | 0 |
| 9921 |  | 1 | 6 | 6 | 6 | 0 | 0 |
| 9941 | Egypt Cup | 9 | 272 | 264 | 251 | 13 | 13 |
| 9942 | King's Cup | 9 | 224 | 216 | 194 | 22 | 22 |
| 9943 | President's Cup | 9 | 149 | 145 | 137 | 8 | 8 |
| 9986 | Premier League | 8 | 919 | 834 | 834 | 0 | 0 |
| 10007 | Copa de la Liga Profesional | 7 | 1102 | 981 | 981 | 0 | 0 |
| 10009 | Super Cup | 7 | 5 | 5 | 4 | 1 | 1 |
| 10022 | Regionalliga Qualification | 6 | 37 | 33 | 25 | 8 | 8 |
| 10023 | 1. Division Qualification | 4 | 14 | 14 | 10 | 4 | 4 |
| 10025 | 1. Liga Qualification | 6 | 24 | 24 | 24 | 0 | 0 |
| 10028 | Premier League Qualification | 7 | 30 | 30 | 26 | 4 | 4 |
| 10034 | Meistriliiga Qualification | 9 | 18 | 16 | 16 | 0 | 0 |
| 10043 | Leagues Cup | 8 | 297 | 297 | 297 | 0 | 0 |
| 10046 | Copa Ecuador | 4 | 140 | 138 | 110 | 28 | 28 |
| 10053 | Trofeo de Campeones | 7 | 8 | 7 | 7 | 0 | 0 |
| 10056 | Liga 2 | 1 | 0 | 0 | 0 | 0 | 0 |
| 10059 | President's Cup | 2 | 24 | 24 | 24 | 0 | 0 |
| 10065 | National Division Qualification | 6 | 151 | 151 | 151 | 0 | 0 |
| 10074 | Super Cup | 8 | 16 | 14 | 14 | 0 | 0 |
| 10075 | Torneo de Verano | 4 | 28 | 28 | 28 | 0 | 0 |
| 10076 | League Cup | 5 | 315 | 298 | 298 | 0 | 0 |
| 10077 | Supercopa do Brasil | 7 | 7 | 7 | 7 | 0 | 0 |
| 10078 | Recopa Gaúcha | 7 | 7 | 7 | 5 | 2 | 2 |
| 10084 | NISA | 7 | 495 | 468 | 468 | 0 | 0 |
| 10161 | Cúp Quốc gia | 2 | 44 | 35 | 35 | 0 | 0 |
| 10167 |  | 1 | 1 | 1 | 1 | 0 | 0 |
| 10175 | Superkupa e Shqipërisë | 6 | 6 | 6 | 5 | 1 | 1 |
| 10178 |  | 1 | 132 | 0 | 0 | 0 | 0 |
| 10188 | K League 3 Qualification | 5 | 6 | 6 | 5 | 1 | 1 |
| 10195 | World Cup Qualification UEFA | 4 | 1009 | 1008 | 1008 | 0 | 0 |
| 10196 | World Cup Qualification CAF | 4 | 702 | 699 | 699 | 0 | 0 |
| 10197 | World Cup Qualification AFC | 5 | 841 | 825 | 825 | 0 | 0 |
| 10198 | World Cup Qualification CONCACAF | 4 | 476 | 472 | 451 | 21 | 21 |
| 10199 | World Cup Qualification CONMEBOL | 4 | 343 | 342 | 342 | 0 | 0 |
| 10200 | World Cup Qualification OFC | 7 | 103 | 98 | 98 | 0 | 0 |
| 10201 | World Cup Qualification Inter-confederation | 5 | 14 | 14 | 14 | 0 | 0 |
| 10215 | Liga Portugal Qualification | 6 | 12 | 12 | 12 | 0 | 0 |
| 10216 | Conference League | 6 | 837 | 783 | 783 | 0 | 0 |
| 10223 | Super Cup | 7 | 7 | 7 | 6 | 1 | 1 |
| 10226 | 2. Deild | 6 | 814 | 792 | 776 | 16 | 16 |
| 10230 | Copa Paraguay | 6 | 190 | 188 | 188 | 0 | 0 |
| 10242 | FIFA Arab Cup | 2 | 64 | 64 | 64 | 0 | 0 |
| 10244 | Paulista A1 | 7 | 616 | 616 | 616 | 0 | 0 |
| 10259 | Supercopa | 4 | 4 | 4 | 4 | 0 | 0 |
| 10269 |  | 1 | 27 | 27 | 27 | 0 | 0 |
| 10270 | League Cup | 6 | 260 | 236 | 229 | 7 | 7 |
| 10272 | Carioca | 7 | 464 | 464 | 464 | 0 | 0 |
| 10273 | Mineiro | 6 | 338 | 337 | 337 | 0 | 0 |
| 10274 | Gaúcho | 7 | 377 | 377 | 377 | 0 | 0 |
| 10282 | MLS Next Pro | 5 | 1929 | 1929 | 1929 | 0 | 0 |
| 10288 | Super Cup | 4 | 4 | 4 | 4 | 0 | 0 |
| 10289 |  | 1 | 90 | 0 | 0 | 0 | 0 |
| 10290 | Baiano | 6 | 297 | 251 | 251 | 0 | 0 |
| 10291 | Goiano | 5 | 384 | 384 | 384 | 0 | 0 |
| 10304 | Finalissima | 2 | 2 | 1 | 1 | 0 | 0 |
| 10308 |  | 1 | 182 | 84 | 84 | 0 | 0 |
| 10309 | Durand Cup | 5 | 225 | 218 | 213 | 5 | 5 |
| 10310 | Stars League Relegation Playoff | 4 | 6 | 6 | 5 | 1 | 1 |
| 10314 | Super Cup | 5 | 15 | 14 | 14 | 0 | 0 |
| 10316 |  | 1 | 2 | 2 | 2 | 0 | 0 |
| 10342 | Copa Uruguay | 4 | 115 | 113 | 113 | 0 | 0 |
| 10343 | Supercopa | 9 | 9 | 9 | 5 | 4 | 4 |
| 10357 |  | 1 | 198 | 150 | 150 | 0 | 0 |
| 10359 |  | 1 | 7 | 7 | 7 | 0 | 0 |
| 10366 | Super Cup | 4 | 102 | 102 | 102 | 0 | 0 |
| 10433 | Challenge League Qualification | 1 | 0 | 0 | 0 | 0 | 0 |
| 10434 |  | 1 | 0 | 0 | 0 | 0 | 0 |
| 10443 | Bangladesh Football League | 5 | 443 | 394 | 394 | 0 | 0 |
| 10448 | Liga Revelação | 1 | 256 | 256 | 40 | 216 | 216 |
| 10449 |  | 1 | 90 | 90 | 90 | 0 | 0 |
| 10474 | Arab Club Champions Cup | 1 | 31 | 31 | 31 | 0 | 0 |
| 10498 |  | 1 | 2 | 2 | 2 | 0 | 0 |
| 10508 | African Football League | 1 | 14 | 14 | 14 | 0 | 0 |
| 10557 |  | 1 | 8 | 8 | 8 | 0 | 0 |
| 10558 |  | 1 | 4 | 4 | 4 | 0 | 0 |
| 10583 | Super Liga Qualification | 2 | 4 | 4 | 4 | 0 | 0 |
| 10584 | South Africa League Qualification | 3 | 12 | 12 | 12 | 0 | 0 |
| 10607 | EURO Qualification | 6 | 1017 | 1017 | 976 | 41 | 41 |
| 10608 | Africa Cup of Nations Qualification | 10 | 1246 | 1122 | 1014 | 108 | 108 |
| 10609 | Asian Cup  Qualification Playoff | 5 | 206 | 206 | 181 | 25 | 25 |
| 10611 | Champions League Qualification | 17 | 1500 | 1496 | 1496 | 0 | 0 |
| 10613 | Europa League Qualification | 17 | 1403 | 1399 | 1399 | 0 | 0 |
| 10615 | Conference League Qualification | 6 | 1606 | 1602 | 1602 | 0 | 0 |
| 10618 |  | 2 | 60 | 60 | 60 | 0 | 0 |
| 10619 | CAF Champions League Qualification | 4 | 344 | 344 | 344 | 0 | 0 |
| 10622 | AFC Champions League Elite Qualification | 10 | 108 | 102 | 89 | 13 | 13 |
| 10623 | Copa Sudamericana Qualification | 6 | 128 | 128 | 128 | 0 | 0 |
| 10626 | FA Cup Qualification | 17 | 3508 | 3496 | 2925 | 571 | 570 |
| 10628 | Norgesmesterskapet Kvalifisering | 16 | 1225 | 1144 | 1006 | 138 | 138 |
| 10649 |  | 1 | 33 | 33 | 33 | 0 | 0 |
| 10651 |  | 1 | 25 | 25 | 25 | 0 | 0 |
| 10654 | USL Cup | 3 | 225 | 225 | 225 | 0 | 0 |
| 10657 |  | 1 | 4 | 4 | 4 | 0 | 0 |
| 10703 | FIFA Intercontinental Cup | 3 | 12 | 12 | 12 | 0 | 0 |
| 10705 | National League Cup | 3 | 207 | 207 | 207 | 0 | 0 |
| 10708 | League Cup | 3 | 44 | 41 | 41 | 0 | 0 |
| 10713 | Ykkosliiga Qualification | 3 | 4 | 4 | 4 | 0 | 0 |
| 10714 |  | 1 | 2 | 2 | 2 | 0 | 0 |
| 10716 | J. League 3 Qualification | 2 | 2 | 2 | 2 | 0 | 0 |
| 10717 | UEFA Nations League A Qualification | 1 | 8 | 8 | 8 | 0 | 0 |
| 10718 | UEFA Nations League B Qualification | 1 | 8 | 8 | 8 | 0 | 0 |
| 10719 | UEFA Nations League C Qualification | 1 | 4 | 4 | 4 | 0 | 0 |
| 10721 | Saudi First Division | 3 | 755 | 678 | 678 | 0 | 0 |
| 10737 | Super Cup | 3 | 3 | 3 | 3 | 0 | 0 |
| 10791 |  | 1 | 90 | 0 | 0 | 0 | 0 |
| 10832 | Supercopa Internacional | 3 | 3 | 3 | 3 | 0 | 0 |
| 10840 | Baller League | 2 | 69 | 69 | 69 | 0 | 0 |
| 10844 | Baller League | 4 | 243 | 237 | 237 | 0 | 0 |
| 10872 |  | 1 | 75 | 42 | 42 | 0 | 0 |
| 10913 | FIFA Club World Cup Qualification | 1 | 1 | 1 | 1 | 0 | 0 |
| 10967 | II Liga Qualification | 2 | 12 | 12 | 6 | 6 | 6 |
| 11012 | ASEAN Club Championship | 3 | 115 | 69 | 69 | 0 | 0 |
| 11013 |  | 1 | 24 | 24 | 24 | 0 | 0 |
| 11015 |  | 1 | 1 | 1 | 1 | 0 | 0 |
| 11016 | Qatar Cup | 2 | 6 | 5 | 5 | 0 | 0 |
| 11017 | QSL Cup | 3 | 188 | 163 | 163 | 0 | 0 |
| 11018 | Amir of Qatar Cup | 3 | 57 | 57 | 57 | 0 | 0 |
| 11019 |  | 1 | 28 | 21 | 21 | 0 | 0 |
| 11020 |  | 1 | 43 | 43 | 43 | 0 | 0 |
| 11023 | FA Cup | 3 | 219 | 204 | 204 | 0 | 0 |
| 11024 | League Cup | 3 | 205 | 190 | 190 | 0 | 0 |
| 11026 | Super Cup | 3 | 3 | 2 | 2 | 0 | 0 |
| 11027 | League Cup | 2 | 67 | 46 | 46 | 0 | 0 |
| 11028 |  | 1 | 51 | 51 | 51 | 0 | 0 |
| 11029 |  | 1 | 1 | 1 | 1 | 0 | 0 |
| 11030 | Second Division | 3 | 172 | 141 | 141 | 0 | 0 |
| 11031 | Supercopa | 1 | 1 | 1 | 1 | 0 | 0 |
| 11035 | Supercopa de Ecuador | 2 | 2 | 2 | 2 | 0 | 0 |
| 11038 | King's Cup | 2 | 8 | 4 | 4 | 0 | 0 |
| 11039 | Campeón de Campeones | 4 | 4 | 4 | 4 | 0 | 0 |
| 11156 | Arab Cup Qualification | 2 | 14 | 14 | 13 | 1 | 1 |
| 11157 | Baller League | 1 | 48 | 48 | 48 | 0 | 0 |
| 11315 |  | 1 | 56 | 56 | 56 | 0 | 0 |
| 11499 | Japan 2/3 | 1 | 400 | 400 | 400 | 0 | 0 |
| 11527 | OFC Pro League | 2 | 135 | 72 | 72 | 0 | 0 |
| 11536 | Superliga | 10 | 20 | 20 | 20 | 0 | 0 |
| 11646 | Copa Paulista | 1 | 70 | 70 | 70 | 0 | 0 |
| 11648 | Soccer Aid | 13 | 10 | 10 | 10 | 0 | 0 |
| 11697 | Copa de la Liga | 1 | 53 | 52 | 52 | 0 | 0 |
| 11746 |  | 1 | 1 | 1 | 1 | 0 | 0 |
| 11839 |  | 1 | 1 | 1 | 1 | 0 | 0 |
| 11840 | Throne Cup | 1 | 16 | 16 | 14 | 2 | 2 |
| 13169 | MLS All-Star Game | 3 | 3 | 3 | 3 | 0 | 0 |
| 13287 | FIFA ASEAN Cup | 1 | 21 | 21 | 21 | 0 | 0 |

## Payload reasons

| Category:reason | Scoped rows |
| --- | --- |
| confirmed_content:current_payload_present | 555976 |
| not_applicable:cancelled | 17132 |
| not_applicable:future | 22234 |
| not_applicable:unfinished | 1889 |
| source_unavailable:explicit_scope_source_gap | 1242 |
| source_unavailable:repeated_source_data_not_found | 4149 |
| unknown:insufficient_absence_evidence | 547 |
| writer_failed:expected_current_payload_missing | 2887 |

## Latest v2 markers

| Primary category | Targets |
| --- | --- |
| confirmed_content | 431 |
| not_applicable | 116 |
| source_unavailable | 325 |
| unknown | 6898 |
| writer_failed | 33 |

marker-results.csv preserves every target, including interrupted latest rows, all reasons and evidence hashes. source_unavailable and not_applicable are not content confirmation. Unknown has precedence over every content claim.

## Six section comparisons

| Section | Category | Reason | Scoped observations |
| --- | --- | --- | --- |
| lineup | confirmed_content | raw_typed_mask_agree | 266138 |
| lineup | source_unavailable | stored_source_omission | 271888 |
| matchFacts | confirmed_content | raw_typed_mask_agree | 538026 |
| momentum | confirmed_content | raw_typed_mask_agree | 101808 |
| momentum | unknown | malformed_or_unresolved_json | 436210 |
| momentum | writer_failed | raw_present_typed_absent | 8 |
| playerStats | confirmed_content | raw_typed_mask_agree | 152207 |
| playerStats | source_unavailable | stored_source_omission | 385819 |
| shotmap | confirmed_content | raw_typed_mask_agree | 538026 |
| stats | confirmed_content | raw_typed_mask_agree | 198311 |
| stats | source_unavailable | stored_source_omission | 339715 |

Local section-evidence.csv contains all six comparisons, including agreements, for each current payload in marker or historical scopes. section-mismatches.csv is its exact non-confirmed subset. Completion and marker totals exclude ordinary unfinished not-applicable rows; full local evidence retains them. Optional omissions describe the stored observation only; no permanent capability is inferred.

## Late failures and C1 (limited cohort)

Current rows with a later non-success attempt: 3. A later failure does not remove last-good content.

C1 cached kind=input rows: 28053; resolved input rows: 28053; distinct scoped input IDs: 28053; checkpoint/control rows: 38; unresolved rows: 0. C1 ever-ingested no-payload count: 22211. Current no-payload count within comparable C1 IDs: 22286. Current added since the C1 absence observation: 0; ever-ingested but not current: 75. Outside this audit calendar: 0. Exact sets and both snapshot identities are in c1-reconciliation.json. Equality across different snapshots is not required; C1 does not cover whole history.

C1 first_ft known finished: 5830; finished without ever-ingested payload: 3; source-unavailable thresholds met: 0. Deadline-day cohorts, observations, unconfirmed/no-evidence scoped IDs and deterministic set hashes are retained in c1-reconciliation.json. Thresholds require no success and either three runs or two runs spread over 7,200 seconds.

## Historical #1294 baseline — 2026-10-21, not current

The old baseline reported 5,305 finished matches WITHOUT details in total; 1,762 of those belong to the 18 competition/season pairs rechecked here. historical-1294-results.csv keeps those keys, competition names, old signatures and old gap counts separate from current counts, primary outcomes and completion/section reasons. The original historical CSV bytes and SHA256 are preserved as input evidence.

## Validation, timing and limits

Coverage validation: PASS. Replay: PASS. Pass wall time: 9668.73838 seconds; queries: 2121; maximum statement wall time: 20.867489 seconds. Limits are 3,600 seconds per pass, 25 seconds per statement, a 30-second HTTP read deadline and a 35-second local process timeout. Every statement query ID, SQL SHA256 and outcome is saved in the query ledgers.

Unknowns: 6898 marker targets, 547 scoped IDs, 436210 section observations, 1789 calendar scopes. Malformed or unsupported evidence is explicitly unknown. No missing query, timeout or incomplete pass can validate. A coverage PASS does not assert that unknowns are collected. A single-pass build does not certify replay.

Procedure and executable SQL: scripts/research/fotmob_completion_audit/README.md. No live FotMob access, current views, DDL/DML, S3 replay, DAG changes or scheduled work is used.
