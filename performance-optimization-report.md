# Performance results - 29/09/2026

Backend seconds; browser rendering/network time excluded. This pass: medians of two runs on one fixed database snapshot. Earlier passes: previously published conservative timings.

## Optimized this pass

| Path | Before | After | Improvement |
|---|---:|---:|---:|
| `/songs?trackNumber=1&trackNumberMode=equals` | 8.347 | 0.263 | 97% |
| `/listen-years` | 7.686 | 1.161 | 85% |
| `/release-years` | 6.436 | 0.512 | 92% |
| `/listen-years?sortby=plays&sortdir=desc` | 7.421 | 1.132 | 85% |
| `/listen-years?sortby=primary_plays&sortdir=desc` | 7.327 | 1.038 | 86% |
| `/listen-years?sortby=legacy_plays&sortdir=desc` | 7.476 | 1.171 | 84% |
| `/listen-years?sortby=maleplaypct&sortdir=desc` | 7.719 | 1.249 | 84% |
| `/listen-years?sortby=random&sortdir=desc&randomSeed=42` | 7.359 | 1.110 | 85% |
| `/release-years?sortby=plays&sortdir=desc` | 6.231 | 0.477 | 92% |
| `/release-years?sortby=primary_plays&sortdir=desc` | 6.511 | 0.510 | 92% |
| `/release-years?sortby=legacy_plays&sortdir=desc` | 6.138 | 0.492 | 92% |
| `/release-years?sortby=maleplaypct&sortdir=desc` | 6.038 | 0.472 | 92% |
| `/release-years?sortby=random&sortdir=desc&randomSeed=42` | 6.188 | 0.475 | 92% |
| `/artists/2071` | 1.965 | 0.845 | 57% |
| `/artists/2071?includeGroups=true&includeFeatured=true` | 2.119 | 1.146 | 46% |
| `/artists/196` | 1.601 | 0.966 | 40% |
| `/artists/196?includeGroups=true&includeFeatured=true` | 2.053 | 1.320 | 36% |
| `/albums/4743` | 4.131 | 1.507 | 64% |
| `/albums/4741` | 4.673 | 1.018 | 78% |
| `/songs/1467` | 1.849 | 0.999 | 46% |
| `/songs/22554` | 1.947 | 1.103 | 43% |
| `/artists/446` | 1.790 | 0.748 | 58% |
| `/artists/124` | 1.490 | 0.472 | 68% |
| `/artists/16?includeGroups=true&includeFeatured=true&includeMain=true` | 2.159 | 1.056 | 51% |
| `/artists/16?includeGroups=true&includeFeatured=true&includeMain=false` | 1.908 | 1.266 | 34% |
| `/artists/2071?includeMain=false&includeFeatured=true` | 2.065 | 1.246 | 40% |
| `/albums/1088` | 4.104 | 0.953 | 77% |
| `/songs/8829` | 1.649 | 0.964 | 42% |
| `/songs/1570` | 2.014 | 0.898 | 55% |
| `/release-years?sortby=maleplaypct&sortdir=asc` | 4.719 | 0.520 | 89% |

## Optimized in previous passes

| Path | Before | After | Improvement |
|---|---:|---:|---:|
| `/languages` | 5.354 | 1.507 | 72% |
| `/countries` | 5.256 | 1.884 | 64% |
| `/genres` | 4.924 | 2.078 | 58% |
| `/countries?sortby=plays&sortdir=desc` | 5.252 | 2.053 | 61% |
| `/ethnicities` | 4.877 | 1.774 | 64% |
| `/countries?sortby=maleplaypct&sortdir=desc` | 5.773 | 2.614 | 55% |
| `/languages?sortby=winningdays&sortdir=desc` | 5.767 | 1.558 | 73% |
| `/countries?sortby=winningdays&sortdir=desc` | 6.259 | 1.664 | 73% |
| `/languages?sortby=plays&sortdir=desc` | 5.577 | 1.790 | 68% |
| `/languages?sortby=maleplaypct&sortdir=desc` | 5.509 | 1.537 | 72% |
| `/languages?q=English` | 5.422 | 1.334 | 75% |
| `/countries?q=United%20States` | 4.917 | 1.567 | 68% |
| `/ethnicities?q=Asian` | 5.548 | 0.916 | 83% |
| `/genres?sortby=winningdays&sortdir=desc` | 5.300 | 1.707 | 68% |
| `/ethnicities?sortby=maleplaypct&sortdir=desc` | 4.843 | 1.373 | 72% |
| `/genders` | 5.107 | 1.986 | 61% |
| `/countries?sortby=random&randomSeed=42` | 6.326 | 1.642 | 74% |
| `/countries?sortby=maleplaypct&sortdir=asc` | 5.135 | 1.629 | 68% |
| `/albums?fullAlbumPlaysMin=1` | 20.053 | 3.268 | 84% |
| `/decades?malePlayPctMin=50` | 12.465 | 1.950 | 84% |
| `/decades?playsMin=100` | 12.938 | 1.764 | 86% |
| `/decades?sortby=plays&sortdir=desc` | 9.726 | 1.753 | 82% |
| `/decades?sortby=maleplaypct&sortdir=desc` | 13.092 | 1.775 | 86% |
| `/decades?sortby=random&sortdir=desc&randomSeed=42` | 14.069 | 1.779 | 87% |
| `/decades?maleDaysMin=1` | 12.331 | 2.036 | 83% |
| `/decades?sortby=maledays&sortdir=desc` | 14.004 | 2.115 | 85% |
| `/years?malePlayPctMin=50` | 13.978 | 2.373 | 83% |
| `/years?maleDaysMin=1` | 5.193 | 2.736 | 47% |
| `/days` | 2.694 | 1.569 | 42% |
| `/days?winningGender=2&winningGenderMode=includes` | 5.880 | 3.127 | 47% |
| `/weeks` | 4.220 | 1.683 | 60% |
| `/weeks?winningGender=2&winningGenderMode=includes` | 6.163 | 2.429 | 61% |
| `/months` | 5.597 | 1.850 | 67% |
| `/months?winningGender=2&winningGenderMode=includes` | 6.405 | 2.167 | 66% |
| `/seasons` | 10.135 | 3.144 | 69% |
| `/seasons?winningGender=2&winningGenderMode=includes` | 7.006 | 3.042 | 57% |
| `/years` | 7.009 | 2.702 | 61% |
| `/years?winningGender=2&winningGenderMode=includes` | 6.354 | 2.341 | 63% |
| `/decades` | 9.561 | 1.767 | 82% |
| `/decades?winningGender=2&winningGenderMode=includes` | 7.255 | 1.798 | 75% |
| `/albums?sortby=full_album_plays&sortdir=desc` | 12.188 | 1.305 | 89% |
| `/albums?sortby=first_full_listen&sortdir=desc` | 12.162 | 1.302 | 89% |
| `/albums?sortby=last_full_listen&sortdir=desc` | 16.979 | 1.288 | 92% |

## Remaining

### Previously optimized, still measured above 2 seconds

| Path | Latest backend seconds |
|---|---:|
| `/albums?fullAlbumPlaysMin=1` | 3.268 |
| `/seasons` | 3.144 |
| `/days?winningGender=2&winningGenderMode=includes` | 3.127 |
| `/seasons?winningGender=2&winningGenderMode=includes` | 3.042 |
| `/years?maleDaysMin=1` | 2.736 |
| `/years` | 2.702 |
| `/countries?sortby=maleplaypct&sortdir=desc` | 2.614 |
| `/weeks?winningGender=2&winningGenderMode=includes` | 2.429 |
| `/years?malePlayPctMin=50` | 2.373 |
| `/years?winningGender=2&winningGenderMode=includes` | 2.341 |
| `/months?winningGender=2&winningGenderMode=includes` | 2.167 |
| `/decades?sortby=maledays&sortdir=desc` | 2.115 |
| `/genres` | 2.078 |
| `/countries?sortby=plays&sortdir=desc` | 2.053 |
| `/decades?maleDaysMin=1` | 2.036 |

### Not yet optimized (earlier HTTP measurements)

| Path | Seconds |
|---|---:|
| `/songs?sortby=artist&sortdir=desc` | 3.609 |
| `/albums?sortby=weekly_chart_peak_weeks&sortdir=desc` | 2.997 |
| `/songs?billboardWeeksAtPeak=1` | 2.535 |
| `/albums?sortby=weekly_chart_weeks&sortdir=desc` | 2.500 |
| `/songs?sortby=weekly_chart_peak_weeks&sortdir=desc` | 2.454 |
| `/songs?sortby=weekly_chart_weeks&sortdir=desc` | 2.445 |
| `/albums?sortby=weeks_listened&sortdir=desc` | 2.439 |
| `/songs?sortby=vatos_cuntdown_days_at_peak&sortdir=desc` | 2.418 |
| `/songs?sortby=months_listened&sortdir=desc` | 2.415 |
| `/songs?sortby=days_listened&sortdir=desc` | 2.386 |
| `/songs?sortby=seasonal_chart_peak&sortdir=desc` | 2.378 |
| `/songs?sortby=trl_peak&sortdir=desc` | 2.377 |
| `/songs?sortby=weeks_listened&sortdir=desc` | 2.351 |
| `/songs?sortby=yearly_chart_peak&sortdir=desc` | 2.320 |
| `/songs?sortby=trl_days_at_peak&sortdir=desc` | 2.317 |
| `/songs?sortby=trl_days&sortdir=desc` | 2.310 |
| `/songs?sortby=vatos_cuntdown_days&sortdir=desc` | 2.302 |
| `/songs?billboardDateFrom=01/01/2020` | 2.265 |
| `/songs?sortby=weekly_chart_peak&sortdir=desc` | 2.256 |
| `/songs?billboardWeeks=1` | 2.251 |
| `/songs?sortby=vatos_cuntdown_peak&sortdir=desc` | 2.215 |
| `/songs?sortby=billboard_weeks_at_peak&sortdir=desc` | 2.161 |
| `/songs?billboardPeak=1` | 2.057 |
| `/albums?sortby=seasonal_chart_peak&sortdir=desc` | 2.053 |
| `/albums?sortby=days_listened&sortdir=desc` | 2.030 |
| `/songs?sortby=billboard_weeks&sortdir=desc` | 2.025 |
| `/albums?sortby=yearly_chart_peak&sortdir=desc` | 2.005 |

### Shared fixes applied; these variations still need remeasurement

Each brace-separated value is a separate path. Sorts use `sortdir=desc`; random sorts use `randomSeed=42`.

- `/songs?sortby=track_number`
- `/genres?sortby={plays,maleplaypct}`
- `/ethnicities?sortby={plays,winningdays}`
- `/genders?sortby={plays,winningdays}`; `/genders?q=Female`
- `/days?sortby={plays,maledays,random,maleplaypct}`
- `/days?winningGenre=12&winningGenreMode=includes`; `/days?playsMin=100`; `/days?dateFrom=01/01/2020`
- `/days?malePlayPctMin=50`; `/days?maleDaysMin=1`
- `/weeks?sortby={plays,maledays,random,maleplaypct}`
- `/weeks?winningGenre=12&winningGenreMode=includes`; `/weeks?playsMin=100`; `/weeks?dateFrom=01/01/2020`
- `/weeks?malePlayPctMin=50`; `/weeks?maleDaysMin=1`
- `/months?sortby={plays,maledays,random,maleplaypct}`
- `/months?winningGenre=12&winningGenreMode=includes`; `/months?playsMin=100`; `/months?dateFrom=01/01/2020`
- `/months?malePlayPctMin=50`; `/months?maleDaysMin=1`
- `/seasons?sortby={plays,maledays,random,maleplaypct}`
- `/seasons?winningGenre=12&winningGenreMode=includes`; `/seasons?playsMin=100`; `/seasons?dateFrom=01/01/2020`
- `/seasons?malePlayPctMin=50`; `/seasons?maleDaysMin=1`
- `/years?sortby={plays,maledays,random,maleplaypct}`
- `/years?winningGenre=12&winningGenreMode=includes`; `/years?playsMin=100`; `/years?dateFrom=01/01/2020`
- `/decades?winningGenre=12&winningGenreMode=includes`; `/decades?dateFrom=01/01/2020`
