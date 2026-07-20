# Performance benchmarks

Measurement tools, not pass/fail tests. They answer "how much faster / smaller"
rather than "is it correct", so they have no assertions and no exit-code
contract worth gating on.

**Excluded from CI and from the local unit-test run** (`--ignore=tests/benchmark`).
They need a populated database, take minutes to hours, and are run by hand when
a tuning decision needs evidence.

Correctness of the things measured here belongs elsewhere:

| Question | Where it is tested |
|----------|--------------------|
| Does the setting produce the right SQL? | `tests/db/` (unit, CI) |
| Does the setting survive setup → ingest → DDL? | `tests/e2e/` (sysbox, local) |
| How much does the setting actually buy? | here |

## Benchmarks

### `sr_compression.sh`

Compares StarRocks table compression codecs (LZ4 vs ZSTD) on base comments
data — the text-heavy `body` column, where codec choice matters most. Each
codec runs in full isolation on the same freed disk state: create → load →
compact → measure size → query suite → CPU → `DROP ... FORCE` → drain trash →
repeat. The single reused table name keeps compression as the only variable.

Requires a running StarRocks with an existing `comments` table to copy from.

Result that set the `ZSTD` default: on 4 months (~1.33 B rows), ZSTD was 67.4%
of LZ4 on disk with every query faster and lower CPU. Whole-table production
numbers came in at ~75% — the isolated benchmark overstates the win because
full tables also carry BITMAP indexes and low-cardinality columns that barely
compress under either codec.
