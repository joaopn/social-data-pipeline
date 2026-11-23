---
title: "Reddit dumps"
parent: "User guide"
nav_order: 3
---
# Reddit dumps

This page covers the Reddit data dumps from [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift): where to put them, which fields you get, and how they are typed and indexed. You end up with one table (or collection) of submissions and one of comments per database.

## What the pipeline handles

The Reddit platform (`PLATFORM=reddit`) is the default and the only built-in platform. A source named `reddit` gets it: `python sdp.py source add reddit` copies the Reddit template, and any other source name becomes a custom platform (see [Your own data and Hugging Face](custom-data.md)).

| Type | File pattern | Description |
|------|--------------|-------------|
| `submissions` | `RS_YYYY-MM.*` | Reddit posts (submissions) |
| `comments` | `RC_YYYY-MM.*` | Reddit comments |

The parser adds Reddit-specific work on top of plain field extraction: it detects deleted and removed content, converts the base-36 ids to numbers, and reads the retrieval time from whichever field each era of dumps carries.

## Where to put the dumps

Put each type in its own subfolder of the source's dumps directory (asked by `python sdp.py source add reddit`, default `data/dumps/reddit/`). This is the layout of the Arctic Shift torrents:

```
data/dumps/reddit/
├── submissions/
│   ├── RS_2024-01.zst      # one file per month
│   └── RS_2024-02.zst
└── comments/
    ├── RC_2024-01.zst
    └── RC_2024-02.zst
```

Parse looks only inside `submissions/` and `comments/`. Files placed directly in `data/dumps/reddit/` are not detected, and the run reports `Found 0 compressed files`.

Supported compressed formats are `.zst`, `.gz`, `.xz` and `.tar.gz`. Already-decompressed files can go in `data/extracted/reddit/<type>/` instead, and parse picks them up from there.

File patterns (from `config/sources/reddit/platform.yaml`):

| Stage | Pattern | Example |
|-------|---------|---------|
| Compressed dump | `RS_(\d{4}-\d{2})\..+` | `RS_2024-01.zst`, `RS_2024-01.gz` |
| Decompressed | `RS_(\d{4}-\d{2})` | `RS_2024-01` |
| Parsed Parquet | `RS_(\d{4}-\d{2})\.parquet` | `RS_2024-01.parquet` |
| Parsed CSV | `RS_(\d{4}-\d{2})\.csv` | `RS_2024-01.csv` |

Comments use the same patterns with `RC_`.

## Mandatory fields

These fields are always emitted by the parser regardless of the `fields` config and are `NOT NULL` in the database schema:

| Field | Type | Description |
|-------|------|-------------|
| `dataset` | char(7) | Derived from the file name (`RS_2024-01` → `2024-01`) |
| `id` | varchar(7) | Reddit's base-36 unique identifier |

`retrieved_utc` is always emitted by the parser too, but its database column is nullable by design: pre-2010 Arctic Shift dumps have no retrieval timestamp, so those rows store NULL. When the same id appears in several dumps, a row with a real timestamp always wins over one with NULL. How the parser picks the timestamp, and the full deduplication order, are in [Platforms](../architecture/platforms.md#retrieved_utc-format-compatibility) and [Databases and ingestion](../architecture/databases.md#fast-initial-load-new-tables).

## Computed fields

| Field | Type | What it means |
|-------|------|---------------|
| `id10` | bigint | The base-10 value of the base-36 `id`, for applications that need numeric ids or sorting. Include `id10` in the platform config `fields` to get it. See [Platforms](../architecture/platforms.md#id10-base-36-conversion). |
| `is_deleted` | boolean | Whether the post or comment was deleted or removed, decided from several fields in priority order. See [Platforms](../architecture/platforms.md#is_deleted-and-removal_type-waterfall-algorithm). |
| `removal_type` | text | Who removed it, as one of the values below; empty when not removed. |

`removal_type` values:

| Value | Description |
|-------|-------------|
| `deleted` | User deleted their own content |
| `moderator` | Removed by a subreddit moderator |
| `reddit` | Removed by a Reddit admin or the spam filter (includes anti_evil_ops, shadowbans) |
| `automod_filtered` | Removed by AutoModerator |
| `content_takedown` | Legal or DMCA takedown |
| `copyright_takedown` | Copyright-specific takedown |
| `community_ops` | Reddit Community Operations |
| `''` (empty) | Not removed |

`subreddit` and `author` are lowercased by the parser.

## Field list

Edit the `fields` section in `config/sources/reddit/platform.yaml` to choose which fields are extracted, or run `python sdp.py source configure reddit` to reconfigure interactively (or **Reconfigure…** on the source's page in the [web UI](webui.md#reconfiguring-a-source-and-its-classifiers)).

**Submissions** (43 fields configured):

| Group | Fields |
|-------|--------|
| Core | `created_utc`, `id10`, `score`, `upvote_ratio`, `num_comments`, `num_crossposts`, `total_awards_received` |
| Subreddit | `subreddit` (lowercased), `subreddit_subscribers` |
| Status | `stickied`, `gilded`, `distinguished`, `locked`, `quarantine`, `over_18`, `is_deleted`, `removal_type` |
| Author | `author` (lowercased), `author_flair_text`, `author_created_utc` |
| Content | `link_flair_text`, `domain`, `url`, `title`, `selftext` |

**Comments** (27 fields configured):

| Group | Fields |
|-------|--------|
| Core | `created_utc`, `link_id`, `parent_id`, `score`, `controversiality`, `total_awards_received` |
| Subreddit | `subreddit` (lowercased) |
| Status | `stickied`, `gilded`, `distinguished`, `is_deleted`, `removal_type` |
| Author | `author` (lowercased), `author_flair_text`, `author_created_utc`, `is_submitter` |
| Content | `body` |

## Field types

Defined in the `field_types` section of `config/sources/reddit/platform.yaml`:

| Type | Fields |
|------|--------|
| `integer` | `created_utc`, `author_created_utc`, `score`, `gilded`, `controversiality`, `num_comments`, `num_crossposts`, `subreddit_subscribers`, `total_awards_received` |
| `bigint` | `id10` |
| `float` | `upvote_ratio`, `lang_prob`, `lang2_prob` |
| `boolean` | `stickied`, `is_submitter`, `is_deleted`, `locked`, `quarantine`, `over_18` |
| `text` | `author`, `subreddit`, `body`, `title`, `selftext`, `distinguished`, `author_flair_text`, `link_flair_text`, `domain`, `url`, `removal_type` |
| `varchar(10)` | `link_id`, `parent_id` |
| `varchar(2)` | `lang`, `lang2` |

## Database indexes

Default indexes (from `platform.yaml`):

| Type | Indexed fields |
|------|----------------|
| Submissions | `dataset`, `author`, `subreddit`, `domain` |
| Comments | `dataset`, `author`, `subreddit`, `link_id` |

Override them in the source's `platform.yaml` or `postgres.yaml`, or run `python sdp.py source configure reddit`.

Classifier tables (`comments_lingua`, `submissions_toxicity_en`, and so on) are not covered by these defaults. They have their own sets, `ml_indexes` (postgres_ml) and `sr_ml_indexes` (sr_ml), keyed by table name and shipped empty in the template:

```yaml
ml_indexes:
  comments_lingua: [lang]
sr_ml_indexes:
  comments_lingua: [lang]
```

There is no fallback from these to `indexes`, because the base fields name columns the classifier tables do not have. `python sdp.py db create-indexes` writes to the right set automatically.

For StarRocks, a plain list means a bitmap index; use `{bitmap: [...], bloomfilter: [...]}` to pick per column. See [Choosing a StarRocks index type](databases.md#choosing-a-starrocks-index-type).
