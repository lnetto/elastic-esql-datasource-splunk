# esql-datasource-splunk

An **ES|QL Data Federation** connector for Elasticsearch that queries **Splunk buckets where they already are**: SmartStore remote volumes and frozen archives on Amazon S3 or S3-compatible storage. There's no thawing, no re-indexing, and Splunk doesn't need to be running.

```esql
FROM splunk_archive
| WHERE splunk.sourcetype == "cisco:asa"
| STATS events = COUNT(*) BY host.name
```

The plugin reads each bucket's `rawdata/journal` (gzip, zstd or lz4) and decodes it on the fly into ES|QL columns. Those include `@timestamp`, `message` (`_raw`), `host.name`, `splunk.index`/`host`/`source`/`sourcetype`, and every Splunk indexed field as `splunk.fields.*`.

> **Status: experimental.** ES|QL Data Federation is itself experimental in Elasticsearch 9.5, and this plugin builds on its internal SPI. Each plugin zip works with exactly one Elasticsearch version. This is a community project, not an official Elastic product.

## Requirements

- **Elasticsearch 9.5.4**, self-managed, Docker or Elastic Cloud Hosted. Serverless doesn't support plugins. For another version, [build from source](#build-from-source).
- A license that includes ES|QL Data Federation (Enterprise, or a trial).
- Read access to the S3 bucket holding the Splunk buckets (AWS or S3-compatible), reachable from every Elasticsearch node.

## Install

The ready-to-install plugin is in [`dist/`](dist/): `esql-datasource-splunk-0.2.3-es9.5.4.zip`.

### Self-managed

On **every** node:

```bash
bin/elasticsearch-plugin install file:///path/to/esql-datasource-splunk-0.2.3-es9.5.4.zip
```

Then add to `elasticsearch.yml`:

```yaml
esql.federation.enabled: true
```

Restart the nodes. In `kibana.yml`, set `xpack.dataFederation.enabled: true` to turn on Kibana's Data Federation pages.

### Docker

```bash
docker build -f docker/Dockerfile -t elasticsearch-esql-splunk:9.5.4 .
```

Run the image with `-e esql.federation.enabled=true`. Bake the plugin into the image like this rather than installing it into a running container, which loses it when the container is recreated.

### Elastic Cloud Hosted

Your Cloud organization's subscription must allow custom plugins.

1. **Deployments → Extensions → Create extension**:
   - name `esql-datasource-splunk`, type **plugin**, version `9.5.4`;
   - upload the zip.
2. **Edit deployment**:
   - Elasticsearch → *Manage plugins and extensions* → enable `esql-datasource-splunk`.
   - Elasticsearch → *User settings*: `esql.federation.enabled: true`.
   - Kibana → *User settings*: `xpack.dataFederation.enabled: true`.
3. Save. This applies as a rolling restart, which takes a few minutes.

Keep the S3 bucket in the deployment's region.

## Connect your buckets

Run these in Kibana → Dev Tools. First create a **data source**, which holds shared settings. Kibana's Data Federation UI can't create one of type `splunk`, so use the API:

```
PUT /_query/data_source/splunk
{ "type": "splunk", "settings": { "region": "us-east-1" } }
```

Then add one or more **datasets**, each pointing at a bucket tree:

```
# SmartStore remote volume: the whole volume, or one index (…/volume/firewall)
PUT /_query/dataset/splunk_smartstore
{ "data_source": "splunk",
  "resource": "splunks3://my-smartstore-bucket/volume",
  "settings": { "access_key": "…", "secret_key": "…" } }

# Frozen archive on S3
PUT /_query/dataset/splunk_frozen
{ "data_source": "splunk",
  "resource": "splunks3://my-archive-bucket/frozen",
  "settings": { "access_key": "…", "secret_key": "…" } }
```

Now query them:

```
POST /_query?format=txt
{ "query": "FROM splunk_smartstore | STATS events = COUNT(*) BY splunk.index, splunk.sourcetype" }
```

Or open **Discover** in ES|QL mode and run `FROM splunk_smartstore | LIMIT 100`.

**Credentials:** put S3 credentials on the **dataset**, not the data source, because on 9.5.4 connectors receive data-source secrets still encrypted. Use a read-only IAM user scoped to the bucket: `GET /_query/dataset/<name>` shows the keys to anyone allowed to call it.

**Adding datasets in Kibana's UI:** you can add datasets there once the data source exists. Set **format** to **auto** or leave it empty. An explicit file format such as `parquet` or `csv` can route the dataset to Elasticsearch's built-in file reader instead.

## Settings

Each setting can go on the data source or on the dataset. A dataset setting wins.

| setting | meaning |
|---|---|
| `region` | S3 region. Default `us-east-1`. |
| `endpoint` | S3-compatible endpoint (MinIO, SeaweedFS…). Default: AWS. |
| `path_style` | Path-style S3 URLs. Default `true` when `endpoint` is set. |
| `access_key`, `secret_key`, `session_token` | Static S3 credentials. Omit them for a public bucket. |
| `earliest`, `latest` | Epoch seconds or ISO-8601. Buckets outside the range are skipped, which makes this the easiest way to speed things up. |
| `index` | Report this index name for every bucket. |
| `index_pattern` | Regex over the bucket's path; group 1 is the index name. Use it for frozen archives in custom directory trees. |
| `fields` | `auto` (default) discovers indexed fields by sampling buckets. You can also give `none`, a list such as `user,src_ip`, or `auto,extra_field`. |
| `timestamp_field` | What fills `@timestamp`. Default `splunk.time` (Splunk's `_time`). Also accepts `splunk.index_time`, or an indexed field holding ISO-8601 or epoch values. |
| `connect_timeout_ms`, `request_timeout_ms` | S3 timeouts. Defaults 5000 and 300000. |

### Where the buckets can live

Discovery walks everything under the dataset's S3 prefix, at any depth. Any directory that contains `rawdata/journal*`, SmartStore journal slices, or a loose `journal.gz`/`.zst`/`.lz4` counts as a bucket.

- **SmartStore:** the index name comes from the directory before `db/`. Duplicate uploads are resolved with `receipt.json`.
- **Frozen buckets:** buckets named `db_<newest>_<oldest>_<id>` also give each bucket's time range, so `earliest`/`latest` can skip whole buckets.
- **Replicated copies:** `rb_` copies of a `db_` bucket are read once.

Example of `index_pattern` for an archive laid out as `<year>/<month>/<index>/<bucket>/`:

```
"settings": { "index_pattern": "^[^/]+/[^/]+/([^/]+)/" }
```

## Columns

| column | Splunk | notes |
|---|---|---|
| `@timestamp` | `_time` | Kibana's time field. Can be changed with `timestamp_field`. |
| `message` | `_raw` | The raw event. |
| `host.name` | `host` | |
| `log.file.path` | `source` | Only when the source is a file path; otherwise null. |
| `splunk.index`, `splunk.host`, `splunk.source`, `splunk.sourcetype` | same | |
| `splunk.time` | `_time` | Always the event time. |
| `splunk.index_time` | `_indextime` | |
| `splunk.bucket` | bucket id | |
| `splunk.fields.<name>` | indexed fields | Keyword. Multi-valued when Splunk repeats a key. |

`splunk.fields.*` holds what Splunk stored at index time:
- `INDEXED_EXTRACTIONS` (JSON/CSV) keys and `TRANSFORMS`/`WRITE_META` fields;
- Splunk's `date_*`, `punct` and `timestartpos`/`timeendpos`. `date_*` exist only for events whose timestamp Splunk parsed from the text.

Search-time extractions aren't stored in buckets, so parse `message` with `GROK` or `DISSECT`:

```esql
FROM splunk_smartstore
| WHERE splunk.sourcetype == "access_combined"
| GROK message "%{IP:client} %{NOTSPACE} %{NOTSPACE} \\[%{DATA}\\] \"%{WORD:method} %{NOTSPACE:path} %{DATA}\" %{INT:status}"
| STATS hits = COUNT(*) BY status
```

**Discover's time picker** isn't applied to external datasets on 9.5.4. Reference it in the query instead:

```esql
FROM splunk_smartstore | WHERE @timestamp >= ?_tstart AND @timestamp <= ?_tend
```

## Performance

Decoding runs at about **1 million events per second per query** for gzip journals, and about 1.4 million for zstd. On 9.5.4, each query scans the dataset's buckets sequentially on one core. Elasticsearch doesn't yet hand plugin connectors parallel splits; the plugin already implements them, so they'll activate once the framework does.

To keep queries fast:
- scope a dataset to one index (`…/volume/<index>`);
- set `earliest`/`latest`;
- use `LIMIT`, which stops decoding early.

## Limitations

- Read-only; it reads the journal only. Events removed with Splunk's `| delete` still appear, and metrics indexes return nothing, because their data lives in tsidx files.
- S3 authentication is static keys or anonymous. There's no IAM-role chain yet.
- Tarred or zipped frozen archives aren't unpacked.
- The Kibana Data Federation UI can't create the `splunk` data source; use the API, as shown above.
- Dataset `mappings` aren't supported; use `timestamp_field` to choose `@timestamp`.

## Build from source

You only need Docker. The script pulls the matching Elasticsearch image to compile against, and builds in a JDK 21 Gradle container:

```bash
./build-plugin.sh                       # Elasticsearch 9.5.4 → dist/
ES_VERSION=9.5.5 ./build-plugin.sh      # another version
```

A plugin zip installs only on the exact Elasticsearch version it was built for. Upgrading Elasticsearch means rebuilding the plugin, and on Cloud, uploading it as a new extension.

## License

[Apache 2.0](LICENSE)
