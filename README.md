# GCP-Bucket-Loader

A fast, lightweight CLI tool for uploading and downloading files to/from Google Cloud Storage. Built in Go with gzip compression, CRC32C integrity checks, buffered I/O, concurrent multi-file upload support, and optional retries for transient GCP/network errors.

---

## Build

```bash
make local      # build for the current platform → bin/GCP-Bucket-Loader
make build      # cross-compile for macOS / Linux / Windows (amd64 + arm64)
make clean      # remove the bin/ directory
```

---

## Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `-action` | string | — | **Required.** `upload` or `download` |
| `-file` | string | — | **Required.** Local file path(s). Accepts a single path, comma-separated paths, or a glob pattern (e.g. `"*.txt"`). Glob and comma are mutually exclusive. |
| `-bucket` | string | — | **Required.** GCS bucket name |
| `-object` | string | — | GCS object path. If omitted, derived from the local file path. Acts as a path prefix in multi-file mode. |
| `-key` | string | — | Path to a GCP service-account JSON key file. Required unless `-public` is set. |
| `-public` | bool | `false` | Perform unauthenticated requests (public buckets only). |
| `-type` | string | — | IANA media type (e.g. `text/html`). If omitted, GCS auto-detects. |
| `-extra` | bool | `false` | Enable extra API checks (bucket/object existence, post-upload verification, CRC32C validation on download). |
| `-no-compress` | bool | `false` | Disable gzip compression. Compression is **on** by default. |
| `-buffer` | int | `256` | I/O buffer size in KB. |
| `-timeout` | int | — | Total operation timeout in seconds. Omit or pass `0` for **no** overall limit (runs until completion). |
| `-file-timeout` | int | `0` | Per-file timeout in seconds for multi-file uploads. `0` = no per-file limit; `-timeout` still caps the whole run when set. |
| `-workers` | uint | `0` | Concurrent workers for multi-file upload. `0` = sequential. |
| `-retry` | int | `0` | Extra attempts after a failed GCP/network operation. `0` = no retries; with `-retry N`, each retried operation runs up to **N + 1** times total, waiting **1 second** between attempts. See [Retry behaviour](#retry-behaviour). |

---

## Authentication

By default the tool authenticates with a service account key file:

```bash
-key /path/to/service-account.json
```

For public buckets, skip authentication entirely:

```bash
-public
```

---

## Retry behaviour

When `-retry` is greater than `0`, the tool re-runs failed **remote** steps instead of failing immediately:

- **Bucket checks** (`BucketHandle.Attrs`) when `-extra` triggers them, including the initial multi-file upload check.
- **Object metadata** (`ObjectHandle.Attrs`) when `-extra` is enabled (upload pre-flight, post-upload verification, download pre-flight).
- **Upload**: opening the writer, streaming data, and closing the object (each full upload is retried as a unit; the local file is seeked back to the start on each attempt).
- **Download**: opening the reader, streaming to disk, and closing (the local partial file is truncated and the CRC state reset on each attempt).

Retries use a **1 second** pause between attempts and log a **WARNING** with the attempt number and error.

**Not retried:** client construction (`storage.NewClient`). If that fails, the process exits immediately.

---

## Examples

### Single-file upload

```bash
# Object path derived from file path (uploads as "reports/q1.csv" in the bucket)
GCP-Bucket-Loader -action upload -bucket my-bucket -key sa.json \
  -file reports/q1.csv

# Explicit object path
GCP-Bucket-Loader -action upload -bucket my-bucket -key sa.json \
  -file reports/q1.csv -object archive/2024/q1.csv
```

### Single-file download

```bash
# Object path derived from the local path provided
GCP-Bucket-Loader -action download -bucket my-bucket -key sa.json \
  -file reports/q1.csv

# Explicit object path
GCP-Bucket-Loader -action download -bucket my-bucket -key sa.json \
  -file ./local/q1.csv -object archive/2024/q1.csv
```

### Multi-file upload — glob pattern

```bash
# Upload all .log files; each file keeps its relative path as the GCS object path
GCP-Bucket-Loader -action upload -bucket my-bucket -key sa.json \
  -file "*.log"

# Upload with an object path prefix
GCP-Bucket-Loader -action upload -bucket my-bucket -key sa.json \
  -file "logs/*.log" -object backups/2024/
```

### Multi-file upload — comma-separated

```bash
GCP-Bucket-Loader -action upload -bucket my-bucket -key sa.json \
  -file "file1.txt,file2.txt,data/file3.txt"
```

### Concurrent upload with a worker pool

```bash
# 4 concurrent workers, 30 s per-file timeout
GCP-Bucket-Loader -action upload -bucket my-bucket -key sa.json \
  -file "data/*.csv" -workers 4 -file-timeout 30
```

### Upload without compression

```bash
GCP-Bucket-Loader -action upload -bucket my-bucket -key sa.json \
  -file payload.bin -no-compress
```

### Download with integrity verification

```bash
# -extra enables CRC32C validation; mismatches delete the local file and exit
GCP-Bucket-Loader -action download -bucket my-bucket -key sa.json \
  -file ./local/report.csv -object reports/report.csv -extra
```

### Custom buffer and timeout

By default there is no overall time limit; set `-timeout` when you need a deadline (seconds).

```bash
GCP-Bucket-Loader -action upload -bucket my-bucket -key sa.json \
  -file large-file.tar.gz -buffer 1024 -timeout 300
```

### Retries for flaky networks

Use `-retry` to tolerate short outages or rate limits (each value is the number of *extra* attempts after the first failure):

```bash
GCP-Bucket-Loader -action upload -bucket my-bucket -key sa.json \
  -file reports/q1.csv -retry 3

GCP-Bucket-Loader -action download -bucket my-bucket -key sa.json \
  -file ./local/report.csv -object reports/report.csv -retry 5
```

---

## Compression & Integrity

- **Gzip compression** is enabled by default for all uploads. The `Content-Encoding: gzip` attribute is set on the GCS object, and the original CRC32C is stored in object metadata (`x-original-crc32c`) so integrity can be verified after decompression on download.
- When compression is **disabled** (`-no-compress`), the CRC32C is passed directly to the GCS write API for server-side validation.
- On **download**, if `-extra` is set, the computed CRC32C of the decompressed data is validated against the stored reference. A mismatch deletes the file immediately and exits with a fatal error.
- Without `-extra`, CRC32C is still computed and logged but not validated (no reference value is fetched).

---

## Multi-file behaviour

- Multi-file **download is not supported**. Use a shell loop or scripting if needed.
- Per-file failures are logged as warnings and do not abort the remaining uploads.
- `-workers 0` (default) processes files sequentially.
- `-workers N` (N > 0) launches a pool of N goroutines.
- For single-file operations, `-file-timeout` and `-workers` are ignored.
- `-retry` applies to upload and download (including per-file uploads in multi-file mode).

---

## Exit behaviour

| Condition | Outcome |
|---|---|
| Missing mandatory flag | Fatal exit |
| File is not a regular file | Fatal (single) / skip with warning (multi) |
| Object not found on download | Fatal exit |
| CRC32C mismatch on download | Deletes file, fatal exit |
| Per-file upload failure (multi) | Warning logged, continues |
| All uploads complete with failures (multi-file) | Completion banner reports failure count, process exits with code **1** |
