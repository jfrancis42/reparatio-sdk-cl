# reparatio-sdk-cl

Common Lisp SDK for the [Reparatio](https://reparatio.app) data conversion API.

## Requirements

- SBCL (or any ANSI Common Lisp with ASDF 3)
- [Quicklisp](https://www.quicklisp.org/)

## Installation

```
# from the reparatio-sdk-cl/ directory
sbcl --eval '(load "~/quicklisp/setup.lisp")' \
     --eval '(pushnew #p"." asdf:*central-registry*)' \
     --eval '(ql:quickload "reparatio")'
```

## Quick start

```lisp
(ql:quickload "reparatio")
(use-package :reparatio)

(let ((client (make-client :api-key "rp_your_key_here")))

  ;; List supported formats (no key required)
  (formats client)

  ;; Check your subscription
  (me client)

  ;; Inspect a file
  (inspect-file client #p"data.csv")

  ;; Convert CSV → Parquet
  (let ((result (convert client #p"data.csv" "parquet")))
    (with-open-file (out (reparatio-result-filename result)
                         :direction :output
                         :element-type '(unsigned-byte 8)
                         :if-exists :supersede)
      (write-sequence (reparatio-result-content result) out)))

  ;; SQL query against a file
  (query client #p"sales.csv"
         "SELECT region, SUM(revenue) FROM data GROUP BY region"
         :target-format "json")

  ;; Merge two files (left join on "id")
  (merge-files client #p"orders.csv" #p"customers.csv"
               "left" "parquet"
               :join-on "id")

  ;; Stack files vertically
  (append-files client (list #p"jan.csv" #p"feb.csv" #p"mar.csv")
                "csv")

  ;; Batch-convert a ZIP archive
  (batch-convert client #p"archive.zip" "parquet"))
```

The API key may also be supplied via the `REPARATIO_API_KEY` environment variable.

## API reference

### `(make-client &key api-key base-url timeout)`

| Keyword | Default | Description |
|---------|---------|-------------|
| `api-key` | `$REPARATIO_API_KEY` | Your `rp_…` API key |
| `base-url` | `https://reparatio.app` | API root URL |
| `timeout` | `120` | HTTP timeout in seconds |

### `(formats client)`

Returns an alist with `"input"` and `"output"` keys. No API key required.

### `(me client)`

Returns subscription and usage info for the current API key.

### `(inspect-file client file &key filename no-header fix-encoding preview-rows delimiter sheet)`

Inspect a file; returns an alist with schema, encoding, row count, and preview rows.
`file` may be a pathname, namestring, or `(vector (unsigned-byte 8))`.

### `(convert client file target-format &key filename no-header fix-encoding delimiter sheet select-columns deduplicate sample-n sample-frac geometry-column cast-columns null-values encoding-override)`

Convert a file. Returns a `reparatio-result`.

`target-format` examples: `"parquet"`, `"csv"`, `"xlsx"`, `"json"`, `"tsv"`, `"feather"`, `"arrow"`, `"orc"`, `"avro"`, `"geojson"`, `"jsonl"`.

`encoding-override` accepts any Python codec name, e.g. `"cp037"` (EBCDIC US), `"cp500"` (EBCDIC International), `"latin-1"`.

### `(batch-convert client zip-file target-format &key filename no-header fix-encoding delimiter select-columns deduplicate sample-n sample-frac cast-columns)`

Convert every file in a ZIP archive. Returns a `reparatio-result`; skipped files are described in `reparatio-result-warning`.

### `(merge-files client file1 file2 operation target-format &key filename1 filename2 join-on no-header fix-encoding geometry-column)`

Merge or join two files. `operation` is one of `"append"`, `"left"`, `"right"`, `"outer"`, `"inner"`.

### `(append-files client files target-format &key filenames no-header fix-encoding)`

Stack rows from two or more files vertically. `files` is a list of pathnames or byte vectors (minimum 2).

### `(query client file sql &key filename target-format no-header fix-encoding delimiter sheet)`

Run SQL against a file (table name: `data`). Returns a `reparatio-result`.

### `reparatio-result` struct

| Accessor | Type | Description |
|----------|------|-------------|
| `reparatio-result-content` | `(vector (unsigned-byte 8))` | Raw file bytes |
| `reparatio-result-filename` | `string` | Suggested output filename |
| `reparatio-result-warning` | `string` or `nil` | Server warning / skipped-file errors |

### Conditions

| Condition | HTTP status | Supertype |
|-----------|-------------|-----------|
| `authentication-error` | 401, 403 | `reparatio-error` |
| `insufficient-plan-error` | 402 | `reparatio-error` |
| `file-too-large-error` | 413 | `reparatio-error` |
| `reparatio-parse-error` | 422 | `reparatio-error` |
| `reparatio-error` | other 4xx/5xx | `error` |

All conditions expose `reparatio-error-status` (integer) and `reparatio-error-message` (string).

## Running the tests

```
cd ~/reparatio-sdk-cl
sbcl --load run-tests.lisp
```

Exit code 0 = all tests passed.

---

## Running the Examples

The repository includes 15 runnable examples covering every API method.

```bash
sbcl --noinform \
     --eval '(load "~/quicklisp/setup.lisp")' \
     --eval "(pushnew #p\"$(pwd)/\" asdf:*central-registry* :test #'equal)" \
     --eval '(ql:quickload "reparatio" :silent t)' \
     --load 'examples/examples.lisp' \
     --eval '(reparatio-examples:run-all-examples)' \
     --eval '(uiop:quit 0)'
```

Set `REPARATIO_API_KEY` in your environment before running.

## License

MIT — © Ordo Artificum LLC
