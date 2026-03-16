;;;; examples/examples.lisp
;;;;
;;;; Comprehensive runnable examples for the Reparatio Common Lisp SDK.
;;;;
;;;; Run with:
;;;;   sbcl --noinform \
;;;;        --eval '(load "~/quicklisp/setup.lisp")' \
;;;;        --eval "(pushnew #p\"$(pwd)/\" asdf:*central-registry* :test #'equal)" \
;;;;        --eval '(ql:quickload "reparatio" :silent t)' \
;;;;        --load 'examples/examples.lisp' \
;;;;        --eval '(reparatio-examples:run-all-examples)' \
;;;;        --eval '(uiop:quit 0)'

(defpackage #:reparatio-examples
  (:use #:cl #:reparatio)
  (:export #:run-all-examples))

(in-package #:reparatio-examples)

;;; ---------------------------------------------------------------------------
;;; Configuration
;;; ---------------------------------------------------------------------------

(defparameter *api-key*
  (or (uiop:getenv "REPARATIO_API_KEY") "EXAMPLE-EXAMPLE-EXAMPLE")
  "API key — set the REPARATIO_API_KEY environment variable.")

;;; ---------------------------------------------------------------------------
;;; Helpers
;;; ---------------------------------------------------------------------------

(defun make-test-client ()
  "Return a client using the default Reparatio API endpoint."
  (make-client :api-key *api-key*))

(defun print-header (title)
  (format t "~%~%══════════════════════════════════════════════════~%")
  (format t "  ~A~%" title)
  (format t "══════════════════════════════════════════════════~%"))

(defun bytes-start-with-p (vec &rest expected-bytes)
  "Return T if VEC starts with the given byte values."
  (and (>= (length vec) (length expected-bytes))
       (every #'= vec expected-bytes)))

(defun csv-bytes (text)
  "Encode TEXT (with ~% newlines) as UTF-8 byte vector."
  (babel:string-to-octets (format nil text) :encoding :utf-8))

;;; ---------------------------------------------------------------------------
;;; CRC-32 table (needed by make-simple-zip — must precede it)
;;; ---------------------------------------------------------------------------

(defparameter *crc32-table*
  (let ((tbl (make-array 256 :element-type '(unsigned-byte 32))))
    (dotimes (i 256 tbl)
      (let ((c i))
        (dotimes (_ 8)
          (if (oddp c)
              (setf c (logxor #xEDB88320 (logand (ash c -1) #x7FFFFFFF)))
              (setf c (logand (ash c -1) #x7FFFFFFF))))
        (setf (aref tbl i) c))))
  "CRC-32 lookup table for ZIP generation.")

(defun %crc32 (data)
  "Compute CRC-32 of byte vector DATA."
  (let ((crc #xFFFFFFFF))
    (dotimes (i (length data))
      (let ((byte (aref data i)))
        (setf crc (logxor (logand (ash crc -8) #xFFFFFF)
                          (aref *crc32-table*
                                (logand (logxor crc byte) #xFF))))))
    (logxor crc #xFFFFFFFF)))

(defun %u16le (n)
  "2-byte little-endian."
  (list (logand n #xFF) (logand (ash n -8) #xFF)))

(defun %u32le (n)
  "4-byte little-endian."
  (list (logand n #xFF)
        (logand (ash n  -8) #xFF)
        (logand (ash n -16) #xFF)
        (logand (ash n -24) #xFF)))

(defun %write-bytes (vec offset byte-list)
  "Write BYTE-LIST into VEC starting at OFFSET."
  (loop for b in byte-list
        for i from offset
        do (setf (aref vec i) b))
  (+ offset (length byte-list)))

(defun make-simple-zip (files)
  "Create a minimal ZIP archive in memory.
FILES is a list of (name-string . byte-vector) pairs.
Returns a (vector (unsigned-byte 8))."
  ;; Pass 1: build local-file headers + data, record offsets
  (let* ((entries '())        ; list of (local-offset name-bytes data crc data-len)
         (body-size 0))
    (dolist (entry files)
      (let* ((name      (car entry))
             (data      (cdr entry))
             (name-bytes (babel:string-to-octets name :encoding :utf-8))
             (name-len   (length name-bytes))
             (data-len   (length data))
             (crc        (%crc32 data))
             (lf-size    (+ 30 name-len data-len)))
        (push (list body-size name-bytes data crc data-len) entries)
        (incf body-size lf-size)))
    (setf entries (nreverse entries))
    ;; Pass 2: compute central-directory size
    (let* ((cd-entries-size (reduce #'+ entries
                                    :key (lambda (e) (+ 46 (length (second e))))
                                    :initial-value 0))
           (eocd-size 22)
           (total-size (+ body-size cd-entries-size eocd-size))
           (zip-buf (make-array total-size :element-type '(unsigned-byte 8)
                                :initial-element 0))
           (pos 0))
      ;; Write local file entries
      (dolist (e entries)
        (destructuring-bind (lf-offset name-bytes data crc data-len) e
          (declare (ignore lf-offset))
          (let ((name-len (length name-bytes)))
            ;; Local file header signature
            (setf pos (%write-bytes zip-buf pos '(#x50 #x4B #x03 #x04)))
            ;; version needed (20), GP flag (0), compression (0 = stored)
            (setf pos (%write-bytes zip-buf pos '(20 0 0 0 0 0)))
            ;; last-mod-time, last-mod-date
            (setf pos (%write-bytes zip-buf pos '(0 0 0 0)))
            ;; CRC-32
            (setf pos (%write-bytes zip-buf pos (%u32le crc)))
            ;; compressed size
            (setf pos (%write-bytes zip-buf pos (%u32le data-len)))
            ;; uncompressed size
            (setf pos (%write-bytes zip-buf pos (%u32le data-len)))
            ;; filename length
            (setf pos (%write-bytes zip-buf pos (%u16le name-len)))
            ;; extra field length (0)
            (setf pos (%write-bytes zip-buf pos '(0 0)))
            ;; filename
            (replace zip-buf name-bytes :start1 pos)
            (incf pos name-len)
            ;; file data
            (replace zip-buf data :start1 pos)
            (incf pos data-len))))
      ;; Write central directory
      (let ((cd-start pos))
        (dolist (e entries)
          (destructuring-bind (lf-offset name-bytes data crc data-len) e
            (declare (ignore data))
            (let ((name-len (length name-bytes)))
              (setf pos (%write-bytes zip-buf pos '(#x50 #x4B #x01 #x02)))
              ;; version made by (20), version needed (20)
              (setf pos (%write-bytes zip-buf pos '(20 0 20 0)))
              ;; GP flag, compression
              (setf pos (%write-bytes zip-buf pos '(0 0 0 0)))
              ;; last-mod-time, last-mod-date
              (setf pos (%write-bytes zip-buf pos '(0 0 0 0)))
              ;; CRC-32
              (setf pos (%write-bytes zip-buf pos (%u32le crc)))
              ;; compressed size
              (setf pos (%write-bytes zip-buf pos (%u32le data-len)))
              ;; uncompressed size
              (setf pos (%write-bytes zip-buf pos (%u32le data-len)))
              ;; filename length
              (setf pos (%write-bytes zip-buf pos (%u16le name-len)))
              ;; extra field length, file comment length
              (setf pos (%write-bytes zip-buf pos '(0 0 0 0)))
              ;; disk number start, internal attrs, external attrs
              (setf pos (%write-bytes zip-buf pos '(0 0 0 0 0 0 0 0)))
              ;; offset of local header
              (setf pos (%write-bytes zip-buf pos (%u32le lf-offset)))
              ;; filename
              (replace zip-buf name-bytes :start1 pos)
              (incf pos name-len))))
        ;; End-of-central-directory record
        (let* ((num-entries (length files))
               (cd-size     (- pos cd-start)))
          (setf pos (%write-bytes zip-buf pos '(#x50 #x4B #x05 #x06)))
          ;; disk number, disk with CD start
          (setf pos (%write-bytes zip-buf pos '(0 0 0 0)))
          ;; entries on this disk
          (setf pos (%write-bytes zip-buf pos (%u16le num-entries)))
          ;; total entries
          (setf pos (%write-bytes zip-buf pos (%u16le num-entries)))
          ;; size of CD
          (setf pos (%write-bytes zip-buf pos (%u32le cd-size)))
          ;; offset of CD
          (setf pos (%write-bytes zip-buf pos (%u32le cd-start)))
          ;; comment length
          (setf pos (%write-bytes zip-buf pos '(0 0)))
          (values pos)))
      zip-buf)))

;;; ---------------------------------------------------------------------------
;;; Test counter
;;; ---------------------------------------------------------------------------

(defvar *pass-count* 0)
(defvar *fail-count* 0)

(defmacro with-example (title &body body)
  "Run BODY as a named example, printing PASS or FAIL."
  `(progn
     (print-header ,title)
     (handler-case
         (progn
           ,@body
           (format t "~%PASS~%")
           (incf *pass-count*))
       (error (e)
         (format t "~%FAIL: ~A~%" e)
         (incf *fail-count*)))))

;;; ---------------------------------------------------------------------------
;;; Example 1 — formats (no key required)
;;; ---------------------------------------------------------------------------

(defun example-formats ()
  (with-example "FORMATS — list supported input/output formats"
    (let* ((client (make-test-client))
           (result (formats client)))
      (format t "Result type: ~A~%" (type-of result))
      (let ((inputs  (cdr (assoc "input"  result :test #'string=)))
            (outputs (cdr (assoc "output" result :test #'string=))))
        (format t "Input  formats: ~{~A~^, ~}~%" inputs)
        (format t "Output formats: ~{~A~^, ~}~%" outputs)
        (assert (listp result)
                () "formats result should be a list")
        (assert (assoc "input" result :test #'string=)
                () "result must have 'input' key")
        (assert (assoc "output" result :test #'string=)
                () "result must have 'output' key")
        (assert (member "csv" inputs :test #'string=)
                () "csv must be listed as an input format")
        (assert (member "parquet" outputs :test #'string=)
                () "parquet must be listed as an output format")))))

;;; ---------------------------------------------------------------------------
;;; Example 2 — me (account info)
;;; ---------------------------------------------------------------------------

(defun example-me ()
  (with-example "ME — account / subscription info"
    (let* ((client (make-test-client))
           (result (me client)))
      (format t "Result type: ~A~%" (type-of result))
      (let ((email (cdr (assoc "email"         result :test #'string=)))
            (plan  (cdr (assoc "plan"          result :test #'string=)))
            (count (cdr (assoc "request_count" result :test #'string=))))
        (format t "Email:         ~A~%" email)
        (format t "Plan:          ~A~%" plan)
        (format t "Request count: ~A~%" count)
        (assert (listp result)
                () "me result should be a list")
        (assert (stringp plan)
                () "plan should be a string")
        (assert (string= plan "pro")
                () "API key should be on the pro plan, got: ~A" plan)
        (assert (and count (>= count 0))
                () "request_count should be a non-negative integer")))))

;;; ---------------------------------------------------------------------------
;;; Example 3 — inspect-file from inline CSV bytes
;;; ---------------------------------------------------------------------------

(defun example-inspect-csv-path ()
  (with-example "INSPECT-FILE — CSV from inline byte vector"
    (let* ((client  (make-test-client))
           (raw-csv (csv-bytes "country,county~%England,Kent~%England,Essex~%Wales,Gwent~%"))
           (result  (inspect-file client raw-csv :filename "counties.csv")))
      (format t "filename:          ~A~%" (cdr (assoc "filename"          result :test #'string=)))
      (format t "detected_encoding: ~A~%" (cdr (assoc "detected_encoding" result :test #'string=)))
      (format t "rows:              ~A~%" (cdr (assoc "rows"              result :test #'string=)))
      (let ((cols (cdr (assoc "columns" result :test #'string=))))
        (format t "columns: ~{~A~^, ~}~%"
                (mapcar (lambda (c) (cdr (assoc "name" c :test #'string=))) cols))
        (assert (listp result)
                () "inspect-file should return a list")
        (assert (> (cdr (assoc "rows" result :test #'string=)) 0)
                () "rows should be > 0")
        (assert (listp cols)
                () "columns should be a list")
        (assert (>= (length cols) 2)
                () "CSV should have at least 2 columns, got ~A" (length cols))
        (let ((col-names (mapcar (lambda (c) (cdr (assoc "name" c :test #'string=))) cols)))
          (assert (member "country" col-names :test #'string=)
                  () "Expected 'country' column, got ~A" col-names)
          (assert (member "county" col-names :test #'string=)
                  () "Expected 'county' column, got ~A" col-names))))))

;;; ---------------------------------------------------------------------------
;;; Example 4 — inspect-file from raw byte vector (in-memory CSV)
;;; ---------------------------------------------------------------------------

(defun example-inspect-bytes ()
  (with-example "INSPECT-FILE — raw byte vector (in-memory CSV)"
    (let* ((client  (make-test-client))
           (raw-csv (csv-bytes "id,name,score~%1,Alice,95~%2,Bob,87~%3,Carol,92~%"))
           (result  (inspect-file client raw-csv :filename "scores.csv")))
      (format t "filename:          ~A~%" (cdr (assoc "filename"          result :test #'string=)))
      (format t "detected_encoding: ~A~%" (cdr (assoc "detected_encoding" result :test #'string=)))
      (format t "rows:              ~A~%" (cdr (assoc "rows"              result :test #'string=)))
      (let ((cols (cdr (assoc "columns" result :test #'string=))))
        (format t "columns: ~{~A~^, ~}~%"
                (mapcar (lambda (c) (cdr (assoc "name" c :test #'string=))) cols))
        (assert (listp result)
                () "inspect-file should return a list")
        (assert (= 3 (cdr (assoc "rows" result :test #'string=)))
                () "Expected 3 rows, got ~A" (cdr (assoc "rows" result :test #'string=)))
        (let ((col-names (mapcar (lambda (c) (cdr (assoc "name" c :test #'string=))) cols)))
          (assert (member "id"    col-names :test #'string=) () "Expected 'id' column")
          (assert (member "name"  col-names :test #'string=) () "Expected 'name' column")
          (assert (member "score" col-names :test #'string=) () "Expected 'score' column"))))))

;;; ---------------------------------------------------------------------------
;;; Example 5 — inspect-file TSV (tab-separated) from inline bytes
;;; ---------------------------------------------------------------------------

(defun example-inspect-excel ()
  (with-example "INSPECT-FILE — TSV from inline byte vector"
    (let* ((client  (make-test-client))
           (raw-tsv (babel:string-to-octets
                     (format nil "city~Tpopulation~%London~T9000000~%Paris~T2100000~%Berlin~T3700000~%")
                     :encoding :utf-8))
           (result  (inspect-file client raw-tsv :filename "cities.tsv")))
      (format t "filename: ~A~%" (cdr (assoc "filename" result :test #'string=)))
      (format t "rows:     ~A~%" (cdr (assoc "rows"     result :test #'string=)))
      (let ((cols (cdr (assoc "columns" result :test #'string=))))
        (format t "columns: ~{~A~^, ~}~%"
                (mapcar (lambda (c) (cdr (assoc "name" c :test #'string=))) cols))
        (assert (listp result)
                () "inspect-file should return a list")
        (assert (> (cdr (assoc "rows" result :test #'string=)) 0)
                () "rows should be > 0")
        (assert (listp cols)
                () "columns should be a list")
        (assert (>= (length cols) 2)
                () "TSV should have at least 2 columns, got ~A" (length cols))))))

;;; ---------------------------------------------------------------------------
;;; Example 6 — convert CSV → Parquet (verify PAR1 magic bytes)
;;; ---------------------------------------------------------------------------

(defun example-convert-csv-to-parquet ()
  (with-example "CONVERT — CSV to Parquet (verify PAR1 magic bytes)"
    (let* ((client  (make-test-client))
           (raw-csv (csv-bytes "country,county~%England,Kent~%England,Essex~%Wales,Gwent~%"))
           (result  (convert client raw-csv "parquet" :filename "counties.csv")))
      (assert (reparatio-result-p result)
              () "convert should return a reparatio-result")
      (let ((content  (reparatio-result-content result))
            (filename (reparatio-result-filename result)))
        (format t "Output filename: ~A~%" filename)
        (format t "Content length:  ~A bytes~%" (length content))
        (format t "First 4 bytes:   ~{#x~2,'0X~^ ~}~%"
                (list (aref content 0) (aref content 1)
                      (aref content 2) (aref content 3)))
        ;; Parquet files start with PAR1 (0x50 0x41 0x52 0x31)
        (assert (bytes-start-with-p content #x50 #x41 #x52 #x31)
                () "Parquet output must start with PAR1 magic bytes, got ~{#x~2,'0X~^ ~}"
                (list (aref content 0) (aref content 1)
                      (aref content 2) (aref content 3)))
        (assert (search "parquet" filename)
                () "Output filename should contain 'parquet', got: ~A" filename)))))

;;; ---------------------------------------------------------------------------
;;; Example 7 — convert CSV → JSON Lines
;;; ---------------------------------------------------------------------------

(defun example-convert-excel-to-jsonl ()
  (with-example "CONVERT — CSV to JSON Lines"
    (let* ((client  (make-test-client))
           (raw-csv (csv-bytes "city,country,population~%London,England,9000000~%Paris,France,2100000~%"))
           (result  (convert client raw-csv "jsonl" :filename "cities.csv")))
      (assert (reparatio-result-p result)
              () "convert should return a reparatio-result")
      (let* ((content  (reparatio-result-content result))
             (filename (reparatio-result-filename result))
             (text     (babel:octets-to-string content :encoding :utf-8)))
        (format t "Output filename: ~A~%" filename)
        (format t "Content length:  ~A bytes~%" (length content))
        (format t "First 120 chars: ~A~%" (subseq text 0 (min 120 (length text))))
        ;; JSON Lines: first non-whitespace char is {
        (let ((first-char (char (string-trim " " text) 0)))
          (assert (char= first-char #\{)
                  () "JSONL output should start with '{', got '~A'" first-char))
        ;; Each line should look like valid JSON (contains a colon)
        (let ((first-line (subseq text 0 (or (position #\Newline text) (length text)))))
          (assert (find #\: first-line)
                  () "First JSONL line should contain ':', got: ~A" first-line))))))

;;; ---------------------------------------------------------------------------
;;; Example 8 — convert with select columns
;;; ---------------------------------------------------------------------------

(defun example-convert-select-columns ()
  (with-example "CONVERT — select columns"
    ;; Inline CSV has country, county, population; select only country and county
    (let* ((client  (make-test-client))
           (raw-csv (csv-bytes "country,county,population~%England,Kent,1900000~%England,Essex,1800000~%Wales,Gwent,600000~%"))
           (result  (convert client raw-csv "csv"
                             :filename "counties.csv"
                             :select-columns '("country" "county"))))
      (assert (reparatio-result-p result)
              () "convert should return a reparatio-result")
      (let* ((content  (reparatio-result-content result))
             (filename (reparatio-result-filename result))
             (text     (babel:octets-to-string content :encoding :utf-8))
             (header   (subseq text 0 (or (position #\Newline text) (length text)))))
        (format t "Output filename: ~A~%" filename)
        (format t "Content length:  ~A bytes~%" (length content))
        (format t "Header row:      ~A~%" header)
        (assert (search "country" header)
                () "Header should contain 'country', got: ~A" header)
        (assert (search "county" header)
                () "Header should contain 'county', got: ~A" header)))))

;;; ---------------------------------------------------------------------------
;;; Example 9 — convert with deduplicate + sample
;;; ---------------------------------------------------------------------------

(defun example-convert-deduplicate-sample ()
  (with-example "CONVERT — deduplicate + sample"
    ;; Inline CSV has 20 rows (some duplicates); sample 10 after deduplication
    (let* ((client  (make-test-client))
           (raw-csv (csv-bytes
                     (concatenate 'string
                                  "country,county~%"
                                  "England,Kent~%England,Essex~%England,Surrey~%"
                                  "England,Sussex~%England,Norfolk~%England,Suffolk~%"
                                  "England,Devon~%England,Cornwall~%England,Dorset~%"
                                  "England,Somerset~%Wales,Gwent~%Wales,Powys~%"
                                  "Wales,Dyfed~%Wales,Clwyd~%Scotland,Fife~%"
                                  "Scotland,Lothian~%Scotland,Grampian~%"
                                  "England,Kent~%England,Essex~%Wales,Gwent~%")))
           (result  (convert client raw-csv "csv"
                             :filename "counties.csv"
                             :deduplicate t
                             :sample-n 10)))
      (assert (reparatio-result-p result)
              () "convert should return a reparatio-result")
      (let* ((content  (reparatio-result-content result))
             (filename (reparatio-result-filename result))
             (text     (babel:octets-to-string content :encoding :utf-8))
             ;; Count data rows: total newlines minus header, accounting for trailing newline
             (newlines  (count #\Newline text))
             (data-rows (if (and (> (length text) 0)
                                 (char= (char text (1- (length text))) #\Newline))
                            (1- newlines)
                            newlines)))
        (format t "Output filename:            ~A~%" filename)
        (format t "Content length:             ~A bytes~%" (length content))
        (format t "Rows in output (data only): ~A~%" data-rows)
        ;; Should have at most 10 data rows (the sample)
        (assert (<= data-rows 10)
                () "Sampled output should have at most 10 data rows, got ~A" data-rows)))))

;;; ---------------------------------------------------------------------------
;;; Example 10 — convert with castColumns type overrides
;;; ---------------------------------------------------------------------------

(defun example-convert-cast-columns ()
  (with-example "CONVERT — castColumns type overrides"
    ;; Build a CSV with a score column and override its type to Float64
    (let* ((client  (make-test-client))
           (raw-csv (csv-bytes "id,name,score~%1,Alice,95.5~%2,Bob,87.0~%3,Carol,92.3~%"))
           ;; cast-columns: alist of column-name -> alist of type properties
           (cast    (list (cons "score" (list (cons "type" "Float64")))
                          (cons "id"    (list (cons "type" "Int64")))))
           (result  (convert client raw-csv "csv"
                             :filename "scores.csv"
                             :cast-columns cast)))
      (assert (reparatio-result-p result)
              () "convert should return a reparatio-result")
      (let* ((content  (reparatio-result-content result))
             (filename (reparatio-result-filename result))
             (text     (babel:octets-to-string content :encoding :utf-8)))
        (format t "Output filename: ~A~%" filename)
        (format t "Content length:  ~A bytes~%" (length content))
        (format t "Output text:~%~A~%" text)
        (assert (> (length content) 0)
                () "Output should not be empty")
        ;; Score values should round-trip
        (assert (search "95" text)
                () "Expected score value in output, got: ~A" text)))))

;;; ---------------------------------------------------------------------------
;;; Example 11 — query with SQL aggregation
;;; ---------------------------------------------------------------------------

(defun example-query-sql ()
  (with-example "QUERY — SQL aggregation (GROUP BY country)"
    (let* ((client  (make-test-client))
           (raw-csv (csv-bytes
                     (concatenate 'string
                                  "country,county~%"
                                  "England,Kent~%England,Essex~%England,Surrey~%"
                                  "Wales,Gwent~%Wales,Powys~%Scotland,Fife~%")))
           (result  (query client raw-csv
                           "SELECT country, COUNT(*) AS county_count FROM data GROUP BY country ORDER BY county_count DESC"
                           :filename "counties.csv"
                           :target-format "csv")))
      (assert (reparatio-result-p result)
              () "query should return a reparatio-result")
      (let* ((content  (reparatio-result-content result))
             (filename (reparatio-result-filename result))
             (text     (babel:octets-to-string content :encoding :utf-8))
             (header   (subseq text 0 (or (position #\Newline text) (length text)))))
        (format t "Output filename: ~A~%" filename)
        (format t "Content length:  ~A bytes~%" (length content))
        (format t "Header row: ~A~%" header)
        (format t "First 300 chars:~%~A~%"
                (subseq text 0 (min 300 (length text))))
        (assert (search "country" header)
                () "Header should contain 'country', got: ~A" header)
        (assert (search "county_count" header)
                () "Header should contain 'county_count', got: ~A" header)
        (assert (search "England" text)
                () "Expected 'England' in aggregation result")))))

;;; ---------------------------------------------------------------------------
;;; Example 12 — append-files (stack three in-memory CSVs)
;;; ---------------------------------------------------------------------------

(defun example-append-files ()
  (with-example "APPEND-FILES — stack three in-memory CSVs vertically"
    (let* ((client (make-test-client))
           (csv-a  (csv-bytes "id,name~%1,Alice~%2,Bob~%"))
           (csv-b  (csv-bytes "id,name~%3,Carol~%4,Dave~%"))
           (csv-c  (csv-bytes "id,name~%5,Eve~%6,Frank~%"))
           (result (append-files client
                                 (list csv-a csv-b csv-c)
                                 "csv"
                                 :filenames '("a.csv" "b.csv" "c.csv"))))
      (assert (reparatio-result-p result)
              () "append-files should return a reparatio-result")
      (let* ((content  (reparatio-result-content result))
             (filename (reparatio-result-filename result))
             (text     (babel:octets-to-string content :encoding :utf-8))
             (newlines  (count #\Newline text))
             (data-rows (if (and (> (length text) 0)
                                 (char= (char text (1- (length text))) #\Newline))
                            (1- newlines)
                            newlines)))
        (format t "Output filename:   ~A~%" filename)
        (format t "Content length:    ~A bytes~%" (length content))
        (format t "Total rows (data): ~A~%" data-rows)
        (format t "Output:~%~A~%" text)
        ;; 3 files × 2 rows each = 6 data rows
        (assert (= 6 data-rows)
                () "Expected 6 data rows (3 files x 2 rows each), got ~A" data-rows)
        (dolist (name '("Alice" "Bob" "Carol" "Dave" "Eve" "Frank"))
          (assert (search name text)
                  () "Expected name ~A in appended output" name))))))

;;; ---------------------------------------------------------------------------
;;; Example 13 — merge-files (inner join on key column)
;;; ---------------------------------------------------------------------------

(defun example-merge-files ()
  (with-example "MERGE-FILES — inner join two CSVs on 'id' column"
    (let* ((client  (make-test-client))
           ;; left: id + name
           (left    (csv-bytes "id,name~%1,Alice~%2,Bob~%3,Carol~%"))
           ;; right: id + score (no row for id=2, extra row for id=4)
           (right   (csv-bytes "id,score~%1,95~%3,88~%4,72~%"))
           (result  (merge-files client left right "inner" "csv"
                                :filename1 "people.csv"
                                :filename2 "scores.csv"
                                :join-on   "id")))
      (assert (reparatio-result-p result)
              () "merge-files should return a reparatio-result")
      (let* ((content  (reparatio-result-content result))
             (filename (reparatio-result-filename result))
             (text     (babel:octets-to-string content :encoding :utf-8))
             (header   (subseq text 0 (or (position #\Newline text) (length text)))))
        (format t "Output filename: ~A~%" filename)
        (format t "Content length:  ~A bytes~%" (length content))
        (format t "Output:~%~A~%" text)
        ;; Inner join: ids 1 and 3 match in both; id 2 (Bob) is left-only
        (assert (search "Alice" text)
                () "Expected 'Alice' (id=1) in inner join result")
        (assert (search "Carol" text)
                () "Expected 'Carol' (id=3) in inner join result")
        (assert (not (search "Bob" text))
                () "Did not expect 'Bob' (id=2, no match) in inner join result")
        (assert (search "id" header)
                () "Header should contain 'id', got: ~A" header)))))

;;; ---------------------------------------------------------------------------
;;; Example 14 — batch-convert (ZIP of CSVs → ZIP of Parquets)
;;; ---------------------------------------------------------------------------

(defun example-batch-convert ()
  (with-example "BATCH-CONVERT — ZIP of CSVs to ZIP of Parquets"
    (let* ((client   (make-test-client))
           (csv1     (csv-bytes "x,y~%1,2~%3,4~%"))
           (csv2     (csv-bytes "a,b,c~%10,20,30~%40,50,60~%"))
           (zip-data (make-simple-zip
                      (list (cons "file1.csv" csv1)
                            (cons "file2.csv" csv2))))
           (result   (batch-convert client zip-data "parquet"
                                   :filename "input.zip")))
      (assert (reparatio-result-p result)
              () "batch-convert should return a reparatio-result")
      (let* ((content  (reparatio-result-content result))
             (filename (reparatio-result-filename result)))
        (format t "Output filename: ~A~%" filename)
        (format t "Content length:  ~A bytes~%" (length content))
        (format t "First 4 bytes:   ~{#x~2,'0X~^ ~}~%"
                (list (aref content 0) (aref content 1)
                      (aref content 2) (aref content 3)))
        (format t "Warning: ~A~%" (reparatio-result-warning result))
        ;; Output should be a ZIP (PK magic bytes: 0x50 0x4B)
        (assert (bytes-start-with-p content #x50 #x4B)
                () "Batch-convert output should be a ZIP (PK magic), got ~{#x~2,'0X~^ ~}"
                (list (aref content 0) (aref content 1)))))))

;;; ---------------------------------------------------------------------------
;;; Example 15 — error handling (bad API key → authentication-error)
;;; ---------------------------------------------------------------------------

(defun example-error-handling ()
  (with-example "ERROR HANDLING — bad key signals AUTHENTICATION-ERROR"
    (let ((bad-client (make-client :api-key "invalid-key-xyz")))
      (let ((caught nil))
        (handler-case
            (me bad-client)
          (authentication-error (e)
            (setf caught e)
            (format t "Caught authentication-error as expected~%")
            (format t "Status:  ~A~%" (reparatio-error-status e))
            (format t "Message: ~A~%" (reparatio-error-message e))))
        (assert caught
                () "Expected authentication-error to be signalled for bad API key")
        (assert (typep caught 'authentication-error)
                () "Caught condition should be of type AUTHENTICATION-ERROR")
        (assert (typep caught 'reparatio-error)
                () "AUTHENTICATION-ERROR should be a subtype of REPARATIO-ERROR")
        (assert (member (reparatio-error-status caught) '(401 403))
                () "Status should be 401 or 403, got ~A"
                (reparatio-error-status caught))
        (assert (stringp (reparatio-error-message caught))
                () "Error message should be a string")))))

;;; ---------------------------------------------------------------------------
;;; Top-level runner
;;; ---------------------------------------------------------------------------

(defun run-all-examples ()
  "Run all examples.  Prints a summary and calls (uiop:quit 1) on any failure."
  (setf *pass-count* 0
        *fail-count* 0)
  (format t "~%=================================================~%")
  (format t "  Reparatio Common Lisp SDK -- Examples~%")
  (format t "=================================================~%")
  (example-formats)
  (example-me)
  (example-inspect-csv-path)
  (example-inspect-bytes)
  (example-inspect-excel)
  (example-convert-csv-to-parquet)
  (example-convert-excel-to-jsonl)
  (example-convert-select-columns)
  (example-convert-deduplicate-sample)
  (example-convert-cast-columns)
  (example-query-sql)
  (example-append-files)
  (example-merge-files)
  (example-batch-convert)
  (example-error-handling)
  (format t "~%~%=================================================~%")
  (format t "  Results: ~A/~A passed~%"
          *pass-count* (+ *pass-count* *fail-count*))
  (format t "=================================================~%")
  (when (> *fail-count* 0)
    (format t "~A example(s) FAILED.~%" *fail-count*)
    (uiop:quit 1))
  (format t "All examples passed.~%"))
