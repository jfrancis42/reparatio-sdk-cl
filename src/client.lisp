(in-package #:reparatio)

;;; ---------------------------------------------------------------------------
;;; Constants
;;; ---------------------------------------------------------------------------

(defparameter *default-base-url* "https://reparatio.app")
(defparameter *default-timeout*  120)

;;; ---------------------------------------------------------------------------
;;; HTTP hook — rebind in tests to intercept requests
;;; ---------------------------------------------------------------------------

(defvar *http-request-fn* #'drakma:http-request
  "Function called for every outbound HTTP request.
   Must accept the same arguments as DRAKMA:HTTP-REQUEST and return the
   same multiple values: (body status-code headers uri stream must-close reason).")

;;; ---------------------------------------------------------------------------
;;; Client struct
;;; ---------------------------------------------------------------------------

(defstruct (client (:constructor %make-client))
  "Reparatio API client.  Construct with MAKE-CLIENT."
  base-url
  api-key
  timeout)

(defun make-client (&key api-key
                         (base-url *default-base-url*)
                         (timeout  *default-timeout*))
  "Create a Reparatio API client.

API-KEY  — your rp_... API key.  Falls back to the REPARATIO_API_KEY
           environment variable when omitted.
BASE-URL — override the API root (default https://reparatio.app).
TIMEOUT  — HTTP timeout in seconds (default 120)."
  (let ((key (or api-key
                 (uiop:getenv "REPARATIO_API_KEY")
                 "")))
    (%make-client :base-url (string-right-trim "/" base-url)
                  :api-key  key
                  :timeout  timeout)))

;;; ---------------------------------------------------------------------------
;;; Result struct
;;; ---------------------------------------------------------------------------

(defstruct reparatio-result
  "Holds the output of a file-returning API call.

CONTENT  — (vector (unsigned-byte 8)) of the converted/queried data.
FILENAME — suggested output filename from Content-Disposition (or fallback).
WARNING  — string from X-Reparatio-Warning / X-Reparatio-Errors, or NIL."
  content
  filename
  warning)

;;; ---------------------------------------------------------------------------
;;; Internal helpers
;;; ---------------------------------------------------------------------------

(defun %url (client path)
  (concatenate 'string (client-base-url client) path))

(defun %headers (client)
  "Return the request headers alist for CLIENT."
  (let ((key (client-api-key client)))
    (if (and key (> (length key) 0))
        (list (cons "X-API-Key" key))
        '())))

(defun %body->string (body)
  "Coerce Drakma body (string or byte vector) to a string."
  (etypecase body
    (string body)
    ((vector (unsigned-byte 8)) (babel:octets-to-string body :encoding :utf-8))
    (null "")))

(defun %parse-json (body)
  "Parse a JSON body (string or bytes) into an alist."
  (let ((json:*json-identifier-name-to-lisp* #'identity))
    (json:decode-json-from-string (%body->string body))))

(defun %alist-get (key alist)
  "Case-insensitive header lookup."
  (cdr (assoc key alist :test #'string-equal)))

(defun %filename-from-headers (headers fallback)
  "Extract filename from Content-Disposition header, or return FALLBACK."
  (let ((cd (%alist-get "Content-Disposition" headers)))
    (if cd
        (let ((pos (search "filename=\"" cd)))
          (if pos
              (let* ((start (+ pos 10))
                     (end   (position #\" cd :start start)))
                (if end
                    (subseq cd start end)
                    fallback))
              fallback))
        fallback)))

(defun %raise-for-status (status body)
  "Signal the appropriate condition for HTTP error STATUS codes."
  (when (>= status 400)
    (let* ((text   (%body->string body))
           (detail (handler-case
                       (let* ((parsed (%parse-json text))
                              (d      (cdr (assoc "detail" parsed :test #'string=))))
                         (or d text))
                     (error () text))))
      (cond
        ((member status '(401 403))
         (error 'authentication-error    :status status :message detail))
        ((= status 402)
         (error 'insufficient-plan-error :status status :message detail))
        ((= status 413)
         (error 'file-too-large-error    :status status :message detail))
        ((= status 422)
         (error 'reparatio-parse-error   :status status :message detail))
        (t
         (error 'reparatio-error         :status status :message detail))))))

(defun %request (client method path &key parameters)
  "Issue an HTTP request and return (values body status headers).
   Signals a REPARATIO-ERROR subclass on HTTP error responses."
  (multiple-value-bind (body status headers)
      (funcall *http-request-fn*
               (%url client path)
               :method           method
               :additional-headers (%headers client)
               :parameters       parameters
               :form-data        t
               :force-binary     t
               :connection-timeout (client-timeout client)
               :read-timeout       (client-timeout client))
    (%raise-for-status status body)
    (values body status headers)))

(defun %get (client path)
  "Issue a GET request; return parsed JSON alist."
  (multiple-value-bind (body status headers)
      (funcall *http-request-fn*
               (%url client path)
               :method             :get
               :additional-headers (%headers client)
               :force-binary       nil
               :connection-timeout (client-timeout client)
               :read-timeout       (client-timeout client))
    (declare (ignore headers))
    (%raise-for-status status body)
    (%parse-json body)))

(defun %url-decode (str)
  "Minimal percent-decoding for header values."
  (with-output-to-string (out)
    (loop with i = 0
          while (< i (length str))
          do (let ((ch (char str i)))
               (cond
                 ((and (char= ch #\%) (< (+ i 2) (length str)))
                  (write-char (code-char
                               (parse-integer (subseq str (1+ i) (+ i 3))
                                              :radix 16))
                              out)
                  (incf i 3))
                 ((char= ch #\+)
                  (write-char #\Space out)
                  (incf i))
                 (t
                  (write-char ch out)
                  (incf i)))))))

(defun %load-file (path-or-bytes filename)
  "Return (values octets filename).
   PATH-OR-BYTES may be a pathname, namestring, or byte vector."
  (etypecase path-or-bytes
    ((vector (unsigned-byte 8)) (values path-or-bytes filename))
    ((or pathname string)
     (let* ((p   (pathname path-or-bytes))
            (len (with-open-file (s p :element-type '(unsigned-byte 8))
                   (file-length s)))
            (buf (make-array len :element-type '(unsigned-byte 8))))
       (with-open-file (s p :element-type '(unsigned-byte 8))
         (read-sequence buf s))
       (values buf (file-namestring p))))))

;;; ---------------------------------------------------------------------------
;;; Public API
;;; ---------------------------------------------------------------------------

(defun formats (client)
  "Return an alist with :INPUT and :OUTPUT keys listing supported formats.

No API key required.

Example:
  (let ((c (make-client)))
    (formats c))
  ;; => ((\"input\" \"csv\" \"xlsx\" ...) (\"output\" \"csv\" \"parquet\" ...))"
  (%get client "/api/v1/formats"))

(defun me (client)
  "Return an alist with subscription and usage information for CLIENT's API key.

Keys: email, plan, expires_at, api_access, active, request_count,
      data_bytes_total."
  (%get client "/api/v1/me"))

(defun inspect-file (client file
                     &key (filename  "file")
                          (no-header  nil)
                          (fix-encoding t)
                          (preview-rows 8)
                          (delimiter  "")
                          (sheet      ""))
  "Inspect FILE and return an alist with schema, encoding, row count, and preview.

FILE may be a pathname, namestring, or (vector (unsigned-byte 8)).
FILENAME is used for format detection when FILE is a byte vector.
No API key required.

Returned alist keys: filename, detected_encoding, detected_delimiter,
rows_total, sheets, columns, preview."
  (multiple-value-bind (octets fname)
      (%load-file file filename)
    (multiple-value-bind (body _status _headers)
        (%request client :post "/api/v1/inspect"
                  :parameters
                  (list (list "file" octets :filename fname
                              :content-type "application/octet-stream")
                        (cons "no_header"    (if no-header "true" "false"))
                        (cons "fix_encoding" (if fix-encoding "true" "false"))
                        (cons "preview_rows" (format nil "~A" preview-rows))
                        (cons "delimiter"    delimiter)
                        (cons "sheet"        sheet)))
      (declare (ignore _status _headers))
      (%parse-json body))))

(defun convert (client file target-format
                &key (filename         "file")
                     (no-header         nil)
                     (fix-encoding      t)
                     (delimiter         "")
                     (sheet             "")
                     (select-columns    nil)
                     (deduplicate       nil)
                     (sample-n          0)
                     (sample-frac       0.0)
                     (geometry-column   "geometry")
                     (cast-columns      nil)
                     (null-values       nil)
                     (encoding-override nil))
  "Convert FILE to TARGET-FORMAT.  Returns a REPARATIO-RESULT.

FILE may be a pathname, namestring, or (vector (unsigned-byte 8)).
TARGET-FORMAT is a string such as \"parquet\", \"csv\", \"xlsx\".

SELECT-COLUMNS — list of column name strings to include.
CAST-COLUMNS   — alist of (\"col\" . ((\"type\" . \"Float64\") ...)) overrides.
NULL-VALUES    — list of strings to treat as null (e.g. '(\"N/A\" \"NULL\")).
ENCODING-OVERRIDE — force a specific encoding, e.g. \"cp037\" for EBCDIC US."
  (multiple-value-bind (octets fname)
      (%load-file file filename)
    (let ((params
            (list* (list "file" octets :filename fname
                         :content-type "application/octet-stream")
                   (cons "target_format"   target-format)
                   (cons "no_header"       (if no-header "true" "false"))
                   (cons "fix_encoding"    (if fix-encoding "true" "false"))
                   (cons "delimiter"       delimiter)
                   (cons "sheet"           sheet)
                   (cons "select_columns"  (json:encode-json-to-string
                                            (or select-columns #())))
                   (cons "deduplicate"     (if deduplicate "true" "false"))
                   (cons "sample_n"        (format nil "~A" sample-n))
                   (cons "sample_frac"     (format nil "~F" sample-frac))
                   (cons "geometry_column" geometry-column)
                   (cons "cast_columns"    (json:encode-json-to-string
                                            (or cast-columns #())))
                   (cons "null_values"     (json:encode-json-to-string
                                            (or null-values #())))
                   (when encoding-override
                     (list (cons "encoding_override" encoding-override))))))
      (setf params (remove nil params))
      (multiple-value-bind (body _status headers)
          (%request client :post "/api/v1/convert" :parameters params)
        (declare (ignore _status))
        (let* ((fallback (concatenate 'string
                                      (subseq fname 0
                                              (or (position #\. fname :from-end t)
                                                  (length fname)))
                                      "." target-format))
               (out-name (%filename-from-headers headers fallback))
               (warning  (%alist-get "X-Reparatio-Warning" headers)))
          (make-reparatio-result :content body
                                 :filename out-name
                                 :warning  warning))))))

(defun batch-convert (client zip-file target-format
                      &key (filename      "batch.zip")
                           (no-header      nil)
                           (fix-encoding   t)
                           (delimiter      "")
                           (select-columns nil)
                           (deduplicate    nil)
                           (sample-n       0)
                           (sample-frac    0.0)
                           (cast-columns   nil))
  "Convert every file inside a ZIP archive to TARGET-FORMAT.
Returns a REPARATIO-RESULT whose CONTENT is a ZIP of converted files.
Files that cannot be parsed are skipped; their errors appear in WARNING."
  (multiple-value-bind (octets fname)
      (%load-file zip-file filename)
    (multiple-value-bind (body _status headers)
        (%request client :post "/api/v1/batch-convert"
                  :parameters
                  (list (list "zip_file" octets :filename fname
                              :content-type "application/zip")
                        (cons "target_format"  target-format)
                        (cons "no_header"      (if no-header "true" "false"))
                        (cons "fix_encoding"   (if fix-encoding "true" "false"))
                        (cons "delimiter"      delimiter)
                        (cons "select_columns" (json:encode-json-to-string
                                                (or select-columns #())))
                        (cons "deduplicate"    (if deduplicate "true" "false"))
                        (cons "sample_n"       (format nil "~A" sample-n))
                        (cons "sample_frac"    (format nil "~F" sample-frac))
                        (cons "cast_columns"   (json:encode-json-to-string
                                                (or cast-columns #())))))
      (declare (ignore _status))
      (let* ((out-name (%filename-from-headers headers "converted.zip"))
             (raw-err  (%alist-get "X-Reparatio-Errors" headers))
             (warning  (when raw-err (%url-decode raw-err))))
        (make-reparatio-result :content body
                               :filename out-name
                               :warning  warning)))))

(defun merge-files (client file1 file2 operation target-format
                    &key (filename1       "file1")
                         (filename2       "file2")
                         (join-on         "")
                         (no-header        nil)
                         (fix-encoding     t)
                         (geometry-column  "geometry"))
  "Merge or join FILE1 and FILE2.  Returns a REPARATIO-RESULT.

OPERATION is one of: \"append\" \"left\" \"right\" \"outer\" \"inner\".
JOIN-ON   is a comma-separated list of column names (not needed for append)."
  (multiple-value-bind (oct1 fname1) (%load-file file1 filename1)
    (multiple-value-bind (oct2 fname2) (%load-file file2 filename2)
      (multiple-value-bind (body _status headers)
          (%request client :post "/api/v1/merge"
                    :parameters
                    (list (list "file1" oct1 :filename fname1
                                :content-type "application/octet-stream")
                          (list "file2" oct2 :filename fname2
                                :content-type "application/octet-stream")
                          (cons "operation"       operation)
                          (cons "target_format"   target-format)
                          (cons "join_on"         join-on)
                          (cons "no_header"       (if no-header "true" "false"))
                          (cons "fix_encoding"    (if fix-encoding "true" "false"))
                          (cons "geometry_column" geometry-column)))
        (declare (ignore _status))
        (let* ((base1    (subseq fname1 0
                                 (or (position #\. fname1 :from-end t)
                                     (length fname1))))
               (base2    (subseq fname2 0
                                 (or (position #\. fname2 :from-end t)
                                     (length fname2))))
               (fallback (format nil "~A_~A_~A.~A"
                                 base1 operation base2 target-format))
               (out-name (%filename-from-headers headers fallback))
               (warning  (%alist-get "X-Reparatio-Warning" headers)))
          (make-reparatio-result :content body
                                 :filename out-name
                                 :warning  warning))))))

(defun append-files (client files target-format
                     &key (filenames   nil)
                          (no-header    nil)
                          (fix-encoding t))
  "Stack rows from FILES vertically.  Returns a REPARATIO-RESULT.

FILES     — list of pathnames, namestrings, or byte vectors (minimum 2).
FILENAMES — corresponding original filenames (needed when FILES are byte vectors).
Column mismatches are filled with null."
  (when (< (length files) 2)
    (error "At least 2 files are required for append-files"))
  (let* ((names  (or filenames
                     (loop for i from 0 below (length files)
                           collect (format nil "file~A" i))))
         (loaded (mapcar (lambda (f n) (multiple-value-list (%load-file f n)))
                         files names))
         (file-params (mapcar (lambda (pair)
                                (destructuring-bind (octets fname) pair
                                  (list "files" octets :filename fname
                                        :content-type "application/octet-stream")))
                              loaded))
         (other-params (list (cons "target_format" target-format)
                             (cons "no_header"     (if no-header "true" "false"))
                             (cons "fix_encoding"  (if fix-encoding "true" "false"))))
         (all-params (append file-params other-params)))
    (multiple-value-bind (body _status headers)
        (%request client :post "/api/v1/append" :parameters all-params)
      (declare (ignore _status))
      (let* ((out-name (%filename-from-headers headers
                                               (format nil "appended.~A" target-format)))
             (warning  (%alist-get "X-Reparatio-Warning" headers)))
        (make-reparatio-result :content body
                               :filename out-name
                               :warning  warning)))))

(defun query (client file sql
              &key (filename      "file")
                   (target-format "csv")
                   (no-header      nil)
                   (fix-encoding   t)
                   (delimiter      "")
                   (sheet          ""))
  "Run SQL against FILE (table is named DATA).  Returns a REPARATIO-RESULT.

Example:
  (query client #p\"sales.csv\"
         \"SELECT region, SUM(revenue) FROM data GROUP BY region\")"
  (multiple-value-bind (octets fname)
      (%load-file file filename)
    (multiple-value-bind (body _status headers)
        (%request client :post "/api/v1/query"
                  :parameters
                  (list (list "file" octets :filename fname
                              :content-type "application/octet-stream")
                        (cons "sql"           sql)
                        (cons "target_format" target-format)
                        (cons "no_header"     (if no-header "true" "false"))
                        (cons "fix_encoding"  (if fix-encoding "true" "false"))
                        (cons "delimiter"     delimiter)
                        (cons "sheet"         sheet)))
      (declare (ignore _status))
      (let* ((base     (subseq fname 0
                                (or (position #\. fname :from-end t)
                                    (length fname))))
             (fallback (format nil "~A_query.~A" base target-format))
             (out-name (%filename-from-headers headers fallback)))
        (make-reparatio-result :content body
                               :filename out-name
                               :warning  nil)))))
