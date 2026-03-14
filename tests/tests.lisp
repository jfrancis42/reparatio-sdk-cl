(in-package #:reparatio-tests)

;;; ---------------------------------------------------------------------------
;;; Test helpers
;;; ---------------------------------------------------------------------------

(defun make-mock-response (status body &optional headers)
  "Return a lambda usable as *HTTP-REQUEST-FN* that yields a fixed response.
Values match what Drakma:HTTP-REQUEST returns:
  body  status-code  headers  uri  stream  must-close  reason"
  (let ((body-bytes (etypecase body
                      (string (babel:string-to-octets body :encoding :utf-8))
                      ((vector (unsigned-byte 8)) body)
                      (null   (make-array 0 :element-type '(unsigned-byte 8))))))
    (lambda (&rest _)
      (declare (ignore _))
      (values body-bytes status (or headers '()) nil nil nil "OK"))))

(defun json-bytes (alist)
  "Encode ALIST as JSON bytes."
  (babel:string-to-octets (json:encode-json-alist-to-string alist)
                          :encoding :utf-8))

;;; ---------------------------------------------------------------------------
;;; Sample data
;;; ---------------------------------------------------------------------------

(defparameter *formats-json*
  "{\"input\":[\"csv\",\"xlsx\",\"parquet\",\"json\",\"tsv\"],\
\"output\":[\"csv\",\"xlsx\",\"parquet\",\"json\",\"tsv\"]}")

(defparameter *me-json*
  "{\"email\":\"user@example.com\",\"plan\":\"pro\",\
\"expires_at\":\"2027-01-01\",\"api_access\":true,\"active\":true,\
\"request_count\":42,\"data_bytes_total\":1048576}")

(defparameter *inspect-json*
  "{\"filename\":\"data.csv\",\"detected_encoding\":\"utf-8\",\
\"detected_delimiter\":\",\",\"rows_total\":100,\"sheets\":[],\
\"columns\":[{\"name\":\"id\",\"dtype\":\"Int64\",\
\"null_count\":0,\"unique_count\":100}],\"preview\":[]}")

(defparameter *csv-bytes*
  (babel:string-to-octets "id,name\n1,Alice\n2,Bob\n" :encoding :utf-8))

(defparameter *zip-bytes*
  ;; minimal valid PK zip magic bytes for testing
  (make-array 4 :element-type '(unsigned-byte 8)
              :initial-contents '(#x50 #x4B #x03 #x04)))

;;; ---------------------------------------------------------------------------
;;; Test suite
;;; ---------------------------------------------------------------------------

(def-suite reparatio-suite :description "Reparatio SDK tests")
(in-suite  reparatio-suite)

;;; ── make-client ─────────────────────────────────────────────────────────────

(test make-client/defaults
  (let ((c (make-client :api-key "rp_test")))
    (is (client-p c))
    (is (string= "rp_test" (client-api-key c)))
    (is (string= "https://reparatio.app" (client-base-url c)))
    (is (= 120 (client-timeout c)))))

(test make-client/custom-base-url
  (let ((c (make-client :api-key "rp_k" :base-url "http://localhost:8000/")))
    ;; trailing slash stripped
    (is (string= "http://localhost:8000" (client-base-url c)))))

(test make-client/custom-timeout
  (let ((c (make-client :api-key "rp_k" :timeout 30)))
    (is (= 30 (client-timeout c)))))

(test make-client/reads-env-var
  ;; Verify env var is used when no explicit key given.
  ;; We test indirectly: if REPARATIO_API_KEY is set, make-client uses it.
  ;; If not set, make-client uses "".  Either way, explicit key wins.
  (let ((env-key (uiop:getenv "REPARATIO_API_KEY")))
    (if (and env-key (> (length env-key) 0))
        ;; env var is set — client should pick it up
        (let ((c (make-client)))
          (is (string= env-key (client-api-key c))))
        ;; env var absent — client key should be ""
        (let ((c (make-client)))
          (is (string= "" (client-api-key c)))))))

(test make-client/explicit-key-overrides-env
  ;; Explicit key always wins regardless of env var.
  (let ((c (make-client :api-key "rp_explicit")))
    (is (string= "rp_explicit" (client-api-key c)))))

;;; ── %filename-from-headers ──────────────────────────────────────────────────

(test filename-from-headers/present
  (let ((hdrs '(("Content-Disposition" . "attachment; filename=\"output.parquet\""))))
    (is (string= "output.parquet"
                 (reparatio::%filename-from-headers hdrs "fallback.csv")))))

(test filename-from-headers/absent
  (is (string= "fallback.csv"
               (reparatio::%filename-from-headers '() "fallback.csv"))))

(test filename-from-headers/no-filename-in-header
  (let ((hdrs '(("Content-Disposition" . "attachment"))))
    (is (string= "fallback.csv"
                 (reparatio::%filename-from-headers hdrs "fallback.csv")))))

;;; ── formats ─────────────────────────────────────────────────────────────────

(test formats/success
  (let* ((c (make-client :api-key "rp_test"))
         (*http-request-fn* (make-mock-response 200 *formats-json*))
         (result (formats c)))
    (is (listp result))
    (is (assoc "input"  result :test #'string=))
    (is (assoc "output" result :test #'string=))))

(test formats/sends-get-request
  (let* ((c (make-client :api-key "rp_test"))
         (captured-method nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key method &allow-other-keys)
            (declare (ignore url))
            (setf captured-method method)
            (values *formats-json* 200 '() nil nil nil "OK")))
    (formats c)
    (is (eq :get captured-method))))

(test formats/sends-api-key-header
  (let* ((c (make-client :api-key "rp_mykey"))
         (captured-headers nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key additional-headers &allow-other-keys)
            (declare (ignore url args))
            (setf captured-headers additional-headers)
            (values *formats-json* 200 '() nil nil nil "OK")))
    (formats c)
    (is (equal "rp_mykey"
               (cdr (assoc "X-API-Key" captured-headers :test #'string=))))))

(test formats/uses-correct-url
  (let* ((c (make-client :api-key "rp_k" :base-url "http://local:9000"))
         (captured-url nil))
    (setf *http-request-fn*
          (lambda (url &rest args)
            (declare (ignore args))
            (setf captured-url url)
            (values *formats-json* 200 '() nil nil nil "OK")))
    (formats c)
    (is (string= "http://local:9000/api/v1/formats" captured-url))))

;;; ── me ───────────────────────────────────────────────────────────────────────

(test me/success
  (let* ((c (make-client :api-key "rp_test"))
         (*http-request-fn* (make-mock-response 200 *me-json*))
         (result (me c)))
    (is (listp result))
    (is (string= "user@example.com"
                 (cdr (assoc "email" result :test #'string=))))
    (is (string= "pro"
                 (cdr (assoc "plan" result :test #'string=))))
    (is (= 42 (cdr (assoc "request_count" result :test #'string=))))))

(test me/401-signals-authentication-error
  (let* ((c (make-client :api-key "bad"))
         (*http-request-fn*
          (make-mock-response 401
                              "{\"detail\":\"Invalid API key\"}")))
    (signals authentication-error (me c))))

(test me/403-signals-authentication-error
  (let* ((c (make-client :api-key "bad"))
         (*http-request-fn* (make-mock-response 403 "{\"detail\":\"Forbidden\"}")))
    (signals authentication-error (me c))))

(test me/402-signals-insufficient-plan-error
  (let* ((c (make-client :api-key "rp_free"))
         (*http-request-fn* (make-mock-response 402 "{\"detail\":\"Pro plan required\"}")))
    (signals insufficient-plan-error (me c))))

;;; ── generic error handling ───────────────────────────────────────────────────

(test error/413-signals-file-too-large
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn* (make-mock-response 413 "{\"detail\":\"File too large\"}")))
    (signals file-too-large-error (me c))))

(test error/422-signals-parse-error
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn* (make-mock-response 422 "{\"detail\":\"Unprocessable\"}")))
    (signals reparatio-parse-error (me c))))

(test error/500-signals-reparatio-error
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn* (make-mock-response 500 "{\"detail\":\"Server error\"}")))
    (signals reparatio-error (me c))))

(test error/status-code-preserved
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn* (make-mock-response 503 "{\"detail\":\"unavailable\"}")))
    (handler-case (me c)
      (reparatio-error (e)
        (is (= 503 (reparatio-error-status e)))
        (is (string= "unavailable" (reparatio-error-message e)))))))

(test error/non-json-body-used-as-message
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn* (make-mock-response 500 "Internal Server Error")))
    (handler-case (me c)
      (reparatio-error (e)
        (is (string= "Internal Server Error" (reparatio-error-message e)))))))

;;; ── inspect-file ────────────────────────────────────────────────────────────

(test inspect-file/success
  (let* ((c (make-client :api-key "rp_test"))
         (resp-headers '(("Content-Type" . "application/json")))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values (babel:string-to-octets *inspect-json* :encoding :utf-8)
                    200 resp-headers nil nil nil "OK")))
         (result (inspect-file c *csv-bytes* :filename "data.csv")))
    (is (listp result))
    (is (string= "utf-8"
                 (cdr (assoc "detected_encoding" result :test #'string=))))
    (is (= 100 (cdr (assoc "rows_total" result :test #'string=))))))

(test inspect-file/sends-filename
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values (babel:string-to-octets *inspect-json* :encoding :utf-8)
                    200 '() nil nil nil "OK")))
    (inspect-file c *csv-bytes* :filename "mydata.csv")
    ;; The first parameter should be the file part with our filename
    (let ((file-part (first captured-params)))
      (is (listp file-part))
      (is (string= "file" (first file-part)))
      (is (string= "mydata.csv" (getf (cddr file-part) :filename))))))

(test inspect-file/no-header-param
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values (babel:string-to-octets *inspect-json* :encoding :utf-8)
                    200 '() nil nil nil "OK")))
    (inspect-file c *csv-bytes* :filename "x.csv" :no-header t)
    (let ((nh (assoc "no_header" captured-params :test #'string=)))
      (is (string= "true" (cdr nh))))))

(test inspect-file/fix-encoding-false
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values (babel:string-to-octets *inspect-json* :encoding :utf-8)
                    200 '() nil nil nil "OK")))
    (inspect-file c *csv-bytes* :filename "x.csv" :fix-encoding nil)
    (let ((fe (assoc "fix_encoding" captured-params :test #'string=)))
      (is (string= "false" (cdr fe))))))

;;; ── convert ─────────────────────────────────────────────────────────────────

(test convert/returns-reparatio-result
  (let* ((c (make-client :api-key "rp_test"))
         (out-bytes (babel:string-to-octets "PAR1" :encoding :utf-8))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values out-bytes 200
                    '(("Content-Disposition" . "attachment; filename=\"output.parquet\""))
                    nil nil nil "OK")))
         (result (convert c *csv-bytes* "parquet" :filename "data.csv")))
    (is (reparatio-result-p result))
    (is (string= "output.parquet" (reparatio-result-filename result)))
    (is (equalp out-bytes (reparatio-result-content result)))
    (is (null (reparatio-result-warning result)))))

(test convert/filename-fallback
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *csv-bytes* 200 '() nil nil nil "OK")))
         (result (convert c *csv-bytes* "json" :filename "data.csv")))
    (is (string= "data.json" (reparatio-result-filename result)))))

(test convert/sends-target-format
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *csv-bytes* 200 '() nil nil nil "OK")))
    (convert c *csv-bytes* "xlsx" :filename "x.csv")
    (let ((tf (assoc "target_format" captured-params :test #'string=)))
      (is (string= "xlsx" (cdr tf))))))

(test convert/sends-select-columns
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *csv-bytes* 200 '() nil nil nil "OK")))
    (convert c *csv-bytes* "csv" :filename "x.csv"
             :select-columns '("id" "name"))
    (let ((sc (assoc "select_columns" captured-params :test #'string=)))
      (is-true sc)
      (is (search "\"id\"" (cdr sc))))))

(test convert/sends-null-values
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *csv-bytes* 200 '() nil nil nil "OK")))
    (convert c *csv-bytes* "csv" :filename "x.csv"
             :null-values '("N/A" "NULL"))
    (let ((nv (assoc "null_values" captured-params :test #'string=)))
      (is-true nv)
      ;; cl-json encodes "/" as "\/" so check for a slash-free value
      (is (search "NULL" (cdr nv))))))

(test convert/sends-encoding-override
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *csv-bytes* 200 '() nil nil nil "OK")))
    (convert c *csv-bytes* "csv" :filename "x.ebcdic"
             :encoding-override "cp037")
    (let ((eo (assoc "encoding_override" captured-params :test #'string=)))
      (is-true eo)
      (is (string= "cp037" (cdr eo))))))

(test convert/warning-from-header
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *csv-bytes* 200
                    '(("X-Reparatio-Warning" . "truncated to 10000 rows"))
                    nil nil nil "OK")))
         (result (convert c *csv-bytes* "csv" :filename "x.csv")))
    (is (string= "truncated to 10000 rows" (reparatio-result-warning result)))))

(test convert/deduplicate-flag
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *csv-bytes* 200 '() nil nil nil "OK")))
    (convert c *csv-bytes* "csv" :filename "x.csv" :deduplicate t)
    (let ((dd (assoc "deduplicate" captured-params :test #'string=)))
      (is (string= "true" (cdr dd))))))

(test convert/401-propagates
  (let* ((c (make-client :api-key "bad"))
         (*http-request-fn* (make-mock-response 401 "{\"detail\":\"bad key\"}")))
    (signals authentication-error
      (convert c *csv-bytes* "parquet" :filename "x.csv"))))

;;; ── batch-convert ───────────────────────────────────────────────────────────

(test batch-convert/returns-result
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *zip-bytes* 200
                    '(("Content-Disposition" . "attachment; filename=\"converted.zip\""))
                    nil nil nil "OK")))
         (result (batch-convert c *zip-bytes* "parquet")))
    (is (reparatio-result-p result))
    (is (string= "converted.zip" (reparatio-result-filename result)))))

(test batch-convert/errors-header-in-warning
  (let* ((c (make-client :api-key "rp_k"))
         (encoded "[{\"file\":\"bad.txt\",\"error\":\"unsupported\"}]")
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *zip-bytes* 200
                    `(("X-Reparatio-Errors" . ,encoded))
                    nil nil nil "OK")))
         (result (batch-convert c *zip-bytes* "csv")))
    (is (stringp (reparatio-result-warning result)))
    (is (search "bad.txt" (reparatio-result-warning result)))))

(test batch-convert/sends-target-format
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *zip-bytes* 200 '() nil nil nil "OK")))
    (batch-convert c *zip-bytes* "feather")
    (let ((tf (assoc "target_format" captured-params :test #'string=)))
      (is (string= "feather" (cdr tf))))))

;;; ── merge-files ─────────────────────────────────────────────────────────────

(test merge-files/returns-result
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *csv-bytes* 200
                    '(("Content-Disposition" . "attachment; filename=\"merged.csv\""))
                    nil nil nil "OK")))
         (result (merge-files c *csv-bytes* *csv-bytes* "inner" "csv"
                              :filename1 "a.csv" :filename2 "b.csv")))
    (is (reparatio-result-p result))
    (is (string= "merged.csv" (reparatio-result-filename result)))))

(test merge-files/sends-operation
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *csv-bytes* 200 '() nil nil nil "OK")))
    (merge-files c *csv-bytes* *csv-bytes* "left" "parquet"
                 :filename1 "a.csv" :filename2 "b.csv" :join-on "id")
    (let ((op (assoc "operation" captured-params :test #'string=))
          (jo (assoc "join_on"   captured-params :test #'string=)))
      (is (string= "left" (cdr op)))
      (is (string= "id"   (cdr jo))))))

(test merge-files/fallback-filename
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *csv-bytes* 200 '() nil nil nil "OK")))
         (result (merge-files c *csv-bytes* *csv-bytes* "outer" "csv"
                              :filename1 "sales.csv" :filename2 "costs.csv")))
    (is (string= "sales_outer_costs.csv" (reparatio-result-filename result)))))

;;; ── append-files ────────────────────────────────────────────────────────────

(test append-files/requires-two-files
  (let* ((c (make-client :api-key "rp_k")))
    (signals error
      (append-files c (list *csv-bytes*) "csv"
                    :filenames '("a.csv")))))

(test append-files/returns-result
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *csv-bytes* 200
                    '(("Content-Disposition" . "attachment; filename=\"appended.csv\""))
                    nil nil nil "OK")))
         (result (append-files c (list *csv-bytes* *csv-bytes*) "csv"
                               :filenames '("a.csv" "b.csv"))))
    (is (reparatio-result-p result))
    (is (string= "appended.csv" (reparatio-result-filename result)))))

(test append-files/sends-multiple-files
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *csv-bytes* 200 '() nil nil nil "OK")))
    (append-files c (list *csv-bytes* *csv-bytes*) "parquet"
                  :filenames '("x.csv" "y.csv"))
    ;; Two "files" parts expected
    (let ((file-parts (remove-if-not
                       (lambda (p) (and (listp p) (string= "files" (first p))))
                       captured-params)))
      (is (= 2 (length file-parts))))))

(test append-files/fallback-filename
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *csv-bytes* 200 '() nil nil nil "OK")))
         (result (append-files c (list *csv-bytes* *csv-bytes*) "xlsx"
                               :filenames '("a.csv" "b.csv"))))
    (is (string= "appended.xlsx" (reparatio-result-filename result)))))

;;; ── query ────────────────────────────────────────────────────────────────────

(test query/returns-result
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *csv-bytes* 200
                    '(("Content-Disposition" . "attachment; filename=\"result.csv\""))
                    nil nil nil "OK")))
         (result (query c *csv-bytes* "SELECT * FROM data LIMIT 10"
                        :filename "data.csv")))
    (is (reparatio-result-p result))
    (is (string= "result.csv" (reparatio-result-filename result)))))

(test query/sends-sql
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil)
         (sql "SELECT id, name FROM data WHERE id > 5"))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *csv-bytes* 200 '() nil nil nil "OK")))
    (query c *csv-bytes* sql :filename "x.csv")
    (let ((sp (assoc "sql" captured-params :test #'string=)))
      (is (string= sql (cdr sp))))))

(test query/sends-target-format
  (let* ((c (make-client :api-key "rp_k"))
         (captured-params nil))
    (setf *http-request-fn*
          (lambda (url &rest args &key parameters &allow-other-keys)
            (declare (ignore url args))
            (setf captured-params parameters)
            (values *csv-bytes* 200 '() nil nil nil "OK")))
    (query c *csv-bytes* "SELECT 1" :filename "x.csv" :target-format "json")
    (let ((tf (assoc "target_format" captured-params :test #'string=)))
      (is (string= "json" (cdr tf))))))

(test query/fallback-filename
  (let* ((c (make-client :api-key "rp_k"))
         (*http-request-fn*
          (lambda (&rest _)
            (declare (ignore _))
            (values *csv-bytes* 200 '() nil nil nil "OK")))
         (result (query c *csv-bytes* "SELECT 1"
                        :filename "sales.csv" :target-format "parquet")))
    (is (string= "sales_query.parquet" (reparatio-result-filename result)))))

(test query/401-propagates
  (let* ((c (make-client :api-key "bad"))
         (*http-request-fn* (make-mock-response 401 "{\"detail\":\"Unauthorized\"}")))
    (signals authentication-error
      (query c *csv-bytes* "SELECT 1" :filename "x.csv"))))

;;; ── reparatio-result struct ─────────────────────────────────────────────────

(test reparatio-result/accessors
  (let ((r (make-reparatio-result :content *csv-bytes*
                                  :filename "out.csv"
                                  :warning  "note")))
    (is (equalp *csv-bytes* (reparatio-result-content r)))
    (is (string= "out.csv" (reparatio-result-filename r)))
    (is (string= "note" (reparatio-result-warning r)))))

(test reparatio-result/nil-warning
  (let ((r (make-reparatio-result :content *csv-bytes* :filename "x")))
    (is (null (reparatio-result-warning r)))))

;;; ---------------------------------------------------------------------------
;;; Runner
;;; ---------------------------------------------------------------------------

(defun run-tests ()
  "Run the full Reparatio test suite and return T if all tests pass."
  (let ((results (run 'reparatio-suite)))
    (explain! results)
    (every #'fiveam::test-passed-p results)))
