(defpackage #:reparatio
  (:use #:cl)
  (:export
   ;; Client construction
   #:make-client
   #:client-p
   #:client-api-key
   #:client-base-url
   #:client-timeout
   ;; Conditions
   #:reparatio-error
   #:reparatio-error-status
   #:reparatio-error-message
   #:authentication-error
   #:insufficient-plan-error
   #:file-too-large-error
   #:reparatio-parse-error
   ;; Result struct
   #:reparatio-result
   #:reparatio-result-p
   #:make-reparatio-result
   #:reparatio-result-content
   #:reparatio-result-filename
   #:reparatio-result-warning
   ;; API methods
   #:formats
   #:me
   #:inspect-file
   #:convert
   #:batch-convert
   #:merge-files
   #:append-files
   #:query
   ;; Hook for testing
   #:*http-request-fn*))
