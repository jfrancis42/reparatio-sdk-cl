(in-package #:reparatio)

(define-condition reparatio-error (error)
  ((status  :initarg :status  :reader reparatio-error-status)
   (message :initarg :message :reader reparatio-error-message))
  (:report (lambda (c s)
             (format s "Reparatio API error ~A: ~A"
                     (reparatio-error-status c)
                     (reparatio-error-message c)))))

(define-condition authentication-error (reparatio-error) ()
  (:report (lambda (c s)
             (format s "Authentication error (~A): ~A"
                     (reparatio-error-status c)
                     (reparatio-error-message c)))))

(define-condition insufficient-plan-error (reparatio-error) ()
  (:report (lambda (c s)
             (format s "Insufficient plan (~A): ~A"
                     (reparatio-error-status c)
                     (reparatio-error-message c)))))

(define-condition file-too-large-error (reparatio-error) ()
  (:report (lambda (c s)
             (format s "File too large (~A): ~A"
                     (reparatio-error-status c)
                     (reparatio-error-message c)))))

(define-condition reparatio-parse-error (reparatio-error) ()
  (:report (lambda (c s)
             (format s "Parse error (~A): ~A"
                     (reparatio-error-status c)
                     (reparatio-error-message c)))))
