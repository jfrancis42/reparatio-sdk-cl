;;; Run the test suite from the command line:
;;;   sbcl --load run-tests.lisp
;;;
;;; Exit code 0 = all pass, 1 = failures.

(require "asdf")

;; Point ASDF at the project root (both .asd files live here)
(pushnew (truename ".") asdf:*central-registry* :test #'equal)

;; Bootstrap Quicklisp
(let ((ql-init (merge-pathnames "quicklisp/setup.lisp"
                                (user-homedir-pathname))))
  (when (probe-file ql-init)
    (load ql-init)))

(ql:quickload '("reparatio" "reparatio-tests") :silent t)

(let ((passed (reparatio-tests::run-tests)))
  (uiop:quit (if passed 0 1)))
