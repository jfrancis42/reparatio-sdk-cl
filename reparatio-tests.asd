(defsystem "reparatio-tests"
  :description "Test suite for the Reparatio Common Lisp SDK"
  :depends-on ("reparatio" "fiveam")
  :components
  ((:module "tests"
    :serial t
    :components
    ((:file "package")
     (:file "tests")))))
