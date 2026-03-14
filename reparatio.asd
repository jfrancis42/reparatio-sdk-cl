(defsystem "reparatio"
  :description "Common Lisp SDK for the Reparatio data conversion API"
  :version "0.1.0"
  :author "Ordo Artificum LLC"
  :license "MIT"
  :depends-on ("drakma" "cl-json" "babel")
  :components
  ((:module "src"
    :serial t
    :components
    ((:file "package")
     (:file "conditions")
     (:file "client")))))
