

(defpackage #:lisp-db-system
  (:use #:cl #:asdf))

(in-package #:lisp-db-system)

(defsystem lisp-db
  :description "A Lispy interface to the SQLite RDMS"
  :author "Andy Chambers <achambers.home@gmail.com>"
  :version "0.1.0"
  :licence "MIT"
  :serial t
  :depends-on ("cffi")
  :components
  ((:file "packages")
   (:file "protocol")
   (:file "backend-sqlite")
   (:file "sql")
   (:file "api")))


