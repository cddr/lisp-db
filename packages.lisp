

(defpackage :lisp-db
  (:use :cl :cffi)
  (:export :with-open-db
	   :list-tables
	   :with-transaction
	   :with-profiling
	   :sql-error
	   :sql :sql* :sql> :sql1> :err-msg
	   :do-prepared
	   :insert-plist))
	   
