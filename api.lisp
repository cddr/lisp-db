
(in-package :lisp-db)

(defun sql* (sql) 
  (if (consp sql)
      (sql sql)
      sql))

(defun dml-p (sql)
  (with-input-from-string (s (string-upcase sql))
    (let ((op (read s)))
      (member op '(insert update drop)))))

(let ((cache (make-hash-table :test 'equal)))

  (defun clear-statement-cache ()
    (clrhash cache))

  (defun sql> (sql &optional params)
    "Evaluates `expr' on the currently connected database"
    (let* ((sql (sql* sql))
	   (stmt (or (cache-hit cache sql)
		     (cache-miss cache sql (prepare sql *db*)))))

      ;; (format t "sql> ~a~a (~a)~%" 
      ;; 	      (make-string (* 3 (transaction-level))
      ;; 			   :initial-element #\Space)
      ;; 	      sql params)

      (bind stmt params)

      (if (dml-p sql)
	  (changes *db*)
	  (loop for row = (next-row stmt)
	     while row
	     collect row))))

  (defun sql1> (sql &optional params)
    (first (sql> sql params))))

(defvar *connections* (make-hash-table :test 'equal))

(defun find-or-open (backend db-spec)
  (setf (gethash db-spec *connections*)
	(or (gethash db-spec *connections*)
	    (open-db backend db-spec))))

(defmacro with-open-db ((db-spec &optional (backend *default-backend*))
			&body body)
  "Evaluates body in the context of a new connection to the 
sqlite database specified by `filename'"
  `(let ((*db* (find-or-open ',backend ,db-spec)))
    (incf (nrefs *db*))
    (unwind-protect
	 (progn ,@body)
      (clear-statement-cache)
      (when (zerop (decf (nrefs *db*)))
	(close-db *db*)
	(remhash ,db-spec *connections*)))))

(defun list-tables ()
  (mapcar 'first
	  (sql> '(select (name)
		  (from sqlite-master)
		  (where (= type "table"))
		  (order (name))))))

(let ((transaction-level 0))
  (defun transaction-level ()
    transaction-level)

  (defun begin-transaction ()
    (prog1
	(let ((sql (if (= transaction-level 0)
		       "begin;"
		       (format nil 
			       "savepoint \"~a\";" 
			       (transaction-level)))))
	  (print (list "beginning transaction: " sql))
	  (sql> sql))
      (incf transaction-level)))

  (defun rollback ()
    (assert (> transaction-level 0))
    (decf transaction-level)
    (let ((sql (if (= transaction-level 0)
		   "rollback;"
		   (format nil
			   "rollback to \"~a\";"
			   (transaction-level)))))
      (sql> sql)))

  (defun end-transaction ()
    (assert (> transaction-level 0))
    (decf transaction-level)
    (let ((sql (if (= transaction-level 0)
		   "end;"
		   (format nil 
			   "release \"~a\";" 
			   (transaction-level)))))
      (sql> sql))))

(defmacro with-gensyms ((&rest names) &body body)
  `(let ,(loop for n in names collect `(,n (gensym)))
     ,@body))

(defmacro with-transaction (&body body)
  "Evaluates body as a transaction on the open database connection"
  `(handler-case (progn
		   (begin-transaction)
		   (prog1 ,@body
		     (end-transaction)))
    (sql-error (err)
     (format t "with-transaction: ~a" (err-msg err))
     (rollback))))


(defun do-prepared (sql param-seqs)
  (let ((stmt (prepare (sql* sql) *db*)))
    (unwind-protect
	 (with-transaction
	   (loop for params in param-seqs
	      do (progn
		   (bind stmt params)
		   (next-row stmt))))
      (finalize stmt))))

(defun insert-plist (table p-list &key on-conflict)
  (let* ((keys (remove-if-not 'keywordp p-list))
	 (names (mapcar (lambda (k)
			  (intern (symbol-name k)))
			keys))
	 (vals (remove-if 'keywordp p-list)))
    (let ((sql `(insert (,table ,names)
		 ,keys
		 :on-conflict ,on-conflict)))
      (sql> sql (loop for k in keys
      		   for v in vals
      		   append (list k v)))
      (most-recent-insert-id *db*))))

(defmacro with-profiling (&body body)
  `(progn
    (sb-profile:profile sql> sql sql* cache-hit cache-miss
     eval-sql finalize prepare reset next-row)
    ,@body
    (sb-profile:report)
    (sb-profile:unprofile sql> sql sql* cache-hit cache-miss
     eval-sql finalize prepare reset next-row)))
