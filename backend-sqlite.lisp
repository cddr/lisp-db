
(in-package :lisp-db)

;;; Utility Macros

(defmacro raise-error (db)
  "If the result of sql-ok was not true, pass the db handle to
this function to raise a Lisp condition which contains details
of the error (i.e. error-code and descriptive  message)"
  `(progn
    (%sqlite-errcode ,db)
    (%sqlite-errmsg ,db)
    (error 'sql-error
     :msg (%sqlite-errmsg ,db)
     :code (foreign-enum-keyword
	    'error-codes
	    (%sqlite-errcode ,db)))))

(defmacro sql-ok (&body body)
  "When calling native SQLITE functions, use this macro to make
sure it returned OK.  This expands into a form that calls the 
function and only returns true if it was carried out successfully"
  `(eq (let ((rc ,@body))
	 rc)
       (foreign-enum-value 'error-codes :SQLITE-OK)))

(define-foreign-type sqlite-object ()
  ((pointer :initarg :pointer :accessor pointer))
  (:actual-type :pointer))

(defclass sqlite-db (sqlite-object)
  ((stmts :initform nil :accessor statements)
   (nrefs :initform 0 :accessor nrefs)))


(defclass sqlite-stmt (sqlite-object)
  ((status :initarg :status)))

(defmethod translate-to-foreign (obj (type sqlite-object))
  (pointer obj))

(defmethod connection-class ((type (eql 'sqlite)))
  'sqlite-db)

(defmethod statement-class ((db sqlite-db))
  'sqlite-stmt)

(defmethod open-db ((type (eql 'sqlite)) filename)
  (make-instance 'sqlite-db
    :pointer (with-foreign-object (db :pointer)
	       (if (sql-ok (%sqlite-open filename db))
		   (mem-ref db :pointer)
		   (raise-error db)))))

(defmethod finalize ((stmt sqlite-stmt))
  (with-slots (pointer status) stmt
    (when (eq status :active)
      (or (prog1 (sql-ok (%sqlite-finalize pointer))
	    (setf status :finalized))
	  (raise-error (%sqlite-db-handle pointer))))))
    
(defmethod finalize ((obj sqlite-db))
  (loop for stmt in (statements obj)
     do (finalize stmt))
  (setf (statements obj) nil))

(defmethod close-db ((db sqlite-db))
  (let ((db (pointer db)))
    (if (sql-ok (%sqlite-close db))
	t
	(raise-error db))))

(defmethod prepare (sql db)
  (let ((ptr (pointer db)))
    (make-instance 'sqlite-stmt
      :status :active
      :pointer (with-foreign-object (stmt :pointer)
		 (with-foreign-object (rest :pointer)
		   (if (sql-ok (%sqlite-prepare ptr sql -1
						stmt
						rest))
		       (mem-ref stmt :pointer)
		       (raise-error ptr)))))))

(defmethod changes ((db sqlite-db))
  (%sqlite-changes (pointer db)))

(defmethod most-recent-insert-id ((db sqlite-db))
  (%sqlite-last-insert-id (pointer db)))

(defmethod reset ((stmt sqlite-stmt))
  (let ((stmt (pointer stmt)))
    (%sqlite-reset stmt)))

(defmethod next-row ((stmt sqlite-stmt))
  (let* ((stmt (pointer stmt))
	 (status (%sqlite-step stmt)))
    (case (foreign-enum-keyword 'error-codes status)
      (:SQLITE-ROW
       (loop for i below (%sqlite-column-count stmt)
	  collect
	    (case (foreign-enum-keyword 'type-codes
					(%sqlite-column-type stmt i))
	      (:SQLITE-INTEGER (%sqlite-column-int64 stmt i))
	      (:SQLITE-FLOAT (%sqlite-column-double stmt i))
	      (:SQLITE-TEXT (%sqlite-column-text stmt i))
	      (:SQLITE-BLOB (%sqlite-column-blob stmt i))
	      (:SQLITE-NULL nil)
	      (t (format t "huh?! ~a" (%sqlite-column-type stmt i))))))
      (:SQLITE-DONE
	 nil)
      (t (let ((db (%sqlite-db-handle stmt)))
	   (raise-error db))))))

(defmethod bind ((stmt sqlite-stmt) params)
  (reset stmt)
  (let ((stmt (pointer stmt)))
    (labels ((bind-function (param-name param-value)
	       #'(lambda ()
		   (let ((idx (%sqlite-bind-parameter-index 
			       stmt 
			       (car (sql-symbol-evaluator param-name)))))
		     (typecase param-value
		       (integer (%sqlite-bind-int stmt idx param-value))
		       (float (%sqlite-bind-double stmt idx (coerce param-value
								    'double-float)))
		       (string (%sqlite-bind-text stmt idx param-value
						  -1
						  (inc-pointer (null-pointer) -1)))
		       (t (%sqlite-bind-text stmt idx
					     (format nil "~a" param-value)
					     -1
					     (inc-pointer (null-pointer) -1))))))))
      (loop for k in params by #'cddr
	 for v in (cdr params) by #'cddr
	 do (funcall (bind-function k v))))))

;;; CFFI Boilerplate

(define-foreign-library libsqlite
  (:unix (:or "libsqlite3.so.0")))

(use-foreign-library libsqlite)

;;; Type Declarations

(defctype sqlite (:pointer)
  (:documentation "Handle to an open database"))

(defctype prepared-statement (:pointer)
  (:documentation "Handle to a prepared statement"))

(defctype sqlite-destructor (:pointer)
  (:documentation "Ugghh"))


;;; SQLite Error codes

(defcenum error-codes
  (:SQLITE-OK 0)            ; Successful result

  (:SQLITE-ERROR 1)         ; SQL error or missing database
  (:SQLITE-INTERNAL 2)      ; Internal logic error in SQLite
  (:SQLITE-PERM 3)          ; Access permission denied 
  (:SQLITE-ABORT 4)         ; Callback routine requested an abort 
  (:SQLITE-BUSY 5)          ; The database file is locked 
  (:SQLITE-LOCKED 6)        ; A table in the database is locked 
  (:SQLITE-NOMEM 7)         ; A malloc() failed 
  (:SQLITE-READONLY 8)	    ; Attempt to write a readonly database 
  (:SQLITE-INTERRUPT 9)	    ; Operation terminated by sqlite3_interrupt()
  (:SQLITE-IOERR 10)	    ; Some kind of disk I/O error occurred 
  (:SQLITE-CORRUPT 11)	    ; The database disk image is malformed 
  (:SQLITE-NOTFOUND 12)	    ; NOT USED. Table or record not found 
  (:SQLITE-FULL 13)         ; Insertion failed because database is full 
  (:SQLITE-CANTOPEN 14)	    ; Unable to open the database file 
  (:SQLITE-PROTOCOL 15)	    ; NOT USED. Database lock protocol error 
  (:SQLITE-EMPTY 16)	    ; Database is empty 
  (:SQLITE-SCHEMA 17)       ; The database schema changed 
  (:SQLITE-TOOBIG 18)       ; String or BLOB exceeds size limit 
  (:SQLITE-CONSTRAINT 19)   ; Abort due to constraint violation 
  (:SQLITE-MISMATCH 20)     ; Data type mismatch 
  (:SQLITE-MISUSE 21)	    ; Library used incorrectly 
  (:SQLITE-NOLFS 22)        ; Uses OS features not supported on host 
  (:SQLITE-AUTH 23)         ; Authorization denied 
  (:SQLITE-FORMAT 24)       ; Auxiliary database format error 
  (:SQLITE-RANGE 25)        ; 2nd parameter to sqlite3_bind out of range 
  (:SQLITE-NOTADB 26)       ; File opened that is not a database file 
  (:SQLITE-ROW 100)         ; sqlite3_step() has another row ready 
  (:SQLITE-DONE 101))       ; sqlite3_step() has finished executing 

(defcenum type-codes
  (:SQLITE-INTEGER 1)
  (:SQLITE-FLOAT 2)
  (:SQLITE-TEXT 3)
  (:SQLITE-BLOB 4)
  (:SQLITE-NULL 5))

(defcenum destructors
  (:SQLITE-STATIC 0)
  (:SQLITE-TRANSIENT -1))



;; Low level C interface

(defmacro define-result-accessors (() &body body)
  `(progn
     ,@(loop for (left right) in body
	     collect
	  (let ((lisp-name (intern 
			    (concatenate 
			     'string
			     "%SQLITE"
			     (subseq 
			      (symbol-name left) 7))))
		(c-name (string-downcase 
			 (substitute #\_ #\- (symbol-name left)))))
	    `(defcfun (,c-name ,lisp-name) ,right
	       (stmt :pointer)
	       (column :int))))))


;; Within the context of a statement, these accessors can be used
;; to get data from the current row.  The define-result-accessors
;; macro generates 

(define-result-accessors ()
  (sqlite3-column-blob :pointer)
  (sqlite3-column-bytes :int)
  (sqlite3-column-bytes16  :int)
  (sqlite3-column-double  :double)
  (sqlite3-column-int64  :int)
  (sqlite3-column-text  :string)
  (sqlite3-column-text16  :string)
  (sqlite3-column-type  :int)

  ;;This one is only included for completeness
  (sqlite3-column-value :pointer))

;; CFFI wrappers around the SQLITE C API.  If any function hasn't
;; been included here its only because I haven't needed it yet
;; and it would be easy to add its corresponding defcfun

(defcfun ("sqlite3_column_count" %sqlite-column-count) :int
  (stmt prepared-statement))

(defcfun ("sqlite3_column_name" %sqlite-column-name) :string
  (stmt prepared-statement)
  (column-index :int))

(defcfun ("sqlite3_errcode" %sqlite-errcode) :int
  (db sqlite))

(defcfun ("sqlite3_errmsg" %sqlite-errmsg) :string
  (db sqlite))

(defcfun ("sqlite3_open" %sqlite-open) :int
  (filename :string)
  (db sqlite))

(defcfun ("sqlite3_exec" %sqlite-exec) :int
  (db sqlite)
  (sql :string)
  (callback :pointer)
  (data :pointer)
  (err :pointer))

(defcfun ("sqlite3_changes" %sqlite-changes) :int
  (db sqlite))

(defcfun ("sqlite3_db_handle" %sqlite-db-handle) :pointer
  (stmt :pointer))

(defcfun ("sqlite3_prepare_v2" %sqlite-prepare) :int
  (db sqlite)
  (sql :string)
  (bytes :int)
  (statement prepared-statement)
  (tail :pointer))

(defcfun ("sqlite3_step" %sqlite-step) :int
  (stmt prepared-statement))

(defcfun ("sqlite3_finalize" %sqlite-finalize) :int
  (stmt prepared-statement))

(defcfun ("sqlite3_next_stmt" %sqlite-next-stmt) :pointer
  (db sqlite)
  (stmt prepared-statement))


(defcfun ("sqlite3_get_table" %sqlite-get-table) :int
  (db sqlite)
  (sql :string)
  (result :pointer)
  (rows :pointer)
  (columns :pointer)
  (err :pointer))

(defcfun ("sqlite3_free_table" %sqlite-free-table) :void
  (result :pointer))

(defcfun ("sqlite3_free" %sqlite-free) :void
  (memory :pointer))

(defcfun ("sqlite3_close" %sqlite-close) :int
  (db sqlite))

(defcfun ("sqlite3_malloc" %sqlite-malloc) :pointer
  (size :int))

(defcfun ("sqlite3_bind_parameter_index" %sqlite-bind-parameter-index) :int
  (stmt prepared-statement)
  (name :string))

(defcfun ("sqlite3_bind_int64" %sqlite-bind-int) :int
  (stmt prepared-statement)
  (index :int)
  (value :int))

(defcfun ("sqlite3_bind_double" %sqlite-bind-double) :int
  (stmt prepared-statement)
  (index :int)
  (value :double))

(defcfun ("sqlite3_bind_text" %sqlite-bind-text) :int
  (stmt :pointer)
  (index :int)
  (value :string)
  (len :int)
  ;; not really an int but cffi takes care of this for us
  (destructor :pointer))

(defcfun ("sqlite3_clear_bindings" %sqlite-clear-bindings) :int
  (stmt prepared-statement))

(defcfun ("sqlite3_reset" %sqlite-reset) :int
  (stmt prepared-statement))
    
(defcfun ("sqlite3_last_insert_rowid" %sqlite-last-insert-id) :int
  (db sqlite))

(defcfun ("sqlite3_create_function16" %sqlite-create-function) :int
  (db sqlite)
  (name :string)
  (narg :int)
  (etext-rep :int)
  (ptr :pointer)
  (xfunc :pointer)
  (xstep :pointer)
  (xfinal :pointer))
