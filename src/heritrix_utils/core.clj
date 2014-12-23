(ns heritrix-utils.core
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [subotai.warc.warc :as warc])
  (:import [com.sleepycat.je
            Cursor
            Database
            DatabaseConfig
            DatabaseEntry
            DatabaseException
            Environment
            EnvironmentConfig
            LockMode
            OperationStatus]))

(defn create-berkeley-db
  [db-path db-name]
  (do (when-not (.exists
                 (io/as-file db-path))
        (.mkdir
         (io/as-file db-path)))
   (let [environment-config (EnvironmentConfig.)
         database-config    (DatabaseConfig.)]
     (do (.setAllowCreate environment-config true)
         (.setAllowCreate database-config true)
         (let [environment (Environment.
                            (io/as-file db-path)
                            environment-config)
               database    (.openDatabase environment
                                          nil
                                          db-name
                                          database-config)]
           {:environment environment
            :database    database})))))

(defn load-warc
  [warc-file]
  (let [instream (warc/warc-input-stream warc-file)]
    (filter
     #(->> % :warc-target-uri (re-find #"robots.txt$") not)
     (warc/stream-html-records-seq instream))))

(defn get-url-src
  ([url log]
   (get-url-src url log 0))

  ([url log n]
   (println :src url)
   (cond (nil? (get log url))
         url

         (= "-" (get log url))
         url

         (= url (get log url))
         url

         (= n 3)
         url

         :else
         (recur (get log url)
                log
                (inc n)))))

(defn index-warc
  [warc-file db-path db-name log]
  (let [{env :environment
         db  :database}  (create-berkeley-db db-path db-name)
         records         (load-warc warc-file)]
    (doall
     (doseq [a-record records]
       (let [url  (:warc-target-uri a-record)
             body (:payload a-record)
             url-src (get-url-src url log)
             ]
         (when-not (or (nil? url)
                       (nil? body))
           (println :doing url)
           (println :actual url-src)
           (println
            (.put db
                  nil
                  (-> url-src
                      (.getBytes "UTF-8")
                      (DatabaseEntry.))
                  (-> body
                      (.getBytes "UTF-8")
                      (DatabaseEntry.))))))))
    (.close db)
    (.close env)))

(defn load-log
  [log-file]
  (let [contents (string/split-lines
                  (slurp log-file))]
    (reduce
     (fn [acc a-log-line]
       (let [[timestamp status size target-url redir-type src-url _ _ _  _ ] (string/split a-log-line #"\s+")]
         (merge acc {target-url src-url})))
     {}
     contents)))

(defn load-logs
  [directory]
  (let [log-files (filter
                   (fn [f]
                     (re-find #"crawl.log"
                              (.getAbsolutePath f)))
                   (file-seq
                    (io/as-file directory)))]
    (reduce
     (fn [acc f]
       (merge acc (load-log
                   (.getAbsolutePath f))))
     {}
     log-files)))

(defn index
  [directory db-path db-name]
  (let [job-log (load-logs directory)]
    (doseq [f (file-seq
               (io/as-file directory))]
      (when (re-find #".warc.gz"
                     (.getAbsolutePath f))
        (index-warc (.getAbsolutePath f)
                    db-path
                    db-name
                    job-log)))))

(defn urls-in-dir
  [directory]
  (doseq [f (file-seq
             (io/as-file directory))]
    (when (re-find #".warc.gz"
                   (.getAbsolutePath f))
      (let [records (load-warc (.getAbsolutePath f))]
        (doseq [record records]
          (println (:warc-target-uri record)))))))

(defn read-all-records
  [db-path db-name]
  (let [{env :environment
         db  :database} (create-berkeley-db db-path db-name)
         cursor (.openCursor db nil nil)

         found-key (DatabaseEntry.)
         found-val (DatabaseEntry.)]
    (while (= (.getNext cursor found-key found-val LockMode/DEFAULT)
              OperationStatus/SUCCESS)
      (let [key (-> found-key
                    (.getData)
                    (String. "UTF-8"))
            val (-> found-val
                    (.getData)
                    (String. "UTF-8"))]
        (println key)))
    (.close db)
    (.close env)))

(defn get-payload-aux
  [env db key]
  (let [key-entry (DatabaseEntry.
                   (.getBytes key "UTF-8"))
        data-entry (DatabaseEntry.)
        outcome   (.get db
                        nil
                        key-entry
                        data-entry
                        LockMode/DEFAULT)]
    {:status outcome
     :value  (when (= outcome OperationStatus/SUCCESS)
               (-> data-entry
                   (.getData)
                   (String. "UTF-8")))}))

(defn get-payload
  [key db-path db-name]
  (let [{env :environment
         db  :database} (create-berkeley-db db-path db-name)
         result (get-payload-aux env db key)]

    (.close db)
    (.close env)
    result))
