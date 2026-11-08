(ns jepsen.enroute.client
  "A Jepsen client that speaks git smart HTTP. Keys are branches, and a list
  is a branch's first-parent chain. Key k is branch k<k mod key-count> of
  repository jepsen-<k quot key-count>."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.enroute [cluster :as cluster]
                            [git :as git]
                            [http :as http]])
  (:import (java.io IOException)
           (java.net ConnectException)
           (java.net.http HttpTimeoutException)))

(defn repo-of [test k] (quot k (:key-count test)))
(defn ref-of [test k] (str "refs/heads/k" (mod k (:key-count test))))
(defn repo-keys [test r] (range (* r (:key-count test)) (* (inc r) (:key-count test))))
(defn repo-url [node r] (str (cluster/git-url node) "/jepsen-" r ".git"))

;; Nodes

(defn pinned-node
  "The compute node of `op`'s thread. A crashed process's successor keeps
  the thread, so it keeps the node."
  [test op]
  (let [computes (get-in test [:topology :computes])]
    (nth computes (mod (mod (:process op) (:concurrency test)) (count computes)))))

(defn node-for [test op]
  (if (:pin? test)
    (pinned-node test op)
    (rand-nth (get-in test [:topology :computes]))))

;; Decoding

(defn decode
  "The list that `tip` of key `k` names: the values along its first-parent
  chain, oldest first. Throws on a commit that the harness never wrote."
  [oids k tip]
  (loop [oid tip, acc ()]
    (if (nil? oid)
      (vec acc)
      (let [c (get oids oid)]
        (when-not c
          (throw (ex-info "phantom commit" {:type :phantom, :key k, :tip tip, :oid oid})))
        (when (not= k (:key c))
          (throw (ex-info "commit of another key" {:type :phantom, :key k, :tip tip,
                                                   :oid oid, :commit c})))
        (recur (:parent c) (conj acc (:value c)))))))

;; Ref reads

(def timeout-ms 10000)

(defn- upload-pack-v2
  "A protocol v2 command to repository `r` on `node`."
  [node r ^bytes body]
  (http/post (str (repo-url node r) "/git-upload-pack") body
             {:timeout timeout-ms
              :headers {"Git-Protocol" "version=2"
                        "Content-Type" "application/x-git-upload-pack-request"}}))

(defn read-refs
  "Branch -> oid of repository `r` on `node`. `mode` is :ls-refs (protocol
  v2), :v0 (the upload-pack advertisement) or :rp (the receive-pack
  advertisement, which takes a read barrier). A repository that does not
  exist has no branches."
  [node r mode]
  (let [url (repo-url node r)
        res (case mode
              :ls-refs (upload-pack-v2 node r (git/ls-refs-request))
              :v0 (http/get (str url "/info/refs?service=git-upload-pack") {:timeout timeout-ms})
              :rp (http/get (str url "/info/refs?service=git-receive-pack") {:timeout timeout-ms}))]
    (case (int (:status res))
      200 (if (= mode :ls-refs)
            (git/parse-ls-refs (:body res))
            (git/parse-advertisement (:body res)))
      404 {}
      (throw (ex-info (str "read failed: " (:status res) " " (git/utf8-str (:body res)))
                      {:type :http, :status (:status res)})))))

(defn- read-txn [test oids refs txn]
  (mapv (fn [[f k _]] [f k (decode oids k (get refs (ref-of test k)))]) txn))

;; Fetch checks

(defn fetch-check
  "Fetches `tips` from `node` and checks the pack holds every commit of
  their chains, plus the empty tree."
  [node r tips oids]
  (try
    (let [res (upload-pack-v2 node r (git/fetch-request tips))]
      (if (not= 200 (:status res))
        {:node node, :error (str "http " (:status res))}
        (let [{:keys [pack error]} (git/parse-fetch (:body res))
              expected (inc (count (into #{} (mapcat (fn [t] (take-while some? (iterate #(:parent (get oids %)) t))))
                                         tips)))]
          (if error
            {:node node, :error error, :not-our-ref? (str/includes? error "not our ref")}
            {:node node, :objects (git/pack-object-count pack), :expected expected}))))
    (catch Exception e
      {:node node, :error (str (class e) ": " (.getMessage e))})))

(defn git-fetch
  "Fetches every branch of repository `r` with the real git client, then
  checks connectivity."
  [node r]
  ;; A directory of its own: concurrent fetches must not share one.
  (let [dir (str (java.nio.file.Files/createTempDirectory
                   "jepsen-fetch-" (make-array java.nio.file.attribute.FileAttribute 0)))]
    (try
      (shell/sh "git" "init" "-q" "--bare" dir)
      ;; git has no request timeout: a paused node would block this thread.
      (let [f (shell/sh "timeout" "30" "git" "-C" dir "-c" "protocol.version=2" "fetch" "-q"
                        (repo-url node r) "+refs/heads/*:refs/heads/*")]
        (if-not (zero? (:exit f))
          {:type :fail, :error [:fetch (str/trim (:err f))]}
          (let [c (shell/sh "git" "-C" dir "fsck" "--connectivity-only")]
            (if (zero? (:exit c))
              {:type :ok, :refs (count (str/split-lines
                                         (:out (shell/sh "git" "-C" dir "for-each-ref"))))}
              {:type :fail, :error [:fsck (str/trim (str (:out c) (:err c)))]}))))
      (finally (shell/sh "rm" "-rf" dir)))))

;; Pushes

(defn- push!
  "Reads the tips through the receive-pack advertisement, then pushes one
  commit per append on top of them."
  [test oids node op]
  (let [txn     (:value op)
        r       (repo-of test (second (first txn)))
        atomic? (:atomic? op)
        refs    (try (read-refs node r :rp)
                     (catch Exception e
                       (throw (ex-info "tip read failed" {:type :not-sent} e))))
        appends (filterv #(= :append (first %)) txn)
        ;; The CAS proves each landed key still held what this read saw.
        olds    (into {} (for [[_ k _] appends]
                           [k (get refs (ref-of test k))]))
        lists   (into {} (for [[k tip] olds] [k (decode @oids k tip)]))
        commits (for [[_ k v] appends]
                  (let [body (git/commit-body (olds k) k v)]
                    {:key k, :value v, :body body, :oid (git/object-id "commit" body)}))
        _       (swap! oids into (for [c commits]
                                   [(:oid c) {:parent (olds (:key c)), :key (:key c),
                                              :value (:value c)}]))
        body    (git/push-request
                  (for [c commits]
                    [(or (olds (:key c)) git/zero-oid) (:oid c) (ref-of test (:key c))])
                  atomic?
                  (cons ["tree" (byte-array 0)] (map (fn [c] ["commit" (:body c)]) commits)))
        res     (try (http/post (str (repo-url node r) "/git-receive-pack") body
                                {:timeout timeout-ms
                                 :headers {"Content-Type" "application/x-git-receive-pack-request"}})
                     (catch ConnectException e
                       (throw (ex-info "not sent" {:type :not-sent} e))))
        report  (when (= 200 (:status res)) (git/parse-report (:body res)))
        outcome (fn [k] (get-in report [:refs (ref-of test k)]))
        txn'    (vec (mapcat (fn [[_ k v]] [[:r k (lists k)] [:append k v]]) appends))]
    (cond
      (not= 200 (:status res))
      (assoc op :type :info, :error [:http (:status res) (git/utf8-str (:body res))])

      (nil? report)
      (assoc op :type :info, :error [:no-report (git/utf8-str (:body res))])

      (not= "ok" (:unpack report))
      (assoc op :type :fail, :error [:unpack (:unpack report)])

      (some (comp nil? outcome second) appends)
      (assoc op :type :info, :error [:partial-report report])

      :else
      (let [landed (set (for [[_ k _] appends :when (= :ok (first (outcome k)))] k))
            ngs    (into {} (for [[_ k _] appends :when (not (landed k))]
                              [k (second (outcome k))]))]
        (cond
          (empty? landed) (assoc op :type :fail, :error [:ng ngs])
          (empty? ngs)    (assoc op :type :ok, :value txn')
          :else (assoc op :type :ok,
                       :value (filterv (comp landed second) txn')
                       :ng ngs))))))

(defn- read-op [test oids node op]
  (let [txn  (:value op)
        r    (repo-of test (second (first txn)))
        refs (read-refs node r (:read-mode test))
        snap @oids
        op'  (assoc op :type :ok, :value (read-txn test snap refs txn))]
    (if-not (:fetch? test)
      op'
      (let [tips  (vec (distinct (keep #(get refs (ref-of test (second %))) txn)))
            other (rand-nth (or (seq (remove #{node} (get-in test [:topology :computes])))
                                [node]))]
        (cond-> op'
          (seq tips) (assoc :fetch (fetch-check other r tips snap)))))))

(defn- final-read
  "Every key of every repository so far, through the receive-pack
  advertisement."
  [test oids node op]
  (let [snap @oids]
    (assoc op :type :ok
           :value (vec (for [r (range (inc @(:max-repo test)))
                             :let [refs (read-refs node r :rp)]
                             k (repo-keys test r)]
                         [:r k (decode snap k (get refs (ref-of test k)))])))))

(defrecord Client [oids]
  client/Client
  (open! [this test node] this)
  (setup! [_ _])
  (invoke! [_ test op]
    (let [node (if (= :final-read (:f op)) (pinned-node test op) (node-for test op))
          op   (assoc op :node node)]
      (try
        (case (:f op)
          :read       (read-op test oids node op)
          :write      (push! test oids node op)
          :final-read (final-read test oids node op)
          :git-fetch  (merge op (git-fetch node (:value op))))
        (catch clojure.lang.ExceptionInfo e
          (let [d (ex-data e)]
            (cond
              (= :phantom (:type d))
              (assoc op :type :fail, :error [:phantom (dissoc d :type)])

              ;; Reads change nothing, and a push that failed before it was
              ;; sent changed nothing.
              :else
              (assoc op :type :fail
                     :error [(:type d) (.getMessage e)
                             (or (some-> (.getCause e) str) (dissoc d :type))]))))
        (catch ConnectException e
          (assoc op :type :fail, :error [:connect (.getMessage e)]))
        (catch HttpTimeoutException e
          (assoc op :type (if (= :write (:f op)) :info :fail), :error [:timeout]))
        (catch IOException e
          (assoc op :type (if (= :write (:f op)) :info :fail),
                 :error [:io (str (class e)) (.getMessage e)])))))
  (teardown! [_ _])
  (close! [_ _]))

(defn client [] (Client. (atom {})))
