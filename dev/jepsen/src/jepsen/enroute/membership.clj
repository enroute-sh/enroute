(ns jepsen.enroute.membership
  "Member changes as a state machine, on `jepsen.nemesis.membership`.

  Each ledger node's ID is a member, removing (removed from the ledger, its
  logs moving off it), or gone. A node can also be wiped: down, with an
  empty disk, until it rejoins with a new ID. The generator picks any change
  that is legal now, so changes overlap with each other and with kills,
  pauses and partitions, in any order:

  - `:remove` removes any member, a down or wiped one too, as long as 3
    members stay.
  - `:free` wipes a removing node and starts it on an empty disk, so that it
    rejoins with a new ID.
  - `:lose-disk` kills a node and wipes its disk, leaving it down. Its old
    ID stays a member until a `:remove` takes it out.
  - `:rejoin` starts a wiped node, which joins with a new ID.

  A wipe goes ahead only while every repository keeps 2 replicas on running
  nodes other than the wiped one, so that no fault loses a majority of a
  log's data. `:settle` ends the test's changes and waits until every log
  lives on members only. A log that never does is reported as stuck."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [util :as util]]
            [jepsen.nemesis.membership :as m]
            [jepsen.nemesis.membership.state :as ms]
            [jepsen.enroute [cluster :as cluster]
                            [http :as http]]))

(defn- ledgers [test] (get-in test [:topology :ledgers]))

(defn- repos
  "The repositories the test has written so far."
  [test]
  (range (inc @(:max-repo test))))

(defn- hosts?
  "Whether ledger `node` hosts the log of repository `r`, or nil if it does
  not answer."
  [node r]
  (try
    (let [res (http/get (str "http://127.0.0.1:" (cluster/internal-port node)
                             "/status/repos/jepsen-" r)
                        {:timeout 1000})]
      (case (long (:status res)) 200 true 404 false nil))
    (catch Exception _ nil)))

(defn- view-of
  "What ledger `node` says now: whether it runs, its ID, how many logs it
  holds, and which of the test's repositories it hosts. Nil while it does
  not answer."
  [test node]
  (if-not (cluster/running? node)
    {:up? false}
    (when-let [id (cluster/node-id node)]
      (let [hosted (map (fn [r] [r (hosts? node r)]) (repos test))]
        (when (every? (comp some? second) hosted)
          {:up?   true
           :id    id
           :logs  (cluster/hosted-logs node)
           :hosts (set (keep (fn [[r h]] (when h r)) hosted))})))))

(defn- members-of
  "The ledger's members, read through any running ledger node."
  [test]
  (some #(cluster/members test %)
        (shuffle (filter cluster/running? (ledgers test)))))

(defn- current
  "The ID that ledger `node` runs as, as the state knows it."
  [s node]
  (some (fn [[id {n :node, st :state}]] (when (and (= n node) (not= :gone st)) id))
        (sort-by (comp :since val) > (:ids s))))

(defn- safe-to-wipe?
  "Whether every repository keeps 2 replicas on running nodes other than
  `node`, as the latest views say."
  [s test node]
  (let [views (:node-views s)]
    (every? (fn [r]
              (<= 2 (count (for [n (ledgers test)
                                 :when (not= n node)
                                 :let [v (get views n)]
                                 :when (and (:up? v) (contains? (:hosts v) r))]
                             n))))
            (repos test))))

(defn- drained?
  "Whether `node` runs and holds no log."
  [s node]
  (let [v (get-in s [:node-views node])]
    (and (:up? v) (= 0 (:logs v)))))

(defn- down
  "State `s` with `node` seen down at once: a view polled before a wipe
  would still count its logs."
  [s node]
  (assoc-in s [:node-views node] {:up? false}))

(defn- add-id [s id node st]
  (cond-> s
    (not (get-in s [:ids id]))
    (assoc-in [:ids id] {:node node, :state st, :since (System/nanoTime)})))

(defn- rejoined!
  "Starts wiped `node` and waits for its new ID."
  [test node old]
  (cluster/start! test node)
  (cluster/await-ready node 60000)
  (cluster/await-new-id node old 60000))

(defn- wipe!
  "Stops `node` and wipes its disk, as if it lost it."
  [test node]
  (cluster/kill! node)
  (cluster/lose-unsynced! test node)
  (cluster/wipe! test node))

(defn- settled
  "Why the logs do not yet live on members only, or nil once they do: each
  repository's log is on min(3, members) running members, and on no other
  node. A repository that no node hosts was never created: every push to it
  failed."
  [test]
  (let [ms    (:members (members-of test))
        ids   (into {} (for [n (ledgers test)
                             :when (cluster/running? n)
                             :let [id (cluster/node-id n)] :when id]
                         [n id]))
        want  (min 3 (count ms))
        wrong (for [r (repos test)
                    :let [hs (filter #(hosts? % r) (keys ids))
                          off (remove #(contains? ms (ids %)) hs)]
                    :when (and (seq hs) (or (seq off) (not= want (count hs))))]
                {:repo r, :hosts (vec (sort hs)), :not-members (vec (sort off))})]
    (cond (nil? ms)   {:members :unknown}
          (seq wrong) {:members (vec (sort ms)), :logs (vec (take 10 wrong))})))

(defn- settle!
  "Ends every change: rejoins wiped nodes, removes the old IDs of nodes that
  rejoined, and waits until the logs live on members only. Returns
  `[result state]`, `result` being :settled or what is stuck."
  [s test]
  (let [s (reduce (fn [s node]
                    (let [id (rejoined! test node (current s node))]
                      (-> s (update :wiped disj node) (add-id id node :member))))
                  s (:wiped s))
        deadline (+ (System/nanoTime) (* 180 1e9))]
    (loop [s s]
      (let [ms    (:members (members-of test))
            ;; IDs whose node now runs as another one.
            stale (for [[id {:keys [node state]}] (:ids s)
                        :when (and (= :member state) (contains? ms id)
                                   (not= id (current s node)))]
                    id)
            s     (reduce (fn [s id]
                            (if (some #(cluster/remove-member! test % id)
                                      (shuffle (filter cluster/running? (ledgers test))))
                              (assoc-in s [:ids id :state] :removing)
                              s))
                          s stale)
            why   (settled test)]
        (cond (nil? why) [:settled s]
              (< deadline (System/nanoTime)) [{:stuck why} s]
              :else (do (Thread/sleep 1000) (recur s)))))))

(defn- choices
  "The changes that are legal in state `s`. They keep 3 members running, and
  a majority of the members running, so that the ledger stays available, and
  at most one member ID whose disk is gone, so that such IDs do not pile up."
  [s test]
  (let [view    (:view s)
        ms      (set (:members view))
        up      (filter cluster/running? (ledgers test))
        ;; A removal signs with the key of a node that runs as a member.
        vias    (filter #(contains? ms (current s %)) up)
        live    (set (keep #(let [id (current s %)] (when (contains? ms id) id)) up))
        kept?   (fn [members live] (< (/ members 2) live))
        lost    (remove live (filter ms (keys (:ids s))))]
    (concat
      (when (and (seq vias) (< 3 (count ms)))
        (for [[id {:keys [node state]}] (:ids s)
              ;; 3 running members stay, also once settle removes the IDs
              ;; of wiped nodes.
              :when (and (= :member state) (contains? ms id)
                         (<= 3 (count (disj live id)))
                         (kept? (dec (count ms)) (count (disj live id))))
              :let [via (rand-nth vias)]
              :when (not= via node)]
          {:type :info, :f :remove, :value {:id id, :node node, :via via}}))
      (for [node (ledgers test)
            :let [id (current s node)]
            :when (and id (= :removing (get-in s [:ids id :state]))
                       (not (contains? (:wiped s) node))
                       (or (drained? s node) (safe-to-wipe? s test node)))]
        {:type :info, :f :free, :value {:node node, :id id}})
      (for [node (ledgers test)
            :let [id (current s node)]
            :when (and (not (contains? (:wiped s) node))
                       id
                       (empty? (remove #{id} lost))
                       (kept? (count ms) (count (disj live id)))
                       (safe-to-wipe? s test node))]
        {:type :info, :f :lose-disk, :value {:node node, :id (current s node)}})
      (for [node (:wiped s)]
        {:type :info, :f :rejoin, :value {:node node}}))))

(defrecord Membership [ids wiped]
  ms/State
  (setup! [this test]
    (reduce (fn [s n] (add-id s (cluster/node-id n) n :member))
            (assoc this :ids {} :wiped #{})
            (ledgers test)))

  (node-view [this test node]
    (when (some #{node} (ledgers test))
      (view-of test node)))

  (merge-views [this test]
    (or (members-of test) (:view this)))

  (fs [this] #{:remove :free :lose-disk :rejoin :settle})

  (op [this test]
    (if-let [cs (seq (choices this test))]
      (rand-nth cs)
      :pending))

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      :remove
      (let [{:keys [id via]} value]
        (if (cluster/remove-member! test via id)
          ;; The view only catches up at the next poll.
          [(assoc op :value (assoc value :result :removed))
           (-> this
               (assoc-in [:ids id :state] :removing)
               (update-in [:view :members] disj id))]
          [(assoc op :value (assoc value :result :refused)) this]))

      :free
      (let [{:keys [node id]} value
            drained (drained? this node)]
        (wipe! test node)
        (let [new (rejoined! test node id)]
          [(assoc op :value (assoc value :new-id new :drained? drained))
           (-> this
               (down node)
               (assoc-in [:ids id :state] :gone)
               (add-id new node :member))]))

      :lose-disk
      (do (wipe! test (:node value))
          [op (-> this (down (:node value)) (update :wiped conj (:node value)))])

      :rejoin
      (let [node (:node value)
            new  (rejoined! test node (current this node))]
        [(assoc op :value (assoc value :new-id new))
         (-> this (update :wiped disj node) (add-id new node :member))])

      :settle
      (let [[result s] (settle! this test)]
        [(assoc op :value result) s])))

  (resolve [this test]
    ;; Take the IDs the nodes run as, and the removals the ledger made that
    ;; an answer did not report.
    (let [ms (:members (:view this))
          ;; A new ID means the node rejoined, perhaps started by another
          ;; fault's recovery.
          s  (reduce (fn [s [node v]]
                       (let [id (:id v)]
                         (if (and id (not (get-in s [:ids id])))
                           (-> s (update :wiped disj node) (add-id id node :member))
                           s)))
                     this (:node-views this))]
      (if (nil? ms)
        s
        (reduce (fn [s [id {:keys [state]}]]
                  (cond-> s
                    (and (= :member state) (not (contains? ms id))
                         (contains? (:removed (:view s)) id))
                    (assoc-in [:ids id :state] :removing)))
                s (:ids s)))))

  (resolve-op [this test pair] this)

  (teardown! [this test]))

(defn package
  "The member-change nemesis, its generator (a change every `interval`
  seconds or so), and its final generator."
  [interval]
  (let [pkg (m/package {:faults     #{:membership}
                        :interval   interval
                        :membership {:state (map->Membership {})}})]
    (assoc pkg :final-generator (gen/once {:type :info, :f :settle}))))
