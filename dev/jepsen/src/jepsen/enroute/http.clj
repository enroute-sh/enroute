(ns jepsen.enroute.http
  "A small HTTP client on java.net.http."
  (:refer-clojure :exclude [get])
  (:import (java.net URI)
           (java.net.http HttpClient HttpClient$Builder HttpClient$Version
                          HttpRequest HttpRequest$Builder HttpRequest$BodyPublishers
                          HttpResponse HttpResponse$BodyHandlers HttpTimeoutException)
           (java.time Duration)
           (java.util.concurrent CompletableFuture ExecutionException TimeUnit TimeoutException)
           (com.fasterxml.jackson.databind ObjectMapper)))

(def ^HttpClient client
  (-> (HttpClient/newBuilder)
      (.version HttpClient$Version/HTTP_1_1)
      ;; JAVA_TOOL_OPTIONS can set a proxy; the nodes are local.
      (.proxy HttpClient$Builder/NO_PROXY)
      (.connectTimeout (Duration/ofSeconds 2))
      .build))

(defn- send!
  "The response, within `timeout` ms for the whole of it: the request's own
  timeout ends with the headers, and a node paused while it sends the body
  would hold the call (and the test) forever."
  [^HttpRequest$Builder b {:keys [timeout headers]}]
  (doseq [[k v] headers] (.header b k v))
  (let [ms (or timeout 10000)
        _  (.timeout b (Duration/ofMillis ms))
        ^CompletableFuture f (.sendAsync client (.build b) (HttpResponse$BodyHandlers/ofByteArray))
        ^HttpResponse r (try (.get f ms TimeUnit/MILLISECONDS)
                             (catch TimeoutException _
                               (.cancel f true)
                               (throw (HttpTimeoutException. "request timed out")))
                             (catch ExecutionException e
                               (throw (or (.getCause e) e))))]
    {:status (.statusCode r), :body (.body r)}))

(defn get [url opts]
  (send! (.GET (HttpRequest/newBuilder (URI/create url))) opts))

(defn post [url ^bytes body opts]
  (send! (.POST (HttpRequest/newBuilder (URI/create url))
                (HttpRequest$BodyPublishers/ofByteArray body))
         opts))

(def ^:private ^ObjectMapper json (ObjectMapper.))

(defn get-json
  "The JSON object at `url`, with keyword keys, or nil if the status is not
  200."
  [url opts]
  (let [res (get url opts)]
    (when (= 200 (:status res))
      (into {} (map (fn [[k v]] [(keyword k) v]))
            (.readValue json ^bytes (:body res) java.util.Map)))))
