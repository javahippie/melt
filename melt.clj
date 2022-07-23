(require '[cheshire.core :as json])
(require '[clojure.java.shell :refer [sh]])
(require '[clojure.tools.cli :refer [parse-opts]])
(require '[taoensso.timbre :as t])

(def cli-options
  [["-a" "--account-id ACCOUNT_ID" "Your AWS accountID"]
   ["-v" "--vault-name VAULT_NAME" "The name of the vault you want to remove"]])

(defn parse-output [output]
  (json/decode (:out output)))

(defn create-job-file! [jobId]
  (spit ".running-job" jobId))

(defn read-job-from-file! []
  (slurp ".running-job"))

(defn start-inventory-retrieval!
  "Starts an inventory retrieval job for the given vault and account id.
  Stores the job-id in a file, to remember it for later."
  [account-id vault-name]
  (t/info "Starting inventory retrieval")
  (->
    (sh "aws" "glacier" "initiate-job"
        "--vault-name" vault-name
        "--account-id" account-id
        "--job-parameters" "{\"Type\":\"inventory-retrieval\"}")
    parse-output
    (get "jobId")
    create-job-file!))

(defn job-finished?
  "Returns true if the triggered job was finished, false otherwise."
  [account-id vault-name]
  (t/info "Sending request for job description")
  (let [job-result (parse-output
                     (sh "aws" "glacier" "describe-job"
                         "--vault-name" vault-name
                         "--account-id" account-id
                         "--job-id" (read-job-from-file!)))]
    (get job-result "Completed")))

(defn get-job-output!
  "Downloads the result of the triggered job to 'output.json'"
  [account-id vault-name]
  (t/info "Downloading result of inventory job to 'output.json")
  (sh "aws" "glacier" "get-job-output"
      "--vault-name" vault-name
      "--account-id" account-id
      "--job-id" (read-job-from-file!)
      "output.json"))

(defn delete-archives!
  "Deletes all archives found in the previously downloaded 'output.json' file"
  [account-id vault-name]
  (let [archives (-> "output.json"
                     slurp
                     json/decode
                     last
                     val)]
    (t/info "Found " (count archives) " archives in the vault. Deleting!")
    ; pmap is usually not safe for side effects, as we have little control
    ; about the context. But here, it is okay-ish, as we just want the side
    ; effects to be executed and don't care about the rest.
    (doall
      (pmap (fn [{:strs [ArchiveId]}]
              (t/info "Deleting " ArchiveId)
              (sh "aws" "glacier" "delete-archive"
                  "--vault-name" vault-name
                  "--account-id" account-id
                  "--archive-id" ArchiveId))
            archives))))


(let [{:keys [account-id vault-name]} (:options (parse-opts *command-line-args* cli-options))]
  (if (not (.exists (File. ".running-job")))
    (start-inventory-retrieval! account-id vault-name)
    (if (job-finished? account-id vault-name)
      (if (not (.exists (File. "output.json")))
        (get-job-output! account-id vault-name)
        (delete-archives! account-id vault-name))
      "Job is still running. This might take several hours!")))


