(ns com.freiheit.gae.local-dev
  "Tools for local development.
   Enables the use of the App Engine APIs on the REPL and in a local Jetty instance.
   Usage: (require 'local-dev) (local-dev/init-app-engine) (local-dev/start-server (var example))"
  (:import [java.io File]
           [java.util HashMap]
           [com.google.apphosting.api ApiProxy ApiProxy$Environment]
           [com.google.appengine.tools.development ApiProxyLocalFactory LocalServerEnvironment]))

(defn- set-app-engine-environment []
  "Sets up the App Engine environment for the current thread."
  (let [att (HashMap. {"com.google.appengine.server_url_key"
                       (str "http://localhost:" *port*)})
        env-proxy (proxy [ApiProxy$Environment] []
                    (isLoggedIn [] (:logged-in? @login-info))
                    (getEmail [] (:email @login-info))
                    (getAuthDomain [] (:auth-domain @login-info))
                    (isAdmin [] (:admin? @login-info))
                    (getRequestNamespace [] "")
                    (getDefaultNamespace [] "")
                    (getAttributes [] att)
                    (getAppId [] "_local_"))]
    (ApiProxy/setEnvironmentForCurrentThread env-proxy)))

(defn- set-app-engine-delegate [dir]
  "Initializes the App Engine services. Needs to be run (at least) per JVM."
  (let [local-env (proxy [LocalServerEnvironment] []
                    (getAppDir [] (File. dir))
                    (getAddress [] "localhost")
                    (getPort [] *port*)
                    (waitForServerToStart [] nil))
        api-proxy (.create (ApiProxyLocalFactory.)
                           local-env)]
    (ApiProxy/setDelegate api-proxy)))

(defn init-app-engine
  "Initializes the App Engine services and sets up the environment. To be called from the REPL."
  ([] (init-app-engine "/tmp"))
  ([dir]
     (set-app-engine-delegate dir)
     (set-app-engine-environment)))

(defn- wrap-local-app-engine [app]
  "Wraps a ring app to enable the use of App Engine Services."
  (fn [req]
    (set-app-engine-environment)
    (app req)))

