(ns com.freiheit.gae.local-dev
  "Tools for local development.
   Enables the use of the App Engine APIs on the REPL and in a local Jetty instance.
   Usage: (require 'local-dev) (local-dev/init-app-engine) (local-dev/start-server (var example))"
  (:use ring.adapter.jetty
        [ring.middleware file file-info]
        [ring.util.response  :only [redirect]]
        compojure.core
        [hiccup.core :only[html]]
        [ring.middleware.cookies]
        [ring.middleware.stacktrace])
  (:require [compojure.route :as route])
  (:import [java.io File]
           [java.util HashMap]
           [com.google.apphosting.api ApiProxy ApiProxy$Environment]
           [com.google.appengine.tools.development
            ApiProxyLocalFactory
            LocalServerEnvironment]))

(defonce *server* (atom nil))
(def *port* 8080)
(def login-info (atom {:logged-in? false
                       :admin? false
                       :email ""
                       :auth-domain ""}))

(defn login
  ([email] (login email false))
  ([email admin?] (swap! login-info merge {:email email
                                           :logged-in? true
                                           :admin? admin?})))

(defn logout []
  (swap! login-info merge {:email ""
                           :logged-in? false
                           :admin? false}))

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

(defn wrap-local-app-engine [app]
  "Wraps a ring app to enable the use of App Engine Services."
  (fn [req]
    (set-app-engine-environment)
    (app req)))

(defn login-form [continue]
  (html [:html
         [:body
          [:form {:method :post :style "text-align:center; font:13px sans-serif"} ;
           [:div {:style "width: 20em; margin: 1em auto; text-align: left; padding: 0 2em 1.25em 2em; background-color: #d6e9f8; border: 2px solid #67a7e3"}
            [:h3 "Not logged in"]
            [:p {:style "padding: 0; margin: 0"}
             [:label {:for :email :style "width: 3em"} "Email:"]
             [:input#email {:type :text :name :email :value "test@example.com"}]]
            [:p {:style "margin: .5em 0 0 3em; font-size:12px"}
             [:input#isAdmin {:type :checkbox :name :isAdmin}]
             [:label {:for :isAdmin} "Sign in as Administrator"]]
            [:input {:type :hidden :name :continue :value continue}]
            [:p {:style "margin-left: 3em;"}
             [:input {:type :submit :name :action :value "Log In"}]
             [:input {:type :submit :name :action :value "Log Out"}]]]]]]))

(defroutes login-routes
  (GET "/_ah/login" [continue] (login-form continue))
  (POST "/_ah/login" [action email isAdmin continue] (do (if (= action "Log In")
                                                           (login email (boolean isAdmin))
                                                           (logout))
                                                         (redirect continue)))
  (GET "/_ah/logout" [continue] (do (logout)
                                    (redirect continue))))

(defn start-server [app-routes]
  "Initializes the App Engine services and (re-)starts a Jetty server
   running the supplied ring app, wrapping it to enable App Engine API use
   and serving of static files."
  (set-app-engine-delegate "/tmp")
  (swap! *server* (fn [instance]
                   (when instance
                     (.stop instance))
                   (let [app (-> (routes login-routes app-routes)
                                 (wrap-local-app-engine)
                                 (wrap-file "./web")
                                 (wrap-file-info))]
                     (System/setProperty "development" "true")
                     (run-jetty app {:port *port*
                                     :join? false})))))

(defn stop-server []
  "Stops the local Jetty server."
  (swap! *server* #(when % (.stop %))))

