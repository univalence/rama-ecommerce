ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

name := "rama-ecommerce"

libraryDependencies ++=
  Seq(
    "com.rpl" % "rama"      % "0.9.1",
    "com.rpl" % "rama-helpers" % "0.9.0"
  )

resolvers += "redplanetlabs" at "https://nexus.redplanetlabs.com/repository/maven-public-releases/"

resolvers += "clojure" at "https://repo.clojars.org/"