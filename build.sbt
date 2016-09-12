enablePlugins(JavaAppPackaging)

name := "poloniex-poller"
organization := "com.markgoldenstein"
version := "1.0"
scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.jcenterRepo
)

libraryDependencies ++= {
  val akkaV       = "2.4.10"
  val slickV      = "3.2.0-M1"
  Seq(
    "com.typesafe.akka"        %% "akka-actor" % akkaV,
    "com.typesafe.akka"        %% "akka-stream" % akkaV,
    "com.typesafe.akka"        %% "akka-http-experimental" % akkaV,
    "com.typesafe.akka"        %% "akka-http-spray-json-experimental" % akkaV,
    "com.typesafe.akka"        %% "akka-http-testkit" % akkaV,
    "com.typesafe.akka"        %% "akka-slf4j" % akkaV,
    "com.enragedginger"        %% "akka-quartz-scheduler" % "1.5.0-akka-2.4.x",
    "com.typesafe.slick"       %% "slick" % slickV,
    "com.typesafe.slick"       %% "slick-hikaricp" % slickV,
    "com.chuusai"              %% "shapeless" % "2.3.1",
    "io.underscore"            %% "slickless" % "0.2.1",
    "codes.reactive"           %% "scala-time" % "0.4.0",
    "com.iheart"               %% "ficus" % "1.2.3",
    "com.github.melrief"       %% "purecsv" % "0.0.6",
    "com.github.nikita-volkov" %  "sext" % "0.2.4",
    "mysql"                    % "mysql-connector-java" % "5.1.39",
    "ch.qos.logback"           % "logback-classic" % "1.1.6",
    "org.scalatest"            %% "scalatest" % "2.2.6" % "test"
  )
}

Revolver.settings
