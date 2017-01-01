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
  val akkaV       = "2.4.16"
  val akkaHttpV   = "10.0.1"
  val slickV      = "3.2.0-M2"
  Seq(
    "com.typesafe.akka"        %% "akka-actor" % akkaV,
    "com.typesafe.akka"        %% "akka-stream" % akkaV,
    "com.typesafe.akka"        %% "akka-http" % akkaHttpV,
    "com.typesafe.akka"        %% "akka-http-spray-json" % akkaHttpV,
    "com.typesafe.akka"        %% "akka-http-testkit" % akkaHttpV,
    "com.typesafe.akka"        %% "akka-slf4j" % akkaV,
    "com.enragedginger"        %% "akka-quartz-scheduler" % "1.6.0-akka-2.4.x",
    "com.typesafe.slick"       %% "slick" % slickV,
    "com.typesafe.slick"       %% "slick-hikaricp" % slickV,
    "com.chuusai"              %% "shapeless" % "2.3.2",
    "io.underscore"            %% "slickless" % "0.3.0",
    "codes.reactive"           %% "scala-time" % "0.4.1",
    "com.iheart"               %% "ficus" % "1.4.0",
    "com.github.melrief"       %% "purecsv" % "0.0.7",
    "com.github.nikita-volkov" %  "sext" % "0.2.4",
    "mysql"                    % "mysql-connector-java" % "5.1.40",
    "ch.qos.logback"           % "logback-classic" % "1.1.8",
    "org.scalatest"            %% "scalatest" % "3.0.1" % "test"
  )
}

Revolver.settings
