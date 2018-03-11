scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "io.kamon"               %% "kamon-core" % "1.1.0",
  "org.slf4j"              %  "slf4j-api"  % "1.7.25",
  "software.amazon.awssdk" %  "cloudwatch" % "2.0.0-preview-8",
  "org.scalatest"          %% "scalatest"  % "3.0.5" % Test
)

