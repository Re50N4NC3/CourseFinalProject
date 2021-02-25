name := "CourseFinalProject1"

scalaVersion := "2.12.11"
scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  ("org.apache.spark" %% "spark-core" % "2.4.3"),
  //"org.apache.spark" % "spark-core_2.10" % "1.6.3",
  ("org.apache.spark" %% "spark-sql" % "2.4.3"),
  "org.apache.spark" %% "spark-avro" % "2.4.3",
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.0.4",
)
dependencyOverrides ++= Seq(
  ("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7")
)