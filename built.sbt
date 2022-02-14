lazy val probsky = project.in(file("."))
  .settings(
    name := "ProbSky",
    version := "1.0",
    scalaVersion := "3.1.0",
    libraryDependencies ++= Seq(
		      ("org.apache.spark" %% "spark-core" % "3.2.1").cross(CrossVersion.for3Use2_13),
		      ("org.apache.spark" %% "spark-sql" % "3.2.1").cross(CrossVersion.for3Use2_13),
		      "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
		)
  )
