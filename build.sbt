name := "TrillionG"

version := "1.0"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-source", "1.8", "-g:none")

scalacOptions ++= Seq("-optimise", "-optimize","-target:jvm-1.8", "-Yinline-warnings")

libraryDependencies += "it.unimi.dsi" % "fastutil" % "7.0.12"

libraryDependencies += "it.unimi.dsi" % "dsiutils" % "2.3.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.1.0"
