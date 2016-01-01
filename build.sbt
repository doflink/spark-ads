name := "dk-graph-dk2"

version := "1.0"

scalaVersion := "2.10.4" //System.getenv.get("SCALA_VERSION")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"  

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.0"

//libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.4.0", "org.apache.spark" %% "spark-graphx" % "1.4.0")
