ThisBuild / scalaVersion := "2.12.15"

lazy val root = project.in(file(".")).
  aggregate(oi.js, oi.jvm).
  settings(
    publish := {},
    publishLocal := {},
  )
lazy val oi = crossProject(JSPlatform, JVMPlatform).in(file(".")).
  settings(
    name := "P1",
    version := "0.1-SNAPSHOT",
    libraryDependencies += "org.scala-lang.modules" %%% "scala-parser-combinators" % "1.1.2"
    //libraryDependencies += "org.singlespaced" %%% "scalajs-d3" % "0.3.4"
  ).
  jvmSettings(
    // Add JVM-specific settings here
    libraryDependencies += "org.apache.spark" %%% "spark-core" % "3.1.2",
    libraryDependencies += "org.apache.spark" %%% "spark-sql" % "3.1.2",
    libraryDependencies += "org.apache.spark" %%% "spark-hive" % "3.1.2",
    libraryDependencies += "org.apache.spark" %%% "spark-streaming" % "3.1.2",
    libraryDependencies += "org.apache.spark" %%% "spark-mllib" % "3.1.2",
    // https://mvnrepository.com/artifact/org.mongodb.scala/mongo-scala-driver
    libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",
    // https://mvnrepository.com/artifact/io.netty/netty-all
    libraryDependencies += "io.netty" % "netty-all" % "4.1.73.Final"

    
    ).
    jsSettings(
      // Add JS-specific settings here
      scalaJSUseMainModuleInitializer := true,
      // libraryDependencies += "org.singlespaced" %%% "scala-js-d3" % "0.3.4"
  )


// Most moderately interesting Scala projects don't make use of the very simple
// build file style (called "bare style") used in this build.sbt file. Most
// intermediate Scala projects make use of so-called "multi-project" builds. A
// multi-project build makes it possible to have different folders which sbt can
// be configured differently for. That is, you may wish to have different
// dependencies or different testing frameworks defined for different parts of
// your codebase. Multi-project builds make this possible.

// Here's a quick glimpse of what a multi-project build looks like for this
// build, with only one "subproject" defined, called `root`:

// lazy val root = (project in file(".")).
//   settings(
//     inThisBuild(List(
//       organization := "ch.epfl.scala",
//       scalaVersion := "2.13.3"
//     )),
//     name := "hello-world"
//   )

// To learn more about multi-project builds, head over to the official sbt
// documentation at http://www.scala-sbt.org/documentation.html
