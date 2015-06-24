name := "geocomm"

version := "0.1.0"

organization := "org.truffulatree"

licenses := Seq(
  "Mozilla Public License Version 2.0" -> url("https://mozilla.org/MPL/2.0/"))

homepage := Some(url("https://github.com/mpokorny/geocomm"))

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-explaintypes",
  "-Xlint")

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "1.0.4",
  "org.scalaz" %% "scalaz-core" % "7.1.3",
  "org.scalaz" %% "scalaz-effect" % "7.1.3",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2")

publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("staging"  at nexus + "service/local/staging/deploy/maven2")
}

credentials += Credentials(Path.userHome / ".sbt" / "sonatype.txt")

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <scm>
    <url>git@github.com:mpokorny/geocomm.git</url>
    <connection>scm:git:git@github.com:mpokorny/geocomm.git</connection>
    </scm>
    <developers>
    <developer>
    <id>martin</id>
    <name>Martin Pokorny</name>
    <email>martin@truffulatree.org</email>
    <timezone>America/Denver</timezone>
    </developer>
    </developers>)

useGpg := true