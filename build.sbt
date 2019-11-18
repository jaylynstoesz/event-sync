name := "data-engineering-event-sync"

version := "0.1"

fork := true

resolvers += Resolver.mavenCentral

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

publishMavenStyle := true

libraryDependencies += "software.amazon.awssdk" % "dynamodb" % "2.10.+"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.0"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "1.1.2"
libraryDependencies += "joda-time" % "joda-time" % "2.10.5"

fork in run := true

lazy val root = (project in file("."))
  .enablePlugins(SbtPlugin)

scalacOptions ++= Seq("-target:jvm-1.8")

import scalariform.formatter.preferences._

scalariformPreferences := scalariformPreferences.value
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(AllowParamGroupsOnNewlines, true)
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(DanglingCloseParenthesis, Force)
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(FirstArgumentOnNewline, Force)
  .setPreference(FirstParameterOnNewline, Force)
  .setPreference(IndentPackageBlocks, true)
  .setPreference(IndentSpaces, 2)
  .setPreference(MultilineScaladocCommentsStartOnFirstLine, true)
  .setPreference(NewlineAtEndOfFile, true)
  .setPreference(SpaceInsideParentheses, false)
  .setPreference(SpaceInsideBrackets, false)
  .setPreference(SpacesWithinPatternBinders, false)
  .setPreference(SpacesAroundMultiImports, false)
