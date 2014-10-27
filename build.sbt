name := "Pastry"

version := "1.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq("com.typesafe.akka" % "akka-actor_2.10" % "2.3.6",
  "com.typesafe.akka" % "akka-remote_2.10" % "2.3.6"
)
