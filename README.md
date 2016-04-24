# sparkee-min
## minimalistic Spark Job

Sparkee-Min is supposed to be a minimalistic approach towards setting up a Spark job in Java. It tries to accomplish the following:

* Built-in defaults for reading from conifiguration files
* default override scheme fore configuration (properties)
* good defaults for Spark configuration, including Application Name, run modes (client, local), etc.
* being able to run out of the box without a cluster setup - runs out of the box from within your IDE.

That seems a lot to accomplish, but most of it is already just built-in into the spark api's. Also because the playground for Spark
was originally set in Scala, this effort will likely slowly add-on to utilities helping to bridge differences between Scala and Java.
The idea is that as this project might enhance, we will also start abstracting an actual API over time.

Either way - this is a good and quick start for your Spark project.

For configuration,
- Define any common properties as part of your default.properties in your src/main/resources, excluding passwords.
- Define passwords, or system local resources, in the ~/.spark/default.properties for the user and system your Driver will be initiated.
