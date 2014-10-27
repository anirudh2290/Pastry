import akka.actor.{Props, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.concurrent.{blocking, future}
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.collection.mutable.ArrayBuffer
import scala.math
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.Future
import akka.pattern.ask

object project3 {

   def main(args: Array[String]): Unit = {
     
     // 2 args : numNodes, numRequests
     val numNodes = (args(0).toInt)
     val numRequests = (args(1).toInt)
     
     println("args are "+ numNodes+" "+numRequests);
     
     val system = ActorSystem("super-boss")//, two)
    
     val superbossservice = system.actorOf(SuperBoss.props(numNodes, system, numRequests), "super-boss")
     println("path is " + superbossservice.path)
     superbossservice ! "Hello"
     superbossservice ! pastryInit(numRequests)
  
     
   }
}