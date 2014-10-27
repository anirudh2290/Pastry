import akka.actor.{ActorRef, ActorSystem, Props, Actor, Inbox}
import SuperBoss._
import akka.util.Timeout
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer
import scala.math
import Worker._
import scala.concurrent.{Await, Future, blocking}
import scala.concurrent.duration._

import scala.collection.mutable.HashSet

import akka.pattern.ask

case class pastryInit(numRequests: Int)
case class setupNodes(numberNodes: Int, msg: String, top:String, algorithm: String, failNodesList:ArrayBuffer[Int])
case class printTopologyNeighbours(neigboursList: ArrayBuffer[ActorRef])
case class executeAlgo(algorithm: String)
case class gossipHeard(gossiper: String)
case class pushSumDone(gossiper: String, ratio: Double)
case class countNodes(gossiper: String)


object SuperBoss {
  
 //copied, modify as needed!
  def props(numberNodes: Int, ac: ActorSystem, numberOfRequests: Int):Props =
    Props(classOf[SuperBoss], numberNodes, ac, numberOfRequests)
}

class SuperBoss(numberNodes: Int, ac: ActorSystem, numberOfRequests: Int) extends Actor {
  //val workingActors: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]
    val rndKey = new scala.util.Random()
    var rndKeyInt : Int = 0
    val b = 4	// Modify as needed
    var base = scala.math.pow(2, b).toInt  // 2^b = 2^4 = 16 ==> hex
    var hashset: HashSet[BigInt] = new HashSet[BigInt]()

    println("b and base are "+b+" "+base)
    
  def receive = {
   
    case pastryInit(numRequests: Int) => init_config()
    
    case "Hello" => println("Pastry begins...");
  }
    
     def init_config() {
       var i = 0
       for (i <- 0 to (numberNodes-1)) {
         println("Inside joinOnebyOne")
         joinOnebyOne(i)
       }
       println("Printing hashset")
       println(hashset)
     }
    
    
  def joinOnebyOne(id: Int) {   
  // TODO non-duplicate check yet to be added   
  println("Inside joinOnebyOne :::: "  + id)
  // Create random ID, non-duplicates
  // as of now - base b = 4, key length is 128 bits
  // max value possible is 2^128-1 = 340282366920938463463374607431768211456
    /*
    var r:Array[Byte] = new Array[Byte](16)
    rndKey.nextBytes(r)
    var z=0
    for(z <- 0 to r.length-1) {
    if(r(z) < 0) {
      r(z) = (-1*r(z)).toByte
    }
      println(r(z))
    }
    */
 /*   
 // DEBUG :  
    var i = 0
    for(i <- 0 to 15) {  
    println(" "+r(i).toString())
  }
  */   
    // this conversion works only for base 16
    /*
    val byte_hex = new StringBuffer();
        for (i <- 0 to r.length-1) {
          byte_hex.append(Integer.toString((r(i) & 0xff) + 0x100, base).substring(1));
        }
    println("key of actor #"+id + " "+ byte_hex)
    */
    // create the actor with key as name


    /*
     val actr: ActorRef = context.actorOf(
        Worker.props(ac, self, numberNodes, base, r), byte_hex.toString()
     )

     */
     // DEBUG
     /*
     val x = byte_hex.toString()
     var i = 0
     for(i<-0 to x.length-1)
       println(x(i))

     println("Actor name is " + actr.path.name + " .")
     var lastJoined = actr
     
     actr ! "test"
     */
     var nodeId: BigInt = BigInt.apply(63, scala.util.Random)

     while(hashset.contains(nodeId)) {
       nodeId = BigInt.apply(63, scala.util.Random)
       hashset.add(nodeId)
     }

    if(!hashset.contains(nodeId))
      hashset.add(nodeId)

    var nodeIdString = BigIntToHexString(nodeId)
    //println("hexString " + hexString)
    //println("zeroAppendSize " + zeroAppendSize)
    //println("nodeIdString " + nodeIdString)
    //println("nodeIdString size is " + nodeIdString.size)
    val actr: ActorRef = context.actorOf(
      Worker.props(ac, self, numberNodes, b), nodeIdString
    )
    //used futures because we need the join to be sequential and not parallel
    var neighbourId = ""

    if(id > 0) {
      neighbourId = BigIntToHexString(hashset.last)
    }
    implicit val timeout = Timeout(15 seconds)
    val future = actr ? join(neighbourId)
    val result = Await.result(future, timeout.duration).asInstanceOf[Int]

    //println("Got return for join id :: " + id)
    /*
    val future: Future[Int] = ask(actr, testInit).mapTo[Int]
    println("Got return for join id :: " + id)
    */
    println("result for id " + id + "is " + result )
  }

  private def BigIntToHexString(input: BigInt): String = {
    var hexString = input.toString(16)
    var zeroAppendSize: Int = 16 - hexString.size
    var nodeIdString = "0"*zeroAppendSize + hexString
    nodeIdString
  }
 // Reference : http://www.dzone.com/snippets/scala-code-convert-tofrom-hex

  /*Commented by Anirudh Subramanian Begin*/
  /*
  def hex2Bytes( hex: String ): Array[Byte] = {

    	 (for { i <- 0 to hex.length-1 by 2 if i > 0 || !hex.startsWith( "0x" )}
    	 yield hex.substring( i, i+2 ))
    	 .map( Integer.parseInt( _, 16 ).toByte ).toArray
  }

  */
  /*Commented by Anirudh Subramanian End*/

}