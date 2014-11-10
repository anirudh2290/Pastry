import akka.actor._
import SuperBoss._
import akka.util.Timeout
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.math
import Worker._
import scala.concurrent.{Await, Future, blocking}
import scala.concurrent.duration._

import scala.collection.mutable.HashSet

import akka.pattern.ask

case class pastryInit(numRequests: Int)
case class calculateAverageHops(hops: Int)
case class doneWithRequests()
case class incrementActorcount()
case class joinCheck()
case class joinCalls()

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
    var actorsArray: Array[BigInt] = new Array[BigInt](numberNodes)
    var actorsArrayBuf:ArrayBuffer[BigInt] = new ArrayBuffer[BigInt]()
    var lastInserted:BigInt = 0
    var totalHopsForNodes:Int = 0
    var totalNodesVisited: Int = 0
    var averageHops: Int = 0
    var completedActors: Int = 0
    var setupActors: Int = 0
    val neighbouringActors: ArrayBuffer[String] = new ArrayBuffer[String]
    var joinCallNumber:BigInt = 0
    var cancellable:Cancellable = new Cancellable {override def isCancelled: Boolean = false
      override def cancel(): Boolean = false
    }
    var cancellable2:Cancellable = new Cancellable {override def isCancelled: Boolean = false
      override def cancel(): Boolean = false
    }
    println("b and base are "+b+" "+base)

    // DR
    var patternA : BigInt =0
    var patternB : BigInt =1
    
  def receive = {
   
    case pastryInit(numRequests: Int) => init_config()
    
    case "Hello" => println("Pastry begins...");

    case calculateAverageHops(hops: Int) => calculateAverageHops(sender, hops)

    case incrementActorcount() => incrementActorcount(sender)

    case doneWithRequests() => doneWithRequests(sender)

    case joinCheck() => checkJoin()

    case joinCalls() => initiateJoinCalls()
  }
    
     def init_config() {
       var i = 0
       import ac.dispatcher
       cancellable = ac.scheduler.schedule(0 milliseconds, 50 milliseconds, self, joinCheck())
       for (i <- 0 to (numberNodes-1)) {
         println("Inside joinOnebyOne")
         joinOnebyOne(i)
       }
       println("Printing hashset")
       println(hashset)
     }

     def checkJoin(): Unit = {
       println("Inside checkJoin")
       println(hashset.size)
       println(neighbouringActors.size)
       println(numberNodes)
       if(hashset.size == numberNodes && neighbouringActors.size == numberNodes) {
         println("Inside cancel")
         cancellable.cancel()
         println("hashset size is" + hashset.size)
         actorsArray = hashset.toArray
         import ac.dispatcher
         cancellable2 = ac.scheduler.schedule(0 milliseconds, 10 milliseconds, self, joinCalls())

       }
     }

     def initiateJoinCalls(): Unit = {
       if(joinCallNumber == numberNodes) {
         cancellable2.cancel()
         println("Inside cancellable2 cancel")
       }
       else {
         println("actorsArray is " + actorsArrayBuf(joinCallNumber.intValue()))
         var actr = context.actorSelection(BigIntToHexString(actorsArrayBuf(joinCallNumber.intValue())))
         println("="*40)
         println("Inside join " + actr.pathString + " neighbour is " + neighbouringActors(joinCallNumber.intValue()))
         println("="*40)
         println("joinCallNumber is " + joinCallNumber)
         println("neighboring Actor is " + neighbouringActors(joinCallNumber.intValue()))
         actr ! join(neighbouringActors(joinCallNumber.intValue()))
         joinCallNumber = joinCallNumber + 1

       }
     }


  def incrementActorcount(worker: ActorRef): Unit ={
    setupActors = setupActors + 1
    if(setupActors == numberNodes) {
      worker ! getStartedWithRequests()
    }
  }

  def doneWithRequests(worker: ActorRef): Unit ={
    completedActors = completedActors + 1
    if(completedActors == numberNodes){
      //shutdown here
      println("="*20)
      println("Average number of hops are " + averageHops)
      println("="*20)
      println("Shutting down")
      ac.shutdown()
    }
  }

  def calculateAverageHops(sender: ActorRef, numberOfHops: Int): Unit ={
    println("sender is " + sender.path.name)
    println("number of Hops is " + numberOfHops)
    totalHopsForNodes = numberOfHops + totalHopsForNodes
    totalNodesVisited = totalNodesVisited + 1
    averageHops = totalHopsForNodes/totalNodesVisited
    println("Average till now :::  " + averageHops)

  }

  private def generateRandomNumber(id: Int): BigInt = {
    var nodeId: BigInt = 0
    if (id == 0)
      patternA = BigInt.apply(63, scala.util.Random)

    if (id == 1)
      patternB =BigInt.apply(63, scala.util.Random)

    if (id % 3 == 0) {
      nodeId = patternA+BigInt.apply(15, scala.util.Random)
    }
    else if (id % 7 == 0) {
      nodeId = patternB+BigInt.apply(10, scala.util.Random)
    }
    else {
      nodeId = BigInt.apply(63, scala.util.Random)
    }

    println("patternA is " + patternA)

    println("patternB is " + patternB)

    println("nodeId is " + nodeId)

    nodeId

  }

  def joinOnebyOne(id: Int) {   
  // TODO non-duplicate check yet to be added   

    println("Inside joinOnebyOne :::: "  + id)

    //DR: create a pattern
    var nodeId : BigInt = generateRandomNumber(id)
    

  	var addedNumber: BigInt = 0
  	while(hashset.contains(nodeId)) {
      nodeId = generateRandomNumber(id)
    }

    if(!hashset.contains(nodeId)) {
      hashset.add(nodeId)
      addedNumber = nodeId
      actorsArrayBuf += nodeId
    }

    var nodeIdString = BigIntToHexString(nodeId)
    println("nodeIdString " + nodeIdString)
    val actr: ActorRef = context.actorOf(
      Worker.props(ac, self, numberNodes, b, numberOfRequests), nodeIdString
    )

    var neighbourId = ""

    if(id > 0) {
      neighbourId = BigIntToHexString(lastInserted)
    }
    /*Commenting for debugging*/
    /*
    actr ! join(neighbourId)
    */
    /*Commenting for debugging*/
    lastInserted = addedNumber
    println("="*30)
    println("neighbourId is " + neighbourId)
    println("="*30)
    neighbouringActors += neighbourId
  }

  private def BigIntToHexString(input: BigInt): String = {
    var hexString = input.toString(16)
    var zeroAppendSize: Int = 16 - hexString.size
    var nodeIdString = "0"*zeroAppendSize + hexString
    nodeIdString
  }

}
