import scala.concurrent.Future
import akka.actor.Actor.Receive
import akka.actor._
import java.security.MessageDigest  
import Worker._
import scala.collection.mutable.ListBuffer
import scala.collection.TraversableOnce
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

case class join(neighbourId: String)
case class updateTables(hopNo: Int, rTable: Array[String], lsMinus: ArrayBuffer[String], lsPlus: ArrayBuffer[String], finalNode: Boolean)
case class route(msg: String, neighbourNodeId: String, senderNodeId: String, join: Boolean, newNode: Boolean, hopNumber: Int, lastNode: Boolean)
case class newNodeState(snId: String, rTable: Array[Array[String]])
case class getStartedWithRequests()

object Worker {
    
  
  def props(ac: ActorSystem, senderBoss: ActorRef, numNodes:Int, base:Int, numberOfRequests: Int): Props =
    Props(classOf[Worker], ac, senderBoss, numNodes, base, numberOfRequests)
    // NOTE: byteKey is a nodeId expressed simply as an array of bytes (no base conversion)
}

 class Worker(ac: ActorSystem, superBoss: ActorRef, numNodes:Int, b:Int, numberOfRequest: Int) extends Actor {
  val RTrows = 64/b
  val RTcols = scala.math.pow(2, b).toInt

   var cancellable:Cancellable = new Cancellable {override def isCancelled: Boolean = false

     override def cancel(): Boolean = false
   }
  var isSetupDone: Boolean = false
  // Each actor will have a routing table,  neighbour set, leaf set

  var routingTable = Array.ofDim[String](RTrows, RTcols) //, RTcols)       //of size log(numNodes)/log(2^b) rows X 2^b columns
  //var leafSetMinus = Array.ofDim[String](RTcols/2)
  //var leafSetPlus  = Array.ofDim[String](RTcols/2)
  var leafSetMinus:ArrayBuffer[String] = new ArrayBuffer[String]
  var leafSetPlus:ArrayBuffer[String] = new ArrayBuffer[String]
  var leafSetSize: Int = 8
  var leafMinusIndex: Int = 0
  var leafPlusIndex: Int = 0
  var neighbourIdString: String = ""
  var routeMessageCount: Int = 0
  /*
  var neighbourSet: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]	//of size 2*(2^b)
  var leafSetMinus: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]	//of size (2^b)/2
  var leafSetPlus: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]    //of size (2^b)/2
  */
  
  def receive = {
    
    case "test" => println("test")
    case join(neighbourId: String) => join(sender, neighbourId)
    case newNodeState(senderNodeId: String, rTable: Array[Array[String]]) => updateTablesAsPerNew(senderNodeId: String, rTable: Array[Array[String]])
    case getStartedWithRequests() => getStartedWithRequests()
    //case default => println("Entered default : Received message "+default);

    
  }

   def getStartedWithRequests(): Unit = {
     isSetupDone = true
   }

   def compfn1(e1: String, e2: String) = (BigInt.apply(e1, 16) < BigInt.apply(e2, 16))
   def compfn2(e1: String, e2: String) = (BigInt.apply(e1, 16) > BigInt.apply(e2, 16))
  
   def join(senderBoss: ActorRef, neighbourId: String):Unit = {
    neighbourIdString = neighbourId
    println("Inside testInit")
    var i = 0
    var sum = 0
    /*
    for (i <- 0 to (16-1)) {
      println("Inside joinOnebyOne")
      sum = sum + i
    }
    */
    
    // filling up the corresponding RT entry
     // Find common prefix between current node and updating node
     /*
     var length:Int =  neighbourIdString.zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.size
     var column:BigInt = BigInt.apply(neighbourIdString.charAt(length).toString(), 16)
     */
     var currentNode = self.path.name
     var column:BigInt = 0
     for(i<-0 to currentNode.length - 1) {
        column = BigInt.apply(currentNode.charAt(i).toString(), 16)
       routingTable(i)(column.intValue()) = currentNode
     }
     column = BigInt.apply(currentNode.charAt(0).toString(), 16)
    println(routingTable(0)(column.intValue()))
    //routingTable(length)(column.intValue()) = neighbourIdString
    println("NeighbourId: " +neighbourIdString+" added to the RT of "+self.path.name)
     
    
    println("neighbourId is " + neighbourId)
    println("sum is " + sum)
    println("Completing testInit")
    if(neighbourId == "") {
      println("="*10)
      println("Starting node")
      println("="*10)
    } else {
      println("="*10)
      println("Node >= 1")
      println("="*10)
      /*Added by Anirudh Subramanian for testing Begin*/
      /*Tests for findMinimumLeafSet*/
      /*
      leafSetMinus += "03"
      leafSetMinus += "04"
      leafSetMinus += "05"
      leafSetMinus += "07"

      leafSetPlus += "A"
      leafSetPlus += "B"
      leafSetPlus += "C"
      leafSetPlus += "D"
      leafSetPlus += "E"
      println("="*10 + "findMinimumLeafSet output" + "="*10)
      println(findMinimumLeafSet(12).toString(16))
      println("="*10 + "findMinimumLeafSet output" + "="*10)
      */
      /*findMinimumLeafSet working well*/
      /*
      leafSetMinus += "03"
      leafSetMinus += "04"
      leafSetMinus += "05"
      leafSetMinus += "07"

      leafSetPlus += "A"
      leafSetPlus += "B"
      leafSetPlus += "C"
      leafSetPlus += "D"
      leafSetPlus += "E"
      println("="*10 + "findMinimumLeafSet output" + "="*10)
      var find_route = searchInTables("02", "random")
      println("Tuple value for bigint is " + find_route._1)
      println("Tuple value for boolean is " + find_route._2)
      println("="*10 + "findMinimumLeafSet output" + "="*10)

      var currentNode = ""
      */
      /*searchInLeft working for leafset*/
      /*Test for routing table*/
      /*
      leafSetMinus += "03"
      leafSetMinus += "04"
      leafSetMinus += "05"
      leafSetMinus += "07"

      leafSetPlus += "A"
      leafSetPlus += "B"
      leafSetPlus += "C"
      leafSetPlus += "D"
      leafSetPlus += "E"


      println("="*10 + "findMinimumLeafSet output" + "="*10)
      var find_route = searchInTables("04", "random")
      println("Tuple value for bigint is " + find_route._1)
      println("Tuple value for boolean is " + find_route._2)
      println("="*10 + "findMinimumLeafSet output" + "="*10)
      var currentNode = "4abc678def770224"
      var searchNode  = "4abc679adf774321"
      routingTable(6) = Array("4abc670def123456", "4abc671def123456", "4abc672def123456", "4abc673def123456", "4abc674def123456"
                              , "4abc675def123456", "4abc676def123456", "4abc677def123456", "4abc678def123456", "4abc679def123456"
                              , "4abc67adef123456", "4abc67bdef123456", "4abc67cdef123456", "4abc67ddef123456", "4abc67edef123456",
        "4abc67fdef123456")
      var find_route2 = searchInTables(searchNode, currentNode)
      println("Tuple for string of hex is " + find_route2._1.toString(16))
      println("Tuple for boolean is " + find_route2._2)
      */
      /*Added by Anirudh Subramanian for testing End*/
      /*Commented by Anirudh Subramanian for testing Begin*/
      //route("timepass", neighbourId, self.path.name, true, true, 0, false)
      /*Commented by Anirudh Subramanian for testing End*/
    }
    import ac.dispatcher
    cancellable = ac.scheduler.schedule(0 seconds, 1 seconds, self, routeRandom())

    senderBoss ! sum
  }

  private def routeRandom(): Unit ={
    if (isSetupDone) {
      if(routeMessageCount == numberOfRequest) {
        cancellable.cancel()
        context.actorSelection("..") ! doneWithRequests()
      } else {
        var nodeId: BigInt = BigInt.apply(63, scala.util.Random)
        var nodeIdString = BigIntToHexString(nodeId)
        self ! route(nodeIdString, neighbourIdString, self.path.name, false, true, 0, false)
        routeMessageCount = routeMessageCount + 1
      }
    }
  }

   private def BigIntToHexString(input: BigInt): String = {
     var hexString = input.toString(16)
     var zeroAppendSize: Int = 16 - hexString.size
     var nodeIdString = "0"*zeroAppendSize + hexString
     nodeIdString
   }

  def updateTables(hopNo: Int, rTable: Array[String], lsMinus: ArrayBuffer[String], lsPlus: ArrayBuffer[String], finalNode: Boolean, currentNodeName: String): Unit = {
      if(finalNode) {
        var dummy = leafSetMinus.clone
        leafSetMinus ++= lsMinus
        leafSetPlus ++= lsPlus
        leafSetMinus = leafSetMinus.sortWith(compfn1)
        leafSetMinus = leafSetMinus.reverse
        if(leafSetMinus.size >= leafSetSize)
          leafSetMinus.reduceToSize(leafSetSize)
        leafSetMinus = leafSetMinus.sortWith(compfn1)
        leafSetPlus.sortWith(compfn1)
        if(leafSetPlus.size >= leafSetSize)
          leafSetPlus.reduceToSize(leafSetSize)
        /*
        dummy = leafSetPlus.sortWith(compfn1)
        var i = 0

        leafSetPlus.reduceToSize(0)
        leafSetPlus ++= dummy
        leafSetPlus.reduceToSize(leafSetSize)
       
        dummy = leafSetMinus.sortWith(compfn2)
        dummy.reduceToSize(leafSetSize)
        dummy = leafSetMinus.sortWith(compfn1)
        leafSetMinus.reduceToSize(0)
        leafSetMinus ++= dummy
        leafSetMinus.reduceToSize(leafSetSize)
        */
        println("leafsetPlus" + leafSetPlus)
        println("leafsetPlus" + leafSetMinus)
        println("Node setup done !")
      }
      if(hopNo >= 0) {
        println("="*20)
        println("Inside hop greater than 0")
        println("="*20)
        var column: BigInt = BigInt.apply(currentNodeName.charAt(hopNo).toString(), 16)
        var temp: String   = routingTable(hopNo)(column.intValue())
        rTable.copyToArray(routingTable(hopNo))
        routingTable(hopNo)(column.intValue()) = temp
        var i = 0
        for (i<-0 to rTable.length-1) {
          println("table: "+rTable(i))
        }
      }
  }

  def route(msg: String, neighbourNodeId: String, senderNodeId: String, join: Boolean, newNode: Boolean, hopNumber: Int, lastNode: Boolean): Unit = {
       var currentNodeName: String = self.path.name
       println("currentNodeName is " + currentNodeName )
       if(join) {
         if(lastNode) {
           var updateHopsLast = hopNumber + 1
           var senderNode    = context.actorSelection(senderNodeId)
           senderNode ! updateTables(updateHopsLast - 1, routingTable(updateHopsLast - 1), leafSetMinus, leafSetPlus, true, currentNodeName)
           println("currentNodeName is " + currentNodeName )
           println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updateHopsLast )
           sendState()
           return
         }

          if(newNode) {
            val neighbouringActor = context.actorSelection(neighbourNodeId)
            print("neighbouringActor is " + neighbouringActor)
            neighbouringActor ! route(msg, "",senderNodeId, join, false, hopNumber, false)
          }
          else {
            var updatedHopNumber = hopNumber + 1
            var findRoute:(BigInt, Boolean) = searchInTables(senderNodeId, currentNodeName)
            //Leafset true
            if(findRoute._2) {
              /*If not null route to that node with minimum */
              if(findRoute._1 != null) {
                var nextInRouteId = BigIntToHexString(findRoute._1)
                var nextInRoute   = context.actorSelection(nextInRouteId)
                var senderNode    = context.actorSelection(senderNodeId)
                senderNode ! updateTables(updatedHopNumber - 1, routingTable(updatedHopNumber - 1), leafSetMinus, leafSetPlus, false, currentNodeName)
                nextInRoute ! route(msg, "", senderNodeId, join, false, updatedHopNumber, true)
              } else {
                var senderNode    = context.actorSelection(senderNodeId)
                senderNode ! updateTables(updatedHopNumber - 1, routingTable(updatedHopNumber - 1), leafSetMinus, leafSetPlus, true)
                println("Nearest key " + currentNodeName)
                println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updatedHopNumber )
                sendState()
                return
              }
              /*If null then print the hopping ends here*/
              println("")
            } else {
              if (findRoute._1 != null) {
                var nextInRouteId = BigIntToHexString(findRoute._1)
                var nextInRoute = context.actorSelection(nextInRouteId)
                var senderNode = context.actorSelection(senderNodeId)
                senderNode ! updateTables(updatedHopNumber - 1, routingTable(updatedHopNumber - 1), leafSetMinus, leafSetPlus, false, currentNodeName)
                nextInRoute ! route(msg, "", senderNodeId, join, false, updatedHopNumber, false)
              } else {
                var senderNode    = context.actorSelection(senderNodeId)
                senderNode ! updateTables(updatedHopNumber - 1, routingTable(updatedHopNumber - 1), leafSetMinus, leafSetPlus, true)

                println("Nearest key " + currentNodeName)
                println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updatedHopNumber )
                sendState()
                return
              }

            }
          }

        } else {
         //not join.. so normal routing
         if(lastNode) {
           var updateHopsLast = hopNumber + 1
           var senderNode    = context.actorSelection(senderNodeId)
           println("Nearest key " + currentNodeName)
           println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updateHopsLast )
           calculateAverageHops(updateHopsLast)
           return
         }
         else {
          if(newNode) {
            val neighbouringActor = context.actorSelection(neighbourNodeId)
            print("neighbouringActor is " + neighbouringActor)
            neighbouringActor ! route(msg, "",senderNodeId, false, false, hopNumber, false)
          }
           else {
            var updatedHopNumber = hopNumber + 1
            var findRoute:(BigInt, Boolean) = searchInTables(msg, currentNodeName)
            //Leafset true
            if(findRoute._2) {
              /*If not null route to that node with minimum */
              if(findRoute._1 != null) {
                var nextInRouteId = BigIntToHexString(findRoute._1)
                var nextInRoute   = context.actorSelection(nextInRouteId)
                var senderNode    = context.actorSelection(senderNodeId)
                nextInRoute ! route(msg, "", senderNodeId, false, false, updatedHopNumber, true)
              } else {
                var senderNode    = context.actorSelection(senderNodeId)
                calculateAverageHops(updatedHopNumber)
                println("Nearest key " + currentNodeName)
                println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updatedHopNumber  )
                return
              }
              /*If null then print the hopping ends here*/
              println("")
            } else {
              if (findRoute._1 != null) {
                var nextInRouteId = BigIntToHexString(findRoute._1)
                var nextInRoute = context.actorSelection(nextInRouteId)
                var senderNode = context.actorSelection(senderNodeId)
                nextInRoute ! route(msg, "", senderNodeId, false, false, updatedHopNumber, false)
              } else {
                var senderNode = context.actorSelection(senderNodeId)
                calculateAverageHops(updatedHopNumber)
                println("Nearest key " + currentNodeName)
                println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updatedHopNumber  )
                return
              }
            }
          }
         }
       }

  }

  def searchInTables(senderNodeId: String, currentNodeName: String): (BigInt, Boolean) = {
   var searchInMinLeaf: Boolean = false
   var searchInMaxLeaf: Boolean = false
   var min: BigInt = Long.MaxValue
   var max: BigInt = Long.MinValue
   var numericallyClosest: BigInt = null
   var minLeft: Int = 0
   var maxRight: Int = leafSetPlus.size - 1
   var inLeaf = false
   if (senderNodeId == null)
    return (null, false)
   var key: BigInt = BigInt.apply(senderNodeId, 16)
    if(leafSetMinus.size > 0) {
      min = BigInt.apply(leafSetMinus(minLeft), 16)
      searchInMinLeaf = true
    }

    if(leafSetPlus.size > 0) {
      max = BigInt.apply(leafSetPlus(maxRight), 16)
      searchInMaxLeaf = true
    }

    if(searchInMinLeaf && searchInMaxLeaf) {
      if (min <= key && key <= max) {
        numericallyClosest = findMinimumLeafSet(key)
        if(numericallyClosest != null) {
          return (numericallyClosest, true)
        }
      }
    }
    //Longest Common Prefix size
    var length:Int =  senderNodeId.zip(currentNodeName).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.size
    println("senderNodeId is " + senderNodeId)
    println("currentNodeName is " + currentNodeName)
    println("Accessing character" + length)
    if(length == 16)
      return (numericallyClosest, false)
    var column:BigInt = BigInt.apply(senderNodeId.charAt(length).toString(), 16)
    println("="*20)
    println("length is " + length + " column is " + column)
    println("="*20)
    if(routingTable(length)(column.intValue()) != null) {
      numericallyClosest = BigInt.apply(routingTable(length)(column.intValue()), 16)
    }

    return(numericallyClosest, false)
  }

   private def findMinimumLeafSet(key: BigInt): BigInt = {
      var rightMostOfLeft: Int = leafSetMinus.size - 1
      var leftMostOfRight: Int = 0
      var maxLeft = BigInt.apply(leafSetMinus(rightMostOfLeft), 16)
      var minRight = BigInt.apply(leafSetPlus(leftMostOfRight), 16)
      var output: BigInt = null
      println("maxLeft is " + maxLeft)
      println("minRight is " + minRight)
      if(maxLeft <= key && key <= minRight) {
        println("Inside key greater than maxLeft and less than minRight")
        var diff: BigInt = key - maxLeft
        var diff2: BigInt = key - minRight
        if(key == maxLeft)
          output = maxLeft
        if(key == minRight)
          output = minRight
        if(output == null) {
          if (diff.abs < diff2.abs) {
            output = maxLeft
          } else {
            output = minRight
          }
        }
      }
      if(key < maxLeft) {
        println("Inside maxLeft")
        output = findNearestNumericalLeaf(leafSetMinus,key)
        println("output is " + output)
      }
      if(key > minRight) {
        //TODO compare between two smallest elements
        println("Inside minRight")
        output = findNearestNumericalLeaf(leafSetPlus, key)
      }

      output
   }

   private def findNearestNumericalLeaf(leafSet: ArrayBuffer[String],key: BigInt): BigInt = {
     println("Inside findNearestNumericalLeaf")
     var leafSetSize: Int = leafSet.size - 1
     println("leafSetSize is " + leafSetSize)
     var output: BigInt = null
     if(leafSetSize < 0) return output
     if(leafSetSize == 0 || leafSetSize == 1) return BigInt.apply(leafSet(leafSetSize), 16)
     for(i <- 0 to leafSetSize) {
        println("Inside for")
        if(BigInt.apply(leafSet(i), 16) >= key ) {
          println("Inside if of leafSet")
          if(i == 0)
            return BigInt.apply(leafSet(i), 16)
          var diff1: BigInt = key - BigInt.apply(leafSet(i), 16)
          var diff2: BigInt = key - BigInt.apply(leafSet(i - 1), 16)
          if(diff1.abs  < diff2.abs) {
            output = BigInt.apply(leafSet(i), 16)
          } else {
            output = BigInt.apply(leafSet(i - 1), 16)
          }
          return output
        }

     }
     return output
   }
   private def sendState() {
     // for each member of the table sets, send own state
     var i = 0
     var j = 0
     for(i <- 0 to RTrows-1) {
       for(i <- 0 to RTcols-1) {
    	          
         if (routingTable(i)(j) != null) {
        	 var node = context.actorSelection(routingTable(i)(j))
        	 node ! newNodeState(self.path.name, routingTable)
         }
       }
     }
     for(i <- 0 to leafSetMinus.size-1) {
       
       if (leafSetMinus(i) != null) {
    	   var node = context.actorSelection(leafSetMinus(i))
    	   node ! newNodeState(self.path.name, routingTable)
       }
     }
     for(i <- 0 to leafSetPlus.size-1) {
       
       if (leafSetPlus(i) != null) {
    	   var node = context.actorSelection(leafSetPlus(i))
    	   node ! newNodeState(self.path.name, routingTable)
       }
     }

     context.actorSelection("..") ! incrementActorcount()
   }

   private def updateTablesAsPerNew(senderNodeId: String,  rTable: Array[Array[String]]) {

	   println("Inside updateTablesAsPerNew")
	   var ownNode = BigInt.apply((self.path.name), 16) //BigInt values
     var updater = BigInt.apply(senderNodeId, 16)  // BigInt values
     
     //If senderId falls in leafset
     //if left set
    if ((leafSetMinus.size == 0) &&  (updater<ownNode)) {
    	leafSetMinus(0) = senderNodeId
    	println("First nodeID:"+ senderNodeId+" added to leaf table minus of "+ self.path.name)
      }
    else if (((BigInt.apply(leafSetMinus(0), 16)) <  updater) && (ownNode >  updater)) {
       var dummy = leafSetMinus.clone
       var i = leafSetMinus.size-1
       var j = 0
       while(updater < (BigInt.apply(leafSetMinus(i), 16))) {
         i -= 1
       }
       dummy(i) = BigIntToHexString(updater)
       // put values from i...1 --> i-1..0
       for(j <- i to 1 by 1) {
         if(leafSetMinus(j) != null) {
           dummy(j-1) = leafSetMinus(j)
         }
       }
       leafSetMinus = dummy.clone
     }


     // if right set
	   if ((leafSetPlus.size == 0) &&  (updater>ownNode)) {
	       leafSetPlus(0) = senderNodeId
	       println("First nodeID:"+ senderNodeId+" added to leaf table plus of "+ self.path.name)
	  }
	else if ((ownNode <  updater) && (updater <  (BigInt.apply(leafSetPlus(leafSetPlus.size-1), 16)))) {
       var dummy = leafSetPlus.clone
       var i = 0
       var j = 0
       while(updater > (BigInt.apply(leafSetPlus(i), 16))) {
         i += 1
       }
       dummy(i) = BigIntToHexString(updater)
       // put values from i...size-2 --> i+1..size-1
       for(j <- i to leafSetPlus.size-2) {
         if(leafSetPlus(j) != null) {
           dummy(j+1) = leafSetMinus(j)
         }
       }
       leafSetPlus = dummy.clone
     }

     // if routing table
     //Longest Common Prefix size
     var i = 0
     var j = 0

     // Find common prefix between current node and updating node
     var length:Int =  senderNodeId.zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.size
     var column:BigInt = BigInt.apply(senderNodeId.charAt(length).toString(), 16)
     if(routingTable(length)(column.intValue()) == null) {
       routingTable(length)(column.intValue()) = senderNodeId
     }

     for(i <- 0 to length) {
       for(j <- 0 to RTcols-1) {
         var currentNodeofUpdater = rTable(i)(j) //nodeId in updater's table
         var currentNodeofOwn = routingTable(i)(j)
         if(currentNodeofUpdater != null) {
           if(currentNodeofOwn == null) {
             routingTable(i)(j) = currentNodeofUpdater
           }
           else
           {
             var currentNodeofOwnBigInt = BigInt.apply(currentNodeofOwn, 16)
             var currentNodeofUpdaterBigInt = BigInt.apply(currentNodeofUpdater, 16)
             if ((currentNodeofOwnBigInt - ownNode).abs > (currentNodeofUpdaterBigInt - ownNode).abs) {
               routingTable(i)(j) = currentNodeofUpdater
             }
           }
         }
       }
     }
   }
 }  
