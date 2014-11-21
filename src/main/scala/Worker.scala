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
case class updateTablesTo(hopNo: Int, rTable: Array[Array[String]], lsMinus: ArrayBuffer[String], lsPlus: ArrayBuffer[String], finalNode: Boolean, senderNodeName: String)
case class routeTo(msg: String, neighbourNodeId: String, senderNodeId: String, join: Boolean, newNode: Boolean, hopNumber: Int, lastNode: Boolean)
case class newNodeState(snId: String, rTable: Array[Array[String]])
case class getStartedWithRequests()
case class sendStateTo()
case class routeRandom()

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
  var ideal_leafSetSize: Int = 8
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
    case routeTo(msg: String, neighbourNodeId: String, senderNodeId: String, join: Boolean, newNode: Boolean, hopNumber: Int, lastNode: Boolean) =>  route(msg: String, neighbourNodeId: String, senderNodeId: String, join: Boolean, newNode: Boolean, hopNumber: Int, lastNode: Boolean)
    case updateTablesTo(hopNo: Int, rTable: Array[Array[String]], lsMinus: ArrayBuffer[String], lsPlus: ArrayBuffer[String], finalNode: Boolean, senderNodeName: String) => updateTables(hopNo: Int, rTable: Array[Array[String]], lsMinus: ArrayBuffer[String], lsPlus: ArrayBuffer[String], finalNode: Boolean, senderNodeName: String)
    case sendStateTo() => sendState()
    case routeRandom() => randomRoute()
    //case default => println("Entered default : Received message "+default);
    case "print" => printTables() 
  }
  
  private def printTables() {
     printRoutingTable()
     printPrivateLeafSet()
   }

  def getStartedWithRequests(): Unit = {
    isSetupDone = true
    //println("Inside getStartedWithRequests")
    //println("For  :: " + self.path.name)
  }

  def compfn1(e1: String, e2: String) = (BigInt.apply(e1, 16) < BigInt.apply(e2, 16))
  def compfn2(e1: String, e2: String) = (BigInt.apply(e1, 16) > BigInt.apply(e2, 16))

  def join(senderBoss: ActorRef, neighbourId: String):Unit = {
    neighbourIdString = neighbourId
    var i = 0
    var sum = 0

    // Adding self to RT
    var currentNode = self.path.name
    var column:BigInt = 0
    for(i<-0 to currentNode.length - 1) {
      column = BigInt.apply(currentNode.charAt(i).toString(), 16)
      if (routingTable(i)(column.intValue()) != currentNode)
        routingTable(i)(column.intValue()) = currentNode
    }

    column = BigInt.apply(currentNode.charAt(0).toString(), 16)
    if(neighbourId == "") {
    }
    else {
      var length:Int =  neighbourIdString.zip(currentNode).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.size
      column = BigInt.apply(neighbourIdString.charAt(length).toString(), 16)
      routingTable(length)(column.intValue()) = neighbourIdString
      if(BigInt.apply(neighbourIdString, 16) < (BigInt.apply(currentNode, 16)))
      {
        leafSetMinus += neighbourIdString
      }
      else if(BigInt.apply(neighbourIdString, 16) > (BigInt.apply(currentNode, 16))){
        leafSetPlus += neighbourIdString
      }

    }
    if(neighbourId != "") {
      self ! routeTo("timepass", neighbourId, self.path.name, true, true, 0, false)
     }
    import ac.dispatcher
    cancellable = ac.scheduler.schedule(0 microseconds, 1 microseconds, self, routeRandom())

  }


  private def printRoutingTable(): Unit = {
    println("Printing routing table for " + self.path.name)
    for(i <- 0 to RTrows - 1 ) {
      System.out.print("Row# "  + i + "---->   ")
      for(j <- 0 to RTcols - 1 ) {

        System.out.print(routingTable(i)(j))
        if(j < RTcols - 1)
          System.out.print("     ")
      }
      System.out.println()
    }
  }

  private def printPrivateLeafSet(): Unit ={
    println("Printing leaf set for " + self.path.name)
    for(i <- 0 to leafSetMinus.length - 1) {
      if (i == 0) {
        print("leafSetMinus ------>            ")
        print(leafSetMinus(i) + " ")
      }
      else {
        print(leafSetMinus(i)+ " ")
      }
    }
    println()
    for(j <- 0 to leafSetPlus.length - 1) {
      if (j == 0) {
        print("leafSetPlus ------>            ")
        print(leafSetPlus(j)+ " ")
      } else {
        print(leafSetPlus(j)+ " ")
      }
    }
    println()
  }

  private def randomRoute(): Unit ={
    if (isSetupDone) {
      if(routeMessageCount == numberOfRequest) {
        cancellable.cancel()
        context.actorSelection("../") ! doneWithRequests()
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

  def updateTables(hopNo: Int, rTable: Array[Array[String]], lsMinus: ArrayBuffer[String], lsPlus: ArrayBuffer[String], finalNode: Boolean, senderNodeName: String): Unit = {
    if(finalNode) {
      var p:BigInt = BigInt.apply(self.path.name, 16)
      leafSetMinus ++= lsMinus.filter(x => BigInt.apply(x, 16) < p)
      leafSetMinus ++= lsPlus.filter(x => BigInt.apply(x, 16) < p)

      leafSetPlus ++= lsMinus.filter(x => BigInt.apply(x, 16) > p)
      leafSetPlus ++= lsPlus.filter(x => BigInt.apply(x, 16) > p)

      leafSetPlus = leafSetPlus.distinct
      leafSetMinus = leafSetMinus.distinct

      leafSetMinus = leafSetMinus.sortWith(compfn1)
      leafSetMinus = leafSetMinus.reverse
      if(leafSetMinus.size >= ideal_leafSetSize)
        leafSetMinus.reduceToSize(ideal_leafSetSize)
      leafSetMinus = leafSetMinus.sortWith(compfn1)
      leafSetPlus.sortWith(compfn1)
      if(leafSetPlus.size >= ideal_leafSetSize)
        leafSetPlus.reduceToSize(ideal_leafSetSize)
    }
    if(hopNo >= 0) {
      var currentNodeName: String = self.path.name

      var length:Int =  senderNodeName.zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.size
      var column:BigInt = BigInt.apply(senderNodeName.charAt(length).toString(), 16)
      if(routingTable(length)(column.intValue()) == null) {
        routingTable(length)(column.intValue()) = senderNodeName
      }
      var ownNode: Int = BigInt.apply(self.path.name, 16).intValue()

      for(i <- 0 to length) {
        for(j <- 0 to RTcols-1) {
          var currentNodeofUpdater = rTable(i)(j) //nodeId in updater's table
          var currentNodeofOwn = routingTable(i)(j)
          if(currentNodeofOwn != self.path.name) {
            if (currentNodeofUpdater != null) {
              if (currentNodeofOwn == null) {
                routingTable(i)(j) = currentNodeofUpdater
              }
              else {
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


      var i = 0
    }
  }

  def route(msg: String, neighbourNodeId: String, senderNodeId: String, join: Boolean, newNode: Boolean, hopNumber: Int, lastNode: Boolean): Unit = {
    var currentNodeName: String = self.path.name
    if(join) {
      if(lastNode) {
        var updateHopsLast = hopNumber + 1
        var senderNode    = context.actorSelection("../" + senderNodeId)
        senderNode ! updateTablesTo(updateHopsLast - 1, routingTable, leafSetMinus, leafSetPlus, true, currentNodeName)
        senderNode ! sendStateTo()
        return
      }

      if(newNode) {
        val neighbouringActor = context.actorSelection("../" + neighbourNodeId)
        neighbouringActor ! routeTo(msg, "",senderNodeId, join, false, hopNumber, false)
      }
      else {
        var updatedHopNumber = hopNumber + 1
        var findRoute:(BigInt, Boolean) = searchInTables(senderNodeId, currentNodeName)
        if(findRoute._2) {
          if(findRoute._1 != null) {
            var nextInRouteId = BigIntToHexString(findRoute._1)
            var nextInRoute   = context.actorSelection("../" + nextInRouteId)
            var senderNode    = context.actorSelection("../" + senderNodeId)
            senderNode ! updateTablesTo(updatedHopNumber - 1, routingTable, leafSetMinus, leafSetPlus, false, currentNodeName)
            nextInRoute ! routeTo(msg, "", senderNodeId, join, false, updatedHopNumber, true)
          } else {
            var senderNode    = context.actorSelection("../" + senderNodeId)
            senderNode ! updateTablesTo(updatedHopNumber - 1, routingTable, leafSetMinus, leafSetPlus, true, currentNodeName)
            senderNode ! sendStateTo()
            return
          }
        } else {
          if (findRoute._1 != null) {
            var nextInRouteId = BigIntToHexString(findRoute._1)
            var nextInRoute = context.actorSelection("../" + nextInRouteId)
            var senderNode = context.actorSelection("../" + senderNodeId)
            senderNode ! updateTablesTo(updatedHopNumber - 1, routingTable, leafSetMinus, leafSetPlus, false, currentNodeName)
            nextInRoute ! routeTo(msg, "", senderNodeId, join, false, updatedHopNumber, false)
          } else {
            var senderNode    = context.actorSelection("../" + senderNodeId)
            senderNode ! updateTablesTo(updatedHopNumber - 1, routingTable, leafSetMinus, leafSetPlus, true, currentNodeName)
            senderNode ! sendStateTo()
            return
          }

        }
      }

    } else {
      if(lastNode) {
        var updateHopsLast = hopNumber + 1
        var senderNode    = context.actorSelection("../" + senderNodeId)
        superBoss ! calculateAverageHops(updateHopsLast)
        return
      }
      else {
        if(newNode) {
          val neighbouringActor = context.actorSelection("../" + neighbourNodeId)
          neighbouringActor ! routeTo(msg, "",senderNodeId, false, false, hopNumber, false)
        }
        else {
          var updatedHopNumber = hopNumber + 1
          var findRoute:(BigInt, Boolean) = searchInTables(msg, currentNodeName)
          if(findRoute._2) {
            /*If not null route to that node with minimum */
            if(findRoute._1 != null) {
              var nextInRouteId = BigIntToHexString(findRoute._1)
              var nextInRoute   = context.actorSelection("../" + nextInRouteId)
              var senderNode    = context.actorSelection("../" + senderNodeId)
              nextInRoute ! routeTo(msg, "", senderNodeId, false, false, updatedHopNumber, true)
            } else {
              var senderNode    = context.actorSelection("../" + senderNodeId)
              superBoss ! calculateAverageHops(updatedHopNumber)
              return
            }
          } else {
            if (findRoute._1 != null) {
              var nextInRouteId = BigIntToHexString(findRoute._1)
              var nextInRoute = context.actorSelection("../" + nextInRouteId)
              var senderNode = context.actorSelection("../" + senderNodeId)
              nextInRoute ! routeTo(msg, "", senderNodeId, false, false, updatedHopNumber, false)
            } else {
              var senderNode = context.actorSelection("../" + senderNodeId)
              superBoss ! calculateAverageHops(updatedHopNumber)
              return
            }
          }
        }
      }
    }

  }

  def searchInTables(senderNodeId: String, currentNodeName: String): (BigInt, Boolean) = {
    var searchInMaxLeaf: Boolean = false
    var searchInMinLeaf: Boolean = false
    var min: BigInt = Long.MinValue
    var max: BigInt = Long.MaxValue
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

    if(searchInMinLeaf || searchInMaxLeaf) {
      if (min <= key && key <= max) {
        numericallyClosest = findMinimumLeafSet(key, searchInMinLeaf, searchInMaxLeaf)
        if(numericallyClosest != null) {
          return (numericallyClosest, true)
        }
      }
    }
    //Longest Common Prefix size
    var length:Int =  senderNodeId.zip(currentNodeName).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.size
    if(length == 16)
      return (numericallyClosest, false)
    var column:BigInt = BigInt.apply(senderNodeId.charAt(length).toString(), 16)
    if(routingTable(length)(column.intValue()) != null) {
      numericallyClosest = BigInt.apply(routingTable(length)(column.intValue()), 16)
    }

    return(numericallyClosest, false)
  }

  private def findMinimumLeafSet(key: BigInt, searchMinimum: Boolean, searchMaximum: Boolean): BigInt = {

    var output: BigInt = null
    var rightMostOfLeft: Int = leafSetMinus.size - 1
    var leftMostOfRight: Int = 0
    var leftMostOfLeft: Int = 0
    var rightMostOfRight: Int = leafSetPlus.size - 1
    var maxLeft: BigInt = Long.MinValue
    var minLeft: BigInt = Long.MaxValue
    var minRight: BigInt = Long.MaxValue
    var maxRight: BigInt = Long.MinValue
    if(leafSetMinus.size > 0) {
      maxLeft = BigInt.apply(leafSetMinus(rightMostOfLeft), 16)
      minLeft = BigInt.apply(leafSetMinus(leftMostOfLeft), 16)
    }


    if(leafSetPlus.size > 0) {
      minRight = BigInt.apply(leafSetPlus(leftMostOfRight), 16)
      maxRight = BigInt.apply(leafSetPlus(rightMostOfRight), 16)
    }

    if(!(searchMaximum && searchMinimum)) {


      if (!searchMinimum) {
        maxLeft = minRight
        minLeft = minRight
      }

      if (!searchMaximum) {
        minRight = maxLeft
        maxRight = maxLeft
      }

    }
    if(key < minLeft || key > maxRight) {
      return output
    }

    if(maxLeft <= key && key <= minRight) {
      //    println("Inside key greater than maxLeft and less than minRight")
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
      //   println("Inside maxLeft")
      output = findNearestNumericalLeaf(leafSetMinus,key)
      //    println("output is " + output)
    }
    if(key > minRight) {
      //TODO compare between two smallest elements
      //  println("Inside minRight")
      output = findNearestNumericalLeaf(leafSetPlus, key)
    }

    output
  }

  private def findNearestNumericalLeaf(leafSet: ArrayBuffer[String],key: BigInt): BigInt = {
    var leafSetSize: Int = leafSet.size - 1
    var output: BigInt = null
    if(leafSetSize < 0) return output
    if(leafSetSize == 0 || leafSetSize == 1) return BigInt.apply(leafSet(leafSetSize), 16)
    for(i <- 0 to leafSetSize) {
      if(BigInt.apply(leafSet(i), 16) >= key ) {
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
    var i = 0
    var j = 0
    for(i <- 0 to RTrows-1) {
      for(i <- 0 to RTcols-1) {

        if ((routingTable(i)(j) != null) && (routingTable(i)(j) != self.path.name)) {
          var node = context.actorSelection("../" + routingTable(i)(j))
          node ! newNodeState(self.path.name, routingTable)
        }
      }
    }
    for(i <- 0 to leafSetMinus.size-1) {

      if ((leafSetMinus(i) != null) && (leafSetMinus(i) != self.path.name)) {
        var node = context.actorSelection("../" + leafSetMinus(i))
        node ! newNodeState(self.path.name, routingTable)
      }
    }
    for(i <- 0 to leafSetPlus.size-1) {

      if ((leafSetPlus(i) != null) && (leafSetPlus(i) != self.path.name)) {
        var node = context.actorSelection("../" + leafSetPlus(i))
        node ! newNodeState(self.path.name, routingTable)
      }
    }

    context.actorSelection("../") ! incrementActorcount()

  }

  private def updateTablesAsPerNew(senderNodeId: String,  rTable: Array[Array[String]]) {

    var ownNode = BigInt.apply((self.path.name), 16) //BigInt values
    var updater = BigInt.apply(senderNodeId, 16)  // BigInt values
    var end: Int = 0
    if ((leafSetMinus.size == 0) &&  (updater<ownNode)) {
      leafSetMinus += senderNodeId
    }
    else if ((leafSetMinus.size > 0) && ((BigInt.apply(leafSetMinus(0), 16)) <  updater) && (ownNode >  updater)) {
      if((leafSetMinus.contains(senderNodeId))) {
        leafSetMinus += senderNodeId
        leafSetMinus = leafSetMinus.sortWith(compfn2)

        if(leafSetMinus.size >= ideal_leafSetSize) {
          leafSetMinus.reduceToSize(ideal_leafSetSize)
        }
        leafSetMinus = leafSetMinus.sortWith(compfn1)
      }
    }


    // if right set
    if ((leafSetPlus.size == 0) &&  (updater>ownNode)) {
      //leafSetPlus.append(senderNodeId)
      leafSetPlus += senderNodeId
      //println("First nodeID:"+ senderNodeId+" added to leaf table plus of "+ self.path.name)
    }
    else if ((leafSetPlus.size > 0) && (ownNode <  updater) && (updater <  (BigInt.apply(leafSetPlus(leafSetPlus.size-1), 16)))) {
      if(!(leafSetPlus.contains(senderNodeId))) {

        leafSetPlus += senderNodeId
        leafSetPlus.sortWith(compfn1)
        if(leafSetPlus.size >= ideal_leafSetSize) {
          leafSetPlus.reduceToSize(ideal_leafSetSize)
        }
      }
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
