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
    //println("--"*25)
    //println("*"*50)
    //println("Inside testInit")
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
      //println("length of the match is " + length)
      //println("neighbourIdString is " + neighbourIdString)
      //println("currentNode is " + currentNode)
      column = BigInt.apply(neighbourIdString.charAt(length).toString(), 16)
      routingTable(length)(column.intValue()) = neighbourIdString
      //println("routingTable value is " + routingTable(length)(column.intValue()) )

      //println("########################### Adding neighbour to LeafTable ###################################")
      if(BigInt.apply(neighbourIdString, 16) < (BigInt.apply(currentNode, 16)))
      {
      //  println("Trying to insert in left + "+ neighbourIdString)
        leafSetMinus += neighbourIdString
      }
      else if(BigInt.apply(neighbourIdString, 16) > (BigInt.apply(currentNode, 16))){
    //    println("Trying to insert in right + "+ neighbourIdString)
        leafSetPlus += neighbourIdString
      }

    }
    println("Completing testInit")
    if(neighbourId == "") {
      println("="*10)
      println("Starting node")
      println("="*10)
    } else {
      println("="*10)
      println("Node >= 1")
      println("="*10)

  //    println("before routing from inside join : "+self.path.name)
      self ! routeTo("timepass", neighbourId, self.path.name, true, true, 0, false)
      //printRoutingTable()
      //var findRoute:(BigInt, Boolean) = searchInTables(neighbourId, self.path.name)
      //System.out.println("Search table results id to route to::: " + BigIntToHexString(findRoute._1) )
      //System.out.println("Search table results is boolean true ::: " + findRoute._2 )
    }
    import ac.dispatcher
    cancellable = ac.scheduler.schedule(0 seconds, 1 milliseconds, self, routeRandom())

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
    //Added for now to not start routeRandom
    //isSetupDone = false
   // println("Inside randomRoute")
   // println("isSetupDone is " + isSetupDone)
    if (isSetupDone) {
     // println("routeMessageCount is " + routeMessageCount)
      if(routeMessageCount == numberOfRequest) {
        cancellable.cancel()
       // println("inside done with requests")
        context.actorSelection("../") ! doneWithRequests()
      } else {
        var nodeId: BigInt = BigInt.apply(63, scala.util.Random)
        var nodeIdString = BigIntToHexString(nodeId)
        self ! route(nodeIdString, neighbourIdString, self.path.name, false, true, 0, false)
        routeMessageCount = routeMessageCount + 1
      //  println("routeMessageCount  is " + routeMessageCount)
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
     // println("inside UpdateTables for " + self.path.name)
     /* if(lsPlus.size != 0) {
        println("inside UpdateTables - lsPlus " + lsPlus(lsPlus.size-1))
      }
      else {
        println("inside UpdateTables - lsPlus size = 0 ")
      }
      if(lsMinus.size != 0) {
        println("inside UpdateTables - lsMinus " + lsMinus(lsMinus.size-1))
      }
      else {
        println("inside UpdateTables - lsMinus size = 0 ")
      }
*/
      var p:BigInt = BigInt.apply(self.path.name, 16)
      //leafSetMinus = leafSetMinus.filter(x => BigInt.apply(x, 16) < p)
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
      /*
      dummy = leafSetPlus.sortWith(compfn1)
      var i = 0
      leafSetPlus.reduceToSize(0)
      leafSetPlus ++= dummy
      leafSetPlus.reduceToSize(ideal_leafSetSize)
      dummy = leafSetMinus.sortWith(compfn2)
      dummy.reduceToSize(ideal_leafSetSize)
      dummy = leafSetMinus.sortWith(compfn1)
      leafSetMinus.reduceToSize(0)
      leafSetMinus ++= dummy
      leafSetMinus.reduceToSize(ideal_leafSetSize)
      */
      //  println("leafsetPlus : " + leafSetPlus)
      //  println("leafsetPlus : " + leafSetMinus)
  //    println("Node setup done !")
    }
    if(hopNo >= 0) {
    //  println("="*20)
    //  println("Inside hop greater than 0")
    //  println("hopNo is " + hopNo)
    //  println("="*20)
      var currentNodeName: String = self.path.name
    //  println("Inside update tables path name value is  " + currentNodeName)
      //var column: BigInt = BigInt.apply(currentNodeName.charAt(rowNumber).toString(), 16)
      /*TODO || Consider this . The neighbour can have a match of more than one character with the currentNode. The paper assumes
        TODO || the general case wherein there is no match. This may not always be true. Have to handle this in the future
      */
      //var temp: String   = routingTable(hopNo)(column.intValue())
      //println("Inside updateTables temp value is " + temp)
      /* println("inside updateTables ")
       println("row received from " + senderNodeName)
      for(i <-0 to rTable.length - 1) {
        print(rTable(i))
        print("               ")
      }
      println() */
      /*Change made to allow to copy first n rows or nth row*/
      /*
      if(rowNumber < 16) {
        if (hopNo == 0) {
          //logic for coying rows till rowNumber
          for (i <- 0 to rowNumber) {
            var column: BigInt = BigInt.apply(currentNodeName.charAt(i).toString(), 16)
            var temp2: String = routingTable(i)(column.intValue())
            rTable(i).copyToArray(routingTable(i))
            if (temp2 != null) {
              routingTable(i)(column.intValue()) = temp2
            }
          }
        } else {
          var column: BigInt = BigInt.apply(currentNodeName.charAt(rowNumber).toString(), 16)
          var temp2: String = routingTable(rowNumber)(column.intValue())
          rTable(rowNumber).copyToArray(routingTable(rowNumber))
          if (temp2 != null) {
            routingTable(hopNo)(column.intValue()) = temp2
          }
        }
      }
      */
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

      /*
      for (i<-0 to rTable.length-1) {
        if(rTable(i)!=null)
        println("of own : " + self.path.name+" table: "+rTable(i)+ " at " +hopNo+" , "+i)
      }
      */

      //printRoutingTable()
      //printPrivateLeafSet()
    }
  }

  def route(msg: String, neighbourNodeId: String, senderNodeId: String, join: Boolean, newNode: Boolean, hopNumber: Int, lastNode: Boolean): Unit = {
    var currentNodeName: String = self.path.name
    //println("Inside route: currentNodeName is " + currentNodeName )
    if(join) {
      if(lastNode) {
        //println("inside route: lastNode for currentNode :"+senderNodeId)
        var updateHopsLast = hopNumber + 1
        var senderNode    = context.actorSelection("../" + senderNodeId)
        senderNode ! updateTablesTo(updateHopsLast - 1, routingTable, leafSetMinus, leafSetPlus, true, currentNodeName)
        //println("currentNodeName is " + currentNodeName )
        println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updateHopsLast )
        senderNode ! sendStateTo()
        return
      }

      if(newNode) {
        //println("inside route: newNode for currentNode :"+senderNodeId)
        //println("neighbournodeid is " + neighbourNodeId)
        val neighbouringActor = context.actorSelection("../" + neighbourNodeId)
        //print("Inside route : neighbouringActor is " + neighbouringActor)
        //println("Routing to :::::::::::::" + neighbouringActor.pathString)
        neighbouringActor ! routeTo(msg, "",senderNodeId, join, false, hopNumber, false)
      }
      else {
        // DR
        //println("Entered else loop.")
        var updatedHopNumber = hopNumber + 1
        println("self path name is  " + self.path.name)
        var findRoute:(BigInt, Boolean) = searchInTables(senderNodeId, currentNodeName)
        //println("findroute._1 is " + findRoute._1)
        //println("findroute._2 is " + findRoute._2)
        //Leafset true
        if(findRoute._2) {
          /*If not null route to that node with minimum */
          if(findRoute._1 != null) {
            var nextInRouteId = BigIntToHexString(findRoute._1)
            var nextInRoute   = context.actorSelection("../" + nextInRouteId)
            var senderNode    = context.actorSelection("../" + senderNodeId)
            senderNode ! updateTablesTo(updatedHopNumber - 1, routingTable, leafSetMinus, leafSetPlus, false, currentNodeName)
            nextInRoute ! routeTo(msg, "", senderNodeId, join, false, updatedHopNumber, true)
          } else {
            var senderNode    = context.actorSelection("../" + senderNodeId)
            senderNode ! updateTablesTo(updatedHopNumber - 1, routingTable, leafSetMinus, leafSetPlus, true, currentNodeName)
         //   println("Nearest key " + currentNodeName)
            println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updatedHopNumber )
            senderNode ! sendStateTo()
            return
          }
          /*If null then print the hopping ends here*/
          println("")
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

         //   println("Nearest key " + currentNodeName)
            println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updatedHopNumber )
            senderNode ! sendStateTo()
            return
          }

        }
      }

    } else {
      //not join.. so normal routing
      if(lastNode) {
        var updateHopsLast = hopNumber + 1
        var senderNode    = context.actorSelection("../" + senderNodeId)
        //println("Nearest key " + currentNodeName)
        println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updateHopsLast )
        superBoss ! calculateAverageHops(updateHopsLast)
        return
      }
      else {
        if(newNode) {
          val neighbouringActor = context.actorSelection("../" + neighbourNodeId)
          //print("neighbouringActor is " + neighbouringActor)
          neighbouringActor ! routeTo(msg, "",senderNodeId, false, false, hopNumber, false)
        }
        else {
          var updatedHopNumber = hopNumber + 1
          var findRoute:(BigInt, Boolean) = searchInTables(msg, currentNodeName)
          //Leafset true
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
            //  println("Nearest key " + currentNodeName)
              println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updatedHopNumber  )
              return
            }
            /*If null then print the hopping ends here*/
            println("")
          } else {
            if (findRoute._1 != null) {
              var nextInRouteId = BigIntToHexString(findRoute._1)
              var nextInRoute = context.actorSelection("../" + nextInRouteId)
              var senderNode = context.actorSelection("../" + senderNodeId)
              nextInRoute ! routeTo(msg, "", senderNodeId, false, false, updatedHopNumber, false)
            } else {
              var senderNode = context.actorSelection("../" + senderNodeId)
              superBoss ! calculateAverageHops(updatedHopNumber)
            //  println("Nearest key " + currentNodeName)
              println("Received the following msg : " + msg + "from senderNode " + senderNode.pathString + ". Hops latest " + updatedHopNumber  )
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
    //println("senderNodeId is " + senderNodeId)
    //println("currentNodeName is " + currentNodeName)
    // println("Accessing character" + length)
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

  private def findMinimumLeafSet(key: BigInt, searchMinimum: Boolean, searchMaximum: Boolean): BigInt = {

    /*
    var rightMostOfLeft: Int = leafSetMinus.size - 1
    var leftMostOfRight: Int = 0
    var maxLeft = BigInt.apply(leafSetMinus(rightMostOfLeft), 16)
    var minRight = BigInt.apply(leafSetPlus(leftMostOfRight), 16)
    var output: BigInt = null
   var leftMostOfLeft: Int = 0
   var rightMostOfRight: Int = leafSetPlus.size - 1
   var minLeft = BigInt.apply(leafSetMinus(leftMostOfLeft), 16)
   var maxRight = BigInt.apply(leafSetPlus(rightMostOfRight), 16)
    */
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
    println("Inside findNearestNumericalLeaf")
    var leafSetSize: Int = leafSet.size - 1
    // println("leafSetSize is " + leafSetSize)
    var output: BigInt = null
    if(leafSetSize < 0) return output
    if(leafSetSize == 0 || leafSetSize == 1) return BigInt.apply(leafSet(leafSetSize), 16)
    for(i <- 0 to leafSetSize) {
      // println("Inside for")
      if(BigInt.apply(leafSet(i), 16) >= key ) {
        //println("Inside if of leafSet")
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
    // DR
    //println("entered send state")
    var i = 0
    var j = 0
    for(i <- 0 to RTrows-1) {
      for(i <- 0 to RTcols-1) {

        if ((routingTable(i)(j) != null) && (routingTable(i)(j) != self.path.name)) {
          // DR
          // println("Ting ting tidin")
          var node = context.actorSelection("../" + routingTable(i)(j))
          node ! newNodeState(self.path.name, routingTable)
        }
      }
    }
    for(i <- 0 to leafSetMinus.size-1) {

      if ((leafSetMinus(i) != null) && (leafSetMinus(i) != self.path.name)) {
        // DR
        //println("Ting ting tidin")
        var node = context.actorSelection("../" + leafSetMinus(i))
        node ! newNodeState(self.path.name, routingTable)
      }
    }
    for(i <- 0 to leafSetPlus.size-1) {

      if ((leafSetPlus(i) != null) && (leafSetPlus(i) != self.path.name)) {
        // DR
        //println("Ting ting tidin")
        var node = context.actorSelection("../" + leafSetPlus(i))
        node ! newNodeState(self.path.name, routingTable)
      }
    }

    // DR
    context.actorSelection("../") ! incrementActorcount()
    println("exit send state")
  }

  private def updateTablesAsPerNew(senderNodeId: String,  rTable: Array[Array[String]]) {

    var ownNode = BigInt.apply((self.path.name), 16) //BigInt values
    var updater = BigInt.apply(senderNodeId, 16)  // BigInt values
   // println("Inside updateTablesAsPerNew")
   // println("ownNode = "+self.path.name)
   // println("updater = "+senderNodeId)
   // println("sizes = "+leafSetMinus.size+" "+leafSetPlus.size)
    var end: Int = 0
    //If senderId falls in leafset
    //if left set
    if ((leafSetMinus.size == 0) &&  (updater<ownNode)) {
      leafSetMinus += senderNodeId
      //leafSetMinus.append(senderNodeId)
     // println("First nodeID:"+ senderNodeId+" added to leaf table minus of "+ self.path.name)
    }
    else if ((leafSetMinus.size > 0) && ((BigInt.apply(leafSetMinus(0), 16)) <  updater) && (ownNode >  updater)) {
      //println("Inside left leaf table else loop: ")


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
      //println("Inside right leaf table else loop: ")
      if(!(leafSetPlus.contains(senderNodeId))) {

        leafSetPlus += senderNodeId
        leafSetPlus.sortWith(compfn1)
        if(leafSetPlus.size >= ideal_leafSetSize) {
          leafSetPlus.reduceToSize(ideal_leafSetSize)
        }
      }
    }

    //println("New leaflet sizes = "+leafSetMinus.size+" "+leafSetPlus.size)

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
