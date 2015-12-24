import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorRef
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.Array._
import scala.collection.mutable.HashMap
import scala.concurrent.impl.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Await

object GossipSimulator {

  val gossip = "gossip"
  val pushsum = "push-sum"
  val line = "line"
  val threeD = "3d"
  val full = "full"
  val imp3D = "imp3d"

  sealed trait Seal
  case class setNeighbours(neighs: Array[Int]) extends Seal
  case class pushsumMsg(s: Double, w: Double) extends Seal
  case class gossipMsg(rumour: String) extends Seal
  case class startPush() extends Seal
  case class gossipMsgBroadcast() extends Seal
  case class initWeightNSum(s: Double, w: Double) extends Seal

  var actors = new ArrayBuffer[ActorRef]()
  var convergedActors = new ArrayBuffer[ActorRef]()
  val actorsMap = new HashMap[String, String]
  var convergedActorsCount = 0
  var coOrdinates: Map[Int, Array[Int]] = Map();

  val actorNamePrefix = "storm"
  val defaultSum = 10.0
  val maxGossipCount = 10
  var randActorIndex = 17
  var startTime = System.currentTimeMillis()
  var lastConvergedTime = System.currentTimeMillis()
  var atleastOneConverged = false
  var isTerminated = false

  val printName = "printName"
  val printNeighbours = "printNeighbours"
  var sumEstConvRatio = 0.0000000001

  var lastTimeStamp = System.currentTimeMillis()
  var curTimeStamp = System.currentTimeMillis()

  def main(args: Array[String]): Unit = {

    var numNodes = 15625
    var topology = line
    topology = full
    //topology = threeD
    //topology = imp3D
    var algo = gossip
    algo = pushsum

    if (args.length > 0) {
      numNodes = args(0).toInt
      topology = args(1)
      algo = args(2)
    }

    if (topology.equals(threeD) || topology.equals(imp3D)) {
      //finding nearest cube-root
      numNodes = (math.pow(math.ceil(math.pow(numNodes.toDouble, 1.0 / 3.0)).toInt, 3)).toInt
    }
    start(topology, algo, numNodes)
  }

  def start(topology: String, algo: String, numNodes: Int): Unit = {
    val system = ActorSystem("ActorSys")
    val namingPrefix: String = "akka://ActorSys/"

    var fullNetwork = range(0, numNodes - 1).toBuffer

    for (i <- 0 to numNodes - 1) {
      var actor = system.actorOf(Props(new ActorNode(namingPrefix, algo, topology)), actorNamePrefix + i);

      if (topology.equals(line)) {

        var neighs = new Array[Int](2)
        neighs = if (i == 0) Array(1) else if (i == numNodes - 1) Array(i - 1) else Array(i - 1, i + 1)
        actor ! setNeighbours(neighs)

      } else if (topology.equals(full)) {

        var neighs = { if (i == 0) range(1, numNodes) else if (i == numNodes - 1) range(0, numNodes - 1) else range(0, i) ++ range(i + 1, numNodes) }
        actor ! setNeighbours(neighs)

      } else if (topology.equals(threeD)) {
        var neighs = getNeighboursIn3d(numNodes, i)
        actor ! setNeighbours(neighs.toArray)

      } else if (topology.equals(imp3D)) {

        var neighs = getNeighboursIn3d(numNodes, i)
        neighs.append(getRandomNeighbour(neighs, numNodes))
        actor ! setNeighbours(neighs.toArray)

      }
      actors += actor
    }
    printf("Build topology time taken : " + (System.currentTimeMillis() - startTime))
    startTime = System.currentTimeMillis();

    var actorRef = actors(randActorIndex)
    println(">>>>" + actorRef.path.name)
    actorRef ! printNeighbours

    if (algo.equals(gossip)) {
      println("\nStarting gossip\n")
      actorRef ! gossipMsg("Fire in the hole!!")
    } else {
      println("\nStarting push sum\n")
      actorRef ! startPush()
    }
    Thread.sleep(100)
    printf(actorRef.path.name)
    //system.stop(actorRef)

    var monitor = new Thread {
      override def run() {
        while (!isTerminated) {
          printf("Monitor is running : " + convergedActorsCount)
          if (convergedActorsCount > 0) {
            if (math.abs(System.currentTimeMillis() - curTimeStamp) > 1000) {
              println("\n#$%@#$% Actors converged : " + convergedActorsCount + "/" + actors.size)
              println("\n$$%%Time taken for protocol to converge : " + (curTimeStamp - startTime))
              //println(actorsMap.toSeq.sorted.toString())
              system.shutdown()
              isTerminated = true;
              System.exit(0)
            }
          }
          Thread.sleep(5000)
        }
      }
    }
    if (algo.equals(gossip)) {
      monitor.start()
    }
  }

  def getRandomNeighbour(neighs: ArrayBuffer[Int], numNodes: Int): Int = {
    var x = Random.nextInt(numNodes)
    while (neighs.contains(x)) x = Random.nextInt(numNodes)
    x
  }

  def getNeighboursIn3d(numNodes: Int, value: Int): ArrayBuffer[Int] = {

    var n = math.ceil(math.pow(numNodes.toDouble, 1.0 / 3.0)).toInt
    var A = construct3DGrid(n)
    var index = coOrdinates.get(value);
    var i = index.get(0)
    var j = index.get(1)
    var k = index.get(2)

    var neighs = new ArrayBuffer[Int]()

    if (i + 1 < n) {
      neighs.append(A(i + 1)(j)(k));
    }
    if (i - 1 > -1) {
      neighs.append(A(i - 1)(j)(k));
    }
    if (j + 1 < n) {
      neighs.append(A(i)(j + 1)(k));
    }
    if (j - 1 > -1) {
      neighs.append(A(i)(j - 1)(k));
    }
    if (k + 1 < n) {
      neighs.append(A(i)(j)(k + 1));
    }
    if (k - 1 > -1) {
      neighs.append(A(i)(j)(k - 1));
    }
    neighs
  }

  def construct3DGrid(n: Int): Array[Array[Array[Int]]] = {

    var A = Array.ofDim[Int](n, n, n);

    var count = 0;
    for (i <- 0 to n - 1) {
      for (j <- 0 to n - 1) {
        for (k <- 0 to n - 1) {
          A(i)(j)(k) = count
          var arr = Array(i, j, k)
          coOrdinates += (count -> arr)
          count = count + 1
        }
      }
    }
    A
  }
  class ActorNode(namingPrefix: String, algo: String, topology: String) extends Actor {

    var alphaNeighs = Array[Int]()
    var currentIndex = 0
    var gossipCount = 0
    var rumour = ""
    var sum = 0.0
    var weight = 1.0

    var lastSumEstimate = 0.0
    var consecutiveDiffCounter = 0

    var isConverged = false

    def receive = {
      case n: setNeighbours => {
        alphaNeighs = n.neighs
        //println("Me " + self.path.name + " neighbours : " + alphaNeighs.toBuffer)
        actorsMap += (self.path.name -> 0.toString())
        var y = self.path.name.split(actorNamePrefix)
        sum = defaultSum + y(y.length - 1).toDouble
      }

      case r: gossipMsg => {
        if (!isConverged) {
          curTimeStamp = System.currentTimeMillis()
          if (gossipCount == 0) {
            self ! gossipMsgBroadcast()
          }
          gossipCount += 1
          if (gossipCount <= maxGossipCount) {
            // printf("\n Received message %s for the count : %d from : %s ", self.path.name, gossipCount, sender.path.name)

            actorsMap(self.path.name) = gossipCount.toString()
            rumour = r.rumour

            // var neighbour = actors(alphaNeighs(currentIndex % alphaNeighs.length))
            var x = Random.nextInt(alphaNeighs.length)
            var neighbour = actors(alphaNeighs(x))
            currentIndex = currentIndex + 1;
            neighbour ! gossipMsg(rumour)
          } else {
            isConverged = true
            convergedActorsCount += 1
            //printf("\nGossip Converged message %s for the count : %d", self.path.name, gossipCount)
            //printf("\nConverged act count %d for actors count : %d", convergedActorsCount, actors.length)
            lastConvergedTime = System.currentTimeMillis()
          }
        }
      }
      case rb: gossipMsgBroadcast => {
        if (!isConverged) {
          var x = Random.nextInt(alphaNeighs.length)
          var neighbour = actors(alphaNeighs(x))
          currentIndex = currentIndex + 1;
          neighbour ! gossipMsg(rumour)
          var f = Future {
            Thread.sleep(1)
          }
          f.onComplete { case x => self.tell(gossipMsgBroadcast(), self) }
        }
      }

      case sp: startPush => {

        var neighbour = actors(alphaNeighs(Random.nextInt(alphaNeighs.length)))
        // half of s and w is kept by the sending actor
        sum = sum / 2
        weight = weight / 2

        neighbour ! pushsumMsg(sum, weight)

        if (!topology.equals(line)) {
          if (!isConverged) {
            var f = Future {
              Thread.sleep(1)
            }
            f.onComplete { case x => self.tell(startPush(), self) }
          }
        }

      }

      case p: pushsumMsg => {

        if (!isConverged) {
          // println(self.path.name + "Received (s,w) = (" + p.s + "," + p.w + ") from " + sender.path.name)
          // add received pair to own corresponding values 
          sum += p.s
          weight += p.w

          var ratio = sum / weight;
          if (math.abs(ratio - lastSumEstimate) <= sumEstConvRatio) {
            consecutiveDiffCounter += 1
            if (consecutiveDiffCounter >= 3 && (!isConverged)) {
              isConverged = true
              if (!isTerminated) {
                isTerminated = true
                convergedActorsCount += 1
                // println("Push sum converged first at" + self.path.name)
                lastConvergedTime = System.currentTimeMillis()
                println(actorsMap.toSeq.sorted.toString())
                println("\n$$%%Time taken for protocol to converge : " + (lastConvergedTime - startTime))
                context.system.shutdown()
              }
            }
          } else {
            consecutiveDiffCounter = 0
          }

          // half of s and w is kept by the sending actor
          sum = sum / 2
          weight = weight / 2

          actorsMap(self.path.name) = sum + " , " + weight + " , " + lastSumEstimate
          // This will be referred next time
          lastSumEstimate = sum / weight

          // Half is placed in the message. 
          var neighbour = actors(alphaNeighs(Random.nextInt(alphaNeighs.length)))
          neighbour ! pushsumMsg(sum, weight)
        } else {

          isConverged = true
          convergedActorsCount += 1
          //printf("\nPush Sum Converged %s for the count : %d", self.path.name, lastSumEstimate)
          //printf("\nConverged act count %d for actors count : %d", convergedActorsCount, actors.length)
          lastConvergedTime = System.currentTimeMillis()
        }

      }

      case x: String => {
        if (x.equals(printName)) {
          //print("##" + self.path.name)
        } else if (x.equals(printNeighbours)) {
          //println("\nMy name : " + self.path.name)
          //print("and my neighbours are ")
          for (i <- 0 to alphaNeighs.length - 1) {
            val p = context.actorSelection(namingPrefix + "user/" + actorNamePrefix + alphaNeighs(i));
            p.tell(printName, self)
          }
        }
      }
    }
  }
}