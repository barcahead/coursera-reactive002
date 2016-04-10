package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._
import scala.math._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: Int, b: Int) =>
    val h = insert(a, empty)
    findMin(insert(b, h)) == min(a, b)
  }

  property("del") = forAll { a: Int =>
    val h = insert(a, empty)
    isEmpty(deleteMin(h)) == true
  }

  property("sort") = forAll { h: H =>
    def isSorted(list: List[Int]): Boolean = list match {
      case Nil => true
      case x :: Nil => true
      case x :: xs => x <= xs.head && isSorted(xs)
    }

    isSorted(h2s(h)) == true
  }

  property("meld") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, h2)) == min(findMin(h1), findMin(h2))
  }

  property("meld1") = forAll { (list: List[Int]) =>
    def addAll(list: List[Int]): H = list match {
      case Nil => empty
      case x :: xs => insert(x, addAll(xs))
    }

    h2s(addAll(list)) == list.sorted
  }

  def h2s(h: H): List[Int] = {
    if (isEmpty(h)) List[Int]()
    else {
      findMin(h) +: h2s(deleteMin(h))
    }
  }


  property("duplicated") = forAll { (a: Int) =>
    val h = insert(a, insert(a, empty))

    findMin(deleteMin(h)) == a
  }

  lazy val genHeap: Gen[H] = for {
    v <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(v, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)


}
