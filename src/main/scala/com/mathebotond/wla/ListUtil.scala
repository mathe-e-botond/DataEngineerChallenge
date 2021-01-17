package com.mathebotond.wla

object ListUtil {
  def mergeOrdered[A](i: List[A], j: List[A], ordering: Ordering[A]): List[A] = {
    (i, j) match {
      case (Nil, Nil) => Nil
      case (_ :: _, Nil) => i
      case (Nil, _ :: _) => j
      case (x :: _, y :: _) => {
        if (ordering.lteq(x, y))
          x :: mergeOrdered(i.tail, j, ordering)
        else
          y :: mergeOrdered(i, j.tail, ordering)
      }
    }
  }
}
