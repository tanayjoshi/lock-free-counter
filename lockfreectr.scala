import java.util.concurrent.atomic.AtomicReferenceArray 
import java.util.concurrent.atomic.AtomicReference 
import java.util.concurrent.atomic.AtomicInteger
import ox.cads.util.ThreadID
import ox.cads.util.ThreadUtil

object lockfreectr {
    abstract class Iterator {
//A counter for the iterator. Since updates have to be atomic, it is an AtomicInteger
        var from: AtomicInteger
//Function to return the current value (either Some(from), or Some(items(pos))/None, depending on the type of iterator)        
        def cur(): Option[Int]
//Function to return the last value. Used in the merge iterator to coordinate CAS updates among different threads.
        def past(): Option[Int]
//Optionally get and remove the first element of the sequence. Returns None if the sequence is empty
        def next(): Option[Int]
    }

//A binary tree class, for the difficult bit. It just helps in creating a binary tree. It takes an array of Iterators, and creates a tree and returns the root.
    class BT    {
        def create(iter: Array[_ <: Iterator]): Iterator = {
            var size = iter.length
            var inter = new scala.collection.mutable.Queue[Iterator]()
            var i = 0
            while (i<size) {
                inter.enqueue(new MergeIterator(iter(i),iter(i+1)))
                i+=2
            }
            while(inter.size!=1)    {
                inter.enqueue(new MergeIterator(inter.dequeue, inter.dequeue))
            }
            return inter.dequeue    
        }
    }
        
//The MergeIterator class takes in two iterators and outputs the correct next value. We have two atomic pairs of (Option[Int], Int), for the two iterators. They keep a track of the past value, and the thread using that pair (details shown later); initialized with the value of -1, to show that no thread is using it initially. The functions cur and past call respective functions in iter0/iter1. The atomic integer, from, serves the purpose of keeping a track of which iterator was last used to obtain the latest value. Based on that, cur and past are called.

    class MergeIterator(var iter0: Iterator, var iter1: Iterator) extends Iterator  {
        var pair0 = new ox.cads.atomic.AtomicPair[Option[Int], Int](iter0.past,-1)
        var pair1 = new ox.cads.atomic.AtomicPair[Option[Int], Int](iter1.past,-1)
        var from = new AtomicInteger(-1)

        def cur(): Option[Int] = {
            if (from.get==(-1))   None
            else if (from.get==0)   return iter0.cur
            else iter1.cur
        }
        def past(): Option[Int] = {
            if (from.get==(-1))   None
            else if (from.get==0)   return iter0.past
            else iter1.past
        }

//This function returns whether the first or the second element should come first. If one of the iterator is returning None, the other one is triggered.
        def minPos(x: Option[Int], y: Option[Int]): Int = {
            (x,y) match {
                case (Some(a),Some(b)) => if (a<b) return 0 else 1
                case (Some(a),None) => return 0
                case _ => return 1    
                }
        }

//The next function returns the next element to be returned.
        def next(): Option[Int] = {
            while(true) {
                var pos = minPos(iter0.cur(),iter1.cur())
//After finding which iterator to trigger, we use the first CAS to check that no other thread has already triggered the iterator corresponding to this value. This is done by comparing the corresponding pair (pair0 for iter0, pair1 for iter1) for the past value stored, and then updating that to the current value, along with the thread number. This signifies that the job of updating the current value (by invoking next) is of that thread. The second CAS initializes the pair to the current value, and initializes the thread number to -1. This makes sure that the iterator is ready for incrementing on the next round, as on the next round, some thread will find the past value of next round (which is equal to the current value of this round), and take the responsibility of updating the current value of the next round (or the next value of the current round). Only one thread with the correct thread ID passes through the second CAS, after which it returns iter.next. The linearization point is the return statement, iter0.next or iter1.next, depending on which iterator is called. [While minPos returns the correct iterator to trigger, if both of them are out of bounds, it will return 1, but the underlying iterator will itself return None, so this will work for that as well.]
                if (pos==0)  {
                    var result = iter0.from.get 
                    (pair0.compareAndSet((iter0.past,-1), (iter0.cur, ThreadID.get)))
                    if (pair0.compareAndSet((iter0.cur,ThreadID.get),(iter0.cur,-1)))    {
                        (from.set(0))
                        return (iter0.next())
                    }
                }
                else  {
                    var result = iter1.from.get 
                    (pair1.compareAndSet((iter1.past,-1), (iter1.cur, ThreadID.get)))
                    if (pair1.compareAndSet((iter1.cur,ThreadID.get),(iter1.cur,-1)))    {
                        (from.set(1))
                        return (iter1.next())
                    }
                }
            }
            return None
        }
    }

//We have added functions, cur and past. cur returns the current value, while past returns the last returned value.

    class FromIterator(start: Int) extends Iterator {
        var from = new AtomicInteger(start)
        
        def cur(): Option[Int] = {
            return Some(from.get)
        }

        def past(): Option[Int] = {
            if (from.get>0)
                return Some(from.get-1)
            return None
        }
        def next(): Option[Int] = {
            return Some(from.getAndIncrement)
        }
    }

//We have similar added functions. cur returns the current item, and past returns the last item returned.

    class ArrayIterator(items: Array[Int]) extends Iterator {
        var size = items.length
        var from = new AtomicInteger(0)
        @volatile var pos: Int = -1;

        def cur(): Option[Int] = {
            if (from.get>=0 && from.get<size) return Some(items(from.get))
            return None
        }

        def past(): Option[Int] = {
            if (from.get>0 && from.get<=size)
                return Some(items(from.get-1))
            else return None
        }

        def next(): Option[Int] = {
            pos = from.getAndIncrement
            if (pos<size)   {
                return Some(items(pos))
            }
            return None
        }
    }

//A small testing script. We essentially create a FromIterator, and an ArrayIterator, and then merge them using the MergeIterator.[Note: We also output the timings of the return, as the print statements are often displayed out of order, but on checking the times of individual elements, it can be found that they are in order.]
//A second test for the binary tree of iterators is also carried out. We create four FromIterators (all starting from different positions), and use the create method of the class BT, which creates a tree and returns the root node (root MergeIterator).
//Some sample outputs are shown. It is interesting to note in the binary tree test that an iterator is not triggered (i.e. next on that iterator is not called) unless that is required. This is checked by separately calling iterator b2 (FromIterator(17)) afterwards. Since we ran the test for only a few values, this iterator shouldn’t be triggered, as we don’t reach a value up till 17. This is confirmed when we call it later, outside of our original loop. It starts its output from 17 itself, like it normally would (confirming that it hasn’t been called before). The code, thus, runs in time proportional to the depth of the tree, i.e. only one iterator’s next is called at a time.

    def main(args:Array[String]) = {
        var b = new FromIterator(10)
        var list = Array(1,2,3,4,5,6,7,8,9,10,11,12,13) 
        var d = new ArrayIterator(list)
        var c = new MergeIterator(b,d)
        var limit = 20

        var a1 = new FromIterator(1) 
        var a2 = new FromIterator(3) 
        var b1 = new FromIterator(5) 
        var b2 = new FromIterator(17) 
        var in = Array(a1,a2,b1,b2) 
        var bt = new BT
        var d1 = bt.create(in)
        
        //Single MergeIterator Test
        ThreadUtil.runSystem(size, while(i<limit) { (println(c.next()+” Time: “+(java.lang.System.nanoTime%10000000).toString)); i+=1})
        
        /************************************************** 
        Output:
        Some(1) Time: 7075421
        Some(2) Time: 7075850
        Some(4) Time: 7083595 Some(3) Time: 7079363
        Some(6) Time: 8299617
        Some(7) Time: 8326116
        Some(8) Time: 8373021
        10
        Some(9)
        Some(5)
        Some(10)
        Some(10)
        Some(11)
        Some(11)
        Some(12)
        .
        Time: 8419221 Time: 8288065
        Time: 8468789 Time: 8578685 Time: 8597129 Time: 8653600 Time: 8747569
        .
        . 
        *****************************************************/ 

        //Binary Tree Test
        i=0
        ThreadUtil.runSystem(size, while(i<limit) { 
            (println(d1.next()+” “+(java.lang.System.nanoTime%10000000).toString)); i+=1
            })
        println() 
        println("B2.next()") 
        while(start<limit) {
            println(b2.next())
            start += 1 
        }


        /*********************************************************** 
        Output:
        Some(1) Time: 955132
        Some(3) Time: 1019715
        Some(2) Time: 968275
        Some(3) Time: 978465
        Some(4) Time: 2350548
        Some(4) Time: 2351857
        Some(5) Time: 2432562
        Some(6) Time: 2475313
        Some(5) Time: 2412748 
        Some(7) Time: 2635235 
        Some(6) Time: 2533437 
        Some(8) Time: 2712676 
        Some(8) Time: 2764334 
        .
        .
        .
        B2.next()
        Some(17)
        Some(18)
        Some(19)
        .
        .
        . 
        **********************************************************/

    }  
}
