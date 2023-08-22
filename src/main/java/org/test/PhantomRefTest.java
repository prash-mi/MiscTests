package org.example;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * This Program shows atomically clearing resources help by phantom references using the following logic:
 *        1> Starts a `DemonPoller` Thread which will poll the ReferenceQueue
 *        2> Create ResultSet and emulate that the user isn't actually iterating it. That is they do not have a Hard Reference pointing to the created resultset for testing purpose
 *        3> Call GC and wait for it to happen
 *        4> We then get ResultSetFinalizer's reference using the referenceQueue.poll() and this is used to interrupt the threads
 *
 *        Note:
 *                1> Since PhantomReference.get() returns null (As per specs: "the referent of a phantom reference may not be retrieved: The get method of a phantom reference always returns null.
 *                      " https://docs.oracle.com/javase/8/docs/api/java/lang/ref/PhantomReference.html), hence ResultSetFinalizer has been shared the references of the threads owned by the corresponding ResultSet,
 *                      so that they can be interrupted
 *                2> During implementation we must make sure that no hard references of ResultSets is be maintained by this implementation
 *
 *
 *=======================================================================================================
 * Program's Raw output (Shows the threads owned by ResultSets being interrupted using PhantomRefs):
 *=======================================================================================================
 *
 * Creating 5 ResultSets
 * Demon running
 * RPC Thread Thread-1 is Running ...
 * RPC Thread Thread-5 is Running ...
 * RPC Thread Thread-4 is Running ...
 * RPC Thread Thread-3 is Running ...
 *
 * Calling System.gc()
 *
 * RPC Thread Thread-2 is Running ...
 * Demon running
 * RPC Thread Thread-1 is Running ...
 * RPC Thread Thread-4 is Running ...
 * RPC Thread Thread-5 is Running ...
 * RPC Thread Thread-2 is Running ...
 * RPC Thread Thread-3 is Running ...
 * reference.isEnqueued(): false
 * reference.isEnqueued(): false
 * reference.isEnqueued(): false
 * reference.isEnqueued(): false
 * reference.isEnqueued(): false
 * Demon running
 * RPC Thread Thread-1 is Running ...
 * RPC Thread Thread-4 is Running ...
 * RPC Thread Thread-3 is Running ...
 * RPC Thread Thread-5 is Running ...
 * RPC Thread Thread-2 is Running ...
 * reference.isEnqueued(): false
 * reference.isEnqueued(): true
 * reference.isEnqueued(): true
 * reference.isEnqueued(): true
 * reference.isEnqueued(): true
 * Clearing Task...
 * Thread Thread-5 interrupted
 * Interrupted Thread-5
 * Clearing Task...
 * Interrupted Thread-4
 * Clearing Task...
 * Thread Thread-4 interrupted
 * Thread Thread-3 interrupted
 * Interrupted Thread-3
 * Clearing Task...
 * Interrupted Thread-2
 * Clearing Task...
 * Interrupted Thread-1
 * Thread Thread-2 interrupted
 * Thread Thread-1 interrupted
 * Demon running
 * reference.isEnqueued(): false
 * reference.isEnqueued(): false
 * reference.isEnqueued(): false
 * reference.isEnqueued(): false
 * reference.isEnqueued(): false
 *
 * Process finished with exit code 0
 */

public class PhantomRefTest {//This logic will be at the statement layer (or will be triggered from the statement layer)

  ReferenceQueue<ResultSetImpl> referenceQueue = new ReferenceQueue<>();
  List<ResultSetFinalizer> resultSetFinalizers = new ArrayList<>();

  //List<ResultSetImpl> resultSetList = new ArrayList<>();//Imp Note: Keeping any hard reference will prevent GC. No hard references of ResultSets should be maintained

  PhantomRefTest(){
    new DemonPoller().start();
  }
  public static void main(String[] args) throws InterruptedException {

    PhantomRefTest test = new PhantomRefTest();

    System.out.println("Creating 5 ResultSets");
    test.createResultSets();//Create ResultSet and emulate that the user isn't actually processing it
    System.out.println("\nCalling System.gc()\n");
    Thread.sleep(2000);//Give time to RPC threads to start
    System.gc();

    Thread.sleep(2000);//give time for GC
  }

  void createResultSets(){
    for (int i = 0; i < 5; ++i) {
      ResultSetImpl resultSet = new ResultSetImpl();
      Set<Thread> ownedThreads = resultSet.getOwnedThreads();
      //  resultSetList.add(resultSet); : Imp Note: Keeping any hard reference will prevent GC. No hard references of ResultSets should be maintained
      resultSetFinalizers.add(new ResultSetFinalizer(resultSet, referenceQueue, ownedThreads));
    }
  }

  class DemonPoller extends Thread{
    DemonPoller(){
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true){
        System.out.println("Demon running");
        for (PhantomReference<ResultSetImpl> reference : resultSetFinalizers) {
          System.out.println("reference.isEnqueued(): "+reference.isEnqueued());//tells if the reference has been enqueued by the GC
        }

        Reference<?> referenceFromQueue;
        while ((referenceFromQueue = referenceQueue.poll()) != null) {
          ((ResultSetFinalizer)referenceFromQueue).finalizeResources();
          referenceFromQueue.clear();
        }

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  class ResultSetFinalizer extends PhantomReference<ResultSetImpl> {
    Set<Thread> ownedThreads;
    public ResultSetFinalizer(
        ResultSetImpl referent, ReferenceQueue<? super ResultSetImpl> q,  Set<Thread> ownedThreads) {
      super(referent, q);
      this.ownedThreads = ownedThreads;
    }


    public void finalizeResources() { // free resources. Remove all the hard refs
      System.out.println("Clearing Task...");
      for (Thread t :ownedThreads){
        if ((t.isAlive())){
          t.interrupt();
          System.out.println("Interrupted "+t.getName());
        }
      }
    }
  }

  class ResultSetImpl implements AutoCloseable {//this owns or has reference to the underlying threads. Practically it will implement java.sql.ResultSet
    boolean isClosed = false;
    Runnable rpcRunnable =
        () -> {
          while (true){
            System.out.printf("RPC Thread %s is Running ...\n", Thread.currentThread().getName());
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              System.out.printf("Thread %s interrupted\n", Thread.currentThread().getName());
              break;
            }
          }
        };
    Thread rpcTask = new Thread(rpcRunnable);

    Set<Thread> getOwnedThreads(){
      return Set.of(rpcTask);
    }
    ResultSetImpl(){
      rpcTask.setDaemon(true);
      rpcTask.start();
    }

    @Override
    public void close() throws Exception {//clear the resources
      isClosed =true;

      if ((rpcTask.isAlive())){
        rpcTask.interrupt();
        System.out.println("Interrupted "+rpcTask.getName());
      }

      System.out.println("Closing RS");
    }
  }

}
