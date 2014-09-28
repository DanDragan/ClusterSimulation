"""
    This module represents a cluster's computational node.

    Computer Systems Architecture Course
    Assignment 1 - Cluster Activity Simulation
    March 2014
"""
from threading import *
from Queue import *
from barrier import *

class Node:
    """
        Class that represents a cluster node with computation and storage
        functionalities.
    """

    def __init__(self, node_id, matrix_size):
        """
            Constructor.

            @type node_id: Integer
            @param node_id: an integer less than 'matrix_size' uniquely
                identifying the node
            @type matrix_size: Integer
            @param matrix_size: the size of the matrix A
        """

        self.barrier = None
        self.node_id = node_id
        self.matrix_size = matrix_size
        self.datastore = None
        self.nodes = None
        self.thread = None
        self.max_requests = None
        self.semaphore = None
        self.event = Event()
        self.queue = None
        self.tlist = []


    def __str__(self):
        """
            Pretty prints this node.

            @rtype: String
            @return: a string containing this node's id
        """
        return "Node %d" % self.node_id


    def set_datastore(self, datastore):
        """
            Gives the node a reference to its datastore. Guaranteed to be called
            before the first call to 'get_x'.

            @type datastore: Datastore
            @param datastore: the datastore associated with this node
        """
        self.datastore = datastore
        self.max_requests = self.datastore.get_max_pending_requests()
        if self.max_requests == 0:
            self.max_requests = self.matrix_size
        self.semaphore = Semaphore(self.max_requests)
        self.queue = Queue(self.max_requests)


    def set_nodes(self, nodes):
        """
            Informs the current node of the other nodes in the cluster. 
            Guaranteed to be called before the first call to 'get_x'.

            @type nodes: List of Node
            @param nodes: a list containing all the nodes in the cluster
        """
        self.nodes = nodes
        if self.node_id == 0:
            self.barrier = Barrier(self.matrix_size)
        for i in range(1, self.matrix_size):
            self.nodes[i].barrier = self.barrier


    def get_x(self):
        """
            Computes the x value corresponding to this node. This method is
            invoked by the tester. This method must block until the result is
            available.

            @rtype: (Float, Integer)
            @return: the x value and the index of this variable in the solution
                vector
        """
      
        self.thread = NodeThread(self)
        self.thread.start()
        self.thread.join()

        return (self.thread.get_result(), self.node_id)


    def shutdown(self):
        """
            Instructs the node to shutdown (terminate all threads). This method
            is invoked by the tester. This method must block until all the
            threads started by this node terminate.
        """
        for thread in self.tlist:
            thread.join()
            del thread
        self.queue.put(None)
        del self.thread


class NodeThread(Thread):
    """
        Clasa folosita pentru a implementa thread-ul de management pentru
        fiecare nod al clusterului
    """
    def __init__(self, node):
        Thread.__init__(self)
        self.node = node
        self.result = None
        self.node.datastore.register_thread(self.node, self)

        
    def triangulateMatrix(self):
        """
            Functie pentru aducerea matricii la forma superior triunghiulara
        """
        for iteration in range(0, self.node.matrix_size - 1):
            "Se asteapta ca toate threadurile sa ajunga aici"
            self.node.barrier.wait()
            if self.node.node_id > iteration:
                result1 = self.node.datastore.get_A(self.node, iteration)
                
                self.node.nodes[iteration].semaphore.acquire()
                thread = AccessThread(self.node, self.node.nodes[iteration], "get_A", iteration)
                self.node.tlist.append(thread)
                thread.start()
                result2 = self.node.queue.get()
                self.node.nodes[iteration].semaphore.release()

                multiplier = result1 / result2
                
                self.node.datastore.put_A(self.node, iteration, multiplier)

                for col in range(iteration + 1, self.node.matrix_size):
                    result1 = self.node.datastore.get_A(self.node, col)

                    self.node.nodes[iteration].semaphore.acquire()
                    thread = AccessThread(self.node, self.node.nodes[iteration], "get_A", col)
                    self.node.tlist.append(thread)
                    thread.start()
                    result2 = self.node.queue.get()
                    self.node.nodes[iteration].semaphore.release()

                    res = result1-multiplier*result2
                    self.node.datastore.put_A(self.node, col, res)

                result1 = self.node.datastore.get_b(self.node)

                self.node.nodes[iteration].semaphore.acquire()
                thread = AccessThread(self.node, self.node.nodes[iteration], "get_b")
                self.node.tlist.append(thread)
                thread.start()
                result2 = self.node.queue.get()
                self.node.nodes[iteration].semaphore.release()

                res = result1-multiplier*result2
                self.node.datastore.put_b(self.node, res)

    def compute_x(self):
        """
            Functie ce rezolva un sistem de ecuatii superior triunghiular
        """

        """
            Executia va incepe cu ultimul nod si se va
           continua in ordine descrescatoare a node_id-ului
        """

        if self.node.node_id == self.node.matrix_size -1:
            self.node.event.set()
        else:
            self.node.nodes[self.node.node_id+1].event.wait()
            self.node.nodes[self.node.node_id+1].event.clear()
            self.node.nodes[self.node.node_id].event.set()
        
        k = self.node.matrix_size-1
        self.node.nodes[k].semaphore.acquire()
        thread = AccessThread(self.node, self.node.nodes[k], "get_b")
        self.node.tlist.append(thread)
        thread.start()
        result1 = self.node.queue.get()
        self.node.nodes[k].semaphore.release()

        self.node.nodes[k].semaphore.acquire()
        thread = AccessThread(self.node, self.node.nodes[k], "get_A", k)
        self.node.tlist.append(thread)
        thread.start()
        result2 = self.node.queue.get()
        self.node.nodes[k].semaphore.release()

        x = [0 for _ in range(k+1)]
        x[k] = result1 / result2

        while k >= self.node.node_id:
            self.node.nodes[k].semaphore.acquire()
            thread = AccessThread(self.node, self.node.nodes[k], "get_b")
            self.node.tlist.append(thread)
            thread.start()
            result1 = self.node.queue.get()
            self.node.nodes[k].semaphore.release()

            self.node.nodes[k].semaphore.acquire()
            thread = AccessThread(self.node, self.node.nodes[k], "get_A", k)
            self.node.tlist.append(thread)
            thread.start()
            result2 = self.node.queue.get()
            self.node.nodes[k].semaphore.release()
            
            sum = 0
            for j in range(k+1, self.node.matrix_size):
                self.node.nodes[k].semaphore.acquire()
                thread = AccessThread(self.node, self.node.nodes[k], "get_A", j)
                self.node.tlist.append(thread)
                thread.start()
                result3 = self.node.queue.get()
                self.node.nodes[k].semaphore.release()

                sum += result3 * x[j]

            x[k] = (result1 - sum) / result2
            k -= 1
        
            self.result = x[self.node.node_id]

    def run(self):
        """
            Executia algoritmului de eliminare gausiana
        """
        self.triangulateMatrix()
        self.compute_x()


    def get_result(self):
        return self.result

class AccessThread(Thread):
    """
        Clasa ce abstractizeaza obiecte de tip thread de acces, folosite
        doar pentru adaugarea in coada nodului apelant a unui rezultat
        din baza de date a altui nod
    """
    def __init__(self, caller, node, operation, column = None, value = None):
        Thread.__init__(self)
        self.node = node
        self.operation = operation
        self.column = column
        self.value = value
        self.result = None
        self.caller = caller

    def run(self):
        """
            Intoarce un rezultat din baza de date a nodului apelat
            si il pune in coada nodului apelant
        """
        self.node.datastore.register_thread(self.node, self)
        if self.operation == "get_b":
            self.result = self.get_b()
            self.caller.queue.put(self.result)
        elif self.operation == "get_A":
            self.result = self.get_A(self.column)
            self.caller.queue.put(self.result)

    def get_b(self):
        b = self.node.datastore.get_b(self.node)
        return b

    def get_A(self, column):
        A = self.node.datastore.get_A(self.node, column)
        return A