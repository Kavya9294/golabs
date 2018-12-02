package mapreduce

import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

var done chan struct{}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Workers = make(map[string]*WorkerInfo)
	mr.freeWorkers = make(chan *WorkerInfo)
	mapworkNotDone := make(chan *DoJobArgs, mr.nMap)
	mapworkDone := make(chan struct{}, mr.nMap)
	reduceworkNotDone := make(chan *DoJobArgs, mr.nReduce)
	reduceworkDone := make(chan struct{}, mr.nReduce)
	done = make(chan struct{})

	log.Print("Length of nWorkers: ", mr.nWorkers)
	log.Print("num of maps: ", mr.nMap)

	go mr.RegisterWorker()

	for i := 0; i < mr.nMap; i++ {
		args := &DoJobArgs{
			File:          mr.file,
			NumOtherPhase: mr.nReduce,
		}
		args.File = mr.file
		args.NumOtherPhase = mr.nReduce
		var job JobType = "Map"
		args.Operation = job
		args.JobNumber = i
		var reply DoJobReply
		fmt.Printf("In call map: %d \n", i)
		go AssignWorker(args, reply, mr.nMap, mr, mapworkNotDone, mapworkDone)
	}

	for workMapDone := 0; workMapDone < mr.nMap; {
		fmt.Println("Waiting for mapwork to get done: ", workMapDone)
		select {
		case <-mapworkDone:
			fmt.Println("In map work done")
			workMapDone++
		case w := <-mapworkNotDone:
			var reply DoJobReply
			fmt.Println("in worknotDone Map")
			go AssignWorker(w, reply, mr.nMap, mr, mapworkNotDone, mapworkDone)
		}
	}

	for i := 0; i < mr.nReduce; i++ {
		args := &DoJobArgs{}
		args.File = mr.file
		args.NumOtherPhase = mr.nMap
		var job JobType = "Reduce"
		args.Operation = job
		args.JobNumber = i
		var reply DoJobReply
		fmt.Printf("In call reduce: %d \n", i)
		go AssignWorker(args, reply, mr.nReduce, mr, reduceworkNotDone, reduceworkDone)
	}
	for workDone := 0; workDone < mr.nReduce; {
		fmt.Println("Waiting for reducework to get done: ", workDone)
		select {
		case <-reduceworkDone:
			workDone++
		case w := <-reduceworkNotDone:
			var reply DoJobReply
			go AssignWorker(w, reply, mr.nReduce, mr, reduceworkNotDone, reduceworkDone)
		}
	}

	done <- struct{}{}
	return mr.KillWorkers()
}

func (mr *MapReduce) RegisterWorker() {
	for {
		select {
		case w := <-mr.registerChannel:
			// Registration signal received
			mr.Workers[w] = &WorkerInfo{
				address: w,
			}
			// Add to free worker queue
			mr.freeWorkers <- mr.Workers[w]
		case <-done:
			break
		}
	}
}

func AssignWorker(args *DoJobArgs, reply DoJobReply, nums int, mr *MapReduce, workNotDone chan *DoJobArgs, workDone chan struct{}) {

	w := <-mr.freeWorkers
	fmt.Print("in default")
	ok := call(w.address, "Worker.DoJob", args, &reply)
	if !ok {
		fmt.Printf("DoMap by worker %s failed due to error", w.address)
		workNotDone <- args
	} else {
		log.Print("Value of OK: ", reply.OK)
		workDone <- struct{}{}
		mr.freeWorkers <- w
	}

}
