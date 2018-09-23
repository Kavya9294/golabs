package mapreduce

import "container/list"
import "fmt"
import "log"

//import "strconv"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	idle   bool
	failed bool
}

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
	//l := list.New()
	mr.Workers = make(map[string]*WorkerInfo)
	log.Print("Length of nWorkers: ", mr.nWorkers)
	for i := 0; i < mr.nWorkers; i++ {
		addr := <-mr.registerChannel
		mr.Workers[addr] = &WorkerInfo{address: addr, idle: true, failed: false}
	}

	for i := 0; i < mr.nMap; i++ {
		args := &DoJobArgs{}
		args.File = mr.file
		args.NumOtherPhase = mr.nReduce
		var job JobType = "Map"
		args.Operation = job
		args.JobNumber = i
		var reply DoJobReply
		AssignWorker(args, reply, mr.nMap, mr)
	}

	for i := 0; i < mr.nReduce; i++ {
		args := &DoJobArgs{}
		args.File = mr.file
		args.NumOtherPhase = mr.nMap
		var job JobType = "Reduce"
		args.Operation = job
		args.JobNumber = i
		var reply DoJobReply
		AssignWorker(args, reply, mr.nReduce, mr)

	}

	return mr.KillWorkers()
}

func AssignWorker(args *DoJobArgs, reply DoJobReply, nums int, mr *MapReduce) {
	i := 1
	for i > 0 && mr.Workers != nil {
		for _, w := range mr.Workers {
			if w.idle == true && w.failed == false {
				w.idle = false
				i = 0
				ok := call(w.address, "Worker.DoJob", args, &reply)
				if ok == false {
					w.idle = true
					w.failed = true
					delete(mr.Workers, w.address)
					i = 1
					fmt.Printf("DoMap by worker %s failed due to error", w.address)
				} else {
					w.idle = true
					log.Print("Value of OK: ", reply.OK)
				}
				break
			} else {
				continue
			}
		}
	}
}
