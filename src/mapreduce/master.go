package mapreduce

import "container/list"
import "fmt"
import "log"
import "strconv"

type WorkerInfo struct {
	address string
	// You can add definitions here.
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
  log.Print("Length of nWorkers: ",mr.nWorkers)
  for i:=0;i<mr.nWorkers;i++ {
    address := <-mr.registerChannel
    log.Print("Value of address ",address)
    p := &WorkerInfo{address}
    log.Print("Value of p ",*p)
    index := strconv.Itoa(i)
    log.Print("Value of Index ",index)
    mr.Workers[index] = p
    log.Print("Value of Workers",mr.Workers)
  }
  
  for i:=0;i<mr.nMap; i++ {
    address := <-mr.registerChannel
    log.Print("Value of address ",address)
    p := &WorkerInfo{address}
    log.Print("Value of p ",*p)
    index := strconv.Itoa(i)
    log.Print("Value of Index ",index)
    mr.Workers[index] = p
    log.Print("Value of Workers",mr.Workers)
    w := mr.Workers[index]
    
    DPrintf("DoWork: Map %s\n", w.address)
    args := &DoJobArgs{}
    args.File = mr.file
    args.NumOtherPhase = mr.nReduce
    var job JobType = "Map"
    args.Operation = job
    args.JobNumber = i
    var reply DoJobReply
    //AssignWorker(args, reply)
    ok := call(w.address, "Worker.DoJob",args, &reply)
    if ok == false {
      fmt.Printf("DoMap by worker %s failed due to error",w.address)
    } else{
      log.Print("Value of OK: ",reply.OK)
      mr.workDone <- true
    }

  }

  for i:=0;i<mr.nMap;i++ {
    <-mr.workDone
  }
	return mr.KillWorkers()
}

//func AssignWorker(args *DoJobArgs, reply DoJobReply ) {
  //for _,w := range mr.Workers {
    //ok := call(w.address, "Worker.DoJob",args, &reply)
    //if ok == false {
      //fmt.Printf("DoMap by worker %s failed due to error",w.address)
    //} else{
      //log.Print("Value of OK: ",reply.OK)
      //workDone <- true
    //}
  //}
//}
