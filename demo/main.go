package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/carr123/fmx"
	"github.com/carr123/taskq"
)

func main() {
	log.Println("version:", taskq.Version())

	Recover := fmx.RecoverFn(func(s string) {
		log.Println(s)
	})

	queue := taskq.NewtaskQ()
	queue.SetDBURL("postgresql://root:@127.0.0.1:26257/dadibaostat?sslmode=disable")
	queue.SetPollingInterval(time.Second * 6)
	queue.SetExecTimeout(time.Second * 12)
	queue.SetMaxGoroutine(2)
	queue.SetRecover(Recover)

	if err := queue.Start(); err != nil {
		fmt.Println(err)
		return
	}

	if err := queue.CreateTaskIfNotExist("task", time.Now().Add(time.Second*5), "hello world"); err != nil {
		fmt.Println(err)
		return
	}

	// tasklist := make([]taskq.TaskCreateInfo, 0, 5)
	// for k := 0; k < 5; k++ {
	// 	tasklist = append(tasklist, taskq.TaskCreateInfo{
	// 		TaskName:    fmt.Sprintf("task%d", k),
	// 		Nextruntime: time.Now().Add(-time.Hour),
	// 		Content:     "taskinfo",
	// 	})
	// }
	// queue.CreateMultiTasksIfNotExist(tasklist)

	queue.Subscribe("task", func(task taskq.ITask) error {
		taskName := task.GetTaskName()
		content := task.GetContent()
		//fails := task.GetExecFailCount()
		tm := task.GetNextRuntime()
		log.Println("NextRuntime:", tm.Local())

		Lasttm := task.GetLastRuntime()
		log.Println("GetLastRuntime:", Lasttm.Local())

		task.SetNextRuntime(time.Now().Add(time.Second * 10))
		log.Println("recv task:", taskName, " content:", content)
		time.Sleep(time.Second * 2)
		log.Println("task ", taskName, " exec exit")
		return nil
	})

	time.Sleep(time.Second * 130)

	log.Println("closing...")
	queue.Close()
	log.Println("closed")

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
