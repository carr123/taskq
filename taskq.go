package taskQ

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/carr123/easysql"
	"github.com/carr123/workerpool"

	dbserver "github.com/carr123/easysql/cockroach"
)

const (
	DEFAULT_TABLE_NAME = "task_queue_2021"
)

type STRING = easysql.STRING
type INT64 = easysql.INT64
type DATE = easysql.DATE
type DATETIME = easysql.DATETIME
type FLOAT64 = easysql.FLOAT64

type TASKID = string

type ITask interface {
	GetTaskName() string
	GetContent() string
	SetContent(content string) error
	GetNextRuntime() time.Time
	GetLastRuntime() time.Time
	GetExecFailCount() int
	SetNextRuntime(datetime time.Time) error
	DeleteTask() error
}

type TaskCreateInfo struct {
	TaskName    string
	Nextruntime time.Time
	Content     string
}

type TASKQ struct {
	Recover         func()
	bExit           bool
	dbUrl           string
	tableName       string
	db              *dbserver.DBServer
	workpool        *workerpool.WorkerPool
	pollingInterval time.Duration
	execTimeout     time.Duration
	maxWorkers      int
	wgExit          sync.WaitGroup
	mpSub           map[string]func(ITask) error
	locker          sync.Mutex
	refreshTaskIDs  map[string]struct{}
}

func NewtaskQ() *TASKQ {
	q := &TASKQ{
		Recover:         DEFAULT_RECOVER,
		bExit:           false,
		dbUrl:           "",
		tableName:       DEFAULT_TABLE_NAME,
		workpool:        workerpool.NewWorkerPool(8),
		pollingInterval: time.Second * 20,
		execTimeout:     time.Second * 60,
		maxWorkers:      8,
		mpSub:           make(map[string]func(ITask) error),
		refreshTaskIDs:  make(map[string]struct{}),
	}

	return q
}

//数据库连接字符串: postgresql://user:passwd@10.91.26.225:2625/dbname
func (t *TASKQ) SetDBURL(dbURL string) {
	t.dbUrl = dbURL
}

//pollingInterval: 多久轮询一次数据库看有没有任务要执行
func (t *TASKQ) SetPollingInterval(pollingInterval time.Duration) {
	if pollingInterval < time.Second*3 {
		panic(fmt.Sprintf("pollingInterval too small"))
	}

	t.pollingInterval = pollingInterval
}

//execTimeout: 任务执行多久未结束认为执行失败需重新调度
func (t *TASKQ) SetExecTimeout(execTimeout time.Duration) {
	if execTimeout < time.Second*3 {
		panic(fmt.Sprintf("execTimeout too small"))
	}

	t.execTimeout = execTimeout
}

//maxWorkers : 最多几个协程并发处理消息
func (t *TASKQ) SetMaxGoroutine(maxWorkers int) {
	if maxWorkers < 1 {
		panic(fmt.Sprintf("maxWorkers too small"))
	}

	t.maxWorkers = maxWorkers
}

func (t *TASKQ) SetRecover(fn func()) {
	if fn == nil {
		fn = DEFAULT_RECOVER
	}

	t.Recover = fn
}

func (t *TASKQ) SetTableName(tablename string) {
	if len(tablename) == 0 {
		panic("table name empty")
	}

	t.tableName = tablename
}

func (t *TASKQ) Start() error {
	if len(t.dbUrl) == 0 {
		return errors.New("dbUrl NOT set")
	}

	dbServer, err := dbserver.New(t.dbUrl, 1)
	if err != nil {
		return err
	}

	t.db = dbServer

	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
     taskid UUID NOT NULL DEFAULT gen_random_uuid(),
	 taskname STRING,
	 nextruntime TIMESTAMPTZ NOT NULL,
	 lastruntime TIMESTAMPTZ NULL,
	 heartbeattime TIMESTAMPTZ NULL,
	 working BOOL DEFAULT FALSE,
	 content STRING,
	 fails INT NOT NULL DEFAULT 0,
	 CONSTRAINT "primary" PRIMARY KEY (taskid ASC),
     UNIQUE INDEX taskname_idx (taskname),
	 INDEX nextruntime_idx (nextruntime),
	 INDEX heartbeattime_idx (heartbeattime)
	)`, t.tableName)

	if err := conn.Exec(szSQL); err != nil {
		return err
	}

	t.workpool.SetMaxGoroutine(t.maxWorkers)
	t.workpool.SetMaxIdleGoroutine(2)
	t.workpool.SetMaxIdleTime(time.Second * 60)
	t.workpool.SetHandler(func(a interface{}) {
		defer t.Recover()
		t._handleOneTask(a.(map[string]interface{}))
	})

	t.wgExit.Add(1)
	go t._checkTasks()

	return nil
}

func (t *TASKQ) CreateTaskIfNotExist(taskname string, nextruntime time.Time, content string) error {
	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`INSERT INTO %s (taskname,nextruntime,content) VALUES (?,?,?) ON CONFLICT(taskname) DO NOTHING`, t.tableName)
	if err := conn.Exec(szSQL, taskname, nextruntime.Format("2006-01-02 15:04:05"), content); err != nil {
		return err
	}

	return nil
}

func (t *TASKQ) CreateMultiTasksIfNotExist(taskList []TaskCreateInfo) error {
	if len(taskList) == 0 {
		return nil
	}

	nCol := 3
	args := make([]interface{}, 0, nCol*len(taskList))
	for _, item := range taskList {
		args = append(args, item.TaskName, item.Nextruntime.Format("2006-01-02 15:04:05"), item.Content)
	}

	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`INSERT INTO %s (taskname,nextruntime,content)`, t.tableName)
	if err := conn.BulkInsertEx(szSQL, 3, args, "ON CONFLICT(taskname) DO NOTHING"); err != nil {
		return err
	}

	return nil
}

func (t *TASKQ) SetTaskContent(taskname string, content string) error {
	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`UPDATE %s SET content=? WHERE taskname=?`, t.tableName)
	if err := conn.Exec(szSQL, content, taskname); err != nil {
		return err
	}

	return nil
}

func (t *TASKQ) DeleteTask(taskname string) error {
	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`DELETE FROM %s WHERE taskname=?`, t.tableName)
	if err := conn.Exec(szSQL, taskname); err != nil {
		return err
	}

	return nil
}

func (t *TASKQ) Subscribe(tasknameprefix string, handler func(ITask) error) {
	t.locker.Lock()
	defer t.locker.Unlock()

	if _, ok := t.mpSub[tasknameprefix]; ok {
		panic(fmt.Sprintf("task name %s already in subscription", tasknameprefix))
	}

	t.mpSub[tasknameprefix] = handler
}

func (t *TASKQ) Close() {
	t.bExit = true
	t.wgExit.Wait()

	for !t.workpool.Idle() {
		time.Sleep(time.Millisecond * 10)
	}
}

func (t *TASKQ) _checkTasks() {
	defer t.wgExit.Done()

	lastWorkingTime := time.Now()
	lastRefreshTime := time.Now()

	for {
		time.Sleep(time.Second)
		if t.bExit && t.workpool.Idle() {
			break
		}

		tmNow := time.Now()
		if tmNow.Sub(lastWorkingTime) >= t.pollingInterval && !t.bExit {
			t._updateTaskStatus()
			t._pullNewTask()
			lastWorkingTime = tmNow
		}

		if tmNow.Sub(lastRefreshTime) > t.execTimeout/3 {
			t._reclaimTask() //确保处理中的任务不过时，不被其他人抢去
			lastRefreshTime = tmNow
		}
	}
}

func (t *TASKQ) _pullNewTask() {
	defer t.Recover()

	fetchCount := t.maxWorkers - t.workpool.GetPendingItemCount()
	if fetchCount <= 0 {
		return
	}

	t.locker.Lock()
	names := make([]string, 0, len(t.mpSub))
	for k, _ := range t.mpSub {
		names = append(names, k)
	}
	t.locker.Unlock()

	sort.Slice(names, func(i int, j int) bool {
		return names[i] > names[j]
	})

	for _, name := range names {
		t._allocTaskByName(name, fetchCount)
		fetchCount = t.maxWorkers - t.workpool.GetPendingItemCount()
		if fetchCount <= 0 {
			break
		}
	}
}

func (t *TASKQ) _allocTaskByName(nameprefix string, maxcount int) {
	var taskarray []struct {
		Taskid      STRING   `db:"taskid"`
		Taskname    STRING   `db:"taskname"`
		Lastruntime DATETIME `db:"lastruntime"`
		Nextruntime DATETIME `db:"nextruntime"`
		Content     STRING   `db:"content"`
		Fails       INT64    `db:"fails"`
	}

	var err error
	err = t.db.ExecInTx(func(conn *dbserver.Conn) error {
		szTimeNow := time.Now().Format("2006-01-02 15:04:05")
		szSQL := fmt.Sprintf(`SELECT taskid,taskname,lastruntime,nextruntime,content,fails FROM %s WHERE nextruntime <= ? and working=false and taskname like ? limit ? FOR UPDATE`, t.tableName)
		if err := conn.Select(&taskarray, szSQL, szTimeNow, nameprefix+"%", maxcount); err != nil {
			return err
		}

		if len(taskarray) == 0 {
			return nil
		}

		ids := make([]string, 0, len(taskarray))
		for _, task := range taskarray {
			ids = append(ids, task.Taskid.String())
		}

		szSQL = fmt.Sprintf(`UPDATE %s SET working=true,lastruntime=?,heartbeattime=? WHERE taskid IN (?)`, t.tableName)
		if err := conn.Exec(szSQL, szTimeNow, szTimeNow, ids); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		panic(err)
		return
	}

	for _, task := range taskarray {
		mp := make(map[string]interface{})
		mp["nameprefix"] = nameprefix
		mp["taskid"] = task.Taskid.String()
		mp["taskname"] = task.Taskname.String()
		mp["lastruntime"] = task.Lastruntime.String()
		mp["nextruntime"] = task.Nextruntime.String()
		mp["content"] = task.Content.String()
		mp["fails"] = int(task.Fails.Int64)
		t.workpool.PushItem(mp)
	}

	return
}

//执行超时的任务(通常是执行端意外崩溃)重新加入调度
func (t *TASKQ) _updateTaskStatus() {
	defer t.Recover()

	szTimeDue := time.Now().Add(-t.execTimeout).Format("2006-01-02 15:04:05")

	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`UPDATE %s SET working=false WHERE heartbeattime < ? and working=true`, t.tableName)
	if err := conn.Exec(szSQL, szTimeDue); err != nil {
		panic(err)
		return
	}
}

func (t *TASKQ) _reclaimTask() {
	defer t.Recover()

	t.locker.Lock()
	ids := make([]string, 0, len(t.refreshTaskIDs))
	for k, _ := range t.refreshTaskIDs {
		ids = append(ids, k)
	}
	t.locker.Unlock()

	if len(ids) == 0 {
		return
	}

	szNow := time.Now().Format("2006-01-02 15:04:05")

	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`UPDATE %s SET heartbeattime=? WHERE taskid in (?)`, t.tableName)
	if err := conn.Exec(szSQL, szNow, ids); err != nil {
		panic(err)
		return
	}
}

func (t *TASKQ) _DelTaskByID(taskid string) error {
	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`DELETE FROM %s where taskid=?`, t.tableName)
	if err := conn.Exec(szSQL, taskid); err != nil {
		return err
	}

	return nil
}

func (t *TASKQ) _SetContent(taskid string, content string) error {
	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`UPDATE %s SET content=? WHERE taskid=?`, t.tableName)
	if err := conn.Exec(szSQL, content, taskid); err != nil {
		return err
	}

	return nil
}

func (t *TASKQ) _SetNextRunTime(taskid string, datetime time.Time, fails int) error {
	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`UPDATE %s SET nextruntime=?, working=false, fails=? WHERE taskid=?`, t.tableName)
	if err := conn.Exec(szSQL, datetime.Format("2006-01-02 15:04:05"), fails, taskid); err != nil {
		return err
	}

	return nil
}

func (t *TASKQ) _SetFails(taskid string, fails int) error {
	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`UPDATE %s SET working=false, fails=? WHERE taskid=?`, t.tableName)
	if err := conn.Exec(szSQL, fails, taskid); err != nil {
		return err
	}

	return nil
}

func (t *TASKQ) _addToRefreshTaskList(taskid string) {
	t.locker.Lock()
	t.refreshTaskIDs[taskid] = struct{}{}
	t.locker.Unlock()
}

func (t *TASKQ) _removeFromRefreshTaskList(taskid string) {
	t.locker.Lock()
	delete(t.refreshTaskIDs, taskid)
	t.locker.Unlock()
}

func (t *TASKQ) _handleOneTask(mp map[string]interface{}) {
	var err error

	nameprefix := mp["nameprefix"].(string)
	taskid := mp["taskid"].(string)
	taskname := mp["taskname"].(string)
	fails := mp["fails"].(int)

	handlerFn := t.mpSub[nameprefix]

	obj := &internalTaskImpl{
		info:         mp,
		fnDeltask:    t._DelTaskByID,
		fnSetContent: t._SetContent,
	}

	t._addToRefreshTaskList(taskid)
	defer t._removeFromRefreshTaskList(taskid)

	err = handlerFn(obj)

	if err == nil {
		fails = 0
	} else {
		fails += 1
	}

	if obj.action == "deletetask" {
		return
	}

	if obj.action == "setnextruntime" {
		if err := t._SetNextRunTime(taskid, obj.nextRunTime, fails); err != nil {
			panic(err)
		}

		return
	}

	if err := t._SetFails(taskid, fails); err != nil {
		panic(err)
	}

	if err == nil {
		panic(fmt.Sprintf("you should either DeleteTask %s or SetNextRuntime", taskname))
	}
}

type internalTaskImpl struct {
	info         map[string]interface{}
	fnDeltask    func(taskid string) error
	fnSetContent func(taskid string, content string) error
	nextRunTime  time.Time
	action       string
}

func (t *internalTaskImpl) GetTaskName() string {
	return t.info["taskname"].(string)
}

func (t *internalTaskImpl) GetContent() string {
	return t.info["content"].(string)
}

func (t *internalTaskImpl) GetNextRuntime() time.Time {
	nextruntime := t.info["nextruntime"].(string)
	tm, err := time.Parse("2006-01-02 15:04:05", nextruntime)
	if err != nil {
		panic(err)
	}
	return tm
}

func (t *internalTaskImpl) GetLastRuntime() time.Time {
	var err error
	var tm time.Time

	lastruntime := t.info["lastruntime"].(string)
	if len(lastruntime) == 0 {
		return tm
	}

	tm, err = time.Parse("2006-01-02 15:04:05", lastruntime)
	if err != nil {
		panic(err)
	}
	return tm
}

func (t *internalTaskImpl) SetNextRuntime(datetime time.Time) error {
	t.action = "setnextruntime"
	t.nextRunTime = datetime
	return nil
}

func (t *internalTaskImpl) DeleteTask() error {
	t.action = "deletetask"
	return t.fnDeltask(t.info["taskid"].(string))
}

func (t *internalTaskImpl) SetContent(content string) error {
	taskid := t.info["taskid"].(string)
	return t.fnSetContent(taskid, content)
}

//连续(执行)失败的次数
func (t *internalTaskImpl) GetExecFailCount() int {
	return t.info["fails"].(int)
}

var DEFAULT_RECOVER func() = func() {
	if err := recover(); err != nil {
		log.Println(err)
	}
}

/*
func main() {
	Recover := fmx.RecoverFn(func(s string) {
		log.Println(s)
	})

	queue := taskQ.NewtaskQ()
	queue.SetDBURL("postgresql://root:robotdb@10.91.26.225:2625/dadibaostat")
	queue.SetPollingInterval(time.Second * 6)
	queue.SetExecTimeout(time.Second * 12)
	queue.SetMaxGoroutine(5)
	queue.SetRecover(Recover)

	if err := queue.Start(); err != nil {
		fmt.Println(err)
		return
	}

	if err := queue.CreateTaskIfNotExist("task2", time.Now().Add(time.Hour), "hello world"); err != nil {
		fmt.Println(err)
		return
	}

	tasklist := make([]taskQ.TaskCreateInfo, 0, 100)
	for k := 0; k < 100; k++ {
		tasklist = append(tasklist, taskQ.TaskCreateInfo{
			TaskName:    fmt.Sprintf("task%d", k),
			Nextruntime: time.Now().Add(-time.Hour),
			Content:     "taskinfo",
		})
	}
	queue.CreateMultiTasksIfNotExist(tasklist)

	queue.Subscribe("task", func(task taskQ.ITask) error {
		taskName := task.GetTaskName()
		content := task.GetContent()
		fails := task.GetExecFailCount()
		//tm := task.GetNextRuntime()
		task.SetNextRuntime(time.Now().Add(time.Hour * 160))
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
*/
