package taskq

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
	version            = "1.0.4"
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

func Version() string {
	return version
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

//数据库连接字符串: postgresql://user:passwd@127.0.0.1:26257/dbname
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
	if err := conn.Exec(szSQL, taskname, easysql.NewDateTime(nextruntime), content); err != nil {
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
		args = append(args, item.TaskName, easysql.NewDateTime(item.Nextruntime), item.Content)
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
		timeNow := easysql.NewDateTime(time.Now())
		szSQL := fmt.Sprintf(`SELECT taskid,taskname,lastruntime,nextruntime,content,fails FROM %s WHERE nextruntime <= ? and working=false and taskname like ? order by nextruntime asc limit ? FOR UPDATE`, t.tableName)
		if err := conn.Select(&taskarray, szSQL, timeNow, nameprefix+"%", maxcount); err != nil {
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
		if err := conn.Exec(szSQL, timeNow, timeNow, ids); err != nil {
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
		mp["lastruntime"] = task.Lastruntime.ToTime()
		mp["nextruntime"] = task.Nextruntime.ToTime()
		mp["content"] = task.Content.String()
		mp["fails"] = int(task.Fails.Int64)
		t.workpool.PushItem(mp)
	}

	return
}

//执行超时的任务(通常是执行端意外崩溃)重新加入调度
func (t *TASKQ) _updateTaskStatus() {
	defer t.Recover()

	timeDue := time.Now().Add(-t.execTimeout)

	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`UPDATE %s SET working=false WHERE heartbeattime < ? and working=true`, t.tableName)
	if err := conn.Exec(szSQL, easysql.NewDateTime(timeDue)); err != nil {
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

	conn := t.db.NewConn()
	defer conn.Close()

	szSQL := fmt.Sprintf(`UPDATE %s SET heartbeattime=? WHERE taskid in (?)`, t.tableName)
	if err := conn.Exec(szSQL, easysql.NewDateTime(time.Now()), ids); err != nil {
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
	if err := conn.Exec(szSQL, easysql.NewDateTime(datetime), fails, taskid); err != nil {
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
	return t.info["nextruntime"].(time.Time)
}

func (t *internalTaskImpl) GetLastRuntime() time.Time {
	return t.info["lastruntime"].(time.Time)
}

func (t *internalTaskImpl) SetNextRuntime(tm time.Time) error {
	t.action = "setnextruntime"
	t.nextRunTime = tm
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
