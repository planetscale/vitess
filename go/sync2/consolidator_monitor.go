package sync2

type MonitoredConsolidator interface {
	Consolidator
	OnCreate(func(MonitoredPendingResult, bool))
}

type MonitoredPendingResult interface {
	PendingResult
	OnWaiter(func(MonitoredWaiter))
}

type MonitoredWaiter interface {
	OnWait(func())
}

type monitoredConsolidator struct {
	Consolidator
	onCreateCallbacks []func(MonitoredPendingResult, bool)
}

type monitoredPendingResult struct {
	PendingResult
	Original          bool
	onWaiterCallbacks []func(MonitoredWaiter)
}

type monitoredWaiter struct {
	onWaitCallbacks []func()
	wait            func()
}

func MonitorConsolidator(c Consolidator) MonitoredConsolidator {
	return &monitoredConsolidator{Consolidator: c}
}

func (mc *monitoredConsolidator) Create(sql string) (PendingResult, bool) {
	r, b := mc.Consolidator.Create(sql)
	mr := &monitoredPendingResult{
		PendingResult: r,
	}
	for _, c := range mc.onCreateCallbacks {
		c(mr, b)
	}
	return mr, b
}

func (mc *monitoredConsolidator) OnCreate(cb func(MonitoredPendingResult, bool)) {
	mc.onCreateCallbacks = append(mc.onCreateCallbacks, cb)
}

func (mr *monitoredPendingResult) OnWaiter(cb func(MonitoredWaiter)) {
	mr.onWaiterCallbacks = append(mr.onWaiterCallbacks, cb)
}

func (mr *monitoredPendingResult) Waiter() func() {
	w := mr.PendingResult.Waiter()
	mw := &monitoredWaiter{wait: w}
	for _, c := range mr.onWaiterCallbacks {
		c(mw)
	}
	return mw.Wait
}

func (mw *monitoredWaiter) OnWait(cb func()) {
	mw.onWaitCallbacks = append(mw.onWaitCallbacks, cb)
}

func (mw *monitoredWaiter) Wait() {
	mw.wait()
	for _, c := range mw.onWaitCallbacks {
		c()
	}
}
