package grpcclient

import (
	"errors"
	"strconv"
	"strings"

	"google.golang.org/grpc/naming"
)

var errWatcherClose = errors.New("watcher has been closed")

// NewPseudoResolver creates a new pseudo resolver which returns fixed addrs.
func NewPseudoResolver(addrs []string) naming.Resolver {
	return &pseudoResolver{addrs}
}

type pseudoResolver struct {
	addrs []string
}

func (r *pseudoResolver) Resolve(target string) (naming.Watcher, error) {
	w := &pseudoWatcher{
		updatesChan: make(chan []*naming.Update, 1),
	}
	updates := []*naming.Update{}
	for n, addr := range r.addrs {
		addr = strings.ReplaceAll(addr, "localhost", "127.0.0."+strconv.Itoa(n+1))
		updates = append(updates, &naming.Update{Op: naming.Add, Addr: addr})
	}
	w.updatesChan <- updates
	return w, nil
}

// This watcher is implemented based on ipwatcher below
// https://github.com/grpc/grpc-go/blob/30fb59a4304034ce78ff68e21bd25776b1d79488/naming/dns_resolver.go#L151-L171
type pseudoWatcher struct {
	updatesChan chan []*naming.Update
}

func (w *pseudoWatcher) Next() ([]*naming.Update, error) {
	us, ok := <-w.updatesChan
	if !ok {
		return nil, errWatcherClose
	}
	return us, nil
}

func (w *pseudoWatcher) Close() {
	close(w.updatesChan)
}
