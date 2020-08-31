package flock

import (
	"os"

	flockExt "github.com/gofrs/flock"
)

type Flock struct {
	*flockExt.Flock
}

func New(path string) *Flock {
	return &Flock{flockExt.New(path)}
}

func (l *Flock) Unlock() error {
	os.Remove(l.Path())
	return l.Flock.Unlock()
}
