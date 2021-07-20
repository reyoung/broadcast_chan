// Package broadcast_chan
// Broadcast the input channel to multiple channels.
//
// Each broadcast channel takes a goroutine in the background.
package broadcast_chan

import (
	"reflect"
	"sync"

	"github.com/cheekybits/genny/generic"
	"github.com/edwingeng/deque"
	"github.com/reyoung/dysel"
)

//go:generate genny -in=$GOFILE -out=gen-$GOFILE gen "ItemType=int,string,interface{},bool,uint32,int32,uint64,int64"
type ItemType generic.Type

// OutChanItemType the output channel wrapper
type OutChanItemType struct {
	ch        chan ItemType
	remaining deque.Deque
	stopping  bool
	id        uint32
	parent    *BroadcastChanItemType
	sending   bool
}

// Chan returns the output channel
// The channel will be closed when either `Stop` is invoked or the BroadcastChanItemType is closed.
func (o *OutChanItemType) Chan() <-chan ItemType {
	return o.ch
}

// Stop the current broadcast output channel.
// The items that already sent to BroadcastChanItemType will be sent to these output channel, and then close the Chan
//
// NOTE: User must read the Chan until it is closed even if Stop is invoked.
func (o *OutChanItemType) Stop() {
	defer func() {
		_ = recover()
	}()
	o.parent.stopOutChan <- o.id
}

// BroadcastChanItemType broadcast in channel to multiple output channels.
type BroadcastChanItemType struct {
	in                chan ItemType
	out               map[uint32]*OutChanItemType
	stopOutChan       chan uint32
	newOutChanRequest chan func(*OutChanItemType)
	inIsClosed        bool
	nextID            uint32
	looper            *dysel.Looper
	complete          sync.WaitGroup
}

type outChanSentPayloadItemType struct {
	chID uint32
}

func (c *BroadcastChanItemType) init() (*BroadcastChanItemType, error) {
	c.in = make(chan ItemType)
	c.out = map[uint32]*OutChanItemType{}
	c.stopOutChan = make(chan uint32)
	c.newOutChanRequest = make(chan func(itemType *OutChanItemType))
	looper := dysel.NewLooper(func(chosen int, recv reflect.Value, payload interface{}, recvOK bool) (continue_ bool) {
		panic("unexpect branch")
	})

	type newOutChanRequestPayload struct{}
	err := looper.RecvAndCaseHandler(c.newOutChanRequest, newOutChanRequestPayload{}, c.onNewOutChannelRequest)
	if err != nil {
		return nil, err
	}
	type stopOutChanPayload struct{}
	err = looper.RecvAndCaseHandler(c.stopOutChan, stopOutChanPayload{}, c.onStopOutChan)
	if err != nil {
		return nil, err
	}
	err = looper.AddCaseHandler(reflect.TypeOf(outChanSentPayloadItemType{}), c.onOutChanSent)
	if err != nil {
		return nil, err
	}
	type inPayload struct{}
	err = looper.RecvAndCaseHandler(c.in, inPayload{}, c.onInChanRecv)
	if err != nil {
		return nil, err
	}
	c.looper = looper
	c.complete.Add(1)
	go func() {
		defer c.complete.Done()
		c.looper.Loop()
	}()
	return c, nil
}

func (c *BroadcastChanItemType) onInChanRecv(chosen int, recv reflect.Value, _ interface{}, recvOK bool) bool {
	if !recvOK {
		c.looper.Remove(chosen)
		c.inIsClosed = true
		close(c.stopOutChan)
		close(c.newOutChanRequest)
		var toRemove []uint32
		for k, v := range c.out {
			if v.sending {
				continue
			}
			close(v.ch)
			toRemove = append(toRemove, k)
		}

		for _, k := range toRemove {
			delete(c.out, k)
		}

		return len(c.out) != 0
	}

	item := recv.Interface().(ItemType) // nolint(errcheck)
	for k, v := range c.out {
		if v.sending {
			v.remaining.PushBack(item)
			continue
		}
		v.sending = true
		c.looper.Send(v.ch, item, outChanSentPayloadItemType{k})
	}
	return true
}

func (c *BroadcastChanItemType) onOutChanSent(chosen int, _ reflect.Value, payload interface{}, _ bool) bool {
	chID := payload.(outChanSentPayloadItemType).chID
	ch, ok := c.out[chID]
	if !ok {
		// should not happen
		return true
	}
	if ch.remaining.Len() != 0 {
		item := ch.remaining.Dequeue().(ItemType) // nolint(errcheck)
		c.looper.SendNext(chosen, item)
		return true
	}
	c.looper.Remove(chosen)
	ch.sending = false
	shouldStop := ch.stopping || c.inIsClosed
	if shouldStop {
		delete(c.out, chID)
		close(ch.ch)
	}

	if c.inIsClosed {
		return len(c.out) != 0
	} else {
		return true
	}
}

func (c *BroadcastChanItemType) onStopOutChan(chosen int, recv reflect.Value, _ interface{}, recvOK bool) bool {
	if !recvOK {
		c.looper.Remove(chosen)
		return true
	}
	chID := recv.Interface().(uint32) // nolint(errcheck)
	ch, ok := c.out[chID]
	if !ok {
		return true
	}
	if ch.sending {
		ch.stopping = true
		return true
	}
	close(ch.ch)
	delete(c.out, chID)
	return true
}

func (c *BroadcastChanItemType) onNewOutChannelRequest(
	chosen int, recv reflect.Value, _ interface{}, recvOK bool) bool {
	if !recvOK {
		c.looper.Remove(chosen)
		return true
	}
	callback := recv.Interface().(func(*OutChanItemType)) // nolint(errcheck)
	if c.inIsClosed {
		callback(nil)
	}
	chanID := c.nextID
	c.nextID += 1
	outChan := &OutChanItemType{
		ch:        make(chan ItemType),
		remaining: deque.NewDeque(),
		stopping:  false,
		id:        chanID,
		parent:    c,
		sending:   false,
	}
	c.out[chanID] = outChan
	callback(outChan)
	return true
}

// In input channel
func (c *BroadcastChanItemType) In() chan<- ItemType {
	return c.in
}

// Out create a new output channel
func (c *BroadcastChanItemType) Out() (ch *OutChanItemType) {
	defer func() {
		if recover() != nil {
			ch = nil
		}
	}()
	var complete sync.WaitGroup
	complete.Add(1)
	c.newOutChanRequest <- func(itemType *OutChanItemType) {
		ch = itemType
		complete.Done()
	}
	complete.Wait()
	return
}

// Close close the broadcast channel
func (c *BroadcastChanItemType) Close() {
	close(c.in)
	c.complete.Wait()
}

// NewBroadcastChanItemType create a broadcast channel
func NewBroadcastChanItemType() (*BroadcastChanItemType, error) {
	return (&BroadcastChanItemType{}).init()
}
