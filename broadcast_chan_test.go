package broadcast_chan

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBroadcastChanInt(t *testing.T) {
	const (
		TotalIter     = 1000
		FirstChanIter = 500
	)
	bchan, err := NewBroadcastChanInt()
	require.NoError(t, err)
	bchanClosed := false
	defer func() {
		if !bchanClosed {
			bchan.Close()
		}
	}()

	var complete sync.WaitGroup
	defer func() {
		complete.Wait()
	}()

	outCh1 := bchan.Out()
	require.NotNil(t, outCh1)

	complete.Add(1)
	go func() {
		defer complete.Done()
		recved := 0
		for range outCh1.Chan() {
			recved += 1
			if recved == FirstChanIter {
				outCh1.Stop()
			}
		}

		require.True(t, recved < FirstChanIter+10)
	}()

	outCh2 := bchan.Out()
	require.NotNil(t, outCh2)
	complete.Add(1)
	go func() {
		defer complete.Done()
		recved := 0
		for range outCh2.Chan() {
			recved++
		}
		require.Equal(t, recved, TotalIter)
	}()

	for i := 0; i < TotalIter; i++ {
		bchan.In() <- i
		time.Sleep(time.Millisecond)
	}
	bchan.Close()
	bchanClosed = true
}
