package queryhistory

func Expect(head string, tail ...string) ExpectationSequencer {
	return Immediately(head, tail...)(nil)
}

func ExpectNone() ExpectationSequence {
	return &expectationSequence{}
}

func Eventually(head string, tail ...string) ExpectationSequencerFn {
	return func(sequencer ExpectationSequencer) ExpectationSequencer {
		current := Query(head)
		var head, last SequencedExpectation
		if sequencer != nil && sequencer.Current() != nil {
			head = sequencer.Head()
			sequencer.Current().ExpectEventuallyBefore(current)
		} else {
			head = current
		}
		for _, q := range tail {
			last = current
			current = Query(q)
			last.ExpectEventuallyBefore(current)
		}
		return &expectationSequencer{
			ExpectationSequence: &expectationSequence{head},
			current:             current,
		}
	}
}

func Immediately(head string, tail ...string) ExpectationSequencerFn {
	return func(sequencer ExpectationSequencer) ExpectationSequencer {
		current := Query(head)
		var head, last SequencedExpectation
		if sequencer != nil && sequencer.Current() != nil {
			head = sequencer.Head()
			sequencer.Current().ExpectImmediatelyBefore(current)
		} else {
			head = current
		}
		for _, q := range tail {
			last = current
			current = Query(q)
			last.ExpectImmediatelyBefore(current)
		}
		return &expectationSequencer{
			ExpectationSequence: &expectationSequence{head},
			current:             current,
		}
	}
}

func Query(query string) SequencedExpectation {
	return newSequencedExpectation(newExpectation(query))
}
