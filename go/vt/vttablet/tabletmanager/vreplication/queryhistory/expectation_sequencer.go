package queryhistory

type ExpectationSequencer interface {
	ExpectationSequence
	Current() SequencedExpectation
	Eventually() ExpectationSequencerFn
	Immediately() ExpectationSequencerFn
	Then(ExpectationSequencerFn) ExpectationSequencer
}

type ExpectationSequencerFn func(ExpectationSequencer) ExpectationSequencer

type expectationSequencer struct {
	ExpectationSequence
	current SequencedExpectation
}

func (es *expectationSequencer) Current() SequencedExpectation {
	return es.current
}

func (es *expectationSequencer) Eventually() ExpectationSequencerFn {
	return func(parent ExpectationSequencer) ExpectationSequencer {
		es.Current().ExpectEventuallyAfter(parent.Current())
		return es
	}
}

func (es *expectationSequencer) Immediately() ExpectationSequencerFn {
	return func(parent ExpectationSequencer) ExpectationSequencer {
		es.Current().ExpectImmediatelyAfter(parent.Current())
		return es
	}
}

func (es *expectationSequencer) Then(then ExpectationSequencerFn) ExpectationSequencer {
	return then(es)
}
