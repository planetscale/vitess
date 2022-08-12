package queryhistory

// SequencedExpectation is structured like a node in a doubly-linked
// list, with two link types: immediate and eventual.
//
// It is used to represent the temporal relationships between expected
// queries.
type SequencedExpectation interface {
	Expectation

	EventuallyAfter() SequencedExpectationSet
	EventuallyBefore() SequencedExpectationSet
	ExpectImmediatelyAfter(SequencedExpectation)
	ExpectImmediatelyBefore(SequencedExpectation)
	ExpectEventuallyAfter(SequencedExpectation)
	ExpectEventuallyBefore(SequencedExpectation)
	ImmediatelyAfter() SequencedExpectation
	ImmediatelyBefore() SequencedExpectation
}

type sequencedExpectation struct {
	Expectation
	eventuallyAfter   SequencedExpectationSet
	eventuallyBefore  SequencedExpectationSet
	immediatelyAfter  SequencedExpectation
	immediatelyBefore SequencedExpectation
}

func newSequencedExpectation(expectation Expectation) SequencedExpectation {
	eventuallyAfter := sequencedExpectationSet(make(map[SequencedExpectation]any))
	eventuallyBefore := sequencedExpectationSet(make(map[SequencedExpectation]any))
	return &sequencedExpectation{
		Expectation:      expectation,
		eventuallyAfter:  &eventuallyAfter,
		eventuallyBefore: &eventuallyBefore,
	}
}

func (se *sequencedExpectation) EventuallyAfter() SequencedExpectationSet {
	return se.eventuallyAfter
}

func (se *sequencedExpectation) EventuallyBefore() SequencedExpectationSet {
	return se.eventuallyBefore
}

func (se *sequencedExpectation) ExpectEventuallyAfter(expectation SequencedExpectation) {
	if !se.eventuallyAfter.Contains(expectation) {
		se.eventuallyAfter.Add(expectation)
		expectation.ExpectEventuallyBefore(se)
	}
}

func (se *sequencedExpectation) ExpectEventuallyBefore(expectation SequencedExpectation) {
	if !se.eventuallyBefore.Contains(expectation) {
		se.eventuallyBefore.Add(expectation)
		expectation.ExpectEventuallyAfter(se)
	}
}

func (se *sequencedExpectation) ExpectImmediatelyAfter(expectation SequencedExpectation) {
	if se.immediatelyAfter != expectation {
		se.immediatelyAfter = expectation
		expectation.ExpectImmediatelyBefore(se)
	}
}

func (se *sequencedExpectation) ExpectImmediatelyBefore(expectation SequencedExpectation) {
	if se.immediatelyBefore != expectation {
		se.immediatelyBefore = expectation
		expectation.ExpectImmediatelyAfter(se)
	}
}

func (se *sequencedExpectation) ImmediatelyAfter() SequencedExpectation {
	return se.immediatelyAfter
}

func (se *sequencedExpectation) ImmediatelyBefore() SequencedExpectation {
	return se.immediatelyBefore
}
