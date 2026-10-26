package backfillstate

import (
	"errors"
	"time"
)

type WriterIdentity struct{ configVersion, codeVersion string }

func NewWriterIdentity(configVersion, codeVersion string) (WriterIdentity, error) {
	value := WriterIdentity{configVersion: configVersion, codeVersion: codeVersion}
	if err := value.Validate(); err != nil {
		return WriterIdentity{}, err
	}
	return value, nil
}

func (w WriterIdentity) Validate() error {
	if !validIdentity(w.configVersion) || !validIdentity(w.codeVersion) {
		return errors.New("backfill writer requires actual configuration and code identities")
	}
	return nil
}
func (w WriterIdentity) ConfigVersion() string { return w.configVersion }
func (w WriterIdentity) CodeVersion() string   { return w.codeVersion }

func (o Observation) Validate() error {
	if err := o.reference.Validate(); err != nil {
		return err
	}
	switch selected := o.selection.(type) {
	case Obligation:
		return selected.Validate()
	case Refused:
		return selected.Validate()
	default:
		return errors.New("backfill observation requires an admitted command or explicit refusal")
	}
}

func (o Observation) matches(actual Observation) error {
	if err := actual.Validate(); err != nil {
		return err
	}
	if o.reference != actual.reference {
		return errors.New("backfill write differs from its locked row reference")
	}
	switch selected := o.selection.(type) {
	case Obligation:
		current, ready := actual.selection.(Obligation)
		if !ready || selected.backfillID != current.backfillID || selected.eventID != current.eventID || selected.tokenID != current.tokenID || selected.phase != current.phase || !selected.anchor.Equal(current.anchor) {
			return errors.New("backfill write differs from its locked command facts")
		}
	case Refused:
		current, refused := actual.selection.(Refused)
		if !refused || selected != current {
			return errors.New("backfill write differs from its locked command refusal")
		}
	default:
		return errors.New("backfill write has no admitted observation")
	}
	return nil
}

func collectionInstant(value time.Time) error {
	if value.IsZero() || value.Nanosecond()%1000 != 0 {
		return errors.New("backfill processing requires its actual nonzero microsecond collection clock")
	}
	return nil
}

type Start struct {
	observation       Observation
	started, recorded time.Time
	writer            WriterIdentity
}

func Begin(observation Observation, started, recorded time.Time, writer WriterIdentity) (Start, error) {
	value := Start{observation: observation, started: started, recorded: recorded, writer: writer}
	if err := value.Validate(); err != nil {
		return Start{}, err
	}
	return value, nil
}

func (s Start) Validate() error {
	if err := s.observation.Validate(); err != nil {
		return err
	}
	if _, ready := s.observation.selection.(Obligation); !ready {
		return errors.New("refused backfill command cannot start capture")
	}
	if err := s.writer.Validate(); err != nil {
		return err
	}
	if err := collectionInstant(s.started); err != nil {
		return err
	}
	return collectionInstant(s.recorded)
}
func (s Start) Reference() Reference   { return s.observation.reference }
func (s Start) StartedAt() time.Time   { return s.started }
func (s Start) RecordedAt() time.Time  { return s.recorded }
func (s Start) Writer() WriterIdentity { return s.writer }

func (s Start) Bind(actual Observation) error {
	if err := s.Validate(); err != nil {
		return err
	}
	return s.observation.matches(actual)
}

type Completion struct {
	observation        Observation
	outcome            Outcome
	finished, recorded time.Time
	writer             WriterIdentity
}

func Finish(observation Observation, outcome Outcome, finished, recorded time.Time, writer WriterIdentity) (Completion, error) {
	value := Completion{observation: observation, outcome: outcome, finished: finished, recorded: recorded, writer: writer}
	if err := value.Validate(); err != nil {
		return Completion{}, err
	}
	return value, nil
}

func (c Completion) Validate() error {
	if err := c.observation.Validate(); err != nil {
		return err
	}
	if c.outcome == nil {
		return errors.New("backfill completion requires its actual outcome")
	}
	switch outcome := c.outcome.(type) {
	case CaptureOutcome:
		if err := outcome.Validate(); err != nil {
			return err
		}
		command, ready := c.observation.selection.(Obligation)
		if !ready || command.phase != Running {
			return errors.New("unstarted or refused backfill command cannot acquire captured coverage")
		}
		if err := outcome.result.ValidateScope(command.tokenID, command.anchor); err != nil {
			return err
		}
	case Failure:
		if err := outcome.Validate(); err != nil {
			return err
		}
		if outcome.kind == CommandRefused {
			refusal, refused := c.observation.selection.(Refused)
			if !refused || *outcome.refusal != refusal {
				return errors.New("backfill failure differs from its observed command refusal")
			}
		} else {
			command, ready := c.observation.selection.(Obligation)
			if !ready || (outcome.kind != WorkerPanicked && command.phase != Running) {
				return errors.New("unstarted backfill command cannot acquire another worker's failure")
			}
		}
	default:
		return errors.New("backfill completion outcome is not supported")
	}
	if err := c.writer.Validate(); err != nil {
		return err
	}
	if err := collectionInstant(c.finished); err != nil {
		return err
	}
	return collectionInstant(c.recorded)
}
func (c Completion) Reference() Reference   { return c.observation.reference }
func (c Completion) Outcome() Outcome       { return c.outcome }
func (c Completion) FinishedAt() time.Time  { return c.finished }
func (c Completion) RecordedAt() time.Time  { return c.recorded }
func (c Completion) Writer() WriterIdentity { return c.writer }

func (c Completion) Bind(actual Observation) error {
	if err := c.Validate(); err != nil {
		return err
	}
	return c.observation.matches(actual)
}

func (o Observation) AcknowledgeStart(reference Reference) (Observation, error) {
	if err := o.Validate(); err != nil {
		return Observation{}, err
	}
	if err := reference.Validate(); err != nil {
		return Observation{}, err
	}
	if reference.StorageKey() != o.reference.StorageKey() || reference.Revision() == o.reference.Revision() {
		return Observation{}, errors.New("backfill acknowledgement requires a new observed revision of the same row")
	}
	command, ready := o.selection.(Obligation)
	if !ready {
		return Observation{}, errors.New("refused backfill command cannot acknowledge a capture start")
	}
	command.phase = Running
	return Observation{reference: reference, selection: command}, nil
}
