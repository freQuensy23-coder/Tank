package backfillstate

import "errors"

type WriteDisposition uint8

const (
	WriteApplied WriteDisposition = iota + 1
	WriteSuperseded
	WriteMissing
)

type Acknowledgement struct {
	disposition WriteDisposition
	before      Reference
	after       *Reference
}

func Applied(before, after Reference) (Acknowledgement, error) {
	value := Acknowledgement{disposition: WriteApplied, before: before, after: &after}
	if err := value.Validate(); err != nil {
		return Acknowledgement{}, err
	}
	return value, nil
}

func Unchanged(before Reference, disposition WriteDisposition) (Acknowledgement, error) {
	if disposition != WriteSuperseded && disposition != WriteMissing {
		return Acknowledgement{}, errors.New("unchanged backfill write requires its observed missing or superseded state")
	}
	value := Acknowledgement{disposition: disposition, before: before}
	if err := value.Validate(); err != nil {
		return Acknowledgement{}, err
	}
	return value, nil
}

func (a Acknowledgement) Validate() error {
	if err := a.before.Validate(); err != nil {
		return err
	}
	switch a.disposition {
	case WriteApplied:
		if a.after == nil {
			return errors.New("backfill write acknowledgement has no actual updated revision")
		}
		if err := a.after.Validate(); err != nil {
			return err
		}
		if a.after.StorageKey() != a.before.StorageKey() || a.after.Revision() == a.before.Revision() {
			return errors.New("backfill write acknowledgement differs from its actual row transition")
		}
	case WriteSuperseded, WriteMissing:
		if a.after != nil {
			return errors.New("unchanged backfill write cannot claim an updated revision")
		}
	default:
		return errors.New("backfill write acknowledgement state is not supported")
	}
	return nil
}

func (a Acknowledgement) Disposition() WriteDisposition { return a.disposition }
func (a Acknowledgement) UpdatedReference() (Reference, error) {
	if err := a.Validate(); err != nil {
		return Reference{}, err
	}
	if a.disposition != WriteApplied {
		return Reference{}, errors.New("unchanged backfill write has no updated reference")
	}
	return *a.after, nil
}
