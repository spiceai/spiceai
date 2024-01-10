package registry

type RegistryItemNotFound struct {
	Err error
}

func (e *RegistryItemNotFound) Error() string { return e.Err.Error() }

func NewRegistryItemNotFound(err error) *RegistryItemNotFound {
	return &RegistryItemNotFound{
		Err: err,
	}
}
