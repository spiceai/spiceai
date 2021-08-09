package context

import "os"

type RuntimeContext int

const (
	Undefined = 0
	Docker    = 1
	BareMetal = 2
)

var (
	currentContext RuntimeContext
)

func SetContext(context RuntimeContext) {
	currentContext = context
}

func CurrentContext() RuntimeContext {
	if currentContext == Undefined {
		if os.Getenv("SPICE_ENVIRONMENT") == "docker" {
			currentContext = Docker
			return currentContext
		}

		currentContext = BareMetal
	}

	return currentContext
}
