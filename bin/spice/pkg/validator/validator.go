package validator

import "regexp"

var (
	argsRegex          *regexp.Regexp
	dataspaceNameRegex *regexp.Regexp
)

func GetArgsRegex() *regexp.Regexp {
	return argsRegex
}

func ValidateDataspaceName(name string) bool {
	return dataspaceNameRegex.MatchString(name)
}

func init() {
	argsRegex = regexp.MustCompile("[=| ]args\\.(\\w+)[=| \n]")
	dataspaceNameRegex = regexp.MustCompile(`^[[:alpha:]][\w]*$`)
}
