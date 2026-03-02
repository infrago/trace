package trace

import . "github.com/infrago/base"

func Write(span Span) {
	module.Write(span)
}

func Stats() Map {
	return module.Stats()
}
