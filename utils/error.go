package utils

// Panic 如果err 不为nil 则panicc
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// CondPanic e
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
