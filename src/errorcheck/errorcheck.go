package errorcheck

func CheckError(error error, info string) {
	if error != nil {
		panic("ERROR: " + info + " " + error.Error()) // terminate program
	}
}
