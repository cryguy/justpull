package main

import "os"

// terminationSignals are the signals that interrupt docker.
var terminationSignals = []os.Signal{os.Interrupt}
