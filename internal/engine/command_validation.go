package engine

import (
	"strings"
)

type commandSpec struct {
	minArgs  int
	maxArgs  int
	validate func([]string) bool
}

func (spec commandSpec) valid(args []string) bool {
	if !spec.validArgumentCount(args) {
		return false
	}
	return spec.validate == nil || spec.validate(args)
}

func (spec commandSpec) validArgumentCount(args []string) bool {
	return len(args) >= spec.minArgs && (spec.maxArgs < 0 || len(args) <= spec.maxArgs)
}

func arguments(minArgs, maxArgs int) commandOption {
	return func(command *registeredCommand) {
		command.syntax = commandSpec{minArgs: minArgs, maxArgs: maxArgs}
	}
}

func syntax(minArgs, maxArgs int, validate func([]string) bool) commandOption {
	return func(command *registeredCommand) {
		command.syntax = commandSpec{minArgs: minArgs, maxArgs: maxArgs, validate: validate}
	}
}

func writeCommand(command *registeredCommand) {
	command.write = true
}

func transactionControl(command *registeredCommand) {
	command.transactionControl = true
}

func oddAfterKey(args []string) bool {
	return len(args)%2 == 1
}

func validateSetSyntax(args []string) bool {
	if len(args) == 2 {
		return true
	}
	if len(args) != 4 {
		return false
	}
	switch strings.ToUpper(args[2]) {
	case "EX", "PX", "EXAT", "PXAT":
		return true
	default:
		return false
	}
}

func validateZAddSyntax(args []string) bool {
	return len(args)%2 != 0
}
