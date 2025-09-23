package utils

import (
	"bufio"
	"strings"
)

func Readline(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')

	if err != nil {
		return "", err
	}

	return strings.TrimSuffix(
		strings.TrimSuffix(line, "\n"),
		"\r",
	), nil
}
