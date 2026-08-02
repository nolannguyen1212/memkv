package mutation

type Command struct {
	Name string
	Args []string
}

type Batch []Command

func New(name string, args ...string) Command {
	return Command{Name: name, Args: append([]string(nil), args...)}
}
