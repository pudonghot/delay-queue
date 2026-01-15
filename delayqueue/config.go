package delayqueue

type (
	Beanstalk struct {
		Endpoint string
		Tube     string
	}

	DqConf struct {
		Beanstalks []Beanstalk
	}
)
