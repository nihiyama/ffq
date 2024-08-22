package ffq

// type Queue[T any] struct {
// 	name            string
// 	fileDir         string
// 	queueSize       uint64
// 	pageSize        int
// 	dataFixedLength uint64
// 	maxFileSize     int64
// 	maxIndexSize    uint64
// 	queue           chan *Message[T]
// 	headIndex       uint64
// 	currentPage     int
// 	queueFile       *os.File
// 	indexFile       *os.File
// }

// func NewQueue[T any](config QueueConfig) (*Queue[T], error) {
// 	var err error
// 	name := "ffq"
// 	if config.Name != "" {
// 		name = config.Name
// 	}
// 	fileDir := "/tmp"
// 	if config.FileDir != "" {
// 		fileDir = config.FileDir
// 	}
// 	fileDir = filepath.Join(fileDir, name)
// 	err = createQueueDir(fileDir)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var queueSize uint64 = 1000
// 	if config.QueueSize != 0 {
// 		queueSize = config.QueueSize
// 	}
// 	pageSize := 1
// 	if config.PageSize != 0 {
// 		config.PageSize = config.PageSize
// 	}

// 	if config.DataFixedLength == 0 {
// 		return nil, errors.New("DataFixedLength must be grater than 0")
// 	}

// 	queue := make(chan *Message[T], queueSize)
// 	indexFilepath := filepath.Join(fileDir, "index")
// 	var headIndex uint64 = 0
// 	tailIndex, err := readIndex(indexFilepath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	indexFile, err := createIndexFile(indexFilepath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	maxFileSize := queueSize * config.DataFixedLength
// 	tailPosition := tailIndex * config.DataFixedLength
// 	currentPage := int(tailPosition / maxFileSize)
// 	queueFilepath := filepath.Join(fileDir, fmt.Sprintf("queue.%d", currentPage))

// 	return &Queue[T]{
// 		name:            name,
// 		fileDir:         fileDir,
// 		queueSize:       queueSize,
// 		pageSize:        pageSize,
// 		dataFixedLength: config.DataFixedLength,
// 		queue:           queue,
// 		currentPage:     currentPage,
// 		headIndex:       headIndex,
// 		queueFile:       indexFile,
// 		indexFile:       indexFile,
// 	}, nil
// }
