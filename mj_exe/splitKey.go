package main

//func main() {
//	var of_map map[string]*os.File
//	of_map = make(map[string]*os.File)
//	sdfs_prefix := "sdfs_prefix"
//	input := bufio.NewScanner(os.Stdin)
//	for {
//		if !input.Scan() {
//			break
//		}
//		line := input.Text()
//		line = strings.TrimSpace(line)
//		str := strings.Split(line, " ")
//		key := str[0]
//		f, ok := of_map[key]
//		if !ok {
//			append_file_name := sdfs_prefix + "_" + key
//			//f, err := os.OpenFile(append_file_name, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
//			f, err := os.Create(append_file_name)
//			if err != nil {
//				log.Println("open file error :", err)
//				return
//			}
//			of_map[key] = f
//		}
//		f = of_map[key]
//		_, err := f.WriteString(line + "\n")
//		if err != nil {
//			log.Println(err)
//			return
//		}
//	}
//	for key := range of_map {
//		of_map[key].Close()
//	}
//}
