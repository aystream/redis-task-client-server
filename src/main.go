package main

import "github.com/aystream/redis-task-client-server/src/app"

func main() {
	newApp := &app.App{}
	newApp.Initialize()
}
