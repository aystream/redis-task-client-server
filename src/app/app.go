package app

import (
	"flag"
	"github.com/aystream/redis-task-client-server/src/db"
	"github.com/aystream/redis-task-client-server/src/util"
	"github.com/garyburd/redigo/redis"
	"log"
	"math/rand"
	"os"
	"time"
)

type Role string

// TODO По хорошему наверное стоит это делать выносить в ключи для программы или выносить в конфигурационный файл!
const (
	CHILD Role = "child"
	MAIN  Role = "main"

	mainExpire               = 2000
	probabilityOfError       = 5
	timeGenerateMessage      = 500 // время генерации сообщения
	mainKey                  = "MAIN_KEY"
	messageStringRedisObject = "messages"
	errorRedisObject         = "errors"
)

// Получение всех ошибок
var getErrors = flag.Bool("getErrors", false, "get list errors")

// Приложение
type App struct {
	pool *redis.Pool
	id   int
	role Role
}

// Инициализация приложение
func (a *App) Initialize() {
	a.pool = db.CreateRedisPool(":6379", 5)
	a.id = os.Getpid()

	if flag.Parse(); *getErrors {
		a.getErrors()
		return
	}

	a.electionMain()
}

// Выбор позцию кого мы занимаем главного потока, главного или ребенка
func (a *App) electionMain() {
	connection := a.pool.Get()
	defer connection.Close()

	current, err := connection.Do("SET", mainKey, a.id, "NX", "PX", mainExpire)
	if err != nil {
		log.Printf("Ошибка: %v", err)
		return
	}

	log.Printf("Выбор id=%v success=%v ", a.id, current)

	if current == "OK" {
		a.setMain()
	} else {
		a.setChild()
	}
}

// Получение ошибок
func (a *App) getErrors() {
	connection := a.pool.Get()
	defer connection.Close()

	connection.Send("MULTI")
	// получим список элементов ошибок
	connection.Send("LRANGE", errorRedisObject, 0, -1)
	// удаление
	connection.Send("DEL", errorRedisObject)
	reply, err := redis.Values(connection.Do("EXEC"))

	if err != nil {
		log.Printf(" %v", err)
		return
	}

	messages, err := redis.Strings(reply[0], nil)
	if err != nil {
		log.Printf("Ошибка получения списка ошибок %v", err)
		return
	}

	if len(messages) == 0 {
		log.Print("Пустой массив ошибок")
		return
	}

	for i := 0; i < len(messages); i++ {
		log.Printf("Ошибка сообщения: %v", messages[i])
	}
}

// Запишем главного и откроем 2 интервальных канала в которых сгенерируем сообщения и продлим свой лок на mainExpire / 2
func (a *App) setMain() {
	log.Print("Запуск главного приложения")
	if a.role == MAIN {
		return
	}
	a.role = MAIN
	generateChan := time.NewTicker(time.Millisecond * 200).C
	expireChan := time.NewTicker(time.Millisecond * mainExpire / 2).C

	for {
		select {
		case <-generateChan:
			a.generateMessage()
		case <-expireChan:
			a.getMain()
		}
	}
}

// Получение главного приложения
func (a *App) getMain() {
	connection := a.pool.Get()
	defer connection.Close()

	if _, err := connection.Do("PEXPIRE", mainKey, mainExpire); err != nil {
		log.Printf("EXPIRE ERROR %v", err)
	}
}

// Запишем дочернего и запустим 2 интервальных канала в которых прочитаем сообщения и попытаемся получить свой лок на позицию мастера с итнтервалом
func (a *App) setChild() {
	if a.role == CHILD {
		return
	} else {
		log.Print("Запустить child")
		a.role = CHILD
	}

	readChan := time.NewTicker(time.Millisecond * timeGenerateMessage).C
	electionChan := time.NewTicker(time.Millisecond * mainExpire).C

	for {
		select {
		case <-readChan:
			a.readMessage()
		case <-electionChan:
			a.electionMain()
		}
	}
}

// Чтение сообщения
func (a *App) readMessage() {
	connection := a.pool.Get()
	defer connection.Close()

	reply, err := redis.Values(connection.Do("BLPOP", messageStringRedisObject, mainExpire/2))
	if err != nil {
		log.Printf("BLPOP отправка ошибка: %v", err)
		return
	}

	message, _ := redis.String(reply[1], nil)
	log.Printf("Полученно сообщение %v", message)

	if rand.Intn(100) < probabilityOfError {
		log.Printf("Ошибка PUSH сообщения: %v", message)
		if _, err := connection.Do("RPUSH", errorRedisObject, message); err != nil {
			log.Printf("Ошибка PUSH: %v", err)
			return
		}
	}
}

// Генерация сообщения
func (a *App) generateMessage() {
	connection := a.pool.Get()
	defer connection.Close()

	currentId, err := redis.Int(connection.Do("GET", mainKey))
	if err != nil {
		log.Printf("Проверка главного приложения вернулось с ERROR %v", err)
		a.electionMain()
		return
	}

	if currentId != a.id {
		log.Printf("Неправильный currentId=%d id=%d", currentId, a.id)
		a.setChild()
		return
	}

	message := util.Random()

	if _, err := connection.Do("RPUSH", messageStringRedisObject, message); err != nil {
		log.Printf("Отправка в Redis - ERROR %v", err)
		return
	}

	log.Printf("Отправка сообщения %v", message)
}
