package main

import (
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type SignalingHub struct {
	sync.RWMutex
	clients map[*websocket.Conn]bool
}

var hub = SignalingHub{
	clients: make(map[*websocket.Conn]bool),
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection: %v", err)
		return
	}
	defer ws.Close()

	hub.Lock()
	hub.clients[ws] = true
	hub.Unlock()

	log.Printf("Device connected. Total active devices: %d", len(hub.clients))

	for {
		var msg map[string]interface{}

		err := ws.ReadJSON(&msg)
		if err != nil {
			log.Printf("Device disconnected or read error: %v", err)
			hub.Lock()
			delete(hub.clients, ws)
			hub.Unlock()
			break
		}

		hub.RLock()
		for client := range hub.clients {
			if client != ws {
				err := client.WriteJSON(msg)
				if err != nil {
					log.Printf("Error routing message: %v", err)
					client.Close()
				}
			}
		}
		hub.RUnlock()
	}
}

func main() {
	http.HandleFunc("/ws", handleConnections)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Signaling Server actively running on :%s", port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("Server crashed: ", err)
	}
}
