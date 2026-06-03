package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

type EventMessage struct {
	Type    string          `json:"type"`
	Sender  string          `json:"sender"`
	Target  string          `json:"target,omitempty"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type ClientHub struct {
	sync.RWMutex
	clients map[string]*websocket.Conn
}

var hub = ClientHub{
	clients: make(map[string]*websocket.Conn),
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection: %v", err)
		return
	}

	var deviceName string

	defer func() {
		if deviceName != "" {
			hub.Lock()
			delete(hub.clients, deviceName)
			hub.Unlock()
			log.Printf("Device disconnected: %s. Total active devices: %d", deviceName, len(hub.clients))

			broadcast(EventMessage{
				Type:    "disconnect",
				Sender:  deviceName,
				Message: "Device went offline",
			}, "")
		}
		ws.Close()
	}()

	for {
		var msg EventMessage

		err := ws.ReadJSON(&msg)
		if err != nil {
			log.Printf("Read error or disconnect: %v", err)
			break
		}

		if msg.Type == "connect" {
			deviceName = msg.Sender
			hub.Lock()
			hub.clients[deviceName] = ws
			hub.Unlock()
			log.Printf("🟢 Device registered: %s. Total active devices: %d", deviceName, len(hub.clients))

			broadcast(msg, deviceName)
			continue
		}

		if msg.Target != "" {
			// Targeted 1-to-1 routing (Offers, Answers, Candidates)
			hub.RLock()
			targetConn, exists := hub.clients[msg.Target]
			hub.RUnlock()

			if exists {
				err := targetConn.WriteJSON(msg)
				if err != nil {
					log.Printf("Error routing message to %s: %v", msg.Target, err)
				}
			} else {
				log.Printf("Warning: Target '%s' not found for message type '%s'", msg.Target, msg.Type)
			}
		} else {
			broadcast(msg, deviceName)
		}
	}
}

func broadcast(msg EventMessage, senderName string) {
	hub.RLock()
	defer hub.RUnlock()

	for name, client := range hub.clients {
		if name != senderName {
			err := client.WriteJSON(msg)
			if err != nil {
				log.Printf("Error broadcasting to %s: %v", name, err)
				client.Close()
			}
		}
	}
}

func main() {
	http.HandleFunc("/ws", handleConnections)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Websocket Server actively running on :%s", port)

	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("Server crashed: ", err)
	}
}
