#!/usr/bin/env python3
# server_udp.py
import socket
import threading
import json
import time
import os
from typing import Dict, Tuple

HOST = "0.0.0.0"
PORT = 8888
QUESTION_TIME = 10
MIN_PLAYERS = 1

def send_udp_json(sock: socket.socket, addr, obj):
    try:
        sock.sendto((json.dumps(obj) + "\n").encode(), addr)
    except Exception as e:
        print("UDP send error to", addr, e)

class UDPQuizServer:
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        self.clients: Dict[str, Tuple[str,int]] = {}   # name -> (ip,port)
        self.scores: Dict[str,int] = {}
        self.lock = threading.Lock()
        self.questions = self.load_questions()
        self._answers = {}
        self.running = True

    def load_questions(self):
        path = os.path.join(os.path.dirname(__file__), "questions.json")
        with open(path, "r") as f:
            return json.load(f)

    def start(self):
        print(f"UDP Quiz server listening on {self.host}:{self.port}")
        threading.Thread(target=self.listen_loop, daemon=True).start()
        try:
            self.game_loop()
        except KeyboardInterrupt:
            print("Shutting down.")
            self.running = False
            self.sock.close()

    def listen_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(8192)
                text = data.decode(errors="ignore").strip()
                for line in text.splitlines():
                    try:
                        m = json.loads(line)
                    except Exception:
                        continue
                    t = m.get("type")
                    if t == "register":
                        name = m.get("name")
                        if name:
                            with self.lock:
                                self.clients[name] = addr
                                self.scores.setdefault(name, 0)
                            print(f"[UDP] Registered {name} at {addr}")
                    elif t == "answer":
                        qid = m.get("question_id")
                        ans = m.get("answer")
                        name = m.get("name")
                        if name and qid is not None:
                            with self.lock:
                                self._answers.setdefault(qid, []).append((name, ans, time.time()))
            except Exception as e:
                print("UDP listen error:", e)

    def broadcast(self, obj):
        with self.lock:
            for name, addr in list(self.clients.items()):
                send_udp_json(self.sock, addr, obj)

    def game_loop(self):
        while self.running:
            with self.lock:
                num_players = len(self.clients)
            if num_players < MIN_PLAYERS:
                print("Waiting for players... (connected:", num_players, ")")
                time.sleep(2)
                continue

            print("Starting a quiz round with", num_players, "players.")
            for q in self.questions:
                qmsg = {"type": "question",
                        "question_id": q["id"],
                        "text": q["text"],
                        "choices": q["choices"],
                        "time": QUESTION_TIME}
                with self.lock:
                    self._answers = {}
                self.broadcast(qmsg)
                print("→ Sent question:", q["text"])
                time.sleep(QUESTION_TIME)

                correct_index = q["answer"]
                results = []
                with self.lock:
                    answers = dict(self._answers)  # shallow copy
                for name in list(self.scores.keys()):
                    player_ans = None
                    for ans_tuple in answers.get(q["id"], []):
                        if ans_tuple[0] == name:
                            player_ans = ans_tuple[1]
                            break
                    correct = (player_ans == correct_index)
                    if correct:
                        with self.lock:
                            self.scores[name] = self.scores.get(name, 0) + 1
                    results.append({"name": name, "answer": player_ans, "correct": correct})
                reveal = {"type": "reveal",
                          "question_id": q["id"],
                          "correct": correct_index,
                          "results": results,
                          "scores": self.scores}
                print("→ Reveal:", reveal)
                self.broadcast(reveal)
                time.sleep(3)

            final = {"type": "final", "scores": self.scores}
            self.broadcast(final)
            print("Round finished. Final scores:", self.scores)

            # reset scores if you want subsequent rounds; change if you prefer persistence
            with self.lock:
                for k in self.scores.keys():
                    self.scores[k] = 0
            time.sleep(5)

if __name__ == "__main__":
    server = UDPQuizServer()
    server.start()
