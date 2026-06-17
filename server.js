const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const os = require('os');
const fs = require('fs');

// ========== БАЗА ДАННЫХ ==========
const appDataPath = path.join(os.homedir(), 'AppData', 'Roaming', 'ChatServer');
if (!fs.existsSync(appDataPath)) fs.mkdirSync(appDataPath, { recursive: true });
const dbPath = path.join(appDataPath, 'server_chat.db');

const db = new sqlite3.Database(dbPath);

db.run(`
  CREATE TABLE IF NOT EXISTS users (
    nickname TEXT PRIMARY KEY,
    password TEXT,
    avatar TEXT DEFAULT '😊',
    color TEXT DEFAULT '#000000',
    status TEXT DEFAULT 'online'
  )
`);

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER,
    nickname TEXT,
    message TEXT,
    is_file INTEGER DEFAULT 0,
    is_private INTEGER DEFAULT 0,
    recipient TEXT
  )
`);

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
function getTimestamp() {
  return Date.now();
}

// ========== ОБРАБОТЧИК СООБЩЕНИЙ ==========
class ChatServer {
  constructor(port = 5555) {
    this.port = port;
    this.wss = new WebSocket.Server({ port });
    this.clients = new Map(); // ws -> { nickname, avatar, color, status }
    this.broadcastUsers = this.broadcastUsers.bind(this);
    this.handleConnection = this.handleConnection.bind(this);

    this.wss.on('connection', this.handleConnection);
    console.log(`[СЕРВЕР] Запущен на порту ${port}`);
    console.log(`[СЕРВЕР] База данных: ${dbPath}`);
  }

  handleConnection(ws) {
    console.log('[СЕРВЕР] Новое подключение');

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data);
        this.processMessage(ws, msg);
      } catch (e) {
        console.error('[СЕРВЕР] Ошибка парсинга JSON:', e);
        // Не закрываем сокет, чтобы клиент мог повторить
      }
    });

    ws.on('close', () => {
      this.removeClient(ws);
    });

    ws.on('error', (err) => {
      console.error('[СЕРВЕР] Ошибка сокета:', err);
    });
  }

  processMessage(ws, msg) {
    try {
      const { type } = msg;
      switch (type) {
        case 'auth':
          this.handleAuth(ws, msg.nickname, msg.password);
          break;
        case 'message':
          this.handlePublicMessage(ws, msg.text);
          break;
        case 'private':
          this.handlePrivateMessage(ws, msg.recipient, msg.text);
          break;
        case 'update_profile':
          this.handleUpdateProfile(ws, msg.avatar, msg.color, msg.status);
          break;
        case 'file':
          this.handleFile(ws, msg.filename, msg.filetype, msg.data);
          break;
        default:
          console.warn('[СЕРВЕР] Неизвестный тип:', type);
      }
    } catch (e) {
      console.error('[СЕРВЕР] Ошибка обработки сообщения:', e);
      // Отправляем клиенту сообщение об ошибке, но не закрываем сокет
      this.sendTo(ws, { type: 'error', message: 'Внутренняя ошибка сервера' });
    }
  }

  // ----- Аутентификация -----
  handleAuth(ws, nickname, password) {
    if (!nickname || !password) {
      this.sendTo(ws, { type: 'auth_result', success: false, message: 'Заполните ник и пароль' });
      return;
    }

    db.get('SELECT * FROM users WHERE nickname = ?', [nickname], (err, row) => {
      if (err) {
        console.error('[СЕРВЕР] Ошибка БД:', err);
        this.sendTo(ws, { type: 'auth_result', success: false, message: 'Ошибка БД' });
        return;
      }

      if (row) {
        if (row.password === password) {
          this.registerClient(ws, nickname, row.avatar, row.color, row.status);
          this.sendTo(ws, { type: 'auth_result', success: true, message: 'Добро пожаловать!' });
          this.sendHistory(ws);
          this.broadcastUsers();
        } else {
          this.sendTo(ws, { type: 'auth_result', success: false, message: 'Неверный пароль' });
        }
      } else {
        // Регистрация
        db.run('INSERT INTO users (nickname, password, avatar, color, status) VALUES (?, ?, ?, ?, ?)',
          [nickname, password, '😊', '#000000', 'online'],
          (err) => {
            if (err) {
              console.error('[СЕРВЕР] Ошибка регистрации:', err);
              this.sendTo(ws, { type: 'auth_result', success: false, message: 'Ошибка регистрации' });
              return;
            }
            this.registerClient(ws, nickname, '😊', '#000000', 'online');
            this.sendTo(ws, { type: 'auth_result', success: true, message: 'Регистрация успешна!' });
            this.sendHistory(ws);
            this.broadcastUsers();
          }
        );
      }
    });
  }

  registerClient(ws, nickname, avatar, color, status) {
    this.clients.set(ws, { nickname, avatar, color, status });
    console.log(`[СЕРВЕР] Пользователь ${nickname} вошёл`);
  }

  removeClient(ws) {
    if (this.clients.has(ws)) {
      const info = this.clients.get(ws);
      console.log(`[СЕРВЕР] Пользователь ${info.nickname} отключился`);
      this.clients.delete(ws);
      this.broadcastUsers();
    }
  }

  // ----- Отправка истории -----
  sendHistory(ws) {
    db.all('SELECT timestamp, nickname, message, is_file, is_private, recipient FROM messages ORDER BY timestamp ASC LIMIT 200', (err, rows) => {
      if (err) {
        console.error('[СЕРВЕР] Ошибка истории:', err);
        return;
      }
      const history = rows.map(row => ({
        timestamp: row.timestamp,
        nickname: row.nickname,
        message: row.message,
        is_file: !!row.is_file,
        is_private: !!row.is_private,
        recipient: row.recipient
      }));
      this.sendTo(ws, { type: 'history', messages: history });
    });
  }

  // ----- Публичное сообщение -----
  handlePublicMessage(ws, text) {
    if (!this.clients.has(ws)) return;
    const info = this.clients.get(ws);
    const ts = getTimestamp();

    db.run('INSERT INTO messages (timestamp, nickname, message, is_file, is_private) VALUES (?, ?, ?, ?, ?)',
      [ts, info.nickname, text, 0, 0],
      (err) => {
        if (err) console.error('[СЕРВЕР] Ошибка сохранения сообщения:', err);
      }
    );

    const payload = {
      type: 'message',
      from: info.nickname,
      text: text,
      timestamp: ts,
      avatar: info.avatar,
      color: info.color,
      is_private: false
    };
    this.broadcast(JSON.stringify(payload));
  }

  // ----- Приватное сообщение -----
  handlePrivateMessage(ws, recipient, text) {
    if (!this.clients.has(ws)) return;
    const senderInfo = this.clients.get(ws);

    let targetWs = null;
    for (const [client, info] of this.clients) {
      if (info.nickname === recipient) {
        targetWs = client;
        break;
      }
    }

    if (!targetWs) {
      this.sendTo(ws, { type: 'private_error', message: `Пользователь ${recipient} не в сети` });
      return;
    }

    const ts = getTimestamp();

    db.run('INSERT INTO messages (timestamp, nickname, message, is_private, recipient) VALUES (?, ?, ?, ?, ?)',
      [ts, senderInfo.nickname, text, 1, recipient],
      (err) => {
        if (err) console.error('[СЕРВЕР] Ошибка сохранения приватного сообщения:', err);
      }
    );

    this.sendTo(ws, { type: 'private_sent', to: recipient, text, timestamp: ts });
    this.sendTo(targetWs, { type: 'private', from: senderInfo.nickname, text, timestamp: ts, avatar: senderInfo.avatar, color: senderInfo.color });
  }

  // ----- Файлы -----
  handleFile(ws, filename, filetype, dataBase64) {
    if (!this.clients.has(ws)) return;
    const info = this.clients.get(ws);
    const ts = getTimestamp();

    db.run('INSERT INTO messages (timestamp, nickname, message, is_file) VALUES (?, ?, ?, ?)',
      [ts, info.nickname, filename, 1],
      (err) => {
        if (err) console.error('[СЕРВЕР] Ошибка сохранения файла:', err);
      }
    );

    const payload = {
      type: 'file',
      from: info.nickname,
      filename,
      filetype,
      data: dataBase64,
      timestamp: ts,
      avatar: info.avatar,
      color: info.color
    };
    this.broadcast(JSON.stringify(payload), ws);
  }

  // ----- Обновление профиля -----
  handleUpdateProfile(ws, avatar, color, status) {
    if (!this.clients.has(ws)) return;
    const info = this.clients.get(ws);
    const nickname = info.nickname;

    db.run('UPDATE users SET avatar = ?, color = ?, status = ? WHERE nickname = ?',
      [avatar || '😊', color || '#000000', status || 'online', nickname],
      (err) => {
        if (err) {
          console.error('[СЕРВЕР] Ошибка обновления профиля:', err);
          return;
        }
        info.avatar = avatar || '😊';
        info.color = color || '#000000';
        info.status = status || 'online';
        this.broadcastUsers();
        this.sendTo(ws, { type: 'profile_updated', avatar: info.avatar, color: info.color, status: info.status });
      }
    );
  }

  // ----- Рассылка списка пользователей -----
  broadcastUsers() {
    const users = [];
    for (const [ws, info] of this.clients) {
      users.push({
        nickname: info.nickname,
        avatar: info.avatar,
        color: info.color,
        status: info.status
      });
    }
    const payload = { type: 'user_list', users };
    this.broadcast(JSON.stringify(payload));
  }

  // ----- Безопасная отправка одному клиенту -----
  sendTo(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(JSON.stringify(data));
      } catch (e) {
        console.error('[СЕРВЕР] Ошибка отправки клиенту:', e);
        // Если не удалось отправить – закрываем соединение
        try { ws.close(); } catch (ignored) {}
        this.removeClient(ws);
      }
    }
  }

  // ----- Безопасная рассылка всем -----
  broadcast(json, excludeWs = null) {
    const toRemove = [];
    for (const [client] of this.clients) {
      if (client === excludeWs) continue;
      if (client.readyState === WebSocket.OPEN) {
        try {
          client.send(json);
        } catch (e) {
          console.error('[СЕРВЕР] Ошибка рассылки клиенту:', e);
          toRemove.push(client);
        }
      } else {
        toRemove.push(client);
      }
    }
    // Удаляем проблемные сокеты
    for (const sock of toRemove) {
      this.removeClient(sock);
      try { sock.close(); } catch (ignored) {}
    }
  }
}

// ========== ЗАПУСК ==========
const port = process.argv[2] ? parseInt(process.argv[2]) : 5555;
const server = new ChatServer(port);

process.on('SIGINT', () => {
  console.log('\n[СЕРВЕР] Завершение по Ctrl+C');
  db.close();
  process.exit();
});