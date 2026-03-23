# MEXC Futures RSI Scanner (Telegram Bot)

Асинхронный бот: **4H**, RSI3(24) по **Уайлдеру** (как на MEXC), открытая свеча — **PRE-ALERT** при RSI ≥ 85 и ниже порога main, **основной сигнал** при RSI ≥ 90 (пороги в `settings.yml`). Маркет-муверы **не используются**.

## Особенности

- **Без ожидания закрытия свечи** — RSI считается в реальном времени по текущей цене.
- **Один сигнал за свечу** — повторный сигнал по той же монете в той же часовой свече не отправляется (anti-spam).
- **WebSocket** — обновления приходят с биржи сами, задержка < 1 с.
- **БД** — опционально: Redis или SQLite. При `db.use_redis: true` и `redis_url: localhost:6379` бот при старте сам поднимает Redis в Docker (если установлен). Иначе — SQLite по `db.sqlite_path`.
- **Конфиг в YAML** — ключи и тексты сообщений в файлах, можно менять в любой момент без правки кода.
- **Интерактивный бот (aiogram)** — по `/start` клавиатура с кнопками «Последние уведомления» и «Просмотр БД»; меню (≡) слева внизу с командами.

## Требования

- Python 3.11+ (или Docker)
- Redis (необязательно; при `docker-compose up` поднимается сам)

---

## Конфигурация (YAML)

Вся настройка в папке **`config/`**:

| Файл | Назначение |
|------|------------|
| **`config/keys.yml`** | Секреты: токен Telegram, chat_id, Redis URL, ключи MEXC. Не коммитить. |
| **`config/keys.example.yml`** | Пример ключей — скопируйте в `keys.yml` и заполните. |
| **`config/messages.yml`** | Тексты уведомлений в Telegram (заголовки, подписи полей). |
| **`config/settings.yml`** | Параметры сканера: RSI period/threshold, батчи, URL MEXC. |

Приоритет: сначала читаются YAML-файлы, при отсутствии значения — переменные окружения (`.env`).

### Пример `config/keys.yml`

```yaml
telegram:
  bot_token: "..."
  chat_id: "123456789"

db:
  use_redis: true        # true = Redis (при localhost бот поднимет Docker сам), false = SQLite
  redis_url: "redis://localhost:6379/0"
  sqlite_path: "data/scanner.db"
```

**Chat ID** узнать: напишите боту в Telegram, затем откройте  
`https://api.telegram.org/bot<ВАШ_ТОКЕН>/getUpdates` — в ответе будет `message.chat.id`.  
### Редактирование сообщений

В **`config/messages.yml`** можно менять заголовок сигнала, подписи «Монета», «RSI (24)», «Условие», «Цена» и т.д. После сохранения файла изменения подхватятся при следующей отправке (при перезапуске бота).

---

## Установка (локально)

```bash
cd /mnt/storage/Programms/MXC
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
pip install -r requirements.txt
cp config/keys.example.yml config/keys.yml
# Отредактируйте config/keys.yml: telegram.bot_token, telegram.chat_id, при необходимости redis.url
```

---

## Запуск

**Просто запусти бота — БД (Redis) поднимется автоматически вместе с ним.**

Одна команда (в папке проекта):

```bash
./start.sh
```

(Если не запускается: `chmod +x start.sh`, затем снова `./start.sh`.)

Или напрямую через Docker:

```bash
docker compose up -d
```

При этом сначала поднимается Redis, затем бот подключается к нему и начинает сканировать. Перезапуск при падении и после перезагрузки сервера — автоматический.

Убедись, что в **`config/keys.yml`** заданы `telegram.chat_id` и при использовании Docker — `redis.url: "redis://redis:6379/0"`.

### Запуск только бота (без Docker)

Если запускаешь без Docker (`python -m scanner`), Redis нужно поднять отдельно — иначе бот будет работать с состоянием только в памяти.

Логи:

```bash
docker compose logs -f bot
```

Остановка:

```bash
docker compose down
```

### Запуск 24/7 (постоянно)

Чтобы бот работал без тебя и присылал уведомления в Telegram круглосуточно:

1. **Обязательно укажи `config/keys.yml` → `telegram.chat_id`** (ID чата, куда слать сигналы). Иначе бот не знает, куда писать.
2. Выбери один из вариантов:

**Вариант А — Docker (проще всего)**  
На VPS или своём компе: `./start.sh` или `docker compose up -d`. Бот и Redis поднимутся и будут работать 24/7. При падении контейнер перезапустится (`restart: unless-stopped`). При старте в Telegram придёт сообщение: «Scanner запущен 24/7…» — значит, уведомления настроены и будут приходить сюда.

**Вариант Б — systemd (Linux без Docker)**  
Если хочешь запускать без Docker (например, только Python + Redis на сервере):
```bash
# Подставь свой путь и пользователя в deploy/mexc-scanner.service
sudo cp deploy/mexc-scanner.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable mexc-scanner
sudo systemctl start mexc-scanner
```
Проверка: `sudo systemctl status mexc-scanner`, логи: `journalctl -u mexc-scanner -f`.

После запуска (Docker или systemd) бот сразу пришлёт в Telegram сообщение о старте. Дальше сигналы RSI ≥ 85 будут приходить в тот же чат автоматически.

### Обновление на сервере через Git

Чтобы не перекидывать файлы по SFTP при каждом изменении кода:

1. **Локально (у себя):** создай репозиторий на GitHub/GitLab (приватный при желании), добавь remote и запушь:
   ```bash
   git remote add origin https://github.com/ТВОЙ_АККАУНТ/MEX.git
   git push -u origin main
   ```
2. **На сервере (первый раз):** клонируй репозиторий, настрой окружение и `config/keys.yml`:
   ```bash
   cd /root/programms
   git clone https://github.com/ТВОЙ_АККАУНТ/MEX.git MEXC
   cd MEXC
   cp config/keys.example.yml config/keys.yml
   # отредактируй config/keys.yml (токен, chat_id, SQLite/Redis)
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   mkdir -p data
   # настрой и запусти systemd (см. Вариант Б выше)
   ```
3. **При обновлении кода:** локально делаешь коммиты и `git push`; на сервере подтяни код и перезапусти сервис.

   **Команда для обновления бота на сервере** (выполни по SSH в каталоге проекта или одной строкой с ПК):

   ```bash
   cd /root/programms/MEXC && git pull && sudo systemctl restart mexc-scanner && echo OK
   ```

   Или через скрипт (на сервере):
   ```bash
   cd /root/programms/MEXC && ./scripts/update_on_server.sh
   ```

   С локального ПК одной командой (подставь свой хост):
   ```bash
   ssh root@ТВОЙ_СЕРВЕР "cd /root/programms/MEXC && git pull && sudo systemctl restart mexc-scanner && echo OK"
   ```

Файлы `config/keys.yml` и папка `data/` в Git не попадают (см. `.gitignore`), поэтому на сервере они не перезатрутся при `git pull`. При появлении новых полей в `keys.example.yml` при необходимости допиши их в свой `config/keys.yml`.

---

## Как это работает

1. **Старт** — загрузка списка всех USDT perpetual контрактов (REST), загрузка последних 30 закрытых свечей 1H по каждому (REST).
2. **Инициализация** — для каждой пары в памяти (и при наличии Redis) хранятся: последние 24 close, текущий close, время начала свечи, флаг `alert_sent`.
3. **WebSocket** — подписка на поток `push.kline` (Min60) по всем парам.
4. **При каждом обновлении свечи** — пересчёт RSI(24); если RSI ≥ 85 и по этой свече сигнал ещё не отправлялся → отправка в Telegram и установка `alert_sent`.
5. **Смена свечи** — при приходе новой часовой свечи старая close переносится в историю, `alert_sent` сбрасывается.

Тексты сообщений в Telegram задаются в **`config/messages.yml`**.

---

## Структура проекта

- **`config/`** — ключи и сообщения (YAML)
- **`config.py`** — загрузка конфига из YAML и env
- **`indicators.py`** — расчёт RSI(24)
- **`mexc_client.py`** — REST и WebSocket MEXC
- **`state.py`** — состояние по символам, Redis
- **`telegram_notify.py`** — отправка сигналов (тексты из `config/messages.yml`)
- **`scanner.py`** — точка входа
- **`docker-compose.yml`** — Redis + бот с автозапуском
