#!/usr/bin/env bash
# Запуск бота: поднимаются Redis (БД) и сканер одной командой.
# Использование: ./start.sh   или   bash start.sh

set -e
cd "$(dirname "$0")"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  echo "Запуск бота и Redis (Docker)..."
  docker compose up -d
  echo ""
  echo "Готово. Бот и БД работают."
  echo "Логи: docker compose logs -f bot"
  echo "Остановка: docker compose down"
else
  echo "Docker не найден. Запустите вручную:"
  echo "  1. Redis: redis-server (или docker run -d -p 6379:6379 redis:7-alpine)"
  echo "  2. Бот: python -m scanner"
  exit 1
fi
