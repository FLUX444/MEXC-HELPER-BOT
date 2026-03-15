#!/usr/bin/env bash
# Обновление кода бота на сервере: git pull и перезапуск mexc-scanner.
#
# Вариант 1 — запуск на самом сервере (зайди по SSH и выполни):
#   cd /root/programms/MEXC && ./scripts/update_on_server.sh
#
# Вариант 2 — с локального ПК одной командой (подставь свой хост и пользователя):
#   ssh root@ТВОЙ_СЕРВЕР "cd /root/programms/MEXC && git pull && sudo systemctl restart mexc-scanner && echo OK"
#
set -e
cd "$(dirname "$0")/.."
echo "Обновление репозитория..."
git pull
echo "Перезапуск mexc-scanner..."
sudo systemctl restart mexc-scanner
echo "Готово. Логи: journalctl -u mexc-scanner -f"
