# WhoScored — задача на следующую сессию

Статус: открыта, production activation ещё не завершена.

Нужно сделать:

1. Положить `PROXY_POOL_JSON` в секретное хранилище и убрать временный legacy-файл `proxys.txt`.
2. Настроить настоящий S3 backup: отдельный bucket, off-host/WORM, переменные `WHOSCORED_BACKUP_*`.
3. Выполнить backup/restore полного raw-инвентаря WhoScored и приложить receipts.
4. Провести безопасный cutover с legacy SeaweedFS на supervised four-plane topology.
5. Запустить полный мужской senior backfill всех лиг и закрыть DQ, включая окно 30 дней.
6. Проверить ежедневный Airflow DAG после cutover и записать метрики скорости/памяти/прокси.

Готово сейчас: код и DAG-и обновлены, canary прошёл, малый backup/restore drill прошёл, Airflow/Trino/SeaweedFS/proxy восстановлены и healthy.
